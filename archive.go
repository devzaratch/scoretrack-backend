package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// Season Archive (แผนใหม่ข้อ 4)
// เก็บผลแมตช์ที่จบแล้วไว้เอง (ไม่ต้องพึ่ง provider ย้อนหลัง)
// - หน่วยความจำ: อ่านเร็วสำหรับ API
// - Redis: hash ต่อฤดูกาล (scoretrack:archive:<season>) — อยู่ถาวรข้าม restart
// - sweeper: ดึงรายการวันนี้จาก day cache ทุก 30 นาที เก็บคู่ที่จบแล้ว
//
// หมายเหตุ: season = ปี ค.ศ. ของวันเตะ (เวลาไทย) — พอสำหรับ archive/lookup

const (
	archiveSweepDefaultMin = 30
	archiveMemoryCap       = 1500 // ต่อฤดูกาล (กัน memory โต)
	archiveRedisPrefix     = "scoretrack:archive:"
)

// ArchivedMatch — แมตช์ที่เก็บถาวร (ฟิลด์ครบพอสำหรับหน้า archive/H2H)
type ArchivedMatch struct {
	MatchID    int    `json:"match_id"`
	LeagueID   int    `json:"league_id"`
	League     string `json:"league"`
	Country    string `json:"country,omitempty"`
	Season     string `json:"season"`
	Status     string `json:"status"`
	Kickoff    string `json:"kickoff,omitempty"`
	HomeTeam   string `json:"home_team"`
	AwayTeam   string `json:"away_team"`
	HomeScore  int    `json:"home_score"`
	AwayScore  int    `json:"away_score"`
	ArchivedAt int64  `json:"archived_at"`
}

var (
	archiveMu  sync.Mutex
	archiveMem = make(map[string]map[int]ArchivedMatch) // season -> matchID -> match
)

// seasonKeyFor — ปี ค.ศ. ของวันเตะตามเวลาไทย (kickoff ว่าง = ปีปัจจุบัน)
func seasonKeyFor(kickoff string) string {
	if kickoff != "" {
		if t, err := time.Parse(time.RFC3339, kickoff); err == nil {
			return t.In(bangkokLoc).Format("2006")
		}
	}
	return time.Now().In(bangkokLoc).Format("2006")
}

// archivedFromRealMatch — แปลง RealMatch เป็น record archive
func archivedFromRealMatch(m RealMatch) ArchivedMatch {
	return ArchivedMatch{
		MatchID:    m.MatchID,
		LeagueID:   m.LeagueID,
		League:     m.League,
		Country:    m.Country,
		Season:     seasonKeyFor(m.MatchTime),
		Status:     m.Status,
		Kickoff:    m.MatchTime,
		HomeTeam:   m.HomeTeam.Name,
		AwayTeam:   m.AwayTeam.Name,
		HomeScore:  m.HomeTeam.Score,
		AwayScore:  m.AwayTeam.Score,
		ArchivedAt: time.Now().Unix(),
	}
}

// storeArchivedLocked — upsert เข้า memory (ต้องถือ archiveMu)
func storeArchivedLocked(rec ArchivedMatch) {
	season := rec.Season
	if season == "" {
		season = seasonKeyFor(rec.Kickoff)
		rec.Season = season
	}
	bucket, ok := archiveMem[season]
	if !ok {
		bucket = make(map[int]ArchivedMatch)
		archiveMem[season] = bucket
	}
	// มีอยู่แล้ว = ไม่ต้องเขียนซ้ำ (ประหยัด Redis)
	if prev, exists := bucket[rec.MatchID]; exists && prev.Status == rec.Status &&
		prev.HomeScore == rec.HomeScore && prev.AwayScore == rec.AwayScore {
		return
	}
	bucket[rec.MatchID] = rec
	if len(bucket) > archiveMemoryCap {
		// ตัดตัวเก่าสุด (archived_at น้อยสุด) ออกจาก memory — Redis ยังมี
		var oldestID int
		var oldestAt int64 = 1<<62 - 1
		for id, r := range bucket {
			if r.ArchivedAt < oldestAt {
				oldestAt = r.ArchivedAt
				oldestID = id
			}
		}
		delete(bucket, oldestID)
	}
	if rdb != nil {
		if b, err := json.Marshal(rec); err == nil {
			key := archiveRedisPrefix + season
			if err := rdb.HSet(ctx, key, strconv.Itoa(rec.MatchID), string(b)).Err(); err != nil {
				logWarn("archive: HSET ไม่สำเร็จ", "error", err.Error(), "season", season)
			}
		}
	}
}

// archiveFromMatches — เก็บคู่ที่จบแล้วทั้งหมดจากรายการ (คืนจำนวนที่เก็บ)
func archiveFromMatches(matches []RealMatch) int {
	stored := 0
	archiveMu.Lock()
	defer archiveMu.Unlock()
	for _, m := range matches {
		if !isFinishedStatus(m.Status) {
			continue
		}
		storeArchivedLocked(archivedFromRealMatch(m))
		stored++
	}
	return stored
}

// loadArchivedFromRedis — เรียกเมื่อ memory ว่าง (เช่นหลัง restart)
func loadArchivedFromRedis(season string) []ArchivedMatch {
	if rdb == nil {
		return nil
	}
	key := archiveRedisPrefix + season
	all, err := rdb.HGetAll(ctx, key).Result()
	if err != nil || len(all) == 0 {
		return nil
	}
	out := make([]ArchivedMatch, 0, len(all))
	archiveMu.Lock()
	bucket, ok := archiveMem[season]
	if !ok {
		bucket = make(map[int]ArchivedMatch)
		archiveMem[season] = bucket
	}
	for _, raw := range all {
		var rec ArchivedMatch
		if err := json.Unmarshal([]byte(raw), &rec); err != nil {
			continue
		}
		out = append(out, rec)
		bucket[rec.MatchID] = rec
	}
	archiveMu.Unlock()
	return out
}

// listArchived — คืนรายการของฤดูกาล เรียงใหม่->เก่า (สูงสุด limit)
func listArchived(season string, limit int) []ArchivedMatch {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	archiveMu.Lock()
	bucket := archiveMem[season]
	out := make([]ArchivedMatch, 0, len(bucket))
	for _, rec := range bucket {
		out = append(out, rec)
	}
	archiveMu.Unlock()

	if len(out) == 0 {
		out = loadArchivedFromRedis(season)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Kickoff != out[j].Kickoff {
			return out[i].Kickoff > out[j].Kickoff // RFC3339 = lexicographic ได้
		}
		return out[i].ArchivedAt > out[j].ArchivedAt
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

// findArchived — หาแมตช์เดียวจาก season ที่ระบุ (ว่าง = ค้นทุก season ใน memory)
func findArchived(season string, matchID int) (ArchivedMatch, bool) {
	if season != "" {
		archiveMu.Lock()
		rec, ok := archiveMem[season][matchID]
		archiveMu.Unlock()
		if ok {
			return rec, true
		}
		for _, rec := range loadArchivedFromRedis(season) {
			if rec.MatchID == matchID {
				return rec, true
			}
		}
		return ArchivedMatch{}, false
	}
	archiveMu.Lock()
	defer archiveMu.Unlock()
	for _, bucket := range archiveMem {
		if rec, ok := bucket[matchID]; ok {
			return rec, true
		}
	}
	return ArchivedMatch{}, false
}

// startArchiveSweeper — goroutine เก็บผลแมตช์จบเป็นระยะ
func startArchiveSweeper() {
	if !envBool("ARCHIVE_SWEEP", true) {
		logInfof("🗄️ archive: ปิดด้วย ARCHIVE_SWEEP=off")
		return
	}
	min := envInt("ARCHIVE_SWEEP_MIN", archiveSweepDefaultMin)
	if min < 5 {
		min = 5
	}
	time.Sleep(3 * time.Minute)
	sweep := func() {
		raw, source, ok := fetchMatchesFromProviders(normalizeDateKey(""))
		if !ok || len(raw) == 0 {
			return
		}
		var matches []RealMatch
		if err := json.Unmarshal(raw, &matches); err != nil {
			logWarn("archive: parse fixtures failed", "error", err.Error())
			return
		}
		if n := archiveFromMatches(matches); n > 0 {
			logInfof("🗄️ archive: เก็บผลแล้ว %d คู่ (source: %s)", n, source)
		}
	}
	sweep()
	ticker := time.NewTicker(time.Duration(min) * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		sweep()
	}
}

// ---------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------

// handleArchiveList — GET /api/archive?season=2026&limit=50
func handleArchiveList(c *gin.Context) {
	season := strings.TrimSpace(c.DefaultQuery("season", time.Now().In(bangkokLoc).Format("2006")))
	if _, err := time.Parse("2006", season); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "season ต้องเป็นปี ค.ศ. เช่น 2026"})
		return
	}
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	list := listArchived(season, limit)
	c.Header("X-Archive-Season", season)
	c.Header("X-Archive-Count", fmt.Sprintf("%d", len(list)))
	c.JSON(http.StatusOK, gin.H{
		"season":  season,
		"count":   len(list),
		"matches": list,
	})
}

// handleArchiveMatch — GET /api/archive/:id?season=2026 (ไม่ระบุ season = ค้น memory)
func handleArchiveMatch(c *gin.Context) {
	idRaw := c.Param("id")
	id, err := strconv.Atoi(idRaw)
	if err != nil || id <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id ไม่ถูกต้อง"})
		return
	}
	season := strings.TrimSpace(c.Query("season"))
	if season != "" {
		if _, err := time.Parse("2006", season); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "season ต้องเป็นปี ค.ศ. เช่น 2026"})
			return
		}
	}
	rec, ok := findArchived(season, id)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "ไม่พบแมตช์ใน archive"})
		return
	}
	c.JSON(http.StatusOK, rec)
}
