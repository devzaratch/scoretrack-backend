package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ---------------------------------------------------------------------------
// GoalProvider — แหล่งข้อมูลหลัก (GOAL API, free plan 1,000 req/วัน)
// สัญญาที่พิสูจน์จาก API จริงแล้ว:
//   - base: https://api.goal-api.com/v1 (ไม่ใช่ /api/v1)
//   - GET /fixtures?from=&to=&limit=100&offset=   (limit สูงสุด 100)
//   - GET /fixtures/live                          (1 call ได้ทุกคู่ที่แข่งอยู่)
//   - GET /fixtures/:id                           (คืน events+lineups+statistics ใน call เดียว)
//   - GET /leagues?limit=100&offset=              (ใช้สร้างดัชนี ULID -> apiId)
//   - ทุกคำตอบมี header X-RateLimit-Limit/-Remaining/-Reset
// ---------------------------------------------------------------------------

const goalAPIBase = "https://api.goal-api.com/v1"

type GoalProvider struct {
	baseURL string
	key     string
	client  *http.Client
	budget  *SourceBudget

	mu        sync.Mutex
	dayCache  map[string]goalDayCache
	liveState map[int]RealMatch // สถานะรวมล่าสุด: REST sweep + WS push (key = match_id)
	liveAt    time.Time         // ครั้งสุดท้ายที่ REST sweep สำเร็จ
	liveSwept bool
	lastWS    time.Time         // ข้อความจาก WebSocket ล่าสุด
	lastSeen  map[int]RealMatch // คู่ที่เคยเห็นตอนกำลังแข่ง (ใช้ปิดเป็น FT เมื่อหายไป)
	doneAt    map[int]time.Time // เวลาที่คู่นั้นถูกปิดเป็น FT (ใช้ prune ทีหลัง)

	// WebSocket push (แผนฟรี: subscribe ได้สูงสุด 25 คู่, ไม่กินโควตา)
	wsConn      *websocket.Conn
	wsWriteMu   sync.Mutex
	wsUp        bool
	wsSubs      map[int]bool
	wsNeedResub bool // มีคู่ใหม่ตอนชุดเต็ม -> เปิด session ใหม่ทันที

	leagueIndex   map[string]int // league ULID -> apiId (int)
	leagueIndexOK bool
}

type goalDayCache struct {
	at      time.Time
	matches []RealMatch
}

type goalLeague struct {
	ID    string `json:"id"`
	APIID string `json:"apiId"`
	Name  string `json:"name"`
	Logo  string `json:"logo"`
}

type goalTeamRef struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Badge string `json:"badge"`
}

type goalFixture struct {
	ID            string       `json:"id"`
	APIID         string       `json:"apiId"`
	CountryName   string       `json:"countryName"`
	LeagueID      string       `json:"leagueId"`
	LeagueName    string       `json:"leagueName"`
	MatchDate     string       `json:"matchDate"`
	MatchTime     string       `json:"matchTime"`
	KickoffUTC    string       `json:"kickoffUtc"`
	MatchStatus   string       `json:"matchStatus"`
	MatchLive     string       `json:"matchLive"`
	MatchMinute   string       `json:"matchMinute"`
	MatchElapsed  *int         `json:"matchElapsed"`
	MatchPeriod   string       `json:"matchPeriod"`
	HomeTeamID    string       `json:"homeTeamId"`
	HomeTeamName  string       `json:"homeTeamName"`
	HomeTeamScore *string      `json:"homeTeamScore"`
	HomeTeamFT    *string      `json:"homeTeamFtScore"`
	HomeTeamHT    *string      `json:"homeTeamHalftimeScore"`
	HomeTeamPen   *string      `json:"homeTeamPenaltyScore"`
	AwayTeamID    string       `json:"awayTeamId"`
	AwayTeamName  string       `json:"awayTeamName"`
	AwayTeamScore *string      `json:"awayTeamScore"`
	AwayTeamFT    *string      `json:"awayTeamFtScore"`
	AwayTeamHT    *string      `json:"awayTeamHalftimeScore"`
	AwayTeamPen   *string      `json:"awayTeamPenaltyScore"`
	TeamHomeBadge string       `json:"teamHomeBadge"`
	TeamAwayBadge string       `json:"teamAwayBadge"`
	LeagueLogo    string       `json:"leagueLogo"`
	StageName     string       `json:"stageName"`
	MatchStadium  string       `json:"matchStadium"`
	MatchReferee  string       `json:"matchReferee"`
	League        *goalLeague  `json:"league"`
	HomeTeam      *goalTeamRef `json:"homeTeam"`
	AwayTeam      *goalTeamRef `json:"awayTeam"`

	// มีเฉพาะตอนเรียก GET /fixtures/:id
	Events        []goalEvent        `json:"events"`
	Cards         []goalCard         `json:"cards"`
	Substitutions []goalSubstitution `json:"substitutions"`
	Lineups       []goalLineupEntry  `json:"lineups"`
	Statistics    []goalStatistic    `json:"statistics"`
}

type goalFixturesResponse struct {
	Success    bool          `json:"success"`
	Data       []goalFixture `json:"data"`
	Pagination *struct {
		Total   int  `json:"total"`
		Limit   int  `json:"limit"`
		Offset  int  `json:"offset"`
		HasMore bool `json:"hasMore"`
	} `json:"pagination"`
	Error string `json:"error"`
	Code  string `json:"code"`
}

type goalLeaguesResponse struct {
	Success    bool         `json:"success"`
	Data       []goalLeague `json:"data"`
	Pagination *struct {
		Total   int  `json:"total"`
		Limit   int  `json:"limit"`
		Offset  int  `json:"offset"`
		HasMore bool `json:"hasMore"`
	} `json:"pagination"`
	Error string `json:"error"`
	Code  string `json:"code"`
}

func NewGoalProvider() *GoalProvider {
	key := strings.TrimSpace(getEnv("GOAL_API_KEY", ""))
	return &GoalProvider{
		baseURL:     strings.TrimRight(getEnv("GOAL_API_BASE", goalAPIBase), "/"),
		key:         key,
		client:      &http.Client{Timeout: 12 * time.Second},
		budget:      NewSourceBudget("goalapi", envInt("GOAL_DAILY_LIMIT", 1000), envInt("GOAL_MINUTE_LIMIT", 60)),
		dayCache:    make(map[string]goalDayCache),
		liveState:   make(map[int]RealMatch),
		lastSeen:    make(map[int]RealMatch),
		doneAt:      make(map[int]time.Time),
		wsSubs:      make(map[int]bool),
		leagueIndex: make(map[string]int),
	}
}

func (p *GoalProvider) Name() string          { return "goalapi" }
func (p *GoalProvider) Budget() *SourceBudget { return p.budget }
func (p *GoalProvider) Enabled() bool {
	return p.key != "" && envBool("GOAL_API_ENABLED", true)
}

// do ยิง GET ผ่าน budget governor + ซิงก์ header โควตาจริง
func (p *GoalProvider) do(path string) ([]byte, error) {
	return p.doMethod("GET", path)
}

func (p *GoalProvider) doMethod(method, path string) ([]byte, error) {
	if !p.Enabled() {
		return nil, fmt.Errorf("goalapi disabled")
	}
	if ok, reason := p.budget.Allow(); !ok {
		return nil, fmt.Errorf("goalapi budget: %s", reason)
	}

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		req, err := http.NewRequest(method, p.baseURL+path, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+p.key)
		req.Header.Set("Accept", "application/json")

		resp, err := p.client.Do(req)
		if err != nil {
			p.budget.Failure(err)
			lastErr = err
			continue
		}
		hdr := parseRateLimitHeader(resp.Header)
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			p.budget.Failure(readErr)
			lastErr = readErr
			continue
		}

		if resp.StatusCode == http.StatusOK {
			p.budget.Success(1, hdr)
			return body, nil
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			p.budget.ObserveHeader(hdr)
			code := goalErrorCode(body)
			if code == "QUOTA_EXCEEDED" {
				p.budget.ObserveHeader(&RateLimitHeader{Remaining: 0})
				return nil, fmt.Errorf("goalapi quota exhausted")
			}
			if code == "BURST_LIMIT_EXCEEDED" && attempt == 0 {
				lastErr = fmt.Errorf("goalapi burst limit")
				time.Sleep(1100 * time.Millisecond)
				continue
			}
			err = fmt.Errorf("goalapi 429: %s", code)
			p.budget.Failure(err)
			return nil, err
		}

		err = fmt.Errorf("goalapi status %d: %s", resp.StatusCode, truncateRunes(string(body), 160))
		p.budget.Failure(err)
		return nil, err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("goalapi request failed")
	}
	return nil, lastErr
}

func goalErrorCode(body []byte) string {
	var envelope struct {
		Code  string `json:"code"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil && envelope.Code != "" {
		return envelope.Code
	}
	return ""
}

func truncateRunes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// ---------------------------------------------------------------------------
// Normalization -> RealMatch (รูปเดียวกับที่ frontend หน้าแรกอ่าน)
// ---------------------------------------------------------------------------

var bangkokLoc = time.FixedZone("UTC+7", 7*60*60)

func normalizeDateKey(s string) string {
	s = strings.TrimSpace(s)
	if len(s) == 8 {
		return s
	}
	if t, err := time.ParseInLocation("2006-01-02", s, bangkokLoc); err == nil {
		return t.Format("20060102")
	}
	return time.Now().In(bangkokLoc).Format("20060102")
}

// bangkokDayWindow คืนช่วงเวลา [start, end) ของ "วันไทย" ที่ต้องการ
func bangkokDayWindow(dateKey string) (time.Time, time.Time) {
	start, err := time.ParseInLocation("20060102", dateKey, bangkokLoc)
	if err != nil {
		now := time.Now().In(bangkokLoc)
		start = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, bangkokLoc)
	}
	return start, start.Add(24 * time.Hour)
}

// goalStatusString แปลงสถานะเป็นรูปที่ frontend หน้าแรกเข้าใจ
// (FT / AET / PEN / NS / PST / HT / "45'" / LIVE)
func goalStatusString(f goalFixture) string {
	status := strings.ToUpper(strings.TrimSpace(f.MatchStatus))
	period := strings.ToUpper(strings.TrimSpace(f.MatchPeriod))

	isFinished := status == "FINISHED" || status == "FT" || status == "ENDED"
	isLive := f.MatchLive == "1" && !isFinished

	if isLive {
		if period == "HALF_TIME" || period == "HALF-TIME" || period == "HT" || status == "HALF_TIME" || status == "PAUSED" {
			return "HT"
		}
		if f.MatchElapsed != nil && *f.MatchElapsed > 0 {
			return fmt.Sprintf("%d'", *f.MatchElapsed)
		}
		minute := strings.TrimSuffix(strings.TrimSpace(f.MatchMinute), "'")
		if n, err := strconv.Atoi(minute); err == nil && n > 0 {
			return fmt.Sprintf("%d'", n)
		}
		if minute != "" {
			return minute + "'"
		}
		return "LIVE"
	}

	switch status {
	case "FINISHED", "FT", "ENDED":
		switch period {
		case "PENALTIES", "PENALTY_SHOOTOUT", "SHOOTOUT", "PENALTY":
			return "PEN"
		case "EXTRA_TIME", "AET", "OVERTIME":
			return "AET"
		}
		return "FT"
	case "POSTPONED":
		return "PST"
	case "CANCELLED", "CANCELED":
		return "CANC"
	case "SUSPENDED":
		return "SUSP"
	case "ABANDONED":
		return "ABD"
	case "DELAYED":
		return "DELAY"
	case "":
		if f.MatchLive == "1" {
			return "LIVE"
		}
		return "NS"
	default:
		return "NS"
	}
}

func goalIntScore(v *string) int {
	if v == nil {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(*v))
	if err != nil {
		return 0
	}
	return n
}

// goalLeagueIDFor แปลง league ULID -> int ผ่านดัชนี (fallback = hash คงที่)
func (p *GoalProvider) goalLeagueIDFor(ulid string) int {
	p.mu.Lock()
	if id, ok := p.leagueIndex[ulid]; ok {
		p.mu.Unlock()
		return id
	}
	p.mu.Unlock()

	// fallback: hash คงที่ (กันชนกันได้ยากในพื้นที่ int 4,000 ล้าน)
	h := uint32(2166136261)
	for i := 0; i < len(ulid); i++ {
		h ^= uint32(ulid[i])
		h *= 16777619
	}
	return int(h % 2000000000)
}

func (p *GoalProvider) toRealMatch(f goalFixture) (RealMatch, bool) {
	matchID := atoiOr(f.APIID, 0)
	if matchID == 0 {
		return RealMatch{}, false
	}

	leagueName := strings.TrimSpace(f.LeagueName)
	leagueLogo := f.LeagueLogo
	if f.League != nil {
		if strings.TrimSpace(f.League.Name) != "" {
			leagueName = strings.TrimSpace(f.League.Name)
		}
		if leagueLogo == "" {
			leagueLogo = f.League.Logo
		}
	}

	homeName, homeBadge := f.HomeTeamName, f.TeamHomeBadge
	if f.HomeTeam != nil {
		if f.HomeTeam.Name != "" {
			homeName = f.HomeTeam.Name
		}
		if f.HomeTeam.Badge != "" {
			homeBadge = f.HomeTeam.Badge
		}
	}
	awayName, awayBadge := f.AwayTeamName, f.TeamAwayBadge
	if f.AwayTeam != nil {
		if f.AwayTeam.Name != "" {
			awayName = f.AwayTeam.Name
		}
		if f.AwayTeam.Badge != "" {
			awayBadge = f.AwayTeam.Badge
		}
	}

	kickoff, _ := time.Parse(time.RFC3339, f.KickoffUTC)
	kickoffLocal := ""
	if !kickoff.IsZero() {
		kickoffLocal = kickoff.In(bangkokLoc).Format("15:04")
	}

	return RealMatch{
		MatchID:    matchID,
		LeagueID:   p.goalLeagueIDFor(f.LeagueID),
		League:     leagueName,
		Country:    f.CountryName,
		Status:     goalStatusString(f),
		MatchTime:  f.KickoffUTC,
		Time:       kickoffLocal,
		LeagueLogo: leagueLogo,
		HomeTeam:   RealTeam{Name: homeName, Logo: homeBadge, Score: goalIntScore(firstNonNil(f.HomeTeamScore, f.HomeTeamFT))},
		AwayTeam:   RealTeam{Name: awayName, Logo: awayBadge, Score: goalIntScore(firstNonNil(f.AwayTeamScore, f.AwayTeamFT))},
	}, true
}

func firstNonNil(a, b *string) *string {
	if a != nil {
		return a
	}
	return b
}

// rememberFixture เก็บ mapping match_id(ตัวเลข) -> ULID ของแหล่งข้อมูล
func (p *GoalProvider) rememberFixture(f goalFixture) {
	if f.APIID != "" {
		registry.Remember("match", f.APIID, p.Name(), f.ID)
	}
	// badge รูปทีมมีรูปแบบ .../badges/102_manchester-united.jpg -> เลข 102 คือ apiId
	if f.HomeTeamID != "" {
		if apiID := goalBadgeAPIID(f.TeamHomeBadge); apiID != "" {
			registry.Remember("team", apiID, p.Name(), f.HomeTeamID)
		}
	}
	if f.AwayTeamID != "" {
		if apiID := goalBadgeAPIID(f.TeamAwayBadge); apiID != "" {
			registry.Remember("team", apiID, p.Name(), f.AwayTeamID)
		}
	}
	if f.LeagueID != "" {
		p.mu.Lock()
		apiID, ok := p.leagueIndex[f.LeagueID]
		p.mu.Unlock()
		if ok {
			registry.Remember("league", strconv.Itoa(apiID), p.Name(), f.LeagueID)
		}
	}
}

// goalBadgeAPIID ดึงเลข apiId จากชื่อไฟล์ badge เช่น ".../badges/102_manchester-united.jpg" -> "102"
func goalBadgeAPIID(badge string) string {
	if badge == "" {
		return ""
	}
	base := badge
	if i := strings.LastIndex(base, "/"); i >= 0 {
		base = base[i+1:]
	}
	if i := strings.Index(base, "_"); i > 0 {
		base = base[:i]
	} else {
		return ""
	}
	if base == "" {
		return ""
	}
	for _, r := range base {
		if r < '0' || r > '9' {
			return ""
		}
	}
	return base
}

// ---------------------------------------------------------------------------
// FixturesForDate — รายการแข่งของ "วันไทย" (cache ในตัว provider เอง
// เพื่อไม่ให้ทุก request ไปกินโควตา)
// ---------------------------------------------------------------------------

func (p *GoalProvider) FixturesForDate(bangkokDate string) ([]RealMatch, error) {
	dateKey := normalizeDateKey(bangkokDate)
	ttl := time.Duration(envInt("GOAL_LIST_TTL_MIN", 15)) * time.Minute

	p.mu.Lock()
	if entry, ok := p.dayCache[dateKey]; ok && time.Since(entry.at) < ttl {
		base := entry.matches
		p.mu.Unlock()
		return p.applyLiveOverlay(base), nil
	}
	p.mu.Unlock()

	if err := p.ensureLeagueIndex(); err != nil {
		log.Printf("ℹ️ goalapi league index: %v", err)
	}

	start, end := bangkokDayWindow(dateKey)
	fromUTC := start.UTC().Format("2006-01-02")
	toUTC := end.Add(-time.Nanosecond).UTC().Format("2006-01-02")

	var fixtures []goalFixture
	for offset, pages := 0, 0; pages < 12; pages++ {
		path := fmt.Sprintf("/fixtures?from=%s&to=%s&limit=100&offset=%d", fromUTC, toUTC, offset)
		body, err := p.do(path)
		if err != nil {
			return nil, err
		}
		var resp goalFixturesResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, err
		}
		if len(resp.Data) == 0 {
			break
		}
		fixtures = append(fixtures, resp.Data...)
		if resp.Pagination == nil || !resp.Pagination.HasMore {
			break
		}
		offset += len(resp.Data)
	}

	matches := make([]RealMatch, 0, len(fixtures))
	for _, f := range fixtures {
		kickoff, err := time.Parse(time.RFC3339, f.KickoffUTC)
		if err != nil || kickoff.Before(start) || !kickoff.Before(end) {
			continue
		}
		rm, ok := p.toRealMatch(f)
		if !ok {
			continue
		}
		p.rememberFixture(f)
		matches = append(matches, rm)
	}

	sort.SliceStable(matches, func(i, j int) bool {
		if matches[i].MatchTime == matches[j].MatchTime {
			return matches[i].MatchID < matches[j].MatchID
		}
		return matches[i].MatchTime < matches[j].MatchTime
	})

	p.mu.Lock()
	p.dayCache[dateKey] = goalDayCache{at: time.Now(), matches: matches}
	p.mu.Unlock()

	// บันทึก mapping match_id -> ULID ทันที กันสูญหายตอน process ถูก kill
	registry.Save()

	return p.applyLiveOverlay(matches), nil
}

// applyLiveOverlay ทับคะแนน/สถานะล่าสุดจาก liveState (WS push + REST sweep)
func (p *GoalProvider) applyLiveOverlay(base []RealMatch) []RealMatch {
	p.mu.Lock()
	if len(p.liveState) == 0 || !p.liveFreshLocked() {
		p.mu.Unlock()
		return base
	}
	state := make(map[int]RealMatch, len(p.liveState))
	for k, v := range p.liveState {
		state[k] = v
	}
	p.mu.Unlock()
	return mergeLiveOverlay(base, state)
}

// liveFreshLocked: มี WS ที่เพิ่งอัปเดต หรือ REST sweep ที่ยังไม่เกิน 10 นาที
func (p *GoalProvider) liveFreshLocked() bool {
	if !p.lastWS.IsZero() && time.Since(p.lastWS) < 30*time.Second {
		return true
	}
	return p.liveSwept && time.Since(p.liveAt) < 10*time.Minute
}

func (p *GoalProvider) liveStateSnapshotLocked() []RealMatch {
	out := make([]RealMatch, 0, len(p.liveState))
	for _, m := range p.liveState {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MatchID < out[j].MatchID })
	return out
}

// pruneDoneLocked เอาคู่ที่จบแล้วเกิน 15 นาทีออก (ต้องถือ lock อยู่แล้ว)
func (p *GoalProvider) pruneDoneLocked(now time.Time) {
	for id, t := range p.doneAt {
		if now.Sub(t) > 15*time.Minute {
			delete(p.doneAt, id)
			delete(p.liveState, id)
			delete(p.lastSeen, id)
		}
	}
}

// LiveStateSnapshot คืนสถานะรวมปัจจุบันโดยไม่ยิง upstream
// SweepInterval + LiveSweep ใช้โดย live ticker (GOAL มี WS push จึงไม่ต้องถี่)
func (p *GoalProvider) SweepInterval() time.Duration {
	return time.Duration(envInt("GOAL_LIVE_SWEEP_SEC", 180)) * time.Second
}

func (p *GoalProvider) LiveSweep() error {
	_, err := p.LiveFixtures()
	return err
}

func (p *GoalProvider) LiveStateSnapshot() []RealMatch {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pruneDoneLocked(time.Now())
	return p.liveStateSnapshotLocked()
}

// LiveUpdatesSnapshot รูป broadcast ของ live ticker (ไม่ยิง upstream)
func (p *GoalProvider) LiveUpdatesSnapshot() []map[string]interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pruneDoneLocked(time.Now())

	out := make([]map[string]interface{}, 0, len(p.liveState))
	for _, m := range p.liveState {
		out = append(out, map[string]interface{}{
			"match_id":   m.MatchID,
			"status":     m.Status,
			"home_score": m.HomeTeam.Score,
			"away_score": m.AwayTeam.Score,
			"home_name":  m.HomeTeam.Name,
			"away_name":  m.AwayTeam.Name,
		})
	}
	return out
}

// HasUnknownLiveMatches คืน true เมื่อมีคู่ live ที่ยังไม่อยู่ในรายการวันนี้
// (ใช้เตือนว่าควรโหลดรายการวันนี้ใหม่เพื่อให้คู่ใหม่ขึ้นหน้าแรก)
func (p *GoalProvider) HasUnknownLiveMatches() bool {
	today := normalizeDateKey("")
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, ok := p.dayCache[today]
	if !ok {
		return false
	}
	in := make(map[int]bool, len(entry.matches))
	for _, m := range entry.matches {
		in[m.MatchID] = true
	}
	for id, s := range p.liveState {
		if isLiveStatus(s.Status) && !in[id] {
			return true
		}
	}
	return false
}

// InvalidateTodayCache บังคับให้โหลดรายการวันนี้ใหม่ในรอบถัดไป
func (p *GoalProvider) InvalidateTodayCache() {
	today := normalizeDateKey("")
	p.mu.Lock()
	delete(p.dayCache, today)
	p.mu.Unlock()
}

func isLiveStatus(s string) bool {
	if s == "HT" || s == "LIVE" {
		return true
	}
	return strings.HasSuffix(s, "'")
}

// ---------------------------------------------------------------------------
// LiveFixtures — REST sweep คู่ที่กำลังแข่ง (merge เข้า liveState)
//   - WS push อัปเดต score แบบ realtime อยู่แล้ว จึง sweep ห่าง ๆ ได้ (default 180 วิ)
//   - 1 call ได้ทุกคู่ ดังนั้นค่าใช้จ่ายคงที่ ~20 calls/ชม.
// ---------------------------------------------------------------------------

func (p *GoalProvider) LiveFixtures() ([]RealMatch, error) {
	ttl := time.Duration(envInt("GOAL_LIVE_TTL_SEC", 180)) * time.Second

	p.mu.Lock()
	if p.liveSwept && time.Since(p.liveAt) < ttl {
		out := p.liveStateSnapshotLocked()
		p.mu.Unlock()
		return out, nil
	}
	p.mu.Unlock()

	body, err := p.do("/fixtures/live")
	if err != nil {
		// REST ล้มเหลว -> ยังใช้ state เดิมได้ (WS อาจยัง push อยู่)
		return p.LiveStateSnapshot(), err
	}
	var resp goalFixturesResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	now := time.Now()
	seen := make(map[int]bool, len(resp.Data))
	live := make([]RealMatch, 0, len(resp.Data))
	for _, f := range resp.Data {
		rm, ok := p.toRealMatch(f)
		if !ok {
			continue
		}
		p.rememberFixture(f)
		seen[rm.MatchID] = true
		live = append(live, rm)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, rm := range live {
		p.liveState[rm.MatchID] = rm
		p.lastSeen[rm.MatchID] = rm
		delete(p.doneAt, rm.MatchID)
	}
	// คู่ที่เคยแข่งอยู่แต่หายไปจากรายการ -> ปิดเป็น FT (เก็บคะแนนครั้งสุดท้าย)
	for id, m := range p.liveState {
		if seen[id] || !isLiveStatus(m.Status) {
			continue
		}
		m.Status = "FT"
		p.liveState[id] = m
		p.doneAt[id] = now
	}

	p.liveAt = now
	p.liveSwept = true
	p.pruneDoneLocked(now)

	// ให้ WebSocket subscribe คู่ที่เพิ่งเห็นด้วย (push realtime, 0 โควตา)
	p.requestResubscribe()

	return p.liveStateSnapshotLocked(), nil
}

// ---------------------------------------------------------------------------
// League index — ULID ของลีก -> apiId ตัวเลข (ใช้เป็น league_id ของ frontend)
// ดึงครั้งเดียว ~11 calls แล้วเก็บลง data/id_registry.json ใช้ต่อได้ตลอด
// ---------------------------------------------------------------------------

func (p *GoalProvider) ensureLeagueIndex() error {
	p.mu.Lock()
	ready := p.leagueIndexOK && len(p.leagueIndex) >= 100
	built := make(map[string]int, len(p.leagueIndex))
	for k, v := range p.leagueIndex {
		built[k] = v
	}
	p.mu.Unlock()
	if ready {
		return nil
	}

	// 1) ใช้ข้อมูลที่เก็บไว้แล้ว (จากไฟล์ registry หรือจากหน่วยความจำ)
	if len(built) < 100 {
		built = make(map[string]int)
		for apiID, ref := range registry.All("league") {
			if ref.Source == p.Name() && ref.ULID != "" {
				if id, err := strconv.Atoi(apiID); err == nil {
					built[ref.ULID] = id
				}
			}
		}
	}
	if len(built) >= 100 {
		p.mu.Lock()
		p.leagueIndex = built
		p.leagueIndexOK = true
		p.mu.Unlock()
		return nil
	}

	// 2) ดึงรายการลีกทั้งหมด (100 ต่อหน้า, ~11 calls ครั้งแรกครั้งเดียว)
	built = make(map[string]int)
	for offset, pages := 0, 0; pages < 20; pages++ {
		body, err := p.do(fmt.Sprintf("/leagues?limit=100&offset=%d", offset))
		if err != nil {
			return err
		}
		var resp goalLeaguesResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return err
		}
		if len(resp.Data) == 0 {
			break
		}
		for _, lg := range resp.Data {
			id := atoiOr(lg.APIID, 0)
			if id == 0 || lg.ID == "" {
				continue
			}
			built[lg.ID] = id
			registry.Remember("league", strconv.Itoa(id), p.Name(), lg.ID)
		}
		if resp.Pagination == nil || !resp.Pagination.HasMore {
			break
		}
		offset += len(resp.Data)
	}
	registry.Save()

	ok := len(built) >= 100
	p.mu.Lock()
	p.leagueIndex = built
	p.leagueIndexOK = ok
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("league index incomplete: %d", len(built))
	}
	return nil
}

// ---------------------------------------------------------------------------
// MatchDetails — สร้าง JSON รูป MatchDetails ให้หน้า /match/[id] ใช้
// ดึงจาก /fixtures/:id (คืน events+cards+subs+lineups+statistics ใน call เดียว)
// แล้วเติม h2h + commentary ถ้ายังมีโควตาเหลือ
// ---------------------------------------------------------------------------

type goalDetailEnvelope struct {
	Success bool        `json:"success"`
	Data    goalFixture `json:"data"`
	Error   string      `json:"error"`
}

type goalEvent struct {
	ID         string  `json:"id"`
	Time       string  `json:"time"`
	Type       string  `json:"type"`
	HomeScorer *string `json:"homeScorer"`
	AwayScorer *string `json:"awayScorer"`
	HomeAssist *string `json:"homeAssist"`
	AwayAssist *string `json:"awayAssist"`
	Score      string  `json:"score"`
	Info       *string `json:"info"`
}

type goalCard struct {
	ID        string  `json:"id"`
	Time      string  `json:"time"`
	Card      string  `json:"card"`
	HomeFault *string `json:"homeFault"`
	AwayFault *string `json:"awayFault"`
	Info      *string `json:"info"`
}

type goalSubstitution struct {
	ID           string `json:"id"`
	Time         string `json:"time"`
	Substitution string `json:"substitution"`
	Team         string `json:"team"`
}

type goalLineupEntry struct {
	PlayerID       *string `json:"playerId"`
	PlayerKey      string  `json:"playerKey"`
	LineupPlayer   string  `json:"lineupPlayer"`
	LineupNumber   *string `json:"lineupNumber"`
	LineupPosition string  `json:"lineupPosition"`
	Team           string  `json:"team"`
	Type           string  `json:"type"`
}

type goalStatistic struct {
	Type string `json:"type"`
	Home string `json:"home"`
	Away string `json:"away"`
	Half string `json:"half"`
}

type goalComment struct {
	ID    string `json:"id"`
	Time  string `json:"time"`
	Text  string `json:"text"`
	State string `json:"state"`
}

type goalH2HMatch struct {
	MatchID    string `json:"match_id"`
	MatchDate  string `json:"match_date"`
	MatchTime  string `json:"match_time"`
	LeagueName string `json:"league_name"`
	LeagueLogo string `json:"league_logo"`
	Status     string `json:"match_status"`
	HomeName   string `json:"match_hometeam_name"`
	AwayName   string `json:"match_awayteam_name"`
	HomeScore  string `json:"match_hometeam_score"`
	AwayScore  string `json:"match_awayteam_score"`
	HomeBadge  string `json:"team_home_badge"`
	AwayBadge  string `json:"team_away_badge"`
}

func derefStr(v *string) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(*v)
}

func (p *GoalProvider) fetchH2H(homeID, awayID string) []goalH2HMatch {
	if homeID == "" || awayID == "" {
		return nil
	}
	body, err := p.do(fmt.Sprintf("/h2h/%s/%s", homeID, awayID))
	if err != nil {
		return nil
	}
	var resp struct {
		Data struct {
			DirectMatches []goalH2HMatch `json:"directMatches"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil
	}
	return resp.Data.DirectMatches
}

func (p *GoalProvider) fetchCommentary(ulid string) []goalComment {
	body, err := p.do(fmt.Sprintf("/fixtures/%s/commentary", ulid))
	if err != nil {
		return nil
	}
	var resp struct {
		Data []goalComment `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil
	}
	return resp.Data
}

// goalNameBundle ดึงชื่อ/โลโก้ ทีมและลีก จาก fixture (ใช้ร่วมกับ list ด้วย)
func goalNameBundle(f goalFixture) (leagueName, leagueLogo, country, homeName, homeBadge, awayName, awayBadge string) {
	leagueName = strings.TrimSpace(f.LeagueName)
	leagueLogo = f.LeagueLogo
	country = f.CountryName
	if f.League != nil {
		if strings.TrimSpace(f.League.Name) != "" {
			leagueName = strings.TrimSpace(f.League.Name)
		}
		if leagueLogo == "" {
			leagueLogo = f.League.Logo
		}
	}
	homeName, homeBadge = f.HomeTeamName, f.TeamHomeBadge
	if f.HomeTeam != nil {
		if f.HomeTeam.Name != "" {
			homeName = f.HomeTeam.Name
		}
		if f.HomeTeam.Badge != "" {
			homeBadge = f.HomeTeam.Badge
		}
	}
	awayName, awayBadge = f.AwayTeamName, f.TeamAwayBadge
	if f.AwayTeam != nil {
		if f.AwayTeam.Name != "" {
			awayName = f.AwayTeam.Name
		}
		if f.AwayTeam.Badge != "" {
			awayBadge = f.AwayTeam.Badge
		}
	}
	return
}

func (p *GoalProvider) MatchDetails(matchID string) ([]byte, error) {
	if !p.Enabled() {
		return nil, fmt.Errorf("goalapi disabled")
	}
	ref, ok := registry.Lookup("match", matchID)
	if !ok || ref.Source != p.Name() || ref.ULID == "" {
		return nil, fmt.Errorf("match %s ไม่อยู่ใน goal registry", matchID)
	}

	body, err := p.do("/fixtures/" + ref.ULID)
	if err != nil {
		return nil, err
	}
	var env goalDetailEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, err
	}
	if env.Data.ID == "" {
		return nil, fmt.Errorf("goalapi detail ว่าง (%s)", matchID)
	}
	d := env.Data

	// ส่วนเสริม (h2h + commentary) ดึงเฉพาะตอนที่ยังมีโควตาเหลือพอ
	var h2h []goalH2HMatch
	var commentary []goalComment
	if p.budget.Remaining() >= envInt("GOAL_DETAIL_EXTRAS_MIN", 250) {
		h2h = p.fetchH2H(d.HomeTeamID, d.AwayTeamID)
		commentary = p.fetchCommentary(ref.ULID)
	}

	detail := buildGoalMatchDetails(d, h2h, commentary, p.goalLeagueIDFor(d.LeagueID))
	return json.Marshal(detail)
}

func buildGoalMatchDetails(d goalFixture, h2h []goalH2HMatch, commentary []goalComment, leagueID int) map[string]interface{} {
	leagueName, leagueLogo, country, homeName, homeBadge, awayName, awayBadge := goalNameBundle(d)
	statusShort := goalStatusString(d)

	isFinished := statusShort == "FT" || statusShort == "AET" || statusShort == "PEN"
	isPostponed := statusShort == "PST" || statusShort == "CANC" || statusShort == "ABD" || statusShort == "SUSP"
	isStarted := !isPostponed && statusShort != "NS"

	homeScore := goalIntScore(firstNonNil(d.HomeTeamScore, d.HomeTeamFT))
	awayScore := goalIntScore(firstNonNil(d.AwayTeamScore, d.AwayTeamFT))
	scoreStr := fmt.Sprintf("%d - %d", homeScore, awayScore)

	kickoff, _ := time.Parse(time.RFC3339, d.KickoffUTC)
	startTimeStr := ""
	utcTime := d.KickoffUTC
	if !kickoff.IsZero() {
		startTimeStr = kickoff.In(bangkokLoc).Format("15:04") + " น."
		utcTime = kickoff.UTC().Format(time.RFC3339)
	}

	teams := []interface{}{
		map[string]interface{}{"id": d.HomeTeamID, "name": homeName, "score": homeScore, "imageUrl": homeBadge, "logo": homeBadge},
		map[string]interface{}{"id": d.AwayTeamID, "name": awayName, "score": awayScore, "imageUrl": awayBadge, "logo": awayBadge},
	}

	statusMap := map[string]interface{}{
		"started":      isStarted,
		"finished":     isFinished,
		"liveTime":     statusShort,
		"scoreStr":     scoreStr,
		"startTimeStr": startTimeStr,
		"utcTime":      utcTime,
		"leagueName":   leagueName,
		"leagueId":     leagueID,
		"long":         d.MatchStatus,
		"short":        statusShort,
	}

	header := map[string]interface{}{
		"teams":  teams,
		"status": statusMap,
	}

	general := map[string]interface{}{
		"leagueName":   leagueName,
		"leagueId":     leagueID,
		"leagueLogo":   leagueLogo,
		"country":      country,
		"matchTimeUTC": utcTime,
		"round":        d.StageName,
		"venue":        d.MatchStadium,
		"referee":      d.MatchReferee,
	}

	events := buildGoalEvents(d)
	lineup := buildGoalLineup(d)
	stats := buildGoalStats(d)
	h2hMatches := buildGoalH2H(h2h)
	commentaryList := buildGoalCommentary(commentary)

	return map[string]interface{}{
		"header":  header,
		"general": general,
		"content": map[string]interface{}{
			"matchFacts": map[string]interface{}{
				"events": map[string]interface{}{"events": events},
			},
			"lineup": lineup,
			"stats":  stats,
			"h2h":    map[string]interface{}{"matches": h2hMatches},
		},
		"commentary":    commentaryList,
		"liveStatusStr": statusShort,
		"data_source":   "goalapi",
	}
}

// buildGoalEvents รวม ประตู + ใบเหลือง/แดง + เปลี่ยนตัว เรียงตามนาที
func buildGoalEvents(d goalFixture) []interface{} {
	items := make([]interface{}, 0, len(d.Events)+len(d.Cards)+len(d.Substitutions))

	for _, e := range d.Events {
		isHome := strings.EqualFold(derefStr(e.Info), "home") || derefStr(e.HomeScorer) != ""
		player := derefStr(e.HomeScorer)
		assist := derefStr(e.HomeAssist)
		if !isHome {
			player = derefStr(e.AwayScorer)
			assist = derefStr(e.AwayAssist)
		}
		if player == "" {
			player = derefStr(e.AwayScorer)
		}
		items = append(items, map[string]interface{}{
			"id":     e.ID,
			"time":   e.Time,
			"type":   "goal",
			"isHome": isHome,
			"player": player,
			"assist": assist,
			"score":  e.Score,
			"detail": "ประตู",
		})
	}

	for _, c := range d.Cards {
		isHome := derefStr(c.HomeFault) != ""
		player := derefStr(c.HomeFault)
		if !isHome {
			player = derefStr(c.AwayFault)
		}
		card := "yellow"
		lowerCard := strings.ToLower(c.Card)
		if strings.Contains(lowerCard, "red") && !strings.Contains(lowerCard, "yellow") {
			card = "red"
		}
		items = append(items, map[string]interface{}{
			"id":     c.ID,
			"time":   c.Time,
			"type":   "card",
			"card":   card,
			"isHome": isHome,
			"player": player,
			"detail": c.Card,
		})
	}

	for _, s := range d.Substitutions {
		// รูปแบบข้อมูลต้นทาง: "ผู้เล่นออก | ผู้เล่นเข้า" (พิสูจน์จากผู้เล่นตัวจริงใน lineup)
		subOut, subIn := "", ""
		if parts := strings.Split(s.Substitution, "|"); len(parts) >= 2 {
			subOut = strings.TrimSpace(parts[0])
			subIn = strings.TrimSpace(parts[1])
		} else {
			subIn = strings.TrimSpace(s.Substitution)
		}
		items = append(items, map[string]interface{}{
			"id":     s.ID,
			"time":   s.Time,
			"type":   "sub",
			"isHome": strings.EqualFold(s.Team, "home"),
			"player": subIn,
			"subIn":  subIn,
			"subOut": subOut,
			"detail": "เปลี่ยนตัว",
		})
	}

	sort.SliceStable(items, func(i, j int) bool {
		a, _ := items[i].(map[string]interface{})
		b, _ := items[j].(map[string]interface{})
		return eventMinute(fmt.Sprint(a["time"])) < eventMinute(fmt.Sprint(b["time"]))
	})
	return items
}

func eventMinute(raw string) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	if idx := strings.IndexAny(raw, "+"); idx > 0 {
		raw = raw[:idx]
	}
	n, err := strconv.Atoi(strings.TrimSuffix(raw, "'"))
	if err != nil {
		return 0
	}
	return n
}

var goalPositionLabels = map[string]string{
	"1": "ผู้รักษาประตู",
	"2": "กองหลัง",
	"3": "กองกลาง",
	"4": "กองหน้า",
}

// buildGoalLineup แปลง lineups[] เป็นรูปที่ TacticalPitchView + แท็บแผนตัวจริงอ่าน
func buildGoalLineup(d goalFixture) map[string]interface{} {
	starters := map[string][]interface{}{"home": {}, "away": {}}
	bench := map[string][]interface{}{"home": {}, "away": {}}
	coaches := map[string]string{"home": "", "away": ""}

	for _, lp := range d.Lineups {
		side := "away"
		if strings.EqualFold(lp.Team, "home") {
			side = "home"
		}
		kind := strings.ToLower(strings.TrimSpace(lp.Type))

		player := map[string]interface{}{"name": lp.LineupPlayer}
		if id := derefStr(lp.PlayerID); id != "" {
			player["id"] = id
		}
		if num := derefStr(lp.LineupNumber); num != "" {
			if n, err := strconv.Atoi(num); err == nil {
				player["shirtNumber"] = n
			}
		}
		if pos := strings.TrimSpace(lp.LineupPosition); pos != "" && pos != "0" {
			if label, ok := goalPositionLabels[pos]; ok {
				player["role"] = label
			} else {
				player["role"] = pos
			}
		}

		switch kind {
		case "coach", "manager":
			coaches[side] = lp.LineupPlayer
		case "substitutes", "subs", "bench":
			bench[side] = append(bench[side], player)
		case "starting_lineups", "starting", "starters", "lineup":
			starters[side] = append(starters[side], player)
		default:
			if derefStr(lp.LineupNumber) != "" {
				starters[side] = append(starters[side], player)
			} else {
				bench[side] = append(bench[side], player)
			}
		}
	}

	sideObj := func(side string) map[string]interface{} {
		return map[string]interface{}{
			"formation": "",
			"players":   starters[side],
			"bench":     bench[side],
			"coach":     coaches[side],
		}
	}
	homeSide := sideObj("home")
	awaySide := sideObj("away")

	// ถ้าไม่มีรายชื่อตัวจริงเลย ให้คืน object ว่าง
	// -> TacticalPitchView จะเห็น hasOfficialLineup=false แล้วแสดง "Lineup Preview" แทน
	//    (กัน UI บอกว่าเป็นข้อมูลตัวจริงทางการทั้งที่ไม่มีข้อมูล)
	if len(starters["home"]) == 0 && len(starters["away"]) == 0 {
		return map[string]interface{}{}
	}

	return map[string]interface{}{
		"lineup":   []interface{}{homeSide, awaySide},
		"homeTeam": homeSide,
		"awayTeam": awaySide,
	}
}

// buildGoalStats แปลง statistics[] เป็นรูป Periods.All.stats ที่ parseStatsList อ่าน
func buildGoalStats(d goalFixture) map[string]interface{} {
	items := []interface{}{}
	for _, s := range d.Statistics {
		if s.Half != "" && !strings.EqualFold(s.Half, "full") {
			continue
		}
		homeVal, ok1 := goalStatNumber(s.Home)
		awayVal, ok2 := goalStatNumber(s.Away)
		if !ok1 || !ok2 {
			continue
		}
		items = append(items, map[string]interface{}{
			"title": s.Type,
			"stats": []interface{}{homeVal, awayVal},
		})
	}
	return map[string]interface{}{
		"Periods": map[string]interface{}{
			"All": map[string]interface{}{
				"stats": []interface{}{
					map[string]interface{}{"stats": items},
				},
			},
		},
	}
}

func goalStatNumber(raw string) (int, bool) {
	s := strings.TrimSpace(raw)
	s = strings.TrimSuffix(s, "%")
	s = strings.ReplaceAll(s, ",", "")
	if s == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return int(f), true
}

var thaiMonths = [12]string{"ม.ค.", "ก.พ.", "มี.ค.", "เม.ย.", "พ.ค.", "มิ.ย.", "ก.ค.", "ส.ค.", "ก.ย.", "ต.ค.", "พ.ย.", "ธ.ค."}

func thaiDateLabel(dateStr, timeStr string) string {
	t, err := time.ParseInLocation("2006-01-02", dateStr, bangkokLoc)
	if err != nil {
		if timeStr != "" {
			return timeStr
		}
		return dateStr
	}
	label := fmt.Sprintf("%02d %s %d", t.Day(), thaiMonths[t.Month()-1], t.Year())
	if timeStr != "" {
		label += " · " + timeStr
	}
	return label
}

func buildGoalH2H(h2h []goalH2HMatch) []interface{} {
	out := make([]interface{}, 0, len(h2h))
	for _, m := range h2h {
		out = append(out, map[string]interface{}{
			"id":     m.MatchID,
			"time":   thaiDateLabel(m.MatchDate, m.MatchTime),
			"league": m.LeagueName,
			"status": map[string]interface{}{"scoreStr": m.HomeScore + " - " + m.AwayScore, "long": m.Status},
			"home":   map[string]interface{}{"name": m.HomeName, "score": atoiOr(m.HomeScore, 0), "logo": m.HomeBadge},
			"away":   map[string]interface{}{"name": m.AwayName, "score": atoiOr(m.AwayScore, 0), "logo": m.AwayBadge},
		})
	}
	return out
}

func buildGoalCommentary(commentary []goalComment) []interface{} {
	out := make([]interface{}, 0, len(commentary))
	for i := len(commentary) - 1; i >= 0; i-- {
		c := commentary[i]
		lower := strings.ToLower(c.Text)
		typ := "comment"
		important := false
		switch {
		case strings.Contains(lower, "goal"):
			typ, important = "goal", true
		case strings.Contains(lower, "red card"):
			typ, important = "card", true
		case strings.Contains(lower, "yellow card"), strings.Contains(lower, "booking"):
			typ = "card"
		case strings.Contains(lower, "substitution"):
			typ = "sub"
		case strings.Contains(lower, "corner"):
			typ = "corner"
		}
		out = append(out, map[string]interface{}{
			"id":          c.ID,
			"time":        c.Time,
			"text":        c.Text,
			"type":        typ,
			"isImportant": important,
		})
	}
	return out
}
