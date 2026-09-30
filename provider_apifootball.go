package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// API-Football (api-sports) — ตัวสำรองใน providerChain
// ใช้เฉพาะเมื่อ GOAL ใช้ไม่ได้ (โควตาหมด / circuit breaker เปิด / ล่ม)
//
// พิสูจน์จาก API จริงบนเครื่องนี้แล้ว (30 ก.ย. 2026):
//   - base https://v3.football.api-sports.io + header x-apisports-key
//   - GET /fixtures?date=2026-09-30 -> 209 คู่ (page เดียว)
//   - GET /fixtures?live=all        -> 18 คู่ตอนทดสอบ
//   - GET /fixtures?id=             -> มี events + lineups + statistics ใน 1 call
//   - GET /leagues                  -> 1,247 ลีก (ใช้สร้างตาราง map ลีกใหญ่ด้านล่าง)
//   - ไม่ส่ง header x-api-requests-* เลย -> ต้องนับโควตาเอง (ฟรี 100 req/วัน)
//   - id ของลีก/แมตช์คนละระบบกับ GOAL
//     -> ลีกใหญ่ map กลับเป็น GOAL apiId เพื่อให้หน้าแรก/ชิปใช้ของเดิมได้
// ---------------------------------------------------------------------------

const apiFootAPIBase = "https://v3.football.api-sports.io"

// apiFootGoalLeagueIDs เทียบ league id ของ API-Football -> GOAL apiId
// พิสูจน์จาก GET /leagues จริง (ชื่อลีก + ประเทศ ตรงกันทั้ง 8 รายการ)
var apiFootGoalLeagueIDs = map[int]int{
	39:  152, // England  / Premier League
	140: 302, // Spain    / La Liga
	135: 207, // Italy    / Serie A
	78:  175, // Germany  / Bundesliga
	61:  168, // France   / Ligue 1
	2:   3,   // World    / UEFA Champions League
	3:   4,   // World    / UEFA Europa League
	296: 314, // Thailand / Thai League 1
}

type APIFootProvider struct {
	baseURL string
	key     string
	client  *http.Client
	budget  *SourceBudget

	mu        sync.Mutex
	dayCache  map[string]apiFootDayCache
	liveState map[int]RealMatch
	liveAt    time.Time
	liveSwept bool
	lastSeen  map[int]RealMatch
	doneAt    map[int]time.Time
}

type apiFootDayCache struct {
	at      time.Time
	matches []RealMatch
}

// ---------------------------------------------------------------------------
// รูป JSON ของ API-Football v3 (ตรวจจากคำตอบจริงแล้ว)
// ---------------------------------------------------------------------------

type apiFootEnvelope struct {
	Errors  json.RawMessage `json:"errors"`
	Results int             `json:"results"`
	Paging  struct {
		Current int `json:"current"`
		Total   int `json:"total"`
	} `json:"paging"`
	Response []apiFootFixture `json:"response"`
}

type apiFootFixture struct {
	Ref struct {
		ID       int    `json:"id"`
		Date     string `json:"date"`
		Status   apiFootStatus `json:"status"`
		Referee  *string `json:"referee"`
		Venue    struct {
			ID   *int    `json:"id"`
			Name *string `json:"name"`
			City *string `json:"city"`
		} `json:"venue"`
	} `json:"fixture"`
	League struct {
		ID      int    `json:"id"`
		Name    string `json:"name"`
		Country string `json:"country"`
		Logo    string `json:"logo"`
		Flag    string `json:"flag"`
		Season  int    `json:"season"`
		Round   string `json:"round"`
	} `json:"league"`
	Teams struct {
		Home apiFootTeam `json:"home"`
		Away apiFootTeam `json:"away"`
	} `json:"teams"`
	Goals *apiFootGoals `json:"goals"`
	Score *struct {
		Halftime  *apiFootGoals `json:"halftime"`
		Fulltime  *apiFootGoals `json:"fulltime"`
		Extratime *apiFootGoals `json:"extratime"`
		Penalty   *apiFootGoals `json:"penalty"`
	} `json:"score"`

	// มีเฉพาะตอนเรียก GET /fixtures?id=
	Events     []apiFootEvent    `json:"events"`
	Lineups    []apiFootLineup   `json:"lineups"`
	Statistics []apiFootStatTeam `json:"statistics"`
}

type apiFootStatus struct {
	Long    string `json:"long"`
	Short   string `json:"short"`
	Elapsed *int   `json:"elapsed"`
	Extra   *int   `json:"extra"`
}

type apiFootTeam struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Logo    string `json:"logo"`
	Winner *bool  `json:"winner"`
}

type apiFootGoals struct {
	Home *int `json:"home"`
	Away *int `json:"away"`
}

type apiFootEvent struct {
	Time struct {
		Elapsed *int `json:"elapsed"`
		Extra   *int `json:"extra"`
	} `json:"time"`
	Team   apiFootTeam `json:"team"`
	Player struct {
		ID   *int    `json:"id"`
		Name *string `json:"name"`
	} `json:"player"`
	Assist struct {
		ID   *int    `json:"id"`
		Name *string `json:"name"`
	} `json:"assist"`
	Type     string  `json:"type"`
	Detail   string  `json:"detail"`
	Comments *string `json:"comments"`
}

type apiFootLineup struct {
	Team      apiFootTeam `json:"team"`
	Formation string      `json:"formation"`
	Coach     struct {
		ID   *int    `json:"id"`
		Name *string `json:"name"`
	} `json:"coach"`
	StartXI []apiFootLineupSlot `json:"startXI"`
	Bench   []apiFootLineupSlot `json:"bench"`
}

type apiFootLineupSlot struct {
	Player struct {
		ID     *int    `json:"id"`
		Name   string  `json:"name"`
		Number *int    `json:"number"`
		Pos    *string `json:"pos"`
		Grid   *string `json:"grid"`
	} `json:"player"`
}

type apiFootStatTeam struct {
	Team apiFootTeam `json:"team"`
	Statistics []struct {
		Type  string      `json:"type"`
		Value interface{} `json:"value"`
	} `json:"statistics"`
}

// ---------------------------------------------------------------------------
// DataProvider interface
// ---------------------------------------------------------------------------

func NewAPIFootProvider() *APIFootProvider {
	return &APIFootProvider{
		baseURL:   strings.TrimRight(getEnv("APIFOOTBALL_API_BASE", apiFootAPIBase), "/"),
		key:       strings.TrimSpace(getEnv("APIFOOTBALL_KEY", "")),
		client:    &http.Client{Timeout: 15 * time.Second},
		budget:    NewSourceBudget("apifootball", envInt("APIFOOTBALL_DAILY_LIMIT", 100), envInt("APIFOOTBALL_MINUTE_LIMIT", 10)),
		dayCache:  make(map[string]apiFootDayCache),
		liveState: make(map[int]RealMatch),
		lastSeen:  make(map[int]RealMatch),
		doneAt:    make(map[int]time.Time),
	}
}

func (p *APIFootProvider) Name() string           { return "apifootball" }
func (p *APIFootProvider) Budget() *SourceBudget  { return p.budget }
func (p *APIFootProvider) Enabled() bool          { return p.key != "" }

func (p *APIFootProvider) do(path string) ([]byte, error) {
	if !p.Enabled() {
		return nil, fmt.Errorf("apifootball disabled")
	}
	if ok, reason := p.budget.Allow(); !ok {
		return nil, fmt.Errorf("apifootball budget: %s", reason)
	}

	req, err := http.NewRequest("GET", p.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-apisports-key", p.key)
	req.Header.Set("Accept", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		p.budget.Failure(err)
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		p.budget.Failure(err)
		return nil, err
	}
	if resp.StatusCode == 429 || resp.StatusCode == 403 {
		err := fmt.Errorf("apifootball HTTP %d", resp.StatusCode)
		p.budget.Failure(err)
		return nil, err
	}
	if resp.StatusCode != 200 {
		err := fmt.Errorf("apifootball HTTP %d: %s", resp.StatusCode, truncateRunes(string(body), 160))
		p.budget.Failure(err)
		return nil, err
	}

	// API-Football ไม่ส่ง header โควตา -> นับเอง (ฟรี 100 req/วัน)
	p.budget.Success(1, nil)
	return body, nil
}

// apiFootErrorText อ่านค่า errors ซึ่งมาได้ทั้งรูป []string และ {key: message}
func apiFootErrorText(raw json.RawMessage) string {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "[]" || s == "{}" || s == "null" {
		return ""
	}
	var list []string
	if err := json.Unmarshal(raw, &list); err == nil {
		return strings.Join(list, "; ")
	}
	return s
}

// ---------------------------------------------------------------------------
// status + score + league mapping
// ---------------------------------------------------------------------------

// apiFootStatusString แปลง fixture.status เป็นสถานะที่หน้าแรกใช้
// ค่าจริงที่เจอ: TBD/NS/1H/HT/2H/ET/BT/FT/P/AET/PEN/SUSP/INT/PST/CANC/ABD/AWD/WO
func apiFootStatusString(f apiFootFixture) string {
	short := strings.ToUpper(strings.TrimSpace(f.Ref.Status.Short))
	elapsed := 0
	if f.Ref.Status.Elapsed != nil {
		elapsed = *f.Ref.Status.Elapsed
	}

	switch short {
	case "FT", "AET", "PEN", "AWD", "WO":
		return short
	case "HT", "BT", "P":
		return "HT"
	case "NS", "TBD":
		return "NS"
	case "PST":
		return "PST"
	case "CANC":
		return "CANC"
	case "ABD":
		return "ABD"
	case "SUSP":
		return "SUSP"
	case "INT":
		return "INT"
	}
	// 1H / 2H / LIV / ET  ->  ใช้นาที
	if elapsed > 0 {
		return strconv.Itoa(elapsed) + "'"
	}
	if short == "" {
		return "NS"
	}
	return "LIVE"
}

func apiFootScores(f apiFootFixture) (int, int) {
	if f.Goals == nil {
		return 0, 0
	}
	h, a := 0, 0
	if f.Goals.Home != nil {
		h = *f.Goals.Home
	}
	if f.Goals.Away != nil {
		a = *f.Goals.Away
	}
	return h, a
}

// goalLeagueIDForAPIFoot คืน league_id ที่หน้าแรกใช้
// ลีกใหญ่ -> GOAL apiId (ให้ชิป/ตัวกรอง/ลิงก์ใช้ของเดิมได้) · ลีกอื่น -> id ของตัวเอง
func goalLeagueIDForAPIFoot(id int) int {
	if mapped, ok := apiFootGoalLeagueIDs[id]; ok {
		return mapped
	}
	return id
}

func (p *APIFootProvider) toRealMatch(f apiFootFixture) (RealMatch, bool) {
	if f.Ref.ID == 0 {
		return RealMatch{}, false
	}
	kickoff, _ := time.Parse(time.RFC3339, f.Ref.Date)
	local := ""
	if !kickoff.IsZero() {
		local = kickoff.In(bangkokLoc).Format("15:04")
	}
	homeScore, awayScore := apiFootScores(f)

	return RealMatch{
		MatchID:    f.Ref.ID,
		LeagueID:   goalLeagueIDForAPIFoot(f.League.ID),
		League:     f.League.Name,
		Country:    f.League.Country,
		Status:     apiFootStatusString(f),
		MatchTime:  f.Ref.Date,
		Time:       local,
		LeagueLogo: f.League.Logo,
		HomeTeam:   RealTeam{Name: f.Teams.Home.Name, Logo: f.Teams.Home.Logo, Score: homeScore},
		AwayTeam:   RealTeam{Name: f.Teams.Away.Name, Logo: f.Teams.Away.Logo, Score: awayScore},
	}, true
}

// rememberFixture เก็บ match_id/league_id -> คีย์ของแหล่งข้อมูลตัวเอง
// (คีย์แมตช์ใช้ fixture id ของ API-Football เพราะระบบ id ไม่ตรงกับ GOAL)
func (p *APIFootProvider) rememberFixture(f apiFootFixture) {
	if f.Ref.ID > 0 {
		registry.Remember("match", strconv.Itoa(f.Ref.ID), p.Name(), strconv.Itoa(f.Ref.ID))
	}
	if f.League.ID > 0 {
		registry.Remember("league", strconv.Itoa(goalLeagueIDForAPIFoot(f.League.ID)), p.Name(), strconv.Itoa(f.League.ID))
	}
}

// ---------------------------------------------------------------------------
// FixturesForDate — รายการแข่งของ "วันไทย"
// หน้าต่างวันไทยล้าง 2 วัน UTC (17:00 -> 17:00) จึงต้องยิง 2 วัน (2 calls)
// ---------------------------------------------------------------------------

func (p *APIFootProvider) FixturesForDate(bangkokDate string) ([]RealMatch, error) {
	dateKey := normalizeDateKey(bangkokDate)
	ttl := time.Duration(envInt("APIFOOTBALL_LIST_TTL_MIN", 15)) * time.Minute

	p.mu.Lock()
	if entry, ok := p.dayCache[dateKey]; ok && time.Since(entry.at) < ttl {
		base := entry.matches
		p.mu.Unlock()
		return p.applyLiveOverlay(base), nil
	}
	p.mu.Unlock()

	start, end := bangkokDayWindow(dateKey)
	utcDays := []string{
		start.UTC().Format("2006-01-02"),
		end.Add(-time.Nanosecond).UTC().Format("2006-01-02"),
	}
	if utcDays[0] == utcDays[1] {
		utcDays = utcDays[:1]
	}

	// แผนฟรีของ API-Football เปิดอ่านเฉพาะช่วงวันนี้ ±1 วัน
	// (พิสูจน์จากคำตอบจริง: "Free plans do not have access to this date,
	//  try from 2026-09-29 to 2026-10-01")
	// -> ถ้านอกช่วง ไม่ยิงเปลืองโควตา ให้ provider ถัดไปใน chain รับช่วงต่อ
	utcNow := time.Now().UTC()
	minDay := time.Date(utcNow.Year(), utcNow.Month(), utcNow.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	maxDay := minDay.AddDate(0, 0, 3) // ช่วง [today-1, today+1]
	for _, utcDay := range utcDays {
		t, err := time.Parse("2006-01-02", utcDay)
		if err != nil || t.Before(minDay) || !t.Before(maxDay) {
			return nil, fmt.Errorf("apifootball แผนฟรีเปิดเฉพาะวัน %s ถึง %s (ขอวัน %s)",
				minDay.Format("2006-01-02"), maxDay.AddDate(0, 0, -1).Format("2006-01-02"), utcDay)
		}
	}

	seen := make(map[int]bool, 256)
	matches := make([]RealMatch, 0, 256)
	for _, utcDay := range utcDays {
		body, err := p.do("/fixtures?date=" + utcDay)
		if err != nil {
			return nil, err
		}
		var env apiFootEnvelope
		if err := json.Unmarshal(body, &env); err != nil {
			return nil, err
		}
		if msg := apiFootErrorText(env.Errors); msg != "" && len(env.Response) == 0 {
			return nil, fmt.Errorf("apifootball: %s", msg)
		}

		for _, f := range env.Response {
			kickoff, err := time.Parse(time.RFC3339, f.Ref.Date)
			if err != nil || kickoff.Before(start) || !kickoff.Before(end) {
				continue
			}
			rm, ok := p.toRealMatch(f)
			if !ok || seen[rm.MatchID] {
				continue
			}
			seen[rm.MatchID] = true
			p.rememberFixture(f)
			matches = append(matches, rm)
		}
	}

	sort.SliceStable(matches, func(i, j int) bool {
		if matches[i].MatchTime == matches[j].MatchTime {
			return matches[i].MatchID < matches[j].MatchID
		}
		return matches[i].MatchTime < matches[j].MatchTime
	})

	p.mu.Lock()
	p.dayCache[dateKey] = apiFootDayCache{at: time.Now(), matches: matches}
	p.mu.Unlock()

	registry.Save()
	return p.applyLiveOverlay(matches), nil
}

// ---------------------------------------------------------------------------
// Live — REST sweep (ไม่มี WebSocket push จึงต้อง sweep ถี่กว่า GOAL)
// ---------------------------------------------------------------------------

func (p *APIFootProvider) LiveFixtures() ([]RealMatch, error) {
	ttl := time.Duration(envInt("APIFOOTBALL_LIVE_SWEEP_SEC", 60)) * time.Second

	p.mu.Lock()
	if p.liveSwept && time.Since(p.liveAt) < ttl {
		out := p.liveStateSnapshotLocked()
		p.mu.Unlock()
		return out, nil
	}
	p.mu.Unlock()

	body, err := p.do("/fixtures?live=all")
	if err != nil {
		return p.LiveStateSnapshot(), err
	}
	var env apiFootEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, err
	}
	if msg := apiFootErrorText(env.Errors); msg != "" && len(env.Response) == 0 {
		return nil, fmt.Errorf("apifootball live: %s", msg)
	}

	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()

	seen := make(map[int]bool, len(env.Response))
	for _, f := range env.Response {
		rm, ok := p.toRealMatch(f)
		if !ok {
			continue
		}
		seen[rm.MatchID] = true
		p.liveState[rm.MatchID] = rm
		p.lastSeen[rm.MatchID] = rm
		delete(p.doneAt, rm.MatchID)
		p.rememberFixture(f)
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
	return p.liveStateSnapshotLocked(), nil
}

// SweepInterval + LiveSweep ใช้โดย live ticker (API-Football ไม่มี WebSocket push
// จึงต้อง sweep ถี่กว่า GOAL — ทุกครั้ง = 1 call เท่านั้น)
func (p *APIFootProvider) SweepInterval() time.Duration {
	return time.Duration(envInt("APIFOOTBALL_LIVE_SWEEP_SEC", 60)) * time.Second
}

func (p *APIFootProvider) LiveSweep() error {
	_, err := p.LiveFixtures()
	return err
}

func (p *APIFootProvider) liveStateSnapshotLocked() []RealMatch {
	out := make([]RealMatch, 0, len(p.liveState))
	for _, m := range p.liveState {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MatchID < out[j].MatchID })
	return out
}

func (p *APIFootProvider) pruneDoneLocked(now time.Time) {
	for id, t := range p.doneAt {
		if now.Sub(t) > 15*time.Minute {
			delete(p.doneAt, id)
			delete(p.liveState, id)
			delete(p.lastSeen, id)
		}
	}
}

func (p *APIFootProvider) LiveStateSnapshot() []RealMatch {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pruneDoneLocked(time.Now())
	return p.liveStateSnapshotLocked()
}

func (p *APIFootProvider) LiveUpdatesSnapshot() []map[string]interface{} {
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

func (p *APIFootProvider) HasUnknownLiveMatches() bool {
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

func (p *APIFootProvider) InvalidateTodayCache() {
	today := normalizeDateKey("")
	p.mu.Lock()
	delete(p.dayCache, today)
	p.mu.Unlock()
}

// applyLiveOverlay ทับคะแนน/สถานะล่าสุดจาก liveState (sweep ทุก 60 วิ)
func (p *APIFootProvider) applyLiveOverlay(base []RealMatch) []RealMatch {
	p.mu.Lock()
	if len(p.liveState) == 0 || p.liveAt.IsZero() || time.Since(p.liveAt) > 10*time.Minute {
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

// ---------------------------------------------------------------------------
// MatchDetails — 1 call ได้ events + lineups + statistics
// ---------------------------------------------------------------------------

func (p *APIFootProvider) MatchDetails(matchID string) ([]byte, error) {
	if !p.Enabled() {
		return nil, fmt.Errorf("apifootball disabled")
	}
	if ref, known := registry.Lookup("match", matchID); known && ref.Source != p.Name() {
		return nil, fmt.Errorf("match %s มาจากแหล่งอื่น (%s)", matchID, ref.Source)
	}
	if _, err := strconv.Atoi(matchID); err != nil {
		return nil, fmt.Errorf("match id ไม่ใช่ตัวเลข: %s", matchID)
	}

	body, err := p.do("/fixtures?id=" + matchID)
	if err != nil {
		return nil, err
	}
	var env apiFootEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, err
	}
	if msg := apiFootErrorText(env.Errors); msg != "" && len(env.Response) == 0 {
		return nil, fmt.Errorf("apifootball detail: %s", msg)
	}
	if len(env.Response) == 0 {
		return nil, fmt.Errorf("apifootball detail ว่าง (%s)", matchID)
	}

	d := env.Response[0]
	p.rememberFixture(d)
	detail := buildAPIFootMatchDetails(d)
	return json.Marshal(detail)
}

func buildAPIFootMatchDetails(d apiFootFixture) map[string]interface{} {
	statusShort := apiFootStatusString(d)

	isFinished := statusShort == "FT" || statusShort == "AET" || statusShort == "PEN"
	isPostponed := statusShort == "PST" || statusShort == "CANC" || statusShort == "ABD" || statusShort == "SUSP"
	isStarted := !isPostponed && statusShort != "NS"

	homeScore, awayScore := apiFootScores(d)
	scoreStr := fmt.Sprintf("%d - %d", homeScore, awayScore)

	kickoff, _ := time.Parse(time.RFC3339, d.Ref.Date)
	startTimeStr := ""
	utcTime := d.Ref.Date
	if !kickoff.IsZero() {
		startTimeStr = kickoff.In(bangkokLoc).Format("15:04") + " น."
		utcTime = kickoff.UTC().Format(time.RFC3339)
	}

	venue := ""
	if d.Ref.Venue.Name != nil {
		venue = *d.Ref.Venue.Name
	}
	if venue == "" && d.Ref.Venue.City != nil {
		venue = *d.Ref.Venue.City
	}
	referee := ""
	if d.Ref.Referee != nil {
		referee = *d.Ref.Referee
	}

	leagueID := goalLeagueIDForAPIFoot(d.League.ID)

	teams := []interface{}{
		map[string]interface{}{"id": d.Teams.Home.ID, "name": d.Teams.Home.Name, "score": homeScore, "imageUrl": d.Teams.Home.Logo, "logo": d.Teams.Home.Logo},
		map[string]interface{}{"id": d.Teams.Away.ID, "name": d.Teams.Away.Name, "score": awayScore, "imageUrl": d.Teams.Away.Logo, "logo": d.Teams.Away.Logo},
	}

	header := map[string]interface{}{
		"teams": teams,
		"status": map[string]interface{}{
			"started":      isStarted,
			"finished":     isFinished,
			"liveTime":     statusShort,
			"scoreStr":     scoreStr,
			"startTimeStr": startTimeStr,
			"utcTime":      utcTime,
			"leagueName":   d.League.Name,
			"leagueId":     leagueID,
			"long":         d.Ref.Status.Long,
			"short":        statusShort,
		},
	}

	general := map[string]interface{}{
		"leagueName":   d.League.Name,
		"leagueId":     leagueID,
		"leagueLogo":   d.League.Logo,
		"country":      d.League.Country,
		"matchTimeUTC": utcTime,
		"round":        d.League.Round,
		"venue":        venue,
		"referee":      referee,
	}

	return map[string]interface{}{
		"header":  header,
		"general": general,
		"content": map[string]interface{}{
			"matchFacts": map[string]interface{}{
				"events": map[string]interface{}{"events": buildAPIFootEvents(d)},
			},
			"lineup": buildAPIFootLineup(d),
			"stats":  buildAPIFootStats(d),
			// API-Football (แผนฟรี) ไม่มี h2h/commentary ใน 1 call -> คืน array ว่าง
			// เพื่อให้หน้า UI แสดงสถานะ "ไม่มีข้อมูล" แทนข้อมูลจำลอง
			"h2h": map[string]interface{}{"matches": []interface{}{}},
		},
		"commentary":    []interface{}{},
		"liveStatusStr": statusShort,
		"data_source":   "apifootball",
	}
}

// apiFootMinuteLabel คืนป้ายนาทีรูป "45+2" (หน้า UI เป็นคนเติม ' ให้เอง)
func apiFootMinuteLabel(elapsed, extra *int) string {
	if elapsed == nil {
		return ""
	}
	if extra != nil && *extra > 0 {
		return fmt.Sprintf("%d+%d", *elapsed, *extra)
	}
	return strconv.Itoa(*elapsed)
}

// buildAPIFootEvents เรียงตามนาที + คำนวณสกอร์วิ่งหลังแต่ละประตู
func buildAPIFootEvents(d apiFootFixture) []interface{} {
	if len(d.Events) == 0 {
		return []interface{}{}
	}

	type parsedEvent struct {
		minute  int
		sortOrd int
		raw     apiFootEvent
	}
	parsed := make([]parsedEvent, 0, len(d.Events))
	for _, e := range d.Events {
		ord := 0
		if e.Time.Elapsed != nil {
			ord = *e.Time.Elapsed
		}
		parsed = append(parsed, parsedEvent{minute: ord, raw: e})
	}
	sort.SliceStable(parsed, func(i, j int) bool { return parsed[i].minute < parsed[j].minute })

	items := make([]interface{}, 0, len(parsed))
	homeRunning, awayRunning := 0, 0

	for idx, pe := range parsed {
		e := pe.raw
		kind := strings.ToLower(strings.TrimSpace(e.Type))
		detail := strings.ToLower(strings.TrimSpace(e.Detail))
		isHome := e.Team.ID == d.Teams.Home.ID
		player := ""
		if e.Player.Name != nil {
			player = strings.TrimSpace(*e.Player.Name)
		}
		assist := ""
		if e.Assist.Name != nil {
			assist = strings.TrimSpace(*e.Assist.Name)
		}
		timeLabel := apiFootMinuteLabel(e.Time.Elapsed, e.Time.Extra)

		switch kind {
		case "goal":
			score := ""
			if isHome {
				homeRunning++
			} else {
				awayRunning++
			}
			score = fmt.Sprintf("%d - %d", homeRunning, awayRunning)
			what := "ประตู"
			if strings.Contains(detail, "penalty") {
				what = "จุดโทษ"
			} else if strings.Contains(detail, "own") {
				what = "ทำเข้าประตูตัวเอง"
			}
			items = append(items, map[string]interface{}{
				"id":     fmt.Sprintf("g%d", idx),
				"time":   timeLabel,
				"type":   "goal",
				"isHome": isHome,
				"player": player,
				"assist": assist,
				"score":  score,
				"detail": what,
			})
		case "card":
			card := "yellow"
			if strings.Contains(detail, "red") && !strings.Contains(detail, "yellow") {
				card = "red"
			}
			items = append(items, map[string]interface{}{
				"id":     fmt.Sprintf("c%d", idx),
				"time":   timeLabel,
				"type":   "card",
				"card":   card,
				"isHome": isHome,
				"player": player,
				"detail": card + " card",
			})
		case "subst":
			subOut, subIn := parseAPIFootSub(e)
			items = append(items, map[string]interface{}{
				"id":     fmt.Sprintf("s%d", idx),
				"time":   timeLabel,
				"type":   "sub",
				"isHome": isHome,
				"player": subIn,
				"subIn":  subIn,
				"subOut": subOut,
				"detail": "เปลี่ยนตัว",
			})
		}
	}
	return items
}

// parseAPIFootSub หา "ผู้เล่นออก | ผู้เล่นเข้า" จาก events ของ API-Football
// ค่าจริง: Detail = "Substitution 1", Comments = "Out: X - In: Y" (บางคู่ไม่มี Comments)
func parseAPIFootSub(e apiFootEvent) (subOut, subIn string) {
	if e.Comments != nil {
		raw := strings.TrimSpace(*e.Comments)
		lower := strings.ToLower(raw)
		if idx := strings.Index(lower, "out:"); idx >= 0 {
			rest := raw[idx+4:]
			if end := strings.Index(strings.ToLower(rest), "in:"); end >= 0 {
				subOut = strings.TrimSpace(rest[:end])
				subIn = strings.TrimSpace(rest[end+3:])
				return subOut, subIn
			}
			subOut = strings.TrimSpace(rest)
		}
		if idx := strings.Index(lower, "in:"); idx >= 0 {
			subIn = strings.TrimSpace(raw[idx+3:])
		}
	}
	return subOut, subIn
}

var apiFootPosLabels = map[string]string{
	"G": "ผู้รักษาประตู",
	"D": "กองหลัง",
	"M": "กองกลาง",
	"F": "กองหน้า",
	"GK": "ผู้รักษาประตู",
}

// buildAPIFootLineup — API-Football ให้ formation + startXI + bench มาตรง ๆ
func buildAPIFootLineup(d apiFootFixture) map[string]interface{} {
	if len(d.Lineups) == 0 {
		return map[string]interface{}{}
	}

	starters := map[string][]interface{}{"home": {}, "away": {}}
	bench := map[string][]interface{}{"home": {}, "away": {}}
	coaches := map[string]string{"home": "", "away": ""}
	formations := map[string]string{"home": "", "away": ""}

	slotToPlayer := func(s apiFootLineupSlot) map[string]interface{} {
		player := map[string]interface{}{"name": s.Player.Name}
		if s.Player.ID != nil {
			player["id"] = *s.Player.ID
		}
		if s.Player.Number != nil {
			player["shirtNumber"] = *s.Player.Number
		}
		if s.Player.Pos != nil {
			if label, ok := apiFootPosLabels[strings.ToUpper(strings.TrimSpace(*s.Player.Pos))]; ok {
				player["role"] = label
			} else {
				player["role"] = strings.TrimSpace(*s.Player.Pos)
			}
		}
		return player
	}

	for _, lu := range d.Lineups {
		side := "away"
		if lu.Team.ID == d.Teams.Home.ID {
			side = "home"
		}
		formations[side] = lu.Formation
		if lu.Coach.Name != nil {
			coaches[side] = strings.TrimSpace(*lu.Coach.Name)
		}
		for _, s := range lu.StartXI {
			starters[side] = append(starters[side], slotToPlayer(s))
		}
		for _, s := range lu.Bench {
			bench[side] = append(bench[side], slotToPlayer(s))
		}
	}

	sideObj := func(side string) map[string]interface{} {
		return map[string]interface{}{
			"formation": formations[side],
			"players":   starters[side],
			"bench":     bench[side],
			"coach":     coaches[side],
		}
	}
	homeSide := sideObj("home")
	awaySide := sideObj("away")

	// ไม่มีตัวจริงเลย -> คืน object ว่าง ให้ UI แสดง "Lineup Preview"
	if len(starters["home"]) == 0 && len(starters["away"]) == 0 {
		return map[string]interface{}{}
	}

	return map[string]interface{}{
		"lineup":   []interface{}{homeSide, awaySide},
		"homeTeam": homeSide,
		"awayTeam": awaySide,
	}
}

// buildAPIFootStats แปลง statistics[2 ทีม] -> Periods.All.stats ที่ parseStatsList อ่าน
func buildAPIFootStats(d apiFootFixture) map[string]interface{} {
	empty := map[string]interface{}{
		"Periods": map[string]interface{}{
			"All": map[string]interface{}{
				"stats": []interface{}{
					map[string]interface{}{"stats": []interface{}{}},
				},
			},
		},
	}
	if len(d.Statistics) == 0 {
		return empty
	}

	var home, away *apiFootStatTeam
	for i := range d.Statistics {
		switch d.Statistics[i].Team.ID {
		case d.Teams.Home.ID:
			home = &d.Statistics[i]
		case d.Teams.Away.ID:
			away = &d.Statistics[i]
		}
	}
	if home == nil || away == nil {
		if len(d.Statistics) >= 2 {
			home, away = &d.Statistics[0], &d.Statistics[1]
		} else {
			return empty
		}
	}

	awayValues := make(map[string]interface{}, len(away.Statistics))
	for _, s := range away.Statistics {
		awayValues[s.Type] = s.Value
	}

	items := []interface{}{}
	for _, s := range home.Statistics {
		awayVal, ok := awayValues[s.Type]
		if !ok {
			continue
		}
		h, ok1 := apiFootStatValue(s.Value)
		a, ok2 := apiFootStatValue(awayVal)
		if !ok1 || !ok2 {
			continue
		}
		items = append(items, map[string]interface{}{
			"title": s.Type,
			"stats": []interface{}{h, a},
		})
	}
	if len(items) == 0 {
		return empty
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

// apiFootStatValue ค่าสถิติของ API-Football มาได้ทั้ง number / "53%" / null
func apiFootStatValue(v interface{}) (int, bool) {
	switch t := v.(type) {
	case float64:
		return int(t), true
	case string:
		return goalStatNumber(t)
	case nil:
		return 0, false
	default:
		return goalStatNumber(fmt.Sprint(v))
	}
}
