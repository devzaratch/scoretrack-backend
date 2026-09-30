package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// SourceBudget — ระบบคุมโควตา upstream ราย source (Phase 1: Data Layer)
//   1. นับโควตารายวัน (local) และรับค่าจริงจาก header X-RateLimit-Remaining
//   2. rate limit ต่อนาที กัน burst
//   3. circuit breaker: ล้มติดกัน 3 ครั้ง -> เปิดวงจร 60 วินาที แล้วค่อยลองใหม่
//   4. ทุกอย่างอ่านได้ผ่าน Snapshot() ที่ /api/_sources
// ---------------------------------------------------------------------------

type SourceBudget struct {
	mu sync.Mutex

	name        string
	dailyLimit  int
	minuteLimit int

	dayKey       string
	usedToday    int
	requests     int
	minuteMarks  []time.Time
	hasHeader    bool
	headerLeft   int
	resetUnix    int64

	consecutiveFails int
	openUntil        time.Time
	lastError        string
	lastSuccess      time.Time
	lastAttempt      time.Time
	disabled         bool
}

func NewSourceBudget(name string, dailyLimit, minuteLimit int) *SourceBudget {
	return &SourceBudget{
		name:        name,
		dailyLimit:  dailyLimit,
		minuteLimit: minuteLimit,
		dayKey:      time.Now().Format("2006-01-02"),
	}
}

// rollDayLocked รีเซ็ตตัวนับเมื่อเปลี่ยนวัน (เรียกขณะถือ lock แล้ว)
func (b *SourceBudget) rollDayLocked() {
	today := time.Now().Format("2006-01-02")
	if b.dayKey == today {
		return
	}
	b.dayKey = today
	b.usedToday = 0
	b.requests = 0
	b.minuteMarks = nil
	b.hasHeader = false
	b.headerLeft = 0
	b.resetUnix = 0
	b.consecutiveFails = 0
	b.openUntil = time.Time{}
	b.lastError = ""
}

func (b *SourceBudget) pruneMinuteLocked(now time.Time) {
	cut := now.Add(-time.Minute)
	kept := b.minuteMarks[:0]
	for _, t := range b.minuteMarks {
		if t.After(cut) {
			kept = append(kept, t)
		}
	}
	b.minuteMarks = kept
}

// Allow ตรวจสอบก่อนยิง upstream คืน (false, เหตุผล) เมื่อต้องหยุดยิง
func (b *SourceBudget) Allow() (bool, string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.rollDayLocked()
	now := time.Now()

	if b.disabled {
		return false, "disabled"
	}
	if now.Before(b.openUntil) {
		return false, "circuit-open"
	}

	remaining := b.dailyLimit - b.usedToday
	if b.hasHeader && b.headerLeft < remaining {
		remaining = b.headerLeft
	}
	if remaining <= 0 {
		return false, "daily-exhausted"
	}

	b.pruneMinuteLocked(now)
	if b.minuteLimit > 0 && len(b.minuteMarks) >= b.minuteLimit {
		return false, "minute-limited"
	}
	return true, ""
}

// Success บันทึกว่าใช้โควตาไป 1 ครั้ง (n = จำนวนหน่วยที่คิดจริง)
func (b *SourceBudget) Success(n int, hdr *RateLimitHeader) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.rollDayLocked()
	if n < 1 {
		n = 1
	}
	b.usedToday += n
	b.requests++
	b.lastAttempt = time.Now()
	b.lastSuccess = b.lastAttempt
	b.minuteMarks = append(b.minuteMarks, b.lastAttempt)
	b.consecutiveFails = 0
	b.openUntil = time.Time{}

	if hdr != nil {
		if hdr.Limit > 0 {
			b.dailyLimit = hdr.Limit
		}
		if hdr.Remaining >= 0 {
			b.hasHeader = true
			b.headerLeft = hdr.Remaining
		}
		if hdr.Reset > 0 {
			b.resetUnix = hdr.Reset
		}
	}
}

// Failure บันทึกความล้มเหลว และเปิด circuit breaker เมื่อล้มติดกัน 3 ครั้ง
func (b *SourceBudget) Failure(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.rollDayLocked()
	b.requests++
	b.lastAttempt = time.Now()
	b.consecutiveFails++
	if err != nil {
		b.lastError = err.Error()
	}
	if b.consecutiveFails >= 3 {
		b.openUntil = b.lastAttempt.Add(60 * time.Second)
	}
}

// ObserveHeader ซิงก์ค่าโควตาจริงจาก header โดยไม่เพิ่มจำนวนที่ใช้
func (b *SourceBudget) ObserveHeader(hdr *RateLimitHeader) {
	if hdr == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if hdr.Limit > 0 {
		b.dailyLimit = hdr.Limit
	}
	if hdr.Remaining >= 0 {
		b.hasHeader = true
		b.headerLeft = hdr.Remaining
	}
	if hdr.Reset > 0 {
		b.resetUnix = hdr.Reset
	}
}

func (b *SourceBudget) SetDisabled(v bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.disabled = v
}

func (b *SourceBudget) Remaining() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rollDayLocked()
	remaining := b.dailyLimit - b.usedToday
	if b.hasHeader && b.headerLeft < remaining {
		remaining = b.headerLeft
	}
	if remaining < 0 {
		remaining = 0
	}
	return remaining
}

func (b *SourceBudget) Name() string {
	return b.name
}

func (b *SourceBudget) Snapshot() map[string]interface{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rollDayLocked()
	b.pruneMinuteLocked(time.Now())

	remaining := b.dailyLimit - b.usedToday
	if b.hasHeader && b.headerLeft < remaining {
		remaining = b.headerLeft
	}
	if remaining < 0 {
		remaining = 0
	}

	state := "closed"
	if time.Now().Before(b.openUntil) {
		state = "open"
	} else if b.consecutiveFails > 0 {
		state = "half-open"
	}

	var lastSuccess string
	if !b.lastSuccess.IsZero() {
		lastSuccess = b.lastSuccess.Format(time.RFC3339)
	}
	var resetAt string
	if b.resetUnix > 0 {
		resetAt = time.Unix(b.resetUnix, 0).UTC().Format(time.RFC3339)
	}

	return map[string]interface{}{
		"name":              b.name,
		"enabled":           !b.disabled,
		"daily_limit":       b.dailyLimit,
		"used_today":        b.usedToday,
		"remaining":         remaining,
		"requests_total":    b.requests,
		"last_minute":       len(b.minuteMarks),
		"minute_limit":      b.minuteLimit,
		"circuit":           state,
		"consecutive_fails": b.consecutiveFails,
		"last_error":        b.lastError,
		"last_success":      lastSuccess,
		"quota_reset_at":    resetAt,
	}
}

// ---------------------------------------------------------------------------
// RateLimitHeader — ค่า header มาตรฐานที่แหล่งข้อมูลส่งกลับมาทุกครั้ง
// ---------------------------------------------------------------------------

type RateLimitHeader struct {
	Limit     int
	Remaining int
	Reset     int64
}

func parseRateLimitHeader(h map[string][]string) *RateLimitHeader {
	if h == nil {
		return nil
	}
	pick := func(key string) string {
		for k, v := range h {
			if len(v) > 0 && equalFold(k, key) {
				return v[0]
			}
		}
		return ""
	}
	out := &RateLimitHeader{Limit: 0, Remaining: -1, Reset: 0}
	if s := pick("X-RateLimit-Limit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			out.Limit = n
		}
	}
	if s := pick("X-RateLimit-Remaining"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			out.Remaining = n
		}
	}
	if s := pick("X-RateLimit-Reset"); s != "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			out.Reset = n
		}
	}
	if out.Limit == 0 && out.Remaining < 0 && out.Reset == 0 {
		return nil
	}
	return out
}

func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// IDRegistry — ผูก ID ตัวเลขที่ระบบใช้ (match_id / league_id) ไว้กับ
// ID จริงของแหล่งข้อมูล เพื่อให้หน้า detail รู้ว่าต้องไปดึงจากแหล่งไหน
// เก็บลง data/id_registry.json เพื่อไม่ให้หายเมื่อรีสตาร์ท
// ---------------------------------------------------------------------------

type idRef struct {
	Source string `json:"source"`
	ULID   string `json:"ulid"`
	Extra  string `json:"extra,omitempty"`
	SeenAt int64  `json:"seen_at"`
}

type idRegistry struct {
	mu       sync.RWMutex
	fixtures map[string]idRef
	leagues  map[string]idRef
	teams    map[string]idRef
	players  map[string]idRef
	dirty    bool
	filePath string
	lastSave time.Time
}

var registry = &idRegistry{
	fixtures: make(map[string]idRef),
	leagues:  make(map[string]idRef),
	teams:    make(map[string]idRef),
	players:  make(map[string]idRef),
	filePath: filepath.Join("data", "id_registry.json"),
}

func (r *idRegistry) bucketLocked(kind string) map[string]idRef {
	switch kind {
	case "match":
		return r.fixtures
	case "league":
		return r.leagues
	case "team":
		return r.teams
	case "player":
		return r.players
	}
	return nil
}

func (r *idRegistry) Remember(kind, key, source, ulid string) {
	if key == "" || ulid == "" {
		return
	}
	r.mu.Lock()
	bucket := r.bucketLocked(kind)
	if bucket == nil {
		r.mu.Unlock()
		return
	}
	if prev, ok := bucket[key]; ok && prev.Source == source && prev.ULID == ulid {
		r.mu.Unlock()
		return
	}
	bucket[key] = idRef{Source: source, ULID: ulid, SeenAt: time.Now().Unix()}
	r.dirty = true
	needSave := time.Since(r.lastSave) > 10*time.Second
	r.mu.Unlock()

	if needSave {
		r.Save()
	}
}

func (r *idRegistry) Lookup(kind, key string) (idRef, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	bucket := r.bucketLocked(kind)
	if bucket == nil {
		return idRef{}, false
	}
	ref, ok := bucket[key]
	return ref, ok
}

func (r *idRegistry) All(kind string) map[string]idRef {
	r.mu.RLock()
	defer r.mu.RUnlock()
	bucket := r.bucketLocked(kind)
	out := make(map[string]idRef, len(bucket))
	for k, v := range bucket {
		out[k] = v
	}
	return out
}

func (r *idRegistry) Counts() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return map[string]int{
		"matches": len(r.fixtures),
		"leagues": len(r.leagues),
		"teams":   len(r.teams),
		"players": len(r.players),
	}
}

func (r *idRegistry) Load() {
	r.mu.RLock()
	path := r.filePath
	r.mu.RUnlock()

	raw, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var doc struct {
		Fixtures map[string]idRef `json:"fixtures"`
		Leagues  map[string]idRef `json:"leagues"`
		Teams    map[string]idRef `json:"teams"`
		Players  map[string]idRef `json:"players"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		log.Printf("⚠️ id_registry อ่านไม่ได้: %v", err)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for k, v := range doc.Fixtures {
		r.fixtures[k] = v
	}
	for k, v := range doc.Leagues {
		r.leagues[k] = v
	}
	for k, v := range doc.Teams {
		r.teams[k] = v
	}
	for k, v := range doc.Players {
		r.players[k] = v
	}
}

func (r *idRegistry) Save() {
	r.mu.Lock()
	if !r.dirty {
		r.mu.Unlock()
		return
	}
	doc := map[string]interface{}{
		"fixtures": r.fixtures,
		"leagues":  r.leagues,
		"teams":    r.teams,
		"players":  r.players,
	}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		r.mu.Unlock()
		return
	}
	path := r.filePath
	tmp := path + ".tmp"
	dir := filepath.Dir(path)
	r.mu.Unlock()

	if err := os.MkdirAll(dir, 0755); err != nil {
		return
	}
	if err := os.WriteFile(tmp, raw, 0644); err != nil {
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		return
	}

	r.mu.Lock()
	r.dirty = false
	r.lastSave = time.Now()
	r.mu.Unlock()
}

// ---------------------------------------------------------------------------
// DataProvider — สัญญาที่แหล่งข้อมูลทุกตัวต้องทำตาม
//   ทำหน้าที่ดึง + normalize เป็นรูปเดียวกัน ไม่ให้ส่วนอื่นรู้ว่ามาจากไหน
// ---------------------------------------------------------------------------

type DataProvider interface {
	Name() string
	Enabled() bool
	Budget() *SourceBudget
	// FixturesForDate คืนรายการแข่งของ "วันไทย" รูป RealMatch (id เป็นตัวเลข)
	FixturesForDate(bangkokDate string) ([]RealMatch, error)
	// LiveFixtures คืนเฉพาะคู่ที่กำลังแข่งอยู่ตอนนี้
	LiveFixtures() ([]RealMatch, error)
	// MatchDetails คืน JSON รูป MatchDetails ที่หน้า /match/[id] ใช้
	MatchDetails(matchID string) ([]byte, error)
}

var providerChain []DataProvider

func initProviders() {
	registry.Load()

	providerChain = []DataProvider{
		NewGoalProvider(),
	}

	for _, p := range providerChain {
		state := "ปิด"
		if p.Enabled() {
			state = "เปิด"
		}
		log.Printf("🔌 Data Provider [%s]: %s (โควตาคงเหลือ %d)", p.Name(), state, p.Budget().Remaining())
	}

	// อุ่นดัชนีลีก (ULID -> apiId) ตอนเปิดเครื่อง ไม่ให้ไปหน่วงหน้าแรกครั้งแรก
	go func() {
		for _, p := range providerChain {
			if g, ok := p.(*GoalProvider); ok && g.Enabled() {
				if err := g.ensureLeagueIndex(); err != nil {
					log.Printf("ℹ️ warm league index: %v", err)
				}
			}
		}
	}()

	// flush ID registry ลงไฟล์เป็นระยะ (กันข้อมูลหายตอน kill process)
	go func() {
		for range time.Tick(60 * time.Second) {
			registry.Save()
		}
	}()

	// เปิด WebSocket push ของ GOAL (คะแนน realtime แบบไม่กินโควตา)
	if g := goalProvider(); g != nil {
		g.StartLiveStream()
	}
}

// fetchMatchesFromProviders ดึงรายการแข่งของวันไทยจาก provider ที่ว่างที่สุด
func fetchMatchesFromProviders(bangkokDate string) ([]byte, string, bool) {
	for _, p := range providerChain {
		if !p.Enabled() {
			continue
		}
		if ok, _ := p.Budget().Allow(); !ok {
			continue
		}
		matches, err := p.FixturesForDate(bangkokDate)
		if err != nil {
			log.Printf("⚠️ %s FixturesForDate error: %v", p.Name(), err)
			continue
		}
		if len(matches) == 0 {
			continue
		}
		raw, err := json.Marshal(matches)
		if err != nil {
			continue
		}
		return raw, p.Name(), true
	}
	return nil, "", false
}

// fetchLiveFromProviders ดึงคู่ที่กำลังแข่ง (ใช้โดย live ticker / overlay)
func fetchLiveFromProviders() ([]RealMatch, string, bool) {
	for _, p := range providerChain {
		if !p.Enabled() {
			continue
		}
		if ok, _ := p.Budget().Allow(); !ok {
			continue
		}
		matches, err := p.LiveFixtures()
		if err != nil {
			continue
		}
		if len(matches) == 0 {
			return matches, p.Name(), true
		}
		return matches, p.Name(), true
	}
	return nil, "", false
}

// fetchMatchDetailsFromProviders ดึงหน้ารายละเอียดแมตช์แบบ 1 call
func fetchMatchDetailsFromProviders(matchID string) ([]byte, string, bool) {
	ref, known := registry.Lookup("match", matchID)
	for _, p := range providerChain {
		if !p.Enabled() {
			continue
		}
		if known && ref.Source != p.Name() {
			continue
		}
		if ok, _ := p.Budget().Allow(); !ok {
			continue
		}
		data, err := p.MatchDetails(matchID)
		if err != nil {
			continue
		}
		if len(data) > 0 {
			return data, p.Name(), true
		}
	}
	return nil, "", false
}

// matchMajorLeagueRegex ใช้จัดลำดับสิทธิ์ subscribe WebSocket (ลีกใหญ่ได้ 25 คู่ก่อน)
// ตรงกับตัวกรอง "ลีกใหญ่" ที่หน้าแรกใช้
var matchMajorLeagueRegex = regexp.MustCompile(`Premier|LaLiga|La Liga|Serie A|Bundesliga|Ligue 1|Champions|Europa|Thai`)

// goalProvider คืน provider หลักที่เปิดใช้อยู่ (ใช้โดย live ticker)
func goalProvider() *GoalProvider {
	for _, p := range providerChain {
		if g, ok := p.(*GoalProvider); ok && g.Enabled() {
			return g
		}
	}
	return nil
}

// snapshotProviders ใช้แสดงผลที่ /api/_sources
func snapshotProviders() []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(providerChain)+1)
	for _, p := range providerChain {
		snap := p.Budget().Snapshot()
		snap["enabled"] = p.Enabled()
		out = append(out, snap)
	}
	// แหล่งเดิม (FotMob ผ่าน RapidAPI) เก็บไว้เป็นตัวสำรอง ต้องมีโควตาของตัวเอง
	legacy := rapidBudgetInstance().Snapshot()
	legacy["enabled"] = envBool("FOTMOB_FALLBACK_ENABLED", true)
	out = append(out, legacy)
	return out
}

// matchDetailsFinished ตรวจว่าหน้า detail ที่ได้มาจาก provider เป็นแมตช์ที่จบแล้วหรือยัง
func matchDetailsFinished(data []byte) bool {
	var probe struct {
		Header struct {
			Status struct {
				Finished bool `json:"finished"`
			} `json:"status"`
		} `json:"header"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return false
	}
	return probe.Header.Status.Finished
}

var (
	rapidBudgetOnce sync.Once
	rapidBudget     *SourceBudget
)

// rapidBudgetInstance คุมโควตา RapidAPI (500 req/เดือน เท่านั้น)
func rapidBudgetInstance() *SourceBudget {
	rapidBudgetOnce.Do(func() {
		rapidBudget = NewSourceBudget("fotmob-rapidapi", envInt("RAPIDAPI_DAILY_LIMIT", 15), envInt("RAPIDAPI_MINUTE_LIMIT", 6))
	})
	return rapidBudget
}

func atoiOr(s string, fallback int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v == "1" || v == "true" || v == "TRUE" || v == "yes"
}
