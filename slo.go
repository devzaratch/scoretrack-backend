package main

// slo — SLO + alerting + synthetic monitoring (แผนข้อ 9)
//
// ทำไมต้องมี: มี metrics/log แล้ว (observability.go) แต่ยังไม่มี "ตัวเลขที่ตั้งเป็นเป้าหมาย"
// และ "แจ้งเตือนเมื่อพัง" — เราเคยเจอ prod 404 เงียบ ๆ จนกว่าจะรันเอง
//
// SLO ที่วัดจริง (window 5 นาทีล่าสุด):
//   - availability = 1 - (5xx / requests)          -> เป้า 99.5%
//   - latency p50/p95/p99 จาก histogram bucket     -> เป้า p95 < 2 วินาที
//   - data freshness = อายุข้อมูลที่เสิร์ฟจาก cache -> เป้าไม่เกิน 15 นาที
//   - provider health = circuit state + โควตาที่ใช้ -> เตือนเมื่อโควตาเกือบหมด
//   - synthetic = ยิง endpoint จริงทุก N นาที       -> ต้องตอบ 200 ติดกัน
//
// แจ้งเตือน: Telegram (ALERT_TELEGRAM_TOKEN + ALERT_TELEGRAM_CHAT_ID)
//   - ส่งเฉพาะตอนสถานะเปลี่ยน (ok -> warn -> critical) + ตอนกลับมาปกติ
//   - cooldown กันสแปม (ส่งซ้ำได้ไม่เกิน 1 ครั้งต่อ cooldown)
//   - ยังไม่ได้ตั้ง env = บันทึกลง log อย่างเดียว (ไม่ error ไม่พัง)

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// 1) Rolling window (5 x 1 นาที) — ตัวเลข SLO ต้องเป็น "ตอนนี้" ไม่ใช่ยอดรวมตั้งแต่เริ่ม
// ---------------------------------------------------------------------------

// sloLatencyBounds — ขอบเขต bucket ความเร็ว (ms) ช่วงสุดท้ายคือ "> 5000"
var sloLatencyBounds = []int64{10, 25, 50, 100, 250, 500, 1000, 2500, 5000}

const sloWindowBuckets = 5 // 5 นาที

type sloBucket struct {
	minute      int64
	total       int64
	err5xx      int64
	rateLimited int64
	panics      int64
	latencySum  int64
	lat         []int64 // จำนวน request ที่ตกในแต่ละ bucket
}

// sloStats — ตัวเลขที่คำนวณได้จาก window
type sloStats struct {
	Total        int64   `json:"total"`
	Errors       int64   `json:"errors"`
	RateLimited  int64   `json:"rate_limited"`
	Panics       int64   `json:"panics"`
	ErrorRate    float64 `json:"error_rate"`
	Availability float64 `json:"availability"`
	AvgMs        int64   `json:"avg_ms"`
	P50Ms        int64   `json:"p50_ms"`
	P95Ms        int64   `json:"p95_ms"`
	P99Ms        int64   `json:"p99_ms"`
	MaxMs        int64   `json:"max_ms"`
}

// sloSeries — ชุดข้อมูลแบบเลื่อนหน้าต่างเวลา (global หรือราย route)
type sloSeries struct {
	mu      sync.Mutex
	buckets [sloWindowBuckets]sloBucket
}

func (s *sloSeries) bucketFor(minute int64) *sloBucket {
	idx := int(((minute % sloWindowBuckets) + sloWindowBuckets) % sloWindowBuckets)
	b := &s.buckets[idx]
	if b.minute != minute {
		// ถังเก่า (หรือยังไม่เคยใช้) -> รีเซ็ต
		*b = sloBucket{minute: minute, lat: make([]int64, len(sloLatencyBounds)+1)}
	}
	if b.lat == nil {
		b.lat = make([]int64, len(sloLatencyBounds)+1)
	}
	return b
}

// record — บันทึก request 1 ตัว
func (s *sloSeries) record(status int, ms int64) {
	now := time.Now()
	minute := now.Unix() / 60

	s.mu.Lock()
	defer s.mu.Unlock()

	b := s.bucketFor(minute)
	b.total++
	if status >= 500 {
		b.err5xx++
	}
	if status == http.StatusTooManyRequests {
		b.rateLimited++
	}
	if ms < 0 {
		ms = 0
	}
	b.latencySum += ms

	// หา bucket ความเร็ว
	idx := len(sloLatencyBounds)
	for i, bound := range sloLatencyBounds {
		if ms <= bound {
			idx = i
			break
		}
	}
	b.lat[idx]++
}

// recordPanic — panic ที่ recovery จับได้ (นับแยกจาก 5xx ปกติ)
func (s *sloSeries) recordPanic() {
	now := time.Now()
	minute := now.Unix() / 60
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.bucketFor(minute)
	b.panics++
}

// snapshot — รวมทุกถังใน window -> ตัวเลข SLO
func (s *sloSeries) snapshot() sloStats {
	now := time.Now()
	minute := now.Unix() / 60
	cutoff := minute - int64(sloWindowBuckets) + 1

	s.mu.Lock()
	defer s.mu.Unlock()

	var st sloStats
	var latencySum int64
	merged := make([]int64, len(sloLatencyBounds)+1)
	for i := range s.buckets {
		b := &s.buckets[i]
		if b.minute == 0 || b.minute < cutoff || b.minute > minute {
			continue
		}
		st.Total += b.total
		st.Errors += b.err5xx
		st.RateLimited += b.rateLimited
		st.Panics += b.panics
		latencySum += b.latencySum
		for j, v := range b.lat {
			merged[j] += v
		}
	}
	if st.Total > 0 {
		st.ErrorRate = float64(st.Errors) / float64(st.Total)
		st.Availability = 1 - st.ErrorRate
		st.AvgMs = latencySum / st.Total
	}
	st.P50Ms = percentileFromBuckets(merged, 0.50)
	st.P95Ms = percentileFromBuckets(merged, 0.95)
	st.P99Ms = percentileFromBuckets(merged, 0.99)
	st.MaxMs = upperBoundOf(merged)
	return st
}

// percentileFromBuckets — ประมาณ percentile จาก histogram (ใช้ขอบบนของ bucket)
// ค่าที่ได้เป็น "ไม่เกิน" ค่าจริงเสมอ = ปลอดภัยกว่าการประมาณต่ำ
func percentileFromBuckets(merged []int64, p float64) int64 {
	var total int64
	for _, v := range merged {
		total += v
	}
	if total == 0 {
		return 0
	}
	target := int64(math.Ceil(p * float64(total)))
	if target < 1 {
		target = 1
	}
	var cum int64
	for i, v := range merged {
		cum += v
		if cum >= target {
			return sloLatencyBounds[i]
		}
	}
	return sloLatencyBounds[len(sloLatencyBounds)-1]
}

func upperBoundOf(merged []int64) int64 {
	for i := len(merged) - 1; i >= 0; i-- {
		if merged[i] > 0 {
			return sloLatencyBounds[i]
		}
	}
	return 0
}

// ---------------------------------------------------------------------------
// 2) ตัวเลขรวม + ราย route
// ---------------------------------------------------------------------------

var (
	sloGlobal   = &sloSeries{}
	sloRoutesMu sync.Mutex
	sloRoutes   = map[string]*sloSeries{}
)

func sloRouteSeries(route string) *sloSeries {
	sloRoutesMu.Lock()
	defer sloRoutesMu.Unlock()
	s := sloRoutes[route]
	if s == nil {
		s = &sloSeries{}
		sloRoutes[route] = s
	}
	return s
}

// snapshotSLORoutes — ตัวเลขราย route (เรียงตาม request มากสุด, เอา 20 อันดับแรก)
func snapshotSLORoutes() []map[string]interface{} {
	sloRoutesMu.Lock()
	list := make([]*sloSeries, 0, len(sloRoutes))
	names := make([]string, 0, len(sloRoutes))
	byName := map[string]*sloSeries{}
	for name, s := range sloRoutes {
		list = append(list, s)
		names = append(names, name)
		byName[name] = s
	}
	sloRoutesMu.Unlock()

	type row struct {
		name string
		stat sloStats
	}
	rows := make([]row, 0, len(list))
	for _, name := range names {
		rows = append(rows, row{name: name, stat: byName[name].snapshot()})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].stat.Total != rows[j].stat.Total {
			return rows[i].stat.Total > rows[j].stat.Total
		}
		return rows[i].name < rows[j].name
	})
	if len(rows) > 20 {
		rows = rows[:20]
	}

	out := make([]map[string]interface{}, 0, len(rows))
	for _, r := range rows {
		item := statsToMap(r.stat)
		item["route"] = r.name
		out = append(out, item)
	}
	return out
}

func statsToMap(st sloStats) map[string]interface{} {
	return map[string]interface{}{
		"total":        st.Total,
		"errors":       st.Errors,
		"rate_limited": st.RateLimited,
		"panics":       st.Panics,
		"error_rate":   st.ErrorRate,
		"availability": st.Availability,
		"avg_ms":       st.AvgMs,
		"p50_ms":       st.P50Ms,
		"p95_ms":       st.P95Ms,
		"p99_ms":       st.P99Ms,
		"max_ms":       st.MaxMs,
	}
}

// ---------------------------------------------------------------------------
// 3) Data freshness — อายุข้อมูลที่เสิร์ฟจาก cache
// ---------------------------------------------------------------------------

type freshnessFamily struct {
	Count   int64
	SumSec  int64
	MaxSec  int64
	LastSec int64
	LastAt  time.Time
}

var freshness = struct {
	mu       sync.Mutex
	setAt    map[string]time.Time
	families map[string]*freshnessFamily
}{setAt: map[string]time.Time{}, families: map[string]*freshnessFamily{}}

// cacheFamily — แยกชนิดข้อมูลจาก key (เช่น matches:20260101 -> matches)
func cacheFamily(key string) string {
	if i := strings.Index(key, ":"); i > 0 {
		return key[:i]
	}
	return key
}

// noteCacheSet — จำเวลาที่เขียน cache (ใช้คำนวณอายุตอนอ่าน)
func noteCacheSet(key string) {
	now := time.Now()
	freshness.mu.Lock()
	defer freshness.mu.Unlock()
	// กัน map บวม: ถ้าเกิน 5000 key ให้ล้างของที่ไม่รู้แล้วเก่า
	if len(freshness.setAt) > 5000 {
		for k, t := range freshness.setAt {
			if now.Sub(t) > time.Hour {
				delete(freshness.setAt, k)
			}
		}
	}
	freshness.setAt[key] = now
}

// noteCacheHit — บันทึกอายุข้อมูลที่เสิร์ฟ (อายุ = now - เวลาที่เขียน cache)
func noteCacheHit(key string) {
	now := time.Now()
	freshness.mu.Lock()
	defer freshness.mu.Unlock()

	setAt, ok := freshness.setAt[key]
	if !ok {
		return
	}
	age := int64(now.Sub(setAt).Seconds())
	if age < 0 {
		age = 0
	}
	fam := freshness.families[cacheFamily(key)]
	if fam == nil {
		fam = &freshnessFamily{}
		freshness.families[cacheFamily(key)] = fam
	}
	fam.Count++
	fam.SumSec += age
	if age > fam.MaxSec {
		fam.MaxSec = age
	}
	fam.LastSec = age
	fam.LastAt = now
}

// snapshotFreshness — สรุปอายุข้อมูลรายชนิด + รวม
func snapshotFreshness() map[string]interface{} {
	freshness.mu.Lock()
	defer freshness.mu.Unlock()

	families := map[string]interface{}{}
	var totalCount, totalSum, maxAge int64
	for name, f := range freshness.families {
		avg := int64(0)
		if f.Count > 0 {
			avg = f.SumSec / f.Count
		}
		families[name] = map[string]interface{}{
			"hits":     f.Count,
			"avg_sec":  avg,
			"max_sec":  f.MaxSec,
			"last_sec": f.LastSec,
			"last_at":  f.LastAt.UTC().Format(time.RFC3339),
		}
		totalCount += f.Count
		totalSum += f.SumSec
		if f.MaxSec > maxAge {
			maxAge = f.MaxSec
		}
	}
	avgAll := int64(0)
	if totalCount > 0 {
		avgAll = totalSum / totalCount
	}
	return map[string]interface{}{
		"families":     families,
		"hits":         totalCount,
		"avg_sec":      avgAll,
		"max_sec":      maxAge,
		"tracked_keys": len(freshness.setAt),
	}
}

// ---------------------------------------------------------------------------
// 4) Alert rules + state machine + Telegram
// ---------------------------------------------------------------------------

type alertRule struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	Level   string `json:"level"` // ok | warn | critical
	Message string `json:"message"`
}

type alertRuleState struct {
	Level       string
	LastSent    time.Time
	LastMessage string
}

var alertState = struct {
	mu     sync.Mutex
	states map[string]*alertRuleState
}{states: map[string]*alertRuleState{}}

// alertHistory — ประวัติที่แจ้งไปแล้ว (แยก mutex จาก alertState เพราะ sendAlert
// ถูกเรียกระหว่างที่ยังถือ alertState.mu อยู่ -> ถ้าใช้ mutex เดียวจะ deadlock)
var alertHistory = struct {
	mu     sync.Mutex
	recent []alertRule
}{}

const (
	alertLevelOK       = "ok"
	alertLevelWarn     = "warn"
	alertLevelCritical = "critical"
)

func levelRank(level string) int {
	switch level {
	case alertLevelCritical:
		return 2
	case alertLevelWarn:
		return 1
	default:
		return 0
	}
}

// sloRulesForStats — กฎที่คำนวณจากตัวเลข window (แยกออกมาให้เทสต์ได้โดยไม่ต้องแตะ global)
func sloRulesForStats(global sloStats) []alertRule {
	rules := []alertRule{}

	// 1) availability
	if global.Total >= 10 {
		switch {
		case global.ErrorRate >= sloFloatEnv("SLO_ERROR_RATE_CRIT", 0.10):
			rules = append(rules, alertRule{
				ID:      "availability",
				Title:   "Availability ต่ำกว่าเกณฑ์",
				Level:   alertLevelCritical,
				Message: fmt.Sprintf("5xx %d จาก %d request (%.1f%%) ใน 5 นาทีที่ผ่านมา", global.Errors, global.Total, global.ErrorRate*100),
			})
		case global.ErrorRate >= sloFloatEnv("SLO_ERROR_RATE_WARN", 0.02):
			rules = append(rules, alertRule{
				ID:      "availability",
				Title:   "Availability ต่ำกว่าเกณฑ์",
				Level:   alertLevelWarn,
				Message: fmt.Sprintf("5xx %d จาก %d request (%.1f%%) ใน 5 นาทีที่ผ่านมา", global.Errors, global.Total, global.ErrorRate*100),
			})
		}
	}

	// 2) latency p95
	if global.Total >= 5 {
		switch {
		case global.P95Ms >= sloInt64Env("SLO_P95_CRIT_MS", 5000):
			rules = append(rules, alertRule{
				ID:      "latency",
				Title:   "Latency p95 สูงเกินเกณฑ์",
				Level:   alertLevelCritical,
				Message: fmt.Sprintf("p95 = %d ms (p50 %d ms, p99 %d ms) ใน 5 นาทีที่ผ่านมา", global.P95Ms, global.P50Ms, global.P99Ms),
			})
		case global.P95Ms >= sloInt64Env("SLO_P95_WARN_MS", 2000):
			rules = append(rules, alertRule{
				ID:      "latency",
				Title:   "Latency p95 สูงเกินเกณฑ์",
				Level:   alertLevelWarn,
				Message: fmt.Sprintf("p95 = %d ms (p50 %d ms, p99 %d ms) ใน 5 นาทีที่ผ่านมา", global.P95Ms, global.P50Ms, global.P99Ms),
			})
		}
	}

	// 3) panic
	if global.Panics > 0 {
		rules = append(rules, alertRule{
			ID:      "panic",
			Title:   "มี panic ในเซิร์ฟเวอร์",
			Level:   alertLevelCritical,
			Message: fmt.Sprintf("panic %d ครั้งใน 5 นาทีที่ผ่านมา (ดู log ระดับ error)", global.Panics),
		})
	}

	// 4) rate limit ถูกยิงเยอะ (อาจเป็นการโจมตีหรือ client ซ้ำ)
	if global.RateLimited >= sloInt64Env("SLO_RATE_LIMIT_WARN", 20) {
		rules = append(rules, alertRule{
			ID:      "rate_limit",
			Title:   "โดน rate limit เยอะ",
			Level:   alertLevelWarn,
			Message: fmt.Sprintf("429 จำนวน %d ครั้งใน 5 นาทีที่ผ่านมา", global.RateLimited),
		})
	}

	return rules
}

// evaluateSLO — คำนวณกฎการแจ้งเตือนทั้งหมด ณ ตอนนี้
func evaluateSLO() []alertRule {
	rules := sloRulesForStats(sloGlobal.snapshot())

	// 5) data freshness
	fresh := snapshotFreshness()
	if maxSec, ok := fresh["max_sec"].(int64); ok && maxSec > 0 {
		switch {
		case maxSec >= sloInt64Env("SLO_FRESHNESS_CRIT_SEC", 3600):
			rules = append(rules, alertRule{
				ID:      "freshness",
				Title:   "ข้อมูลเก่าเกินเกณฑ์",
				Level:   alertLevelCritical,
				Message: fmt.Sprintf("ข้อมูลเก่าสุด %d วินาที (%.0f นาที)", maxSec, float64(maxSec)/60),
			})
		case maxSec >= sloInt64Env("SLO_FRESHNESS_WARN_SEC", 900):
			rules = append(rules, alertRule{
				ID:      "freshness",
				Title:   "ข้อมูลเก่าเกินเกณฑ์",
				Level:   alertLevelWarn,
				Message: fmt.Sprintf("ข้อมูลเก่าสุด %d วินาที (%.0f นาที)", maxSec, float64(maxSec)/60),
			})
		}
	}

	// 6) provider: circuit เปิด / โควตาเกือบหมด
	for _, p := range snapshotProviders() {
		name, _ := p["name"].(string)
		if name == "" {
			continue
		}
		circuit, _ := p["circuit"].(string)
		fails, _ := p["consecutive_fails"].(int64)
		if fails == 0 {
			if v, ok := p["consecutive_fails"].(int); ok {
				fails = int64(v)
			}
		}
		if circuit == "open" || fails >= 3 {
			lastErr, _ := p["last_error"].(string)
			rules = append(rules, alertRule{
				ID:      "provider:" + name,
				Title:   "Provider " + name + " มีปัญหา",
				Level:   alertLevelCritical,
				Message: fmt.Sprintf("circuit=%s ล้มเหลวติดกัน %d ครั้ง (%s)", circuit, fails, lastErr),
			})
			continue
		}
		used, _ := p["used_today"].(int64)
		limit, _ := p["daily_limit"].(int64)
		if limit > 0 {
			pct := float64(used) / float64(limit)
			switch {
			case used >= limit:
				rules = append(rules, alertRule{
					ID:      "quota:" + name,
					Title:   "โควตา " + name + " หมดแล้ว",
					Level:   alertLevelCritical,
					Message: fmt.Sprintf("ใช้ไป %d/%d คำขอวันนี้ (คิวตาจะเติมตอนเที่ยงวัน)", used, limit),
				})
			case pct >= sloFloatEnv("SLO_QUOTA_WARN_PCT", 0.9):
				rules = append(rules, alertRule{
					ID:      "quota:" + name,
					Title:   "โควตา " + name + " ใกล้หมด",
					Level:   alertLevelWarn,
					Message: fmt.Sprintf("ใช้ไป %d/%d คำขอวันนี้ (%.0f%%)", used, limit, pct*100),
				})
			}
		}
	}

	// 7) synthetic check ล้มเหลวติดกัน
	syn := snapshotSynthetic()
	if syn != nil {
		if fails, _ := syn["consecutive_failures"].(int); fails >= sloIntEnv("SLO_SYNTHETIC_FAIL_WARN", 3) {
			rules = append(rules, alertRule{
				ID:      "synthetic",
				Title:   "Synthetic check ล้มเหลว",
				Level:   alertLevelCritical,
				Message: fmt.Sprintf("ล้มเหลวติดกัน %d ครั้ง (ตรวจ %s)", fails, syn["last_check_at"]),
			})
		}
	}

	return rules
}

// processAlerts — เทียบสถานะใหม่กับเดิม แล้วส่งเฉพาะตอนเปลี่ยน/ครบ cooldown
func processAlerts(rules []alertRule) {
	now := time.Now()
	cooldown := time.Duration(sloIntEnv("ALERT_COOLDOWN_SEC", 1800)) * time.Second

	type pendingAlert struct {
		rule     alertRule
		recovery bool
	}
	var pending []pendingAlert

	alertState.mu.Lock()
	seen := map[string]bool{}
	for _, r := range rules {
		seen[r.ID] = true
		st := alertState.states[r.ID]
		if st == nil {
			st = &alertRuleState{Level: alertLevelOK}
			alertState.states[r.ID] = st
		}

		switch {
		case r.Level == st.Level:
			// สถานะเหมือนเดิม -> เตือนซ้ำเมื่อ cooldown ผ่าน (กันสแปม)
			if r.Level != alertLevelOK && now.Sub(st.LastSent) >= cooldown {
				st.LastSent = now
				st.LastMessage = r.Message
				pending = append(pending, pendingAlert{rule: r})
			}
		case r.Level == alertLevelOK:
			// กลับมาปกติ -> แจ้งการกู้คืน (ครั้งเดียว)
			if st.Level != alertLevelOK {
				st.LastSent = now
				pending = append(pending, pendingAlert{
					rule: alertRule{
						ID:      r.ID,
						Title:   r.Title,
						Level:   alertLevelOK,
						Message: "กลับมาปกติแล้ว (เคยแจ้ง: " + st.LastMessage + ")",
					},
					recovery: true,
				})
			}
		case levelRank(r.Level) > levelRank(st.Level):
			// แย่ลง (escalate) -> แจ้งทันที
			st.LastSent = now
			st.LastMessage = r.Message
			pending = append(pending, pendingAlert{rule: r})
		default:
			// ดีขึ้นแต่ยังไม่ปกติ -> บันทึกไว้ ไม่ต้องแจ้ง
			st.LastMessage = r.Message
		}
		st.Level = r.Level
	}

	// กฎที่ไม่ applicable อีกต่อไป -> ถือว่าปกติ
	for id, st := range alertState.states {
		if seen[id] || st.Level == alertLevelOK {
			continue
		}
		st.Level = alertLevelOK
		st.LastSent = now
		pending = append(pending, pendingAlert{
			rule: alertRule{
				ID:      id,
				Title:   id,
				Level:   alertLevelOK,
				Message: "กลับมาปกติแล้ว (เคยแจ้ง: " + st.LastMessage + ")",
			},
			recovery: true,
		})
	}
	alertState.mu.Unlock()

	// ส่งหลังปลดล็อก (ไม่ถือ lock ระหว่างยิงเน็ตเวิร์ก)
	for _, p := range pending {
		sendAlert(p.rule, p.recovery)
	}
}

// sendAlert — ส่งแจ้งเตือน (Telegram ถ้าตั้ง env ไว้ ไม่งั้น log อย่างเดียว)
func sendAlert(r alertRule, recovery bool) {
	icon := "🚨"
	if recovery {
		icon = "✅"
	}
	text := fmt.Sprintf("%s [%s] %s\n%s\nเวลา %s",
		icon, strings.ToUpper(r.Level), r.Title, r.Message, time.Now().Format(time.RFC3339))

	alertHistory.mu.Lock()
	alertHistory.recent = append([]alertRule{{
		ID: r.ID, Title: r.Title, Level: r.Level, Message: r.Message,
	}}, alertHistory.recent...)
	if len(alertHistory.recent) > 20 {
		alertHistory.recent = alertHistory.recent[:20]
	}
	alertHistory.mu.Unlock()

	token := strings.TrimSpace(os.Getenv("ALERT_TELEGRAM_TOKEN"))
	chat := strings.TrimSpace(os.Getenv("ALERT_TELEGRAM_CHAT_ID"))
	if token == "" || chat == "" {
		logWarnf("%s", text)
		return
	}
	if err := postTelegram(token, chat, text); err != nil {
		logWarnf("ส่งแจ้งเตือน Telegram ไม่สำเร็จ: %v", err)
	}
}

// postTelegram — ส่งข้อความผ่าน Telegram Bot API
func postTelegram(token, chatID, text string) error {
	body, err := json.Marshal(map[string]string{
		"chat_id": chatID,
		"text":    text,
	})
	if err != nil {
		return err
	}
	url := "https://api.telegram.org/bot" + token + "/sendMessage"
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 10 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram status %d", res.StatusCode)
	}
	return nil
}

// snapshotAlerts — สถานะกฎ + ประวัติที่ส่งล่าสุด (ไว้โชว์ในหน้า admin)
func snapshotAlerts() map[string]interface{} {
	alertState.mu.Lock()
	states := map[string]interface{}{}
	for id, st := range alertState.states {
		lastSent := ""
		if !st.LastSent.IsZero() {
			lastSent = st.LastSent.UTC().Format(time.RFC3339)
		}
		states[id] = map[string]interface{}{
			"level":        st.Level,
			"last_sent":    lastSent,
			"last_message": st.LastMessage,
		}
	}
	alertState.mu.Unlock()

	alertHistory.mu.Lock()
	recent := make([]alertRule, 0, len(alertHistory.recent))
	recent = append(recent, alertHistory.recent...)
	alertHistory.mu.Unlock()

	return map[string]interface{}{
		"states":       states,
		"recent":       recent,
		"telegram":     strings.TrimSpace(os.Getenv("ALERT_TELEGRAM_TOKEN")) != "",
		"cooldown_sec": sloIntEnv("ALERT_COOLDOWN_SEC", 1800),
	}
}

// ---------------------------------------------------------------------------
// 5) Synthetic monitoring — ยิง endpoint จริงของตัวเองเป็นระยะ
// ---------------------------------------------------------------------------

type syntheticResult struct {
	Name      string `json:"name"`
	Status    int    `json:"status"`
	LatencyMs int64  `json:"latency_ms"`
	OK        bool   `json:"ok"`
	Error     string `json:"error,omitempty"`
}

var synthetic = struct {
	mu       sync.Mutex
	LastAt   time.Time
	LastOK   time.Time
	Checks   int
	Failures int
	Consec   int
	Results  []syntheticResult
}{}

// syntheticTargets — ปลายทางที่ยิงตรวจ (เลือกที่ไม่กินโควตา provider)
// /healthz ตรวจว่า process + DB + Redis พร้อม, /api/_sources ตรวจว่า provider chain ยังอ่านได้
func syntheticTargets() []syntheticResult {
	base := "http://127.0.0.1:" + getEnv("PORT", "8080")
	targets := []struct {
		name string
		path string
	}{
		{"healthz", "/healthz"},
		{"sources", "/api/_sources"},
	}

	out := make([]syntheticResult, 0, len(targets))
	client := &http.Client{Timeout: 10 * time.Second}
	for _, t := range targets {
		res := syntheticResult{Name: t.name}
		start := time.Now()
		req, err := http.NewRequest("GET", base+t.path, nil)
		if err != nil {
			res.Error = err.Error()
			out = append(out, res)
			continue
		}
		resp, err := client.Do(req)
		res.LatencyMs = time.Since(start).Milliseconds()
		if err != nil {
			res.Error = err.Error()
			out = append(out, res)
			continue
		}
		_ = resp.Body.Close()
		res.Status = resp.StatusCode
		res.OK = resp.StatusCode >= 200 && resp.StatusCode < 400
		out = append(out, res)
	}
	return out
}

// runSyntheticCheck — ยิงตรวจ 1 รอบ + บันทึกผล
func runSyntheticCheck() {
	results := syntheticTargets()
	allOK := len(results) > 0
	for _, r := range results {
		if !r.OK {
			allOK = false
		}
	}

	synthetic.mu.Lock()
	defer synthetic.mu.Unlock()
	synthetic.Checks++
	synthetic.LastAt = time.Now()
	synthetic.Results = results
	if allOK {
		synthetic.LastOK = synthetic.LastAt
		synthetic.Consec = 0
		return
	}
	synthetic.Failures++
	synthetic.Consec++
}

// snapshotSynthetic — สรุปผล synthetic ล่าสุด
func snapshotSynthetic() map[string]interface{} {
	synthetic.mu.Lock()
	defer synthetic.mu.Unlock()
	if synthetic.Checks == 0 {
		return map[string]interface{}{
			"checks": 0,
			"note":   "ยังไม่ได้ตรวจ (รอบแรกจะเกิดหลังเริ่มเซิร์ฟเวอร์)",
		}
	}
	last := ""
	if !synthetic.LastAt.IsZero() {
		last = synthetic.LastAt.UTC().Format(time.RFC3339)
	}
	lastOK := ""
	if !synthetic.LastOK.IsZero() {
		lastOK = synthetic.LastOK.UTC().Format(time.RFC3339)
	}
	return map[string]interface{}{
		"checks":               synthetic.Checks,
		"failures":             synthetic.Failures,
		"consecutive_failures": synthetic.Consec,
		"last_check_at":        last,
		"last_ok_at":           lastOK,
		"results":              synthetic.Results,
	}
}

// startSyntheticMonitor — ยิงตรวจเป็นระยะ (ค่าเริ่มต้น 5 นาที)
func startSyntheticMonitor() {
	interval := time.Duration(sloIntEnv("SYNTHETIC_INTERVAL_SEC", 300)) * time.Second
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}
	// ตรวจรอบแรกหลังขึ้น 15 วิ (ให้เซิร์ฟเวอร์พร้อมก่อน)
	time.Sleep(15 * time.Second)
	runSyntheticCheck()
	logInfof("🩺 synthetic monitor เริ่มทำงาน (ทุก %v)", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		runSyntheticCheck()
	}
}

// ---------------------------------------------------------------------------
// 6) Loop ประเมิน SLO + แจ้งเตือน
// ---------------------------------------------------------------------------

// startSLOLoop — ประเมินกฎทุก 60 วิ แล้วส่งแจ้งเตือนตาม state machine
func startSLOLoop() {
	interval := time.Duration(sloIntEnv("ALERT_INTERVAL_SEC", 60)) * time.Second
	if interval < 15*time.Second {
		interval = 15 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		rules := evaluateSLO()
		processAlerts(rules)
	}
}

// ---------------------------------------------------------------------------
// 7) Snapshot รวม (ไว้โชว์ใน /api/admin/metrics)
// ---------------------------------------------------------------------------

func snapshotSLO() map[string]interface{} {
	global := sloGlobal.snapshot()
	providers := snapshotProviders()
	// เติม % โควตาไว้ให้หน้า admin วาดแถบได้เลย
	for _, p := range providers {
		used, _ := p["used_today"].(int64)
		limit, _ := p["daily_limit"].(int64)
		pct := 0.0
		if limit > 0 {
			pct = float64(used) / float64(limit)
		}
		p["quota_pct"] = pct
	}
	return map[string]interface{}{
		"window_sec": sloWindowBuckets * 60,
		"global":     statsToMap(global),
		"routes":     snapshotSLORoutes(),
		"freshness":  snapshotFreshness(),
		"synthetic":  snapshotSynthetic(),
		"providers":  providers,
		"alerts":     snapshotAlerts(),
		"warmup":     snapshotWarmup(),
		"targets": map[string]interface{}{
			"availability":  1 - sloFloatEnv("SLO_ERROR_RATE_CRIT", 0.10),
			"p95_ms":        sloIntEnv("SLO_P95_WARN_MS", 2000),
			"freshness_sec": sloIntEnv("SLO_FRESHNESS_WARN_SEC", 900),
		},
	}
}

// ---------------------------------------------------------------------------
// helpers สำหรับ env
// ---------------------------------------------------------------------------

func sloIntEnv(key string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func sloInt64Env(key string, fallback int64) int64 {
	return int64(sloIntEnv(key, int(fallback)))
}

func sloFloatEnv(key string, fallback float64) float64 {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}
