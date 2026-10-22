package main

import (
	"net/http"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// แผนข้อ 9 — SLO: window/percentile, freshness, alert state machine, rules
// ---------------------------------------------------------------------------

func TestSLOSeries_WindowStats(t *testing.T) {
	s := &sloSeries{}
	s.record(http.StatusOK, 10)
	s.record(http.StatusOK, 20)
	s.record(http.StatusInternalServerError, 5000)

	st := s.snapshot()
	if st.Total != 3 {
		t.Errorf("total ผิด: %d", st.Total)
	}
	if st.Errors != 1 {
		t.Errorf("errors ผิด: %d", st.Errors)
	}
	if st.ErrorRate < 0.33 || st.ErrorRate > 0.34 {
		t.Errorf("error_rate ผิด: %v", st.ErrorRate)
	}
	if st.Availability < 0.66 || st.Availability > 0.67 {
		t.Errorf("availability ผิด: %v", st.Availability)
	}
	if st.P95Ms != 5000 {
		t.Errorf("p95 ควรเป็น 5000 ได้ %d", st.P95Ms)
	}
	if st.P50Ms != 25 {
		t.Errorf("p50 ควรเป็น 25 (ขอบบนของ bucket) ได้ %d", st.P50Ms)
	}
}

func TestSLOSeries_RateLimitedCounted(t *testing.T) {
	s := &sloSeries{}
	s.record(http.StatusTooManyRequests, 5)
	if st := s.snapshot(); st.RateLimited != 1 {
		t.Errorf("rate_limited ผิด: %d", st.RateLimited)
	}
}

// ถังเก่านอก window ต้องไม่ถูกนับ
func TestSLOSeries_ExpiredBucketsIgnored(t *testing.T) {
	s := &sloSeries{}
	s.record(http.StatusOK, 10)
	// ย้ายถังปัจจุบันให้เก่ากว่า window ไป
	oldMinute := time.Now().Unix()/60 - int64(sloWindowBuckets)
	s.mu.Lock()
	for i := range s.buckets {
		if s.buckets[i].minute != 0 {
			s.buckets[i].minute = oldMinute
		}
	}
	s.mu.Unlock()

	if st := s.snapshot(); st.Total != 0 {
		t.Errorf("ข้อมูลนอก window ต้องไม่ถูกนับ ได้ %d", st.Total)
	}
}

func TestPercentileFromBuckets(t *testing.T) {
	merged := make([]int64, len(sloLatencyBounds)+1)
	merged[0] = 50 // <= 10ms
	merged[1] = 30 // <= 25ms
	merged[2] = 20 // <= 50ms

	// p50: ครึ่งหนึ่งของ request <= 10ms -> ค่าที่ได้คือ 10
	if got := percentileFromBuckets(merged, 0.50); got != 10 {
		t.Errorf("p50 ผิด: %d", got)
	}
	// p95: ต้องไปถึงกลุ่ม <= 50ms
	if got := percentileFromBuckets(merged, 0.95); got != 50 {
		t.Errorf("p95 ผิด: %d", got)
	}
	if got := percentileFromBuckets(make([]int64, len(sloLatencyBounds)+1), 0.9); got != 0 {
		t.Errorf("ไม่มีข้อมูลต้องได้ 0 ได้ %d", got)
	}
}

// freshness: จำเวลาที่เขียน cache แล้ววัดอายุตอนอ่าน
func TestFreshness_TracksAge(t *testing.T) {
	freshness.mu.Lock()
	freshness.setAt = map[string]time.Time{}
	freshness.families = map[string]*freshnessFamily{}
	freshness.mu.Unlock()

	key := "matches:20260101"
	noteCacheSet(key)
	// ย้อนเวลาที่เขียนไป 5 นาที เพื่อให้อายุที่วัดได้ชัด
	freshness.mu.Lock()
	freshness.setAt[key] = time.Now().Add(-5 * time.Minute)
	freshness.mu.Unlock()

	noteCacheHit(key)

	snap := snapshotFreshness()
	fams, _ := snap["families"].(map[string]interface{})
	fam, ok := fams["matches"].(map[string]interface{})
	if !ok {
		t.Fatalf("ไม่เจอ family 'matches' ใน %v", snap)
	}
	if hits, _ := fam["hits"].(int64); hits != 1 {
		t.Errorf("hits ผิด: %v", fam["hits"])
	}
	// อายุประมาณ 300 วิ (เผื่อเวลาเดินไปเล็กน้อยระหว่างเทสต์)
	if maxSec, _ := fam["max_sec"].(int64); maxSec < 290 || maxSec > 320 {
		t.Errorf("max_sec ควรราว 300 ได้ %d", maxSec)
	}
}

// alert state machine: แจ้งตอนแย่ลง / กลับมาปกติ / ไม่แจ้งซ้ำเกิน cooldown
func TestProcessAlerts_EscalateDedupeRecover(t *testing.T) {
	id := "test-rule-escalate"
	alertState.mu.Lock()
	delete(alertState.states, id)
	alertState.mu.Unlock()
	alertHistory.mu.Lock()
	alertHistory.recent = nil
	alertHistory.mu.Unlock()

	countSent := func() int {
		alertHistory.mu.Lock()
		defer alertHistory.mu.Unlock()
		n := 0
		for _, r := range alertHistory.recent {
			if r.ID == id {
				n++
			}
		}
		return n
	}
	latest := func() alertRule {
		alertHistory.mu.Lock()
		defer alertHistory.mu.Unlock()
		for _, r := range alertHistory.recent {
			if r.ID == id {
				return r
			}
		}
		return alertRule{}
	}

	// 1) ok -> warn ต้องแจ้ง
	processAlerts([]alertRule{{ID: id, Title: "T", Level: alertLevelWarn, Message: "m1"}})
	if countSent() != 1 {
		t.Fatalf("ระดับ warn ต้องแจ้งเตือน 1 ครั้ง ได้ %d", countSent())
	}
	if latest().Level != alertLevelWarn {
		t.Errorf("ระดับล่าสุดต้องเป็น warn ได้ %s", latest().Level)
	}

	// 2) warn ซ้ำ (ยังไม่ครบ cooldown) -> ไม่แจ้งซ้ำ
	processAlerts([]alertRule{{ID: id, Title: "T", Level: alertLevelWarn, Message: "m2"}})
	if countSent() != 1 {
		t.Errorf("ระดับเดิมซ้ำไม่ควรแจ้งซ้ำ (cooldown) ได้ %d ครั้ง", countSent())
	}

	// 3) escalate เป็น critical -> ต้องแจ้งใหม่
	processAlerts([]alertRule{{ID: id, Title: "T", Level: alertLevelCritical, Message: "m3"}})
	if countSent() != 2 {
		t.Fatalf("escalate เป็น critical ต้องแจ้งใหม่ ได้ %d ครั้ง", countSent())
	}
	if latest().Level != alertLevelCritical {
		t.Errorf("ระดับล่าสุดต้องเป็น critical ได้ %s", latest().Level)
	}
	alertState.mu.Lock()
	level := alertState.states[id].Level
	alertState.mu.Unlock()
	if level != alertLevelCritical {
		t.Errorf("state ต้องเป็น critical ได้ %s", level)
	}

	// 4) กลับมาปกติ -> ต้องแจ้งการกู้คืน
	processAlerts([]alertRule{{ID: id, Title: "T", Level: alertLevelOK, Message: ""}})
	if countSent() != 3 {
		t.Fatalf("กลับมาปกติต้องแจ้งการกู้คืน ได้ %d ครั้ง", countSent())
	}
	if latest().Level != alertLevelOK {
		t.Errorf("ระดับล่าสุดต้องเป็น ok ได้ %s", latest().Level)
	}
	if latest().Message == "" {
		t.Error("ข้อความกู้คืนต้องบอกว่ากลับมาปกติแล้ว")
	}
}

// กฎ availability: ยิง error เยอะต้องขึ้น critical
func TestSLORulesForStats_AvailabilityCritical(t *testing.T) {
	s := &sloSeries{}
	for i := 0; i < 20; i++ {
		s.record(http.StatusInternalServerError, 100)
	}
	rules := sloRulesForStats(s.snapshot())
	found := false
	for _, r := range rules {
		if r.ID == "availability" {
			found = true
			if r.Level != alertLevelCritical {
				t.Errorf("availability ควรเป็น critical ได้ %s", r.Level)
			}
		}
	}
	if !found {
		t.Error("ต้องมีกฎ availability ตอน error เยอะ")
	}
}

// request น้อยเกินไป -> ไม่ต้องแจ้ง (กันเตือนเพราะ noise)
func TestSLORulesForStats_IgnoresLowTraffic(t *testing.T) {
	s := &sloSeries{}
	s.record(http.StatusInternalServerError, 100)
	if rules := sloRulesForStats(s.snapshot()); len(rules) != 0 {
		t.Errorf("ข้อมูลน้อยไม่ควรขึ้นกฎ ได้ %v", rules)
	}
}

// latency p95 สูง -> เตือน
func TestSLORulesForStats_LatencyWarn(t *testing.T) {
	s := &sloSeries{}
	for i := 0; i < 6; i++ {
		s.record(http.StatusOK, 2500)
	}
	rules := sloRulesForStats(s.snapshot())
	found := false
	for _, r := range rules {
		if r.ID == "latency" {
			found = true
			if r.Level != alertLevelWarn {
				t.Errorf("latency p95 2500ms ควรเป็น warn ได้ %s", r.Level)
			}
		}
	}
	if !found {
		t.Error("ต้องมีกฎ latency เมื่อ p95 สูง")
	}
}

func TestSnapshotSLO_HasExpectedSections(t *testing.T) {
	snap := snapshotSLO()
	for _, key := range []string{"window_sec", "global", "routes", "freshness", "synthetic", "providers", "alerts", "targets"} {
		if _, ok := snap[key]; !ok {
			t.Errorf("snapshotSLO ขาดคีย์ %s", key)
		}
	}
	global, _ := snap["global"].(map[string]interface{})
	if _, ok := global["availability"]; !ok {
		t.Error("global ต้องมี availability")
	}
	if _, ok := global["p95_ms"]; !ok {
		t.Error("global ต้องมี p95_ms")
	}
}
