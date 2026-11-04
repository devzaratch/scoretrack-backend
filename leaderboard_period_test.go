package main

// leaderboard_period_test.go — จัดอันดับรายสัปดาห์/เดือน (ข้อ 4)
// seed: createdAt RFC3339 — นับเฉพาะ "อยู่ในช่วง ณ วันที่รัน" (fake now ไม่ได้ จึงตั้ง
// createdAt สัมพัทธ์จาก time.Now() แทนค่าวันตายตัว)

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func seedPeriodPred(t *testing.T, user, matchID, createdAtRFC3339, kind string) {
	t.Helper()
	GlobalDB.SavePrediction(user, matchID, map[string]interface{}{
		"home": 2, "away": 1, "createdAt": createdAtRFC3339,
	})
	cacheSet("matchresult:"+matchID, []byte(`{"home":2,"away":1,"finished":true}`), time.Hour)
}

// สัปดาห์นี้: นับเฉพาะทาย "จากวันจันทร์ 00:00 เป็นต้นไป"
func TestPeriodLeaderboardWeek(t *testing.T) {
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = nil }()

	now := time.Now()
	// ทาย "นอกสัปดาห์" เมื่อ 10 วันก่อน = ต้องไม่นับ
	seedPeriodPred(t, "user:a", "m-old", now.AddDate(0, 0, -10).Format(time.RFC3339), "week")
	// ทาย "ในสัปดาห์" 1 ชม.ก่อน = ถูกเป๊ะ (+3)
	seedPeriodPred(t, "user:a", "m-now", now.Add(-time.Hour).Format(time.RFC3339), "week")
	// เพื่อน: ทายในสัปดาห์เช่นกัน แต่แต้มน้อยกว่า (ผลเสมอ = การทาย 2-1 ไม่เป๊ะ = 0 แต้ม)
	seedPeriodPred(t, "user:b", "m-now2", func() string {
		cacheSet("matchresult:m-now2", []byte(`{"home":1,"away":1,"finished":true}`), time.Hour)
		return now.Add(-2 * time.Hour).Format(time.RFC3339)
	}(), "week")

	entries := computePeriodLeaderboard("week")
	if len(entries) != 2 {
		t.Fatalf("ควรมี 2 ผู้ใช้ (คนที่ทายเมื่อ 10 วันก่อนไม่เข้าโวเตอร์): ได้ %d :: %v", len(entries), entries)
	}
	if entries[0].UserID != "user:a" || entries[0].Points != 3 {
		t.Fatalf("user:a ควรคะแนน 3 อันดับ 1: %v", entries)
	}
	if entries[0].Predictions != 1 {
		t.Fatalf("นับเฉพาะทายในสัปดาห์ (predictions=1): %v", entries[0])
	}
}

// เดือนนี้: นับตั้งแต่ต้นเดือน — ทายเมื่อ 10 วันก่อนอาจเข้า/ไม่เข้าตามที่วันรัน
// ใช้ทั้ง "เมื่อวาน" (เข้าเสมอ) และ "40 วันก่อน" (ไม่เข้าเสมอ)
func TestPeriodLeaderboardMonth(t *testing.T) {
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = nil }()

	now := time.Now()
	seedPeriodPred(t, "user:x", "m40", now.AddDate(0, 0, -40).Format(time.RFC3339), "month")
	seedPeriodPred(t, "user:y", "m1d", now.AddDate(0, 0, -1).Format(time.RFC3339), "month")
	// ทายแต่ไม่มี createdAt (ข้อมูลเก่า) — ต้องข้าม ไม่เน่า 500
	GlobalDB.SavePrediction("user:old", "m-noc", map[string]interface{}{"home": 0, "away": 0})
	cacheSet("matchresult:m-noc", []byte(`{"home":2,"away":1,"finished":true}`), time.Hour)

	entries := computePeriodLeaderboard("month")
	if len(entries) != 1 {
		t.Fatalf("นับเฉพาะ user:y (40 วันก่อนตกเดือน, ไม่มี createdAt ข้าม): %v", entries)
	}
	if entries[0].UserID != "user:y" {
		t.Fatalf("entry ไม่ใช่ user:y: %v", entries)
	}
}

// periodStartUnix: สัปดาห์ต้องกลายเป็นจันทร์เที่ยงคืน, เดือนต้องเป็นต้นเดือน (ตรวจด้วยตัวเลข)
func TestPeriodStartUnix(t *testing.T) {
	// 2026-11-04 = พุธ — จันทร์คือ 2026-11-02
	start := periodStartUnix("week", time.Date(2026, 11, 4, 15, 30, 0, 0, time.UTC))
	want := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC).Unix()
	if start != want {
		t.Fatalf("week start ผิด: got %d want %d", start, want)
	}
	// อาทิตย์ (2026-11-08) ต้องนับสัปดาห์ย้อนจันทร์เดิม (ไม่ข้ามไปจันทร์หน้า)
	startSun := periodStartUnix("week", time.Date(2026, 11, 8, 9, 0, 0, 0, time.UTC))
	if startSun != want {
		t.Fatalf("week start on Sunday ผิด: got %d want %d", startSun, want)
	}
	mon := periodStartUnix("month", time.Date(2026, 11, 27, 23, 59, 0, 0, time.UTC))
	wantMon := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).Unix()
	if mon != wantMon {
		t.Fatalf("month start ผิด: got %d want %d", mon, wantMon)
	}
}

// HTTP: /predictions/leaderboard/week — cache MISS -> HIT + สถานะ period ครบ
func TestHandlePeriodLeaderboardHTTP(t *testing.T) {
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = nil }()

	r := gin.New()
	r.GET("/predictions/leaderboard/week", handlePeriodLeaderboard("week"))

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/predictions/leaderboard/week", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("HTTP ผิด: %d body=%s", w.Code, w.Body.String())
	}
	if c := w.Header().Get("X-Cache"); c != "MISS" {
		t.Fatalf("ครั้งแรกต้อง MISS: %q", c)
	}
	var out map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	if out["period"] != "week" {
		t.Fatalf("payload ต้องบอก period=week: %v", out["period"])
	}

	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/predictions/leaderboard/week", nil))
	if c := w2.Header().Get("X-Cache"); c != "HIT" {
		t.Fatalf("เรียกซ้ำต้อง HIT: %q", c)
	}
}
