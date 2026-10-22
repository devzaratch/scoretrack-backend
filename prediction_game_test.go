package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// ---------------------------------------------------------------------------
// แผนข้อ 5 — Prediction 2.0: ปิดรอบทาย + streak/สถิติ
// ---------------------------------------------------------------------------

// แมตช์ที่ยังไม่เริ่ม (NS) — ทายได้
func TestMatchHasStarted_NotStarted(t *testing.T) {
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = nil }()

	body := `{"header":{"status":{"started":false,"finished":false}},"status":"NS"}`
	cacheSet("match:900001", []byte(body), time.Minute)

	started, status := matchHasStarted("900001")
	if started {
		t.Errorf("แมตช์ NS ต้องยังทายได้ ได้ started=%v status=%s", started, status)
	}
}

// แมตช์ที่เริ่มแล้ว — ทายไม่ได้
func TestMatchHasStarted_Started(t *testing.T) {
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = nil }()

	cacheSet("match:900002", []byte(`{"header":{"status":{"started":true}},"status":"LIVE"}`), time.Minute)
	started, _ := matchHasStarted("900002")
	if !started {
		t.Error("แมตช์ที่เริ่มแล้วต้องปิดรอบทาย")
	}
}

// ไม่มีข้อมูลใน cache -> ยังไม่เริ่ม (กันบล็อกทายผิด)
func TestMatchHasStarted_NoCache(t *testing.T) {
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = nil }()

	started, _ := matchHasStarted("ไม่มีข้อมูล")
	if started {
		t.Error("ไม่มีข้อมูลควรปล่อยให้ทายได้")
	}
}

// บันทึกทายผลเมื่อแมตช์เริ่มแล้ว -> 400 + closed=true
func TestHandleSavePrediction_ClosedRound(t *testing.T) {
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = nil }()

	cacheSet("match:900002", []byte(`{"header":{"status":{"started":true}},"status":"LIVE"}`), time.Minute)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/predictions/save", handleSavePrediction)

	body := strings.NewReader(`{"match_id":"900002","prediction":{"home":2,"away":1}}`)
	req := httptest.NewRequest("POST", "/api/predictions/save", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("ควรได้ 400 ได้ %d (%s)", rec.Code, rec.Body.String())
	}
	var out map[string]interface{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if out["closed"] != true {
		t.Errorf("ต้องบอกว่าปิดรอบแล้ว ได้ %v", out)
	}
}

// ยังไม่เริ่ม -> บันทึกได้
func TestHandleSavePrediction_OpenRound(t *testing.T) {
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = nil }()

	cacheSet("match:900001", []byte(`{"header":{"status":{"started":false}},"status":"NS"}`), time.Minute)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/predictions/save", handleSavePrediction)

	body := strings.NewReader(`{"match_id":"900001","prediction":{"home":2,"away":1,"createdAt":"2026-01-01T00:00:00Z"}}`)
	req := httptest.NewRequest("POST", "/api/predictions/save", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ควรได้ 200 ได้ %d (%s)", rec.Code, rec.Body.String())
	}
}

// streak: ทายถูกติดกัน 2 ครั้งล่าสุด = 2
func TestUserPoints_Streak(t *testing.T) {
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = nil }()

	// ผลแมตช์จบแล้ว (cache "matchresult:<id>") เพื่อให้คิดคะแนนได้
	for _, id := range []string{"m1", "m2", "m3"} {
		cacheSet("matchresult:"+id, []byte(`{"home":2,"away":1,"finished":true}`), time.Hour)
	}
	// ทาย: m1 ผิด (0), m2 ถูกผล (1), m3 ถูกเป๊ะ (3) — เรียงเวลาทาย
	GlobalDB.SavePrediction("user:g:s", "m1", map[string]interface{}{
		"home": 0, "away": 0, "createdAt": "2026-01-01T10:00:00Z",
	})
	GlobalDB.SavePrediction("user:g:s", "m2", map[string]interface{}{
		"home": 2, "away": 0, "createdAt": "2026-01-01T11:00:00Z",
	})
	GlobalDB.SavePrediction("user:g:s", "m3", map[string]interface{}{
		"home": 2, "away": 1, "createdAt": "2026-01-01T12:00:00Z",
	})

	stats := userPoints("user:g:s")
	if stats["points"] != 4 {
		t.Errorf("คะแนนต้องเป็น 4 ได้ %v", stats["points"])
	}
	if stats["streak"] != 2 {
		t.Errorf("streak ต้องเป็น 2 ได้ %v", stats["streak"])
	}
	if stats["exact_hits"] != 1 {
		t.Errorf("ถูกเป๊ะต้องเป็น 1 ได้ %v", stats["exact_hits"])
	}
	if stats["correct_hits"] != 1 {
		t.Errorf("ถูกผลต้องเป็น 1 ได้ %v", stats["correct_hits"])
	}
	if stats["wrong"] != 1 {
		t.Errorf("ผิดต้องเป็น 1 ได้ %v", stats["wrong"])
	}
	if stats["predictions"] != 3 {
		t.Errorf("ทายทั้งหมดต้องเป็น 3 ได้ %v", stats["predictions"])
	}
}
