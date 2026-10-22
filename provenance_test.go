package main

import (
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// ---------------------------------------------------------------------------
// แผนข้อ 3 — provenance: บอกที่มาของข้อมูล (แหล่ง + เวลาที่ดึง + อายุ)
// ---------------------------------------------------------------------------

func TestNoteProvenance_AndHeaders(t *testing.T) {
	provenance.mu.Lock()
	provenance.items = map[string]provItem{}
	provenance.mu.Unlock()

	key := "matches:20260101"
	noteProvenance(key, "goalapi")

	item, ok := provenanceOf(key)
	if !ok {
		t.Fatal("ต้องจำ provenance ของ key ได้")
	}
	if item.Source != "goalapi" {
		t.Errorf("source ผิด: %s", item.Source)
	}
	if time.Since(item.FetchedAt) > time.Minute {
		t.Errorf("fetchedAt ต้องเป็นเวลาปัจจุบัน ได้ %v", item.FetchedAt)
	}
}

func TestSetProvenanceHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("GET", "/api/matches", nil)

	fetchedAt := time.Now().Add(-90 * time.Second)
	setProvenanceHeaders(c, "fotmob", fetchedAt)

	if got := rec.Header().Get("X-Data-Source"); got != "fotmob" {
		t.Errorf("X-Data-Source ผิด: %s", got)
	}
	ts, err := strconv.ParseInt(rec.Header().Get("X-Data-Fetched-At"), 10, 64)
	if err != nil {
		t.Fatalf("X-Data-Fetched-At ไม่ใช่ unix ts: %q", rec.Header().Get("X-Data-Fetched-At"))
	}
	if diff := time.Since(time.Unix(ts, 0)) - 90*time.Second; diff > 5*time.Second || diff < -5*time.Second {
		t.Errorf("X-Data-Fetched-At ไม่ตรงกับเวลาที่ส่งมา (คลาดเคลื่อน %v)", diff)
	}
	age, err := strconv.Atoi(rec.Header().Get("X-Data-Age-Seconds"))
	if err != nil {
		t.Fatalf("X-Data-Age-Seconds ไม่ใช่ตัวเลข: %q", rec.Header().Get("X-Data-Age-Seconds"))
	}
	if age < 85 || age > 95 {
		t.Errorf("อายุข้อมูลควรราว 90 วิ ได้ %d", age)
	}
	if rec.Header().Get("X-Data-Stale") != "false" {
		t.Errorf("ข้อมูลอายุ 90 วิ (เกณฑ์ 900 วิ) ต้องไม่ stale ได้ %q", rec.Header().Get("X-Data-Stale"))
	}
}

func TestSetProvenanceHeaders_Stale(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("GET", "/api/matches", nil)

	// เก่ากว่าเกณฑ์ (900 วิ) -> ต้องบอก stale
	setProvenanceHeaders(c, "goalapi", time.Now().Add(-20*time.Minute))
	if rec.Header().Get("X-Data-Stale") != "true" {
		t.Errorf("ข้อมูลเก่า 20 นาที ต้อง stale=true ได้ %q", rec.Header().Get("X-Data-Stale"))
	}
}

func TestSetProvenanceHeadersForKey_UsesStoredItem(t *testing.T) {
	provenance.mu.Lock()
	provenance.items = map[string]provItem{}
	provenance.mu.Unlock()

	key := "match:900001"
	noteProvenance(key, "apifootball")
	// ย้อนเวลาให้เก่า 5 นาที
	provenance.mu.Lock()
	provenance.items[key] = provItem{Source: "apifootball", FetchedAt: time.Now().Add(-5 * time.Minute)}
	provenance.mu.Unlock()

	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("GET", "/api/match/900001", nil)

	setProvenanceHeadersForKey(c, key, "cache")
	if got := rec.Header().Get("X-Data-Source"); got != "apifootball" {
		t.Errorf("ต้องใช้แหล่งที่บันทึกไว้ ได้ %s", got)
	}
	age, _ := strconv.Atoi(rec.Header().Get("X-Data-Age-Seconds"))
	if age < 290 || age > 310 {
		t.Errorf("อายุต้องราว 300 วิ ได้ %d", age)
	}
}

// key ที่ไม่เคยบันทึก -> ใช้ fallback ที่ส่งมา
func TestSetProvenanceHeadersForKey_Fallback(t *testing.T) {
	provenance.mu.Lock()
	provenance.items = map[string]provItem{}
	provenance.mu.Unlock()

	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("GET", "/api/matches", nil)

	setProvenanceHeadersForKey(c, "ไม่เคยมี", "cache")
	if got := rec.Header().Get("X-Data-Source"); got != "cache" {
		t.Errorf("ต้องใช้ fallback ได้ %s", got)
	}
	if rec.Header().Get("X-Data-Age-Seconds") == "" {
		t.Error("ต้องมี X-Data-Age-Seconds เสมอ")
	}
}
