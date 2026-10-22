package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// ---------------------------------------------------------------------------
// แผนข้อ 4 — ความสนใจของผู้ใช้ (ติดตามทีม/ลีก)
// ---------------------------------------------------------------------------

func TestNormalizeInterestKey(t *testing.T) {
	if got := normalizeInterestKey("League", " 47 "); got != "league:47" {
		t.Errorf("key ผิด: %q", got)
	}
	if got := normalizeInterestKey("team", "Arsenal FC"); got != "team:arsenal fc" {
		t.Errorf("key ผิด: %q", got)
	}
	if got := normalizeInterestKey("player", "10"); got != "" {
		t.Errorf("kind อื่นต้องไม่ได้: %q", got)
	}
	if got := normalizeInterestKey("", "10"); got != "" {
		t.Errorf("kind ว่างต้องไม่ได้: %q", got)
	}
}

func TestSetUserInterest_AddRemove(t *testing.T) {
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = nil }()

	keys := GlobalDB.SetUserInterest("user:g:x", "team:arsenal fc", true)
	if len(keys) != 1 || keys[0] != "team:arsenal fc" {
		t.Fatalf("ติดตามแล้วผิด: %v", keys)
	}
	keys = GlobalDB.SetUserInterest("user:g:x", "league:47", true)
	if len(keys) != 2 {
		t.Errorf("ควรมี 2 รายการ ได้ %v", keys)
	}
	// เรียงคงที่
	if keys[0] != "league:47" {
		t.Errorf("ควรเรียงตามตัวอักษร ได้ %v", keys)
	}
	// เลิกติดตาม
	keys = GlobalDB.SetUserInterest("user:g:x", "team:arsenal fc", false)
	if len(keys) != 1 || keys[0] != "league:47" {
		t.Errorf("เลิกติดตามผิด: %v", keys)
	}
	// ผู้อื่นไม่ถูกแตะ
	if got := GlobalDB.GetUserInterests("user:g:other"); len(got) != 0 {
		t.Errorf("ของผู้อื่นต้องว่าง ได้ %v", got)
	}
}

func TestHandleUserInterests_ToggleAndList(t *testing.T) {
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = nil }()

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/user/interests", handleUserInterests)
	r.POST("/api/user/interests", handleUserInterestToggle)

	// ติดตามทีม
	body := strings.NewReader(`{"kind":"team","id":"Arsenal FC","follow":true}`)
	req := httptest.NewRequest("POST", "/api/user/interests", body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer tok-x")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ควรได้ 200 ได้ %d (%s)", rec.Code, rec.Body.String())
	}

	// อ่านกลับ
	req2 := httptest.NewRequest("GET", "/api/user/interests", nil)
	req2.Header.Set("Authorization", "Bearer tok-x")
	rec2 := httptest.NewRecorder()
	r.ServeHTTP(rec2, req2)

	var out map[string]interface{}
	if err := json.Unmarshal(rec2.Body.Bytes(), &out); err != nil {
		t.Fatalf("response ไม่ใช่ JSON: %v", err)
	}
	teams, _ := out["teams"].([]interface{})
	if len(teams) != 1 || teams[0] != "arsenal fc" {
		t.Errorf("teams ผิด: %v", out["teams"])
	}
	leagues, _ := out["leagues"].([]interface{})
	if len(leagues) != 0 {
		t.Errorf("leagues ต้องว่าง ได้ %v", out["leagues"])
	}
}

func TestHandleUserInterestToggle_InvalidKind(t *testing.T) {
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = nil }()

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/user/interests", handleUserInterestToggle)

	body := strings.NewReader(`{"kind":"player","id":"10","follow":true}`)
	req := httptest.NewRequest("POST", "/api/user/interests", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("kind ผิดต้องได้ 400 ได้ %d", rec.Code)
	}
}
