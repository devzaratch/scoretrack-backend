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
// แผนข้อ 10 — PDPA: สิทธิเข้าถึง/ลบข้อมูลส่วนบุคคล
// ---------------------------------------------------------------------------

func setupAccountTest(t *testing.T) {
	t.Helper()
	GlobalDB = newTestDB(t)
	GlobalDB.SaveUserFavorites("user:g:me", []int{900001, 900002})
	GlobalDB.SavePrediction("user:g:me", "900001", map[string]interface{}{
		"home": 2, "away": 1, "createdAt": "2026-01-01T00:00:00Z",
	})
	GlobalDB.SaveNotificationPrefs("user:g:me", map[string]interface{}{"goals": true})
	GlobalDB.SaveUserSession("tok-me", "me")

	// ผู้ใช้คนอื่น (ต้องไม่ถูกแตะ)
	GlobalDB.SaveUserFavorites("user:g:other", []int{900003})
	GlobalDB.SavePrediction("user:g:other", "900002", map[string]interface{}{"home": 0, "away": 0})
}

func accountTestEngine() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/user/data", handleUserDataExport)
	r.DELETE("/api/user/account", handleDeleteAccount)
	return r
}

// export ต้องคืนข้อมูลของผู้ใช้คนนี้ (favorites/predictions/prefs/achievements)
func TestHandleUserDataExport(t *testing.T) {
	setupAccountTest(t)
	defer func() { GlobalDB = nil }()

	r := accountTestEngine()
	req := httptest.NewRequest("GET", "/api/user/data", nil)
	req.Header.Set("Authorization", "Bearer tok-me")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ควรได้ 200 ได้ %d", rec.Code)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response ไม่ใช่ JSON: %v", err)
	}
	if body["user_key"] != "user:g:me" {
		t.Errorf("user_key ผิด: %v", body["user_key"])
	}
	favs, _ := body["favorites"].([]interface{})
	if len(favs) != 2 {
		t.Errorf("favorites ต้องมี 2 รายการ ได้ %v", body["favorites"])
	}
	preds, _ := body["predictions"].(map[string]interface{})
	if len(preds) != 1 {
		t.Errorf("predictions ต้องมี 1 แมตช์ ได้ %v", body["predictions"])
	}
	if _, ok := body["notification_prefs"]; !ok {
		t.Error("ต้องมี notification_prefs ใน export")
	}
	if body["exported_at"] == nil {
		t.Error("ต้องมี exported_at")
	}
}

// ลบข้อมูล: ของตัวเองหาย, ของคนอื่นยังอยู่, session ของตัวเองถูกลบ
func TestHandleDeleteAccount(t *testing.T) {
	setupAccountTest(t)
	defer func() { GlobalDB = nil }()

	r := accountTestEngine()
	req := httptest.NewRequest("DELETE", "/api/user/account", nil)
	req.Header.Set("Authorization", "Bearer tok-me")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ควรได้ 200 ได้ %d (%s)", rec.Code, rec.Body.String())
	}
	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response ไม่ใช่ JSON: %v", err)
	}
	if body["status"] != "deleted" {
		t.Errorf("status ผิด: %v", body["status"])
	}

	// ของตัวเองหายไป
	if favs := GlobalDB.GetUserFavorites("user:g:me"); len(favs) != 0 {
		t.Errorf("favorites ของตัวเองต้องถูกลบ ได้ %v", favs)
	}
	if preds := GlobalDB.GetPredictionsByUser("user:g:me"); len(preds) != 0 {
		t.Errorf("predictions ของตัวเองต้องถูกลบ ได้ %v", preds)
	}
	if sub := GlobalDB.GetSessionSub("tok-me"); sub != "" {
		t.Errorf("session ของตัวเองต้องถูกลบ ได้ %q", sub)
	}

	// ของคนอื่นยังอยู่
	if favs := GlobalDB.GetUserFavorites("user:g:other"); len(favs) != 1 {
		t.Errorf("ข้อมูลผู้ใช้คนอื่นต้องไม่ถูกแตะ ได้ %v", favs)
	}
	if preds := GlobalDB.GetPredictionsByUser("user:g:other"); len(preds) != 1 {
		t.Errorf("predictions ของคนอื่นต้องไม่ถูกแตะ ได้ %v", preds)
	}
}

// ผู้ใช้ที่ล็อกอินผ่าน cookie ก็ลบได้ (ไม่ต้องส่ง Bearer)
func TestHandleDeleteAccount_ViaCookie(t *testing.T) {
	setupAccountTest(t)
	defer func() { GlobalDB = nil }()

	r := accountTestEngine()
	req := httptest.NewRequest("DELETE", "/api/user/account", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "tok-me"})
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ควรได้ 200 ได้ %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "user:g:me") {
		t.Errorf("ต้องลบของ user ที่ login อยู่ ได้ %s", rec.Body.String())
	}
	if favs := GlobalDB.GetUserFavorites("user:g:me"); len(favs) != 0 {
		t.Errorf("favorites ต้องถูกลบ ได้ %v", favs)
	}
}

// ผู้ใช้ที่ไม่ล็อกอิน (device key) ลบของตัวเองได้
func TestHandleDeleteAccount_DeviceKey(t *testing.T) {
	setupAccountTest(t)
	defer func() { GlobalDB = nil }()

	r := accountTestEngine()
	req := httptest.NewRequest("DELETE", "/api/user/account", nil)
	req.Header.Set("X-Device-ID", "device-abc")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ควรได้ 200 ได้ %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "device:device-abc") {
		t.Errorf("ควรลบของ device key ได้ %s", rec.Body.String())
	}
}
