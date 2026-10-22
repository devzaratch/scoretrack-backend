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
// แผนข้อ 7 — Public API: API key auth + โควตาราย key
// ---------------------------------------------------------------------------

func setupAPIKeyTest(t *testing.T) {
	t.Helper()
	GlobalDB = newTestDB(t)
}

func apiKeyTestEngine() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")
	v1.GET("/status", handleAPIStatus)
	auth := v1.Group("", apiKeyAuth())
	auth.GET("/matches", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	return r
}

func TestGenerateAPIKey_Format(t *testing.T) {
	key, err := generateAPIKey()
	if err != nil {
		t.Fatalf("สร้าง key ไม่สำเร็จ: %v", err)
	}
	if !strings.HasPrefix(key, apiKeyIDPrefix) {
		t.Errorf("key ต้องขึ้นต้นด้วย %s ได้ %s", apiKeyIDPrefix, key)
	}
	if len(key) < 20 {
		t.Errorf("key สั้นเกินไป: %s", key)
	}
	other, _ := generateAPIKey()
	if other == key {
		t.Error("key ต้องไม่ซ้ำกัน")
	}
}

// ไม่มี key -> 401
func TestAPIKeyAuth_MissingKey(t *testing.T) {
	setupAPIKeyTest(t)
	defer func() { GlobalDB = nil }()

	r := apiKeyTestEngine()
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/matches", nil))

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("ควรได้ 401 ได้ %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "missing api key") {
		t.Errorf("ข้อความผิด: %s", rec.Body.String())
	}
}

// key ผิด -> 401
func TestAPIKeyAuth_InvalidKey(t *testing.T) {
	setupAPIKeyTest(t)
	defer func() { GlobalDB = nil }()

	r := apiKeyTestEngine()
	req := httptest.NewRequest("GET", "/api/v1/matches", nil)
	req.Header.Set("X-API-Key", "sk_live_not-a-real-key")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("ควรได้ 401 ได้ %d", rec.Code)
	}
}

// key ถูก -> 200 + มี header โควตา + นับการใช้
func TestAPIKeyAuth_ValidKey(t *testing.T) {
	setupAPIKeyTest(t)
	defer func() { GlobalDB = nil }()

	key, _ := generateAPIKey()
	GlobalDB.SaveAPIKey(key, APIKeyRecord{
		Name: "test", Enabled: true, DailyLimit: 100, MinuteLimit: 60,
		DayKey: time.Now().Format("2006-01-02"),
	})

	r := apiKeyTestEngine()
	req := httptest.NewRequest("GET", "/api/v1/matches", nil)
	req.Header.Set("Authorization", "Bearer "+key)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ควรได้ 200 ได้ %d (%s)", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-RateLimit-Limit") != "100" {
		t.Errorf("ขาด header โควตา: %v", rec.Header())
	}
	if rec.Header().Get("X-RateLimit-Remaining") != "99" {
		t.Errorf("remaining ผิด: %s", rec.Header().Get("X-RateLimit-Remaining"))
	}

	// ตัวนับถูกอัปเดต
	rec2, ok := GlobalDB.GetAPIKey(key)
	if !ok {
		t.Fatal("key ต้องยังอยู่")
	}
	if rec2.CallsToday != 1 {
		t.Errorf("calls_today ผิด: %d", rec2.CallsToday)
	}
	if rec2.LastUsedAt == 0 {
		t.Error("ต้องบันทึก last_used_at")
	}
}

// key ถูกปิดใช้ -> 403
func TestAPIKeyAuth_DisabledKey(t *testing.T) {
	setupAPIKeyTest(t)
	defer func() { GlobalDB = nil }()

	key, _ := generateAPIKey()
	GlobalDB.SaveAPIKey(key, APIKeyRecord{Name: "off", Enabled: false, DailyLimit: 100})

	r := apiKeyTestEngine()
	req := httptest.NewRequest("GET", "/api/v1/matches", nil)
	req.Header.Set("X-API-Key", key)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("ควรได้ 403 ได้ %d", rec.Code)
	}
}

// โควตาวันหมด -> 429 + Retry-After
func TestAPIKeyAuth_DailyQuotaExceeded(t *testing.T) {
	setupAPIKeyTest(t)
	defer func() { GlobalDB = nil }()

	key, _ := generateAPIKey()
	GlobalDB.SaveAPIKey(key, APIKeyRecord{
		Name: "full", Enabled: true, DailyLimit: 10, MinuteLimit: 60,
		DayKey: time.Now().Format("2006-01-02"), CallsToday: 10,
	})

	r := apiKeyTestEngine()
	req := httptest.NewRequest("GET", "/api/v1/matches", nil)
	req.Header.Set("X-API-Key", key)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("ควรได้ 429 ได้ %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("ต้องมี Retry-After")
	}
	if !strings.Contains(rec.Body.String(), "daily quota") {
		t.Errorf("ข้อความผิด: %s", rec.Body.String())
	}
}

// โควตาต่อนาทีหมด -> 429
func TestAPIKeyAuth_MinuteQuotaExceeded(t *testing.T) {
	setupAPIKeyTest(t)
	defer func() { GlobalDB = nil }()

	key, _ := generateAPIKey()
	GlobalDB.SaveAPIKey(key, APIKeyRecord{
		Name: "fast", Enabled: true, DailyLimit: 1000, MinuteLimit: 5,
		DayKey: time.Now().Format("2006-01-02"), MinuteCalls: 5, MinuteWindow: 9999999999,
	})

	r := apiKeyTestEngine()
	req := httptest.NewRequest("GET", "/api/v1/matches", nil)
	req.Header.Set("X-API-Key", key)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("ควรได้ 429 ได้ %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "ต่อนาที") {
		t.Errorf("ข้อความผิด: %s", rec.Body.String())
	}
}

// admin: สร้าง key -> ใช้ได้; เพิกถอน -> ใช้ไม่ได้
func TestAPIKeyLifecycle(t *testing.T) {
	setupAPIKeyTest(t)
	defer func() { GlobalDB = nil }()

	gin.SetMode(gin.TestMode)
	admin := gin.New()
	admin.POST("/api/admin/api-keys", handleAPIKeyCreate)
	admin.GET("/api/admin/api-keys", handleAPIKeyList)
	admin.DELETE("/api/admin/api-keys/:key", handleAPIKeyRevoke)

	// สร้าง
	body := strings.NewReader(`{"name":"partner-app","owner":"acme","daily_limit":50}`)
	req := httptest.NewRequest("POST", "/api/admin/api-keys", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("สร้าง key ควรได้ 201 ได้ %d (%s)", rec.Code, rec.Body.String())
	}
	var created map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("response ไม่ใช่ JSON: %v", err)
	}
	key, _ := created["key"].(string)
	if !strings.HasPrefix(key, apiKeyIDPrefix) {
		t.Fatalf("key ที่ได้ผิดรูป: %v", created["key"])
	}

	// ใช้ได้จริง
	r := apiKeyTestEngine()
	useReq := httptest.NewRequest("GET", "/api/v1/matches", nil)
	useReq.Header.Set("X-API-Key", key)
	useRec := httptest.NewRecorder()
	r.ServeHTTP(useRec, useReq)
	if useRec.Code != http.StatusOK {
		t.Fatalf("key ใหม่ต้องใช้ได้ ได้ %d", useRec.Code)
	}

	// เพิกถอน
	delReq := httptest.NewRequest("DELETE", "/api/admin/api-keys/"+key, nil)
	delRec := httptest.NewRecorder()
	admin.ServeHTTP(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("เพิกถอนควรได้ 200 ได้ %d", delRec.Code)
	}

	// ใช้ไม่ได้แล้ว
	useRec2 := httptest.NewRecorder()
	r.ServeHTTP(useRec2, useReq)
	if useRec2.Code != http.StatusUnauthorized {
		t.Errorf("key ที่เพิกถอนต้องใช้ไม่ได้ ได้ %d", useRec2.Code)
	}
}

// status ไม่ต้องใช้ key
func TestHandleAPIStatus(t *testing.T) {
	setupAPIKeyTest(t)
	defer func() { GlobalDB = nil }()

	r := apiKeyTestEngine()
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/status", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("ควรได้ 200 ได้ %d", rec.Code)
	}
	var body map[string]interface{}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body["version"] != "v1" {
		t.Errorf("version ผิด: %v", body["version"])
	}
	if body["status"] != "ok" {
		t.Errorf("status ผิด: %v", body["status"])
	}
}
