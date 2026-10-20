package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestHealthzOK — ทุก component พร้อม → 200 healthy
func TestHealthzOK(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// จำลอง GlobalDB (InitDB จริงแตะไฟล์ .env/จริง ไม่เหมาะใน test)
	prev := GlobalDB
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = prev }()

	r := gin.New()
	r.GET("/healthz", handleHealthz)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}

	var body struct {
		Status string                 `json:"status"`
		Checks map[string]interface{} `json:"checks"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if body.Status != "healthy" {
		t.Fatalf("expected healthy, got %s", body.Status)
	}
	for _, key := range []string{"db", "redis", "live_ws", "providers"} {
		if _, ok := body.Checks[key]; !ok {
			t.Fatalf("checks ไม่มี key %s", key)
		}
	}
}

// TestHealthzUnhealthy — GlobalDB nil → 503 unhealthy (พิสูจน์ว่าไม่ใช่แค่ตอบ 200 เสมอ)
func TestHealthzUnhealthy(t *testing.T) {
	gin.SetMode(gin.TestMode)

	prev := GlobalDB
	GlobalDB = nil
	defer func() { GlobalDB = prev }()

	r := gin.New()
	r.GET("/healthz", handleHealthz)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}

	var body struct {
		Status string `json:"status"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if body.Status != "unhealthy" {
		t.Fatalf("expected unhealthy, got %s", body.Status)
	}
}

// TestFlushNowPersistsData — shutdown path: FlushNow ต้องเขียนข้อมูลล่าสุดลงดิสก์จริง
// regression: ข้อมูล 3 วิท้ายหายตอน Render kill ก่อนหน้าที่จะมี graceful shutdown
func TestFlushNowPersistsData(t *testing.T) {
	db := newTestDB(t)

	// เขียนข้อมูลโดยไม่รอ syncWorker (newTestDB ไม่ได้ start goroutine)
	db.SaveFCMToken("token-flush-test-123")

	if err := db.FlushNow(); err != nil {
		t.Fatalf("FlushNow error: %v", err)
	}

	// ไฟล์ต้องมีอยู่จริงและมี token อยู่ข้างใน
	raw, err := os.ReadFile(db.filePath)
	if err != nil {
		t.Fatalf("อ่านไฟล์หลัง FlushNow ไม่ได้: %v", err)
	}
	var loaded StoreData
	if err := json.Unmarshal(raw, &loaded); err != nil {
		t.Fatalf("ไฟล์หลัง FlushNow ไม่ใช่ JSON ที่ถูกต้อง: %v", err)
	}
	if _, ok := loaded.FCMTokens["token-flush-test-123"]; !ok {
		t.Fatalf("FlushNow ไม่เขียนข้อมูลลงไฟล์ (tokens=%v)", loaded.FCMTokens)
	}
}
