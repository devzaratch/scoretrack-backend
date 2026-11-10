package main

// metrics_action_test.go — แผนข้อ 10: product metrics (action:*)
// ยืนยันว่า handleMetrics รับเฉพาะ action ใน whitelist แล้วนับถูก hash เดียว
// + admin endpoint คืนส่วน "actions" จริง (ทั้ง Redis path และ in-memory)

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func actionTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/metrics", handleMetrics)
	r.GET("/admin/metrics", handleAdminMetrics)
	return r
}

func TestActionMetric_KnownWhitelisted(t *testing.T) {
	r := actionTestRouter()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/metrics", strings.NewReader(`{"name":"action:follow_team","value":1}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("want 204, got %d body=%s", w.Code, w.Body.String())
	}
	metricsMu.Lock()
	if memActions["follow_team"] == nil || *memActions["follow_team"] < 1 {
		t.Fatal("action:follow_team ไม่ถูกนับใน memActions")
	}
	metricsMu.Unlock()
}

func TestActionMetric_UnknownRejected(t *testing.T) {
	r := actionTestRouter()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/metrics", strings.NewReader(`{"name":"action:random_made_up","value":1}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("want 422, got %d", w.Code)
	}
}

func TestAdminMetrics_ExposesActions(t *testing.T) {
	r := actionTestRouter()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/metrics", strings.NewReader(`{"name":"action:search_submit","value":1}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("seed fail: %d", w.Code)
	}

	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/admin/metrics", nil))
	if w2.Code != http.StatusOK {
		t.Fatalf("admin: want 200, got %d", w2.Code)
	}
	if !strings.Contains(w2.Body.String(), `"actions"`) || !strings.Contains(w2.Body.String(), `"search_submit":1`) {
		t.Fatalf("ต้องมี section actions พร้อมยอด search_submit: %s", w2.Body.String())
	}
}
