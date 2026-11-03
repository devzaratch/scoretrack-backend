package main

// admin_alert_test.go — ทดสอบ admin alert: transition logic (pure) + webhook + fallback gating

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAdminWatchdogStep(t *testing.T) {
	cases := []struct {
		name          string
		prevUnhealthy bool
		first         bool
		nowHealthy    bool
		wantFire      bool
		wantHasCrash  bool
	}{
		{"first-degraded-start", false, true, false, true, true},
		{"first-ok-start", false, true, true, false, false},
		{"ok->broken fires", false, false, false, true, true},
		{"still-broken silent", true, false, false, false, false},
		{"broken->ok fires recovery", true, false, true, true, false},
		{"still-ok silent", false, false, true, false, false},
	}
	for _, tc := range cases {
		fire, title := adminWatchdogStep(tc.prevUnhealthy, tc.first, tc.nowHealthy)
		if fire != tc.wantFire {
			t.Fatalf("%s: fire=%v, want %v (title=%q)", tc.name, fire, tc.wantFire, title)
		}
		if tc.wantFire && tc.wantHasCrash && title != "🚨 ScoreTrack ผิดปกติ" {
			t.Fatalf("%s: title ควรเป็น crash alert: %q", tc.name, title)
		}
		if tc.wantFire && !tc.wantHasCrash && title != "✅ ScoreTrack กลับมาปกติ" {
			t.Fatalf("%s: title ควรเป็น recovery: %q", tc.name, title)
		}
	}
}

func TestFireAdminAlertWebhook(t *testing.T) {
	var gotPayload map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotPayload)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	t.Setenv("ADMIN_ALERT_WEBHOOK_URL", srv.URL)

	fireAdminAlert("🚨", "detail")

	if gotPayload["title"] != "🚨" || gotPayload["body"] != "detail" {
		t.Fatalf("webhook payload ผิด: %v", gotPayload)
	}
}

func TestFireAdminAlertFallsBackToLINENotify(t *testing.T) {
	// ไม่ตั้ง webhook, ตั้ง LINE token → เรียก LINE path (broadcast endpoint)
	var hit bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/bot/message/broadcast" {
			hit = true
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	t.Setenv("ADMIN_ALERT_WEBHOOK_URL", "")
	t.Setenv("LINE_CHANNEL_ACCESS_TOKEN", "tok")
	orig := lineAPIBase
	lineAPIBase = srv.URL
	defer func() { lineAPIBase = orig }()

	fireAdminAlert("🚨", "d")
	if !hit {
		t.Fatal("ไม่ตั้ง webhook ต้อง fallback ไป LINE broadcast")
	}
}

func TestGetEnvInt(t *testing.T) {
	t.Setenv("X_SECONDS", "120")
	t.Setenv("X_BAD", "abc")
	if v := getEnvInt("X_SECONDS", 60); v != 120 {
		t.Fatalf("getEnvInt parse 120 ล้มเหลว: %d", v)
	}
	if v := getEnvInt("X_BAD", 60); v != 60 {
		t.Fatalf("getEnvInt on 'abc' ต้องคืน fallback: %d", v)
	}
	if v := getEnvInt("X_MISSING", 60); v != 60 {
		t.Fatalf("getEnvInt missing ต้องคืน fallback: %d", v)
	}
}

// เช็คว่า watchdog ที่ปิด (ไม่ตั้ง env ไหน) ต้อง return เร็ว ไม่ spawned goroutine ยิงไร
func TestStartAdminAlertWatchdogDisabled(t *testing.T) {
	t.Setenv("ADMIN_ALERT_WEBHOOK_URL", "")
	t.Setenv("LINE_CHANNEL_ACCESS_TOKEN", "")
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	startAdminAlertWatchdog(ctx, 50*time.Millisecond) // ต้อง exit เงียบ ๆ
}
