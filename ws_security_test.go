package main

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestIsAllowedHTTPRequestOrigin(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("ALLOWED_ORIGINS", "https://doball.live,https://www.doball.live")
	t.Setenv("ALLOW_VERCEL_PREVIEW", "false")

	cases := []struct {
		origin string
		want   bool
	}{
		{"https://doball.live", true},
		{"https://www.doball.live", true},
		{"https://DOBALL.LIVE", true},
		{"http://localhost:3000", true},
		{"http://127.0.0.1:5173", true},
		{"http://localhost", true},
		{"https://evil.com", false},
		{"https://doball.live.evil.com", false},
		{"", true}, // non-browser / same-process clients may omit Origin
	}

	for _, tc := range cases {
		if got := isAllowedHTTPRequestOrigin(tc.origin); got != tc.want {
			t.Fatalf("isAllowedHTTPRequestOrigin(%q)=%v want %v", tc.origin, got, tc.want)
		}
	}
}

func TestIsAllowedHTTPRequestOriginVercelPreview(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("ALLOWED_ORIGINS", "https://doball.live")
	t.Setenv("ALLOW_VERCEL_PREVIEW", "true")

	if !isAllowedHTTPRequestOrigin("https://my-preview.vercel.app") {
		t.Fatal("expected vercel preview origin allowed when ALLOW_VERCEL_PREVIEW=true")
	}
	if isAllowedHTTPRequestOrigin("https://evil.vercel.app.evil.com") {
		t.Fatal("unexpected allow for non-vercel suffix")
	}
}

func TestWSHandshakeConnectionLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("WS_MAX_CONNECTIONS", "2")

	// no clients registered -> allowed
	if wsAtConnectionLimit(0, 0) {
		t.Fatal("expected not at limit when total is 0")
	}
	if !wsAtConnectionLimit(1, 1) {
		t.Fatal("expected at limit when total equals max")
	}
	if !wsAtConnectionLimit(2, 1) {
		t.Fatal("expected at limit when total exceeds max")
	}
}

func TestWSHandshakeRejectsWhenAtLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("WS_MAX_CONNECTIONS", "0") // impossible to accept new
	r := gin.New()
	r.GET("/ws/live", handleWebSocket)

	req := httptest.NewRequest("GET", "/ws/live", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Origin", "http://localhost:3000")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != 503 {
		t.Fatalf("expected 503 when WS connection limit reached, got %d body=%s", rec.Code, rec.Body.String())
	}
}
