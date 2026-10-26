package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func newRequestIDRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(requestIDMiddleware())
	r.GET("/t", func(c *gin.Context) {
		c.String(http.StatusOK, requestIDFrom(c))
	})
	return r
}

func TestRequestIDMiddlewareGenerates(t *testing.T) {
	r := newRequestIDRouter()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/t", nil))

	body := w.Body.String()
	if len(body) != 32 {
		t.Fatalf("expected 32-char hex id, got %q (%d)", body, len(body))
	}
	if got := w.Header().Get(requestIDHeader); got != body {
		t.Fatalf("response header %q != context id %q", got, body)
	}
}

func TestRequestIDMiddlewareKeepsValidIncoming(t *testing.T) {
	r := newRequestIDRouter()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/t", nil)
	req.Header.Set(requestIDHeader, "abc-123_DEF-456")
	r.ServeHTTP(w, req)

	if got := w.Body.String(); got != "abc-123_DEF-456" {
		t.Fatalf("expected incoming id preserved, got %q", got)
	}
}

func TestRequestIDMiddlewareReplacesInvalidIncoming(t *testing.T) {
	cases := []string{
		"short",                        // < 8
		"has space in it",              // invalid charset
		"bad<script>alert(1)</script>", // invalid charset
		string(make([]byte, 65)),       // > 64
	}
	for _, bad := range cases {
		r := newRequestIDRouter()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/t", nil)
		req.Header.Set(requestIDHeader, bad)
		r.ServeHTTP(w, req)
		if got := w.Body.String(); got == bad || len(got) != 32 {
			t.Fatalf("expected fresh 32-char id for %q, got %q", bad, got)
		}
	}
}

func TestValidRequestID(t *testing.T) {
	ok := []string{
		"aaaaaaaa", // 8 chars ok
		"550e8400-e29b-41d4-a716-446655440000",
		"ABC_123-xyz",
	}
	bad := []string{"", "short", "has space", "semi;colon", string(make([]byte, 65))}
	for _, s := range ok {
		if !validRequestID(s) {
			t.Fatalf("expected valid: %q", s)
		}
	}
	for _, s := range bad {
		if validRequestID(s) {
			t.Fatalf("expected invalid: %q", s)
		}
	}
}
