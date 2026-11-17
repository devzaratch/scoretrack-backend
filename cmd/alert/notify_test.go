package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newTestConfig — config ที่ยิงไป httptest เท่านั้น ปิด backoff เพื่อให้เทสต์เร็ว
func newTestConfig(t *testing.T, srv *httptest.Server) Config {
	t.Helper()
	return Config{
		TelegramToken:  "123:FAKE-TOKEN-abc",
		TelegramChatID: "42",
		Timeout:        2_000_000_000, // 2s
		Attempts:       3,
		Backoff:        0,
		HTTPClient:     srv.Client(),
	}
}

func TestSendTelegramPayload(t *testing.T) {
	var gotPath, gotChat, gotText string
	var gotCalls int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCalls++
		gotPath = r.URL.Path
		var body struct {
			ChatID string `json:"chat_id"`
			Text   string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		gotChat, gotText = body.ChatID, body.Text
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	cfg := newTestConfig(t, srv)
	cfg.TelegramBase = srv.URL
	cfg.WebhookURL = ""

	var log bytes.Buffer
	cfg.Log = &log

	msg := Message{Title: "🔴 backend ไม่ตอบ", Body: "retry ครบ 5 ครั้ง", Level: LevelDown}
	if err := cfg.Send(context.Background(), msg); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if gotCalls != 1 {
		t.Errorf("calls = %d, want 1", gotCalls)
	}
	if gotPath != "/bot123:FAKE-TOKEN-abc/sendMessage" {
		t.Errorf("path = %q", gotPath)
	}
	if gotChat != "42" {
		t.Errorf("chat_id = %q", gotChat)
	}
	for _, want := range []string{"backend ไม่ตอบ", "retry ครบ 5 ครั้ง"} {
		if !strings.Contains(gotText, want) {
			t.Errorf("text %q missing %q", gotText, want)
		}
	}
	if strings.Contains(log.String(), "FAKE-TOKEN") {
		t.Errorf("log leaked token: %q", log.String())
	}
}

func TestSendWebhookPayload(t *testing.T) {
	var got Message
	var gotCalls int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCalls++
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("content-type = %q", ct)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := Config{
		WebhookURL: srv.URL,
		Source:     "uptime",
		Timeout:    2_000_000_000,
		Attempts:   1,
		HTTPClient: srv.Client(),
	}
	msg := Message{Title: "✅ กลับมาออนไลน์", Body: "backend", Level: LevelRecovery}
	if err := cfg.Send(context.Background(), msg); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if gotCalls != 1 {
		t.Errorf("calls = %d, want 1", gotCalls)
	}
	if got.Title != msg.Title || got.Body != msg.Body || got.Level != msg.Level {
		t.Errorf("payload = %+v, want %+v", got, msg)
	}
}

func TestSendBothChannels(t *testing.T) {
	var telegram, webhook int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/bot") {
			telegram++
		} else {
			webhook++
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	cfg := newTestConfig(t, srv)
	cfg.TelegramBase = srv.URL
	cfg.WebhookURL = srv.URL + "/hook"
	if err := cfg.Send(context.Background(), Message{Title: "t", Body: "b"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if telegram != 1 || webhook != 1 {
		t.Errorf("telegram=%d webhook=%d, want 1/1", telegram, webhook)
	}
}

func TestSendRetriesOnServerError(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	cfg := newTestConfig(t, srv)
	cfg.TelegramBase = srv.URL
	if err := cfg.Send(context.Background(), Message{Title: "t"}); err != nil {
		t.Fatalf("Send should succeed on 3rd attempt: %v", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestSendGivesUpAfterAttempts(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := newTestConfig(t, srv)
	cfg.TelegramBase = srv.URL
	err := cfg.Send(context.Background(), Message{Title: "t"})
	if err == nil {
		t.Fatal("want error after all attempts fail")
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
	if !strings.Contains(err.Error(), "telegram") {
		t.Errorf("error should name the channel: %v", err)
	}
	if strings.Contains(err.Error(), "FAKE-TOKEN") {
		t.Errorf("error leaked token: %v", err)
	}
}

func TestSendDoesNotRetryOn4xx(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	cfg := newTestConfig(t, srv)
	cfg.TelegramBase = srv.URL
	if err := cfg.Send(context.Background(), Message{Title: "t"}); err == nil {
		t.Fatal("401 must be an error")
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1 (4xx ไม่ต้อง retry)", calls)
	}
}

func TestSendSkipsWhenNothingConfigured(t *testing.T) {
	cfg := Config{Timeout: 2_000_000_000, Attempts: 3}
	var log bytes.Buffer
	cfg.Log = &log

	if cfg.Enabled() {
		t.Error("Enabled() = true, want false")
	}
	if err := cfg.Send(context.Background(), Message{Title: "t"}); err != nil {
		t.Fatalf("Send on unconfigured must be no-op, got %v", err)
	}
	if !strings.Contains(log.String(), "skip") {
		t.Errorf("log should say skip: %q", log.String())
	}
}

func TestSendReportsEveryFailedChannel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := newTestConfig(t, srv)
	cfg.TelegramBase = srv.URL
	cfg.WebhookURL = srv.URL + "/hook"
	err := cfg.Send(context.Background(), Message{Title: "t"})
	if err == nil {
		t.Fatal("want error")
	}
	for _, want := range []string{"telegram", "webhook"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing channel %q", err, want)
		}
	}
}

func TestMessageTextFormatting(t *testing.T) {
	cases := []struct {
		level string
		icon  string
	}{
		{LevelDown, "🔴"},
		{LevelRecovery, "✅"},
		{LevelInfo, "ℹ️"},
	}
	for _, c := range cases {
		text := Message{Title: "หัวข้อ", Body: "รายละเอียด", Level: c.level}.Text()
		if !strings.HasPrefix(text, c.icon) {
			t.Errorf("level %q text = %q, want prefix %q", c.level, text, c.icon)
		}
		if !strings.Contains(text, "หัวข้อ") || !strings.Contains(text, "รายละเอียด") {
			t.Errorf("level %q text missing title/body: %q", c.level, text)
		}
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	env := map[string]string{
		"ALERT_TELEGRAM_TOKEN":   "tok",
		"ALERT_TELEGRAM_CHAT_ID": "7",
		"ALERT_WEBHOOK_URL":      "https://example.test/hook",
		"ALERT_SOURCE":           "uptime",
	}
	cfg := loadConfig(func(k string) string { return env[k] })
	if !cfg.Enabled() {
		t.Fatal("Enabled() = false, want true")
	}
	if cfg.TelegramToken != "tok" || cfg.TelegramChatID != "7" {
		t.Errorf("telegram config = %q/%q", cfg.TelegramToken, cfg.TelegramChatID)
	}
	if cfg.WebhookURL != "https://example.test/hook" || cfg.Source != "uptime" {
		t.Errorf("webhook config = %q/%q", cfg.WebhookURL, cfg.Source)
	}
	if cfg.Attempts < 1 || cfg.Timeout <= 0 {
		t.Errorf("defaults wrong: attempts=%d timeout=%v", cfg.Attempts, cfg.Timeout)
	}
	if cfg.TelegramBase != defaultTelegramBase {
		t.Errorf("TelegramBase = %q", cfg.TelegramBase)
	}
}

// Telegram ต้องมีทั้ง token และ chat id ถึงจะถือว่าเปิดใช้
func TestTelegramNeedsBothValues(t *testing.T) {
	cfg := loadConfig(func(k string) string {
		if k == "ALERT_TELEGRAM_TOKEN" {
			return "tok"
		}
		return ""
	})
	if cfg.Enabled() {
		t.Error("token without chat id must not enable telegram")
	}
	if err := cfg.Send(context.Background(), Message{Title: "t"}); err != nil {
		t.Fatalf("Send without any channel must be no-op: %v", err)
	}
}

func TestRunExitCodes(t *testing.T) {
	empty := func(string) string { return "" }
	var out bytes.Buffer

	code := run([]string{"-title", "t", "-body", "b"}, empty, &out)
	if code != 0 {
		t.Errorf("unconfigured non-strict exit = %d, want 0", code)
	}

	out.Reset()
	code = run([]string{"-title", "t", "-strict"}, empty, &out)
	if code != 1 {
		t.Errorf("unconfigured strict exit = %d, want 1", code)
	}
	if !strings.Contains(out.String(), "ALERT_TELEGRAM_TOKEN") {
		t.Errorf("strict message should list what to set: %q", out.String())
	}

	out.Reset()
	code = run([]string{}, empty, &out)
	if code != 2 {
		t.Errorf("missing -title exit = %d, want 2", code)
	}
}
