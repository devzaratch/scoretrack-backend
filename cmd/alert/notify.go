// Package main (cmd/alert) — ตัวส่งแจ้งเตือนจากภายนอกระบบ
//
// ทำไมต้องแยกออกมาเป็น command:
//   - alert ในตัว backend (slo.go / admin_alert.go) ตายพร้อม process — แจ้งไม่ได้
//     ตอนที่ต้องแจ้งที่สุด (backend ล่ม) จึงต้องมีตัวส่งที่รันอยู่นอกระบบ
//   - ใช้ได้ทั้ง GitHub Actions (uptime/ci-alert) และ cron ในเครื่อง
//   - เป็น Go ล้วน → ทดสอบ payload/retry/redact ได้จริงด้วย `go test` ไม่ต้องพึ่ง
//     bash + curl ที่ assert อะไรไม่ได้
//
// ช่องทาง (ตั้ง env เท่าไหร่ก็ส่งเท่านั้น ไม่ตั้งเลย = skip ไม่ล้ม):
//   - Telegram : ALERT_TELEGRAM_TOKEN + ALERT_TELEGRAM_CHAT_ID
//   - Webhook  : ALERT_WEBHOOK_URL  (POST JSON {"title","body","level","source"})
//
// กติกา: secret ห้ามหลุดลง log/error — ทุกข้อความที่ออกจากไฟล์นี้ผ่าน sanitize()
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const defaultTelegramBase = "https://api.telegram.org"

// ระดับของเหตุการณ์ — ใช้เลือกไอคอนและส่งต่อไปยัง webhook
const (
	LevelDown     = "down"
	LevelRecovery = "recovery"
	LevelInfo     = "info"
)

// errPermanent — 4xx ที่ไม่ใช่ 429: ยิงซ้ำก็ได้ผลเดิม จึงไม่ retry
var errPermanent = errors.New("permanent")

// Message — ข้อความเดียวที่ส่งออกทุกช่องทาง
type Message struct {
	Title string `json:"title"`
	Body  string `json:"body"`
	Level string `json:"level"`
	// Source ระบุว่ามาจากไหน (uptime / ci-backend / ci-web) ใช้แยกใน webhook
	Source string `json:"source,omitempty"`
}

func levelIcon(level string) string {
	switch level {
	case LevelDown:
		return "🔴"
	case LevelRecovery:
		return "✅"
	default:
		return "ℹ️"
	}
}

// Text — ข้อความที่มนุษย์อ่านใน Telegram/LINE
func (m Message) Text() string {
	head := levelIcon(m.Level) + " " + m.Title
	if strings.TrimSpace(m.Body) == "" {
		return head
	}
	return head + "\n" + m.Body
}

// Config — ตั้งครั้งเดียวต่อการรันหนึ่งครั้ง
type Config struct {
	TelegramToken  string
	TelegramChatID string
	// TelegramBase เปิดให้เทสต์ชี้ไป httptest ได้ (ห้าม hardcode ที่อื่น)
	TelegramBase string
	WebhookURL   string
	Source       string

	Timeout   time.Duration // ต่อ 1 request
	Attempts  int           // จำนวนครั้งที่ยิงสูงสุดต่อช่องทาง
	Backoff   time.Duration // หน่วงระหว่างครั้ง (เพิ่มเป็นเท่าตัว)
	HTTPClient *http.Client
	Log        io.Writer
}

// loadConfig — อ่านจาก env ทั้งหมด (ผู้เรียกส่ง getenv เข้ามาเพื่อให้เทสต์ได้)
func loadConfig(getenv func(string) string) Config {
	cfg := Config{
		TelegramToken:  strings.TrimSpace(getenv("ALERT_TELEGRAM_TOKEN")),
		TelegramChatID: strings.TrimSpace(getenv("ALERT_TELEGRAM_CHAT_ID")),
		TelegramBase:   strings.TrimSpace(getenv("ALERT_TELEGRAM_BASE")),
		WebhookURL:     strings.TrimSpace(getenv("ALERT_WEBHOOK_URL")),
		Source:         strings.TrimSpace(getenv("ALERT_SOURCE")),
		Timeout:        10 * time.Second,
		Attempts:       3,
		Backoff:        time.Second,
	}
	if cfg.TelegramBase == "" {
		cfg.TelegramBase = defaultTelegramBase
	}
	if cfg.Source == "" {
		cfg.Source = "scoretrack"
	}
	return cfg
}

func (c Config) telegramReady() bool {
	return c.TelegramToken != "" && c.TelegramChatID != ""
}

// Enabled — มีอย่างน้อยหนึ่งช่องทางที่ตั้งครบ
func (c Config) Enabled() bool {
	return c.telegramReady() || c.WebhookURL != ""
}

func (c Config) logger() io.Writer {
	if c.Log != nil {
		return c.Log
	}
	return os.Stderr
}

func (c Config) logf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	fmt.Fprintln(c.logger(), "[alert] "+c.sanitize(line))
}

// sanitize — ลบ token ออกจากข้อความใด ๆ ก่อนพิมพ์ (URL ของ Telegram มี token อยู่ใน path)
func (c Config) sanitize(s string) string {
	if c.TelegramToken == "" {
		return s
	}
	return strings.ReplaceAll(s, c.TelegramToken, "***")
}

// Send — ส่งทุกช่องทางที่ตั้งไว้ และคืน error รวมถ้ามีช่องทางไหนล้ม
// ไม่ตั้งช่องทางเลย = no-op (exit 0) เพื่อไม่ให้ pipeline ล้มเพราะยังไม่ตั้ง secret
func (c Config) Send(ctx context.Context, msg Message) error {
	if msg.Source == "" {
		msg.Source = c.Source
	}
	if !c.Enabled() {
		c.logf("skip: ยังไม่ได้ตั้ง ALERT_TELEGRAM_TOKEN/ALERT_TELEGRAM_CHAT_ID หรือ ALERT_WEBHOOK_URL")
		return nil
	}

	var errs []error
	if c.telegramReady() {
		payload := map[string]any{
			"chat_id":                  c.TelegramChatID,
			"text":                     msg.Text(),
			"disable_web_page_preview": true,
		}
		url := c.TelegramBase + "/bot" + c.TelegramToken + "/sendMessage"
		if err := c.deliver(ctx, "telegram", url, payload); err != nil {
			errs = append(errs, err)
		}
	} else if c.TelegramToken != "" || c.TelegramChatID != "" {
		c.logf("telegram: skip — ต้องตั้งทั้ง ALERT_TELEGRAM_TOKEN และ ALERT_TELEGRAM_CHAT_ID")
	}

	if c.WebhookURL != "" {
		if err := c.deliver(ctx, "webhook", c.WebhookURL, msg); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// deliver — ยิงซ้ำเมื่อเจอ 5xx/network error แต่หยุดทันทีเมื่อเจอ 4xx (ยกเว้น 429)
func (c Config) deliver(ctx context.Context, name, url string, payload any) error {
	attempts := c.Attempts
	if attempts < 1 {
		attempts = 1
	}

	var lastErr error
	for i := 1; i <= attempts; i++ {
		err := c.postOnce(ctx, url, payload)
		if err == nil {
			c.logf("%s: ส่งสำเร็จ (ครั้งที่ %d/%d)", name, i, attempts)
			return nil
		}
		if errors.Is(err, errPermanent) {
			return fmt.Errorf("%s: %w", name, err)
		}
		lastErr = err
		c.logf("%s: ครั้งที่ %d/%d ล้มเหลว: %v", name, i, attempts, err)
		if i < attempts && c.Backoff > 0 {
			wait := c.Backoff * time.Duration(1<<(i-1))
			select {
			case <-ctx.Done():
				return fmt.Errorf("%s: %w", name, ctx.Err())
			case <-time.After(wait):
			}
		}
	}
	return fmt.Errorf("%s: ส่งไม่สำเร็จหลัง %d ครั้ง: %w", name, attempts, lastErr)
}

func (c Config) postOnce(ctx context.Context, url string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode payload: %w", err)
	}

	attemptCtx := ctx
	if c.Timeout > 0 {
		var cancel context.CancelFunc
		attemptCtx, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		// ไม่ควรเกิด: url ผิดรูปแบบ = ตั้งค่าใน env ผิด (ไม่ใช่ token)
		return fmt.Errorf("%w: สร้าง request ไม่ได้ (ตรวจ URL ใน env): %v", errPermanent, err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("network: %w", err)
	}
	defer resp.Body.Close()

	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	default:
		return fmt.Errorf("%w: HTTP %d: %s", errPermanent, resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
}
