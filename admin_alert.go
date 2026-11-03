package main

// admin_alert.go — แจ้งเตือนแอดมินเมื่อ health check เปลี่ยนสถานะ (ok→พัง / พัง→ok)
//
// ตามหลักข้อ 8 (roadmap รอบ brainstorming): รายงานสุขภาพรายวันวิ่งแล้ว (cron-logs/)
// ของที่ขาดคือ "พังแล้วแจ้งทันที" — watchdog ตัวนี้เดินเช็คทุก interval แล้วยิง alert
// "เฉพาะตอนสถานะเปลี่ยน" (กันสแปมยิงซ้ำทุกนาที)
//
// ช่องทางแจ้งเตือน:
//   1. ADMIN_ALERT_WEBHOOK_URL  (generic POST JSON {"title","body"}) — ใช้กับ relay ไหนก็ได้
//   2. ไม่ตั้ง → fallback ไป LINE notify เดิม (LINE_CHANNEL_ACCESS_TOKEN)
//   ตั้งไม่ตั้งอะไรเลย = watchdog ไม่ยิง (log เดียวตอน start, ไม่ throw)

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// getEnvInt — helper อ่าน env เป็น int (คืน fallback เมื่อว่าง/parse ไม่ได้)
func getEnvInt(key string, fallback int) int {
	if v := getEnv(key, ""); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// fireAdminAlert — ยิง title/body หาช่องทางที่ตั้งไว้ (webhook ก่อน → แล้ว LINE)
func fireAdminAlert(title, body string) {
	if url := getEnv("ADMIN_ALERT_WEBHOOK_URL", ""); url != "" {
		payload, err := json.Marshal(map[string]string{"title": title, "body": body})
		if err != nil {
			logInfof("❌ admin alert: marshal ล้มเหลว: %v", err)
			return
		}
		client := &http.Client{Timeout: 8 * time.Second}
		resp, err := client.Post(url, "application/json", bytes.NewBuffer(payload))
		if err != nil {
			logInfof("❌ admin alert webhook: %v", err)
			return
		}
		defer resp.Body.Close()
		logInfof("📢 admin alert → webhook (HTTP %d): %s", resp.StatusCode, title)
		return
	}
	// fallback: LINE notify เดิม (ตัวเองก็ env-gated อยู่แล้ว)
	DispatchLINENotify(-1, title, body)
}

// startAdminAlertWatchdog — วนเช็คสถานะ + ยิงเฉพาะ transition (หนึ่ง goroutine พอ)
func startAdminAlertWatchdog(ctx context.Context, interval time.Duration) {
	if getEnv("ADMIN_ALERT_WEBHOOK_URL", "") == "" && getEnv("LINE_CHANNEL_ACCESS_TOKEN", "") == "" {
		logInfof("ℹ️ admin watchdog ปิด: ADMIN_ALERT_WEBHOOK_URL / LINE_CHANNEL_ACCESS_TOKEN ไม่ได้ตั้ง")
		return
	}

	prevUnhealthy := false
	first := true
	ticker := time.NewTicker(interval)
	logInfof("🐕 admin watchdog เริ่มทำงาน: ทุก %s", interval)

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				healthy, checks := runHealthChecks()
				switch {
				case !healthy && (first || !prevUnhealthy):
					fireAdminAlert("🚨 ScoreTrack ผิดปกติ", healthSummary(healthy, checks))
				case healthy && prevUnhealthy:
					fireAdminAlert("✅ ScoreTrack กลับมาปกติ", healthSummary(healthy, checks))
				}
				prevUnhealthy = !healthy
				first = false
			}
		}
	}()
}

// healthSummary — เรนเดอร์ checks เป็นข้อความสั้นสำหรับ alert
func healthSummary(healthy bool, checks map[string]interface{}) string {
	state := "healthy"
	if !healthy {
		state = "unhealthy"
	}
	return fmt.Sprintf("สถานะ: %s\n%v", state, checks)
}

// adminWatchdogShouldFire — transition logic แยกเป็น pure function เพื่อทดสอบเปล่า ๆ
// prev = initial-first-run, nowHealthy — คืน (fireNow, alertTitle) หรือ "" เมื่อเงียบ
func adminWatchdogStep(prevUnhealthy, first, nowHealthy bool) (bool, string) {
	switch {
	case !nowHealthy && (first || !prevUnhealthy):
		return true, "🚨 ScoreTrack ผิดปกติ"
	case nowHealthy && prevUnhealthy:
		return true, "✅ ScoreTrack กลับมาปกติ"
	}
	return false, ""
}
