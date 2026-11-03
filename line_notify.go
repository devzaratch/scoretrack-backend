package main

// line_notify.go — แจ้งเตือนผู้ใช้ผ่าน LINE ขนานกับ FCM (บาลานซ์สำหรับผู้ใช้ไทย)
//
// รูปแบบ:
//   - LINE_CHANNEL_ACCESS_TOKEN ต้องตั้งใน env ถึงจะเปิดใช้ (ไม่ตั้ง = skip, ไม่ throw)
//   - LINE_TARGET_ID (userId / groupId / roomId) ถ้าใส่ → push ตรงถึงเป้าหมายนั้น
//   - ถ้าไม่ใส่ → broadcast หาผู้ติดตาม (เพื่อนที่ add LINE Official Account ไว้)
//
// ข้อจำกัด: Messaging API push/broadcast ฟรี 200 ข้อความ/เดือน (เรตต่อ user ID ไม่จำกัดแบบ webhook)
// เห็นสถิติ: logInfof แสดงสถานะ HTTP กำกับทุกการส่ง

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// lineAPIBase เปิดตัวแปรไว้ให้ทดสอบ override ด้วย httptest แทนยิง api.line.me จริง
var lineAPIBase = "https://api.line.me"

type lineTextMessage struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type linePushPayload struct {
	To       string            `json:"to,omitempty"`
	Messages []lineTextMessage `json:"messages"`
}

// composeLINEText รวม title + body เป็นข้อความเดียว (LINE text จำกัด 5000 ตัวอักษร)
func composeLINEText(title, body string) string {
	return strings.TrimSpace(title + "\n" + body)
}

// buildLINEPayload คืน JSON body — to = "" หมายถึง broadcast
func buildLINEPayload(title, body string, to string) ([]byte, string) {
	payload := linePushPayload{
		Messages: []lineTextMessage{{Type: "text", Text: composeLINEText(title, body)}},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, ""
	}
	return raw, fmt.Sprintf("%s", raw)
}

// DispatchLINENotify ส่งข้อความ LINE — เรียกขนานกับ DispatchFCMPush จาก dispatcher เดียวกัน
func DispatchLINENotify(matchID int, title, body string) {
	token := strings.TrimSpace(getEnv("LINE_CHANNEL_ACCESS_TOKEN", ""))
	if token == "" {
		logInfof("ℹ️ LINE notify skipped (match #%d): LINE_CHANNEL_ACCESS_TOKEN ไม่ได้ตั้ง", matchID)
		return
	}

	linePush, jsonStr := buildLINEPayload(title, body, "")
	if jsonStr == "" {
		logInfof("❌ LINE notify: marshal payload ล้มเหลว (match #%d)", matchID)
		return
	}

	// เป้าหมาย: target ใส่ id → /push, ไม่ใส่ → /broadcast (add friend หมายถึงรับ broadcast)
	endpoint := lineAPIBase + "/v2/bot/message/broadcast"
	if target := strings.TrimSpace(getEnv("LINE_TARGET_ID", "")); target != "" {
		endpoint = lineAPIBase + "/v2/bot/message/push"
		// to field ต้องใส่ target — แก้ payload ก่อนส่ง
		var p map[string]interface{}
		_ = json.Unmarshal([]byte(jsonStr), &p)
		p["to"] = target
		b, err := json.Marshal(p)
		if err != nil {
			logInfof("❌ LINE notify: marshal target payload ล้มเหลว: %v", err)
			return
		}
		linePush = b
	}

	client := &http.Client{Timeout: 8 * time.Second}
	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(linePush))
	if err != nil {
		logInfof("❌ LINE notify: สร้าง request ล้มเหลว: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := client.Do(req)
	if err != nil {
		logInfof("❌ LINE notify delivery: %v (match #%d)", err, matchID)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		logInfof("✅ LINE notify sent (HTTP 200, match #%d)", matchID)
	} else if resp.StatusCode == 429 {
		logInfof("⚠️ LINE notify rate-limited (HTTP 429, ตรวจ quota push/free 200 ข้อความ/เดือน, match #%d)", matchID)
	} else {
		logInfof("⚠️ LINE notify HTTP %d (match #%d) — ตรวจ token/target", resp.StatusCode, matchID)
	}
}
