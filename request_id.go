package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// Correlation ID (แผนใหม่ข้อ 1) — แนบ ID เดียวกันตลอดเส้นทาง
// browser -> Next proxy (/api/[...path]) -> Go backend -> log ทุกจุด
// เดิม: debug prod ต้องเดาจาก timestamp/ip — ตอนนี้ grep request_id ตัวเดียวเจอทั้ง trace

const (
	requestIDHeader = "X-Request-ID"
	requestIDKey    = "request_id"
)

// requestIDMiddleware — วนก่อน middleware อื่นทั้งหมด
// รับ X-Request-ID จาก proxy (ถ้ารูปแบบปลอดภัย) ไม่งั้นสร้างใหม่
// แล้วใส่ทั้งใน context (ให้ handler/log อ่าน) และ response header (ให้ client correlate)
func requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := strings.TrimSpace(c.GetHeader(requestIDHeader))
		if !validRequestID(id) {
			id = newRequestID()
		}
		c.Set(requestIDKey, id)
		c.Header(requestIDHeader, id)
		c.Next()
	}
}

// validRequestID — จำกัด charset/ความยาว กัน header injection + log ยาวผิดปกติ
// รับได้เฉพาะ a-z A-Z 0-9 - _ ยาว 8-64 (uuid/hex ผ่านหมด)
func validRequestID(id string) bool {
	if len(id) < 8 || len(id) > 64 {
		return false
	}
	for _, r := range id {
		ok := (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '-' || r == '_'
		if !ok {
			return false
		}
	}
	return true
}

// newRequestID — 16 bytes สุ่ม = 32 hex chars (เท่า UUIDv4 เรื่อง entropy)
func newRequestID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// Deliberate: entropy ล้ม = แทบเป็นไปไม่ได้ — fallback เป็น timestamp ยังใช้ correlate ได้
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// requestIDFrom — อ่าน request ID ของ request ปัจจุบัน ("" ถ้าไม่มี middleware)
func requestIDFrom(c *gin.Context) string {
	return c.GetString(requestIDKey)
}
