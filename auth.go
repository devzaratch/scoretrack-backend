package main

import (
	"crypto/subtle"
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// adminToken เก็บค่า ADMIN_TOKEN จาก environment
// ถ้าไม่ได้ตั้งค่าไว้ ระบบจะ "ปิดตาย" ทุก route ที่ต้องใช้สิทธิ์แอดมิน
// (fail-closed) ปลอดภัยกว่าการเปิดให้ใครก็เข้าได้
func adminToken() string {
	return strings.TrimSpace(getEnv("ADMIN_TOKEN", ""))
}

// extractToken ดึง token จาก header
// รองรับทั้ง "X-Admin-Token: xxx" และ "Authorization: Bearer xxx"
func extractToken(c *gin.Context) string {
	if t := strings.TrimSpace(c.GetHeader("X-Admin-Token")); t != "" {
		return t
	}
	auth := strings.TrimSpace(c.GetHeader("Authorization"))
	if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		return strings.TrimSpace(auth[7:])
	}
	return ""
}

// AdminAuthMiddleware ป้องกัน route ที่ใช้สิทธิ์แอดมิน
// ใช้ subtle.ConstantTimeCompare เพื่อกัน timing attack
func AdminAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		expected := adminToken()

		if expected == "" {
			log.Printf("🚫 ปฏิเสธคำขอแอดมินจาก %s: ยังไม่ได้ตั้งค่า ADMIN_TOKEN", c.ClientIP())
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"error": "ระบบแอดมินยังไม่ได้ตั้งค่า (ADMIN_TOKEN)",
			})
			return
		}

		provided := extractToken(c)
		if provided == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
			log.Printf("🚫 ปฏิเสธคำขอแอดมินจาก %s ไปที่ %s", c.ClientIP(), c.Request.URL.Path)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "ไม่ได้รับอนุญาต",
			})
			return
		}

		c.Next()
	}
}

// tailOf คืนอักขระ n ตัวสุดท้ายของ s
// ใช้สำหรับ log ค่าที่เป็นความลับ เช่น FCM token โดยไม่เผยค่าเต็ม
func tailOf(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// handleAdminVerify ให้หน้า /admin/streams เช็คว่า token ที่กรอกมาถูกต้องไหม
// ต้องผูกไว้หลัง AdminAuthMiddleware เสมอ
func handleAdminVerify(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
