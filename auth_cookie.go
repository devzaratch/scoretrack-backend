package main

// auth_cookie — session cookie ฝั่ง Go backend (แผนข้อ 10: auth cookie + proxy)
//
// เดิม: session token เก็บใน localStorage ของ client -> XSS อ่านได้ทันที
// (token หลุดไปใช้ยิง API แทนผู้ใช้ได้) และฝั่ง server ไม่มีทางรู้ว่าใคร login อยู่
//
// ตอนนี้: backend ออก session เป็น cookie แทน
//   - HttpOnly  : JS อ่านไม่ได้ -> ขโมยจาก XSS ไม่ได้
//   - SameSite=Lax : ข้าม origin ไม่ติด -> กัน CSRF จากเว็บอื่น
//   - Secure    : ตั้งเมื่อ request เป็น https (prod ผ่าน Vercel/Render -> X-Forwarded-Proto)
//   - Path=/, Max-Age 30 วัน
//
// ฝั่ง client รุ่นเก่าที่ยังส่ง Authorization: Bearer ยังใช้ได้ (compat) —
// ทางหลักของระบบใหม่คือ cookie (ดู sessionTokenFromRequest)
//
// หมายเหตุเรื่องโดเมน: client ยิงผ่าน proxy same-origin (/api/... ของเว็บเอง)
// Set-Cookie ที่ backend ตั้งจึงผูกกับ origin ของเว็บ (host-only) ไม่ใช่โดเมน backend
// -> ไม่ต้องตั้ง Domain (และไม่ควรตั้ง เพราะจะกลายเป็น cross-site)

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

const (
	// ชื่อ cookie — ฝั่ง client อ่านชื่อนี้ได้ (แต่ค่าไม่ได้ = HttpOnly)
	sessionCookieName = "scoretrack_session"
	sessionCookiePath = "/"
	// อายุ session 30 วัน (ต้องตรงกับที่เก็บใน PersistentDB ที่ไม่มี TTL)
	sessionMaxAgeSec = 30 * 24 * 3600
)

// isSecureRequest — request เป็น https จริง หรือผ่าน reverse proxy ที่บอกมา
// (Vercel/Render ใส่ X-Forwarded-Proto) — local dev (http) ต้องได้ Secure=false
// ไม่งั้น browser จะไม่ยอมรับ cookie ตอน dev/E2E
func isSecureRequest(c *gin.Context) bool {
	if c == nil || c.Request == nil {
		return false
	}
	if c.Request.TLS != nil {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(c.GetHeader("X-Forwarded-Proto")), "https")
}

// setSessionCookie — ออก session cookie (HttpOnly)
func setSessionCookie(c *gin.Context, token string) {
	http.SetCookie(c.Writer, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     sessionCookiePath,
		MaxAge:   sessionMaxAgeSec,
		HttpOnly: true,
		Secure:   isSecureRequest(c),
		SameSite: http.SameSiteLaxMode,
	})
}

// clearSessionCookie — ล้าง session cookie (MaxAge -1 = ลบทันที)
func clearSessionCookie(c *gin.Context) {
	http.SetCookie(c.Writer, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     sessionCookiePath,
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   isSecureRequest(c),
		SameSite: http.SameSiteLaxMode,
	})
}

// sessionTokenFromRequest — อ่าน token จาก cookie ก่อน (ทางหลักของระบบใหม่)
// แล้วค่อย fallback ไป Authorization: Bearer (compat กับ client รุ่นเก่า)
func sessionTokenFromRequest(c *gin.Context) string {
	if c == nil {
		return ""
	}
	if c.Request != nil {
		if ck, err := c.Cookie(sessionCookieName); err == nil {
			if t := strings.TrimSpace(ck); t != "" {
				return t
			}
		}
	}
	authHeader := c.GetHeader("Authorization")
	t := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
	if t == "" || t == "null" || t == "undefined" {
		return ""
	}
	return t
}
