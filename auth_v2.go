package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/smtp"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

// Auth V2 (แผนใหม่ข้อ 1 — ส่วนเสริมระบบสมาชิก)
// เพิ่ม: register (email+password), verify-email, forgot/reset password,
// refresh token, role (user/admin), middleware requireAdmin
// ใช้ StoreData.UserProfiles + VerificationCodes + PasswordResets (JSONB รองรับ thiếuได้)

const (
	verifyExpirySec = 600    // 10 นาที
	resetExpirySec  = 600    // 10 นาที
	refreshExpirySec = 7*24*3600 // 7 วัน
)

type UserProfile struct {
	Sub      string `json:"sub"`
	Email    string `json:"email"`
	Role     string `json:"role"` // "user" | "admin"
	Verified bool   `json:"verified"`
	// Banned = แอดมินระงับบัญชี (ข้อ 4: แบน/ปลดแบนจากหน้า admin)
	// login/refresh จะปฏิเสธบัญชีที่ banned เสมอ (ผู้ใช้ session เหลื่อมในเสมอที่รื้อ)
	Banned bool `json:"banned,omitempty"`
}

type VerificationCode struct {
	Code     string `json:"code"`
	Expires  int64  `json:"expires"`
	Attempts int    `json:"attempts"`
}

type PasswordReset struct {
	Token    string `json:"token"`
	Expires  int64  `json:"expires"`
	Attempts int    `json:"attempts"`
}

func hashPassword(pw string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
	return string(b), err
}

func verifyPassword(pw, hashStr string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hashStr), []byte(pw))
	return err == nil
}

func generateCode() string {
	b := make([]byte, 4)
	rand.Read(b)
	return fmt.Sprintf("%06d", int(b[0])<<24|int(b[1])<<16|int(b[2])<<8|int(b[3]))
}

func generateRefreshToken() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// trySendEmail — ถ้ามี SMTP env -> ส่งจริง; ถ้าไม่ -> log เพื่อทดสอบ
func trySendEmail(to, subject, body string) {
	host := strings.TrimSpace(os.Getenv("SMTP_HOST"))
	if host == "" {
		logInfof("✉️ [SMTP OFF] to=%s subj=%s body=%s", to, subject, body[:minInt(len(body), 80)])
		return
	}
	port := os.Getenv("SMTP_PORT")
	if port == "" {
		port = "587"
	}
	user := os.Getenv("SMTP_USER")
	pass := os.Getenv("SMTP_PASS")
	from := os.Getenv("SMTP_FROM")
	if from == "" {
		from = "noreply@scoretrack.local"
	}
	msg := "From: " + from + "\r\n" +
		"To: " + to + "\r\n" +
		"Subject: " + subject + "\r\n\r\n" + body
	addr := host + ":" + port
	auth := smtp.PlainAuth("", user, pass, host)
	_ = smtp.SendMail(addr, auth, from, []string{to}, []byte(msg))
}

func profileBySub(sub string) (UserProfile, bool) {
	if GlobalDB == nil {
		return UserProfile{}, false
	}
	// UserProfiles อยู่ใน StoreData — เข้าถึงผ่าน GlobalDB.data ถ้าตรง แต่ PersistentDB ซ่อน data
	// สร้าง helper ภายใน PersistentDB เพื่อเข้าถึง profiles (เพิ่ม method ถ้ายังไม่มี)
	return GlobalDB.GetUserProfile(sub)
}

// requireAdmin — middleware ตรวจ role == admin (จาก cookie/session -> sub -> profile)
func requireAdmin() gin.HandlerFunc {
	return func(c *gin.Context) {
		sub := ""
		if token := sessionTokenFromRequest(c); token != "" && GlobalDB != nil {
			sub = GlobalDB.GetSessionSub(token)
		}
		if sub == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "login required"})
			c.Abort()
			return
		}
		prof, ok := profileBySub(sub)
		if !ok || prof.Role != "admin" {
			c.JSON(http.StatusForbidden, gin.H{"error": "admin only"})
			c.Abort()
			return
		}
		c.Next()
	}
}

// ----- endpoints -----

func handleRegister(c *gin.Context) {
	var body struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := c.BindJSON(&body); err != nil || !strings.Contains(body.Email, "@") || len(body.Password) < 6 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email + password (>=6 chars) จำเป็น"})
		return
	}
	email := strings.ToLower(strings.TrimSpace(body.Email))
	if GlobalDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "db unavailable"})
		return
	}
	if GlobalDB.GetUserProfileByEmail(email) != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "email ใช้แล้ว"})
		return
	}
	hash, err := hashPassword(body.Password)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "hash error"})
		return
	}
	sub := "email:" + email
	prof := UserProfile{Sub: sub, Email: email, Role: "user", Verified: false}
	GlobalDB.SaveUserProfile(sub, prof, hash) // method ให้ PersistentDB

	code := generateCode()
	GlobalDB.SaveVerificationCode(email, VerificationCode{Code: code, Expires: time.Now().Add(verifyExpirySec * time.Second).Unix(), Attempts: 0})
	trySendEmail(email, "ScoreTrack — ยืนยันอีเมล", "รหัสยืนยัน: "+code+" (10 นาที)")
	logInfof("🆕 register email=%s sub=%s verified=false", email, sub)
	c.JSON(http.StatusOK, gin.H{"status": "registered", "email": email, "verified": false})
}

func handleLogin(c *gin.Context) {
	var body struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := c.BindJSON(&body); err != nil || !strings.Contains(body.Email, "@") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email + password จำเป็น"})
		return
	}
	email := strings.ToLower(strings.TrimSpace(body.Email))
	if GlobalDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "db unavailable"})
		return
	}
	hashStr := GlobalDB.GetPasswordHashByEmail(email)
	if hashStr == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "ไม่พบอีเมล"})
		return
	}
	if !verifyPassword(body.Password, hashStr) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "รหัสผ่านไม่ถูกต้อง"})
		return
	}
	sub := "email:" + email
	prof, _ := profileBySub(sub)
	if prof.Banned {
		logInfof("🚫 login blocked banned=%s", email)
		c.JSON(http.StatusForbidden, gin.H{"error": "บัญชีถูกระงับการใช้งาน — ติดต่อทีมงาน"})
		return
	}
	sess := make([]byte, 32)
	rand.Read(sess)
	sessionToken := hex.EncodeToString(sess)
	GlobalDB.SaveUserSession(sessionToken, sub)

	refresh := generateRefreshToken()
	GlobalDB.SaveRefreshToken(sub, refresh, time.Now().Add(refreshExpirySec*time.Second).Unix())

	setSessionCookie(c, sessionToken)
	c.JSON(http.StatusOK, gin.H{
		"authenticated": true,
		"email":          email,
		"sub":            sub,
		"role":           prof.Role,
		"verified":       prof.Verified,
		"refresh_token":  refresh,
	})
}

func handleVerifyEmail(c *gin.Context) {
	var body struct {
		Email string `json:"email"`
		Code  string `json:"code"`
	}
	if err := c.BindJSON(&body); err != nil || body.Code == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email + code จำเป็น"})
		return
	}
	email := strings.ToLower(strings.TrimSpace(body.Email))
	if GlobalDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "db unavailable"})
		return
	}
	vc, ok := GlobalDB.GetVerificationCode(email)
	if !ok || vc.Code != body.Code || time.Now().Unix() > vc.Expires {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "รหัสไม่ถูกต้องหรือหมดอายุ"})
		return
	}
	sub := "email:" + email
	GlobalDB.MarkUserVerified(sub)
	GlobalDB.DeleteVerificationCode(email)
	logInfof("✅ verified email=%s", email)
	c.JSON(http.StatusOK, gin.H{"status": "verified", "email": email})
}

func handleResendVerify(c *gin.Context) {
	var body struct {
		Email string `json:"email"`
	}
	if err := c.BindJSON(&body); err != nil || body.Email == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email จำเป็น"})
		return
	}
	email := strings.ToLower(strings.TrimSpace(body.Email))
	code := generateCode()
	GlobalDB.SaveVerificationCode(email, VerificationCode{Code: code, Expires: time.Now().Add(verifyExpirySec * time.Second).Unix(), Attempts: 0})
	trySendEmail(email, "ScoreTrack — รหัสยืนยันใหม่", "รหัสใหม่: "+code)
	c.JSON(http.StatusOK, gin.H{"status": "resent"})
}

func handleForgotPassword(c *gin.Context) {
	var body struct {
		Email string `json:"email"`
	}
	if err := c.BindJSON(&body); err != nil || body.Email == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email จำเป็น"})
		return
	}
	email := strings.ToLower(strings.TrimSpace(body.Email))
	if GlobalDB == nil || GlobalDB.GetPasswordHashByEmail(email) == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "ไม่พบอีเมลในระบบ"})
		return
	}
	token := generateCode() + hex.EncodeToString(make([]byte, 8))
	GlobalDB.SavePasswordReset(email, PasswordReset{Token: token, Expires: time.Now().Add(resetExpirySec * time.Second).Unix(), Attempts: 0})
	trySendEmail(email, "ScoreTrack — รีเซ็ตรหัสผ่าน", "รหัสรีเซ็ต: "+token+" (10 นาที)")
	c.JSON(http.StatusOK, gin.H{"status": "reset link sent"})
}

func handleResetPassword(c *gin.Context) {
	var body struct {
		Email       string `json:"email"`
		Code        string `json:"code"`
		NewPassword string `json:"new_password"`
	}
	if err := c.BindJSON(&body); err != nil || body.NewPassword == "" || len(body.NewPassword) < 6 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email + code + password (>=6) จำเป็น"})
		return
	}
	email := strings.ToLower(strings.TrimSpace(body.Email))
	if GlobalDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "db unavailable"})
		return
	}
	res, ok := GlobalDB.GetPasswordReset(email)
	if !ok || res.Token != body.Code || time.Now().Unix() > res.Expires {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "รหัสรีเซ็ตไม่ถูกต้องหรือหมดอายุ"})
		return
	}
	hash, err := hashPassword(body.NewPassword)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "hash error"})
		return
	}
	GlobalDB.UpdatePasswordHash(email, hash)
	GlobalDB.DeletePasswordReset(email)
	c.JSON(http.StatusOK, gin.H{"status": "password reset"})
}

func handleRefresh(c *gin.Context) {
	var body struct {
		RefreshToken string `json:"refresh_token"`
	}
	if err := c.BindJSON(&body); err != nil || body.RefreshToken == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "refresh_token จำเป็น"})
		return
	}
	if GlobalDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "db unavailable"})
		return
	}
	sub := GlobalDB.GetSubByRefreshToken(body.RefreshToken)
	if sub == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "refresh token ไม่ถูกต้อง"})
		return
	}
	prof, _ := profileBySub(sub)
	if prof.Banned {
		GlobalDB.DeleteRefreshTokenBySub(sub)
		GlobalDB.DeleteUserSessionsBySub(sub)
		logInfof("🚫 refresh blocked banned sub=%s", sub)
		c.JSON(http.StatusForbidden, gin.H{"error": "บัญชีถูกระงับการใช้งาน — ติดต่อทีมงาน"})
		return
	}
	sess := make([]byte, 32)
	rand.Read(sess)
	sessionToken := hex.EncodeToString(sess)
	GlobalDB.SaveUserSession(sessionToken, sub)
	newRefresh := generateRefreshToken()
	GlobalDB.SaveRefreshToken(sub, newRefresh, time.Now().Add(refreshExpirySec*time.Second).Unix())
	setSessionCookie(c, sessionToken)
	c.JSON(http.StatusOK, gin.H{
		"authenticated": true,
		"sub": sub,
		"role": prof.Role,
		"verified": prof.Verified,
		"refresh_token": newRefresh,
	})
}

func handleMe(c *gin.Context) {
	sub := ""
	if token := sessionTokenFromRequest(c); token != "" && GlobalDB != nil {
		sub = GlobalDB.GetSessionSub(token)
	}
	if sub == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not logged in"})
		return
	}
	prof, ok := profileBySub(sub)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "profile missing"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"sub": sub,
		"email": prof.Email,
		"role": prof.Role,
		"verified": prof.Verified,
	})
}

// ----- Phase A ข้อ 3: จัดการอุปกรณ์/เซสชัน -----

// subFromCookie — ดึง sub จาก session cookie (pattern เดียวกับ handleMe/requireAdmin)
func subFromCookie(c *gin.Context) string {
	if token := sessionTokenFromRequest(c); token != "" && GlobalDB != nil {
		return GlobalDB.GetSessionSub(token)
	}
	return ""
}

// handleAuthSessions — GET /api/auth/sessions -> สรุปอุปกรณ์/เซสชันที่ยังใช้งานอยู่ของผู้ใช้นี้
func handleAuthSessions(c *gin.Context) {
	sub := subFromCookie(c)
	if sub == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not logged in"})
		return
	}
	if GlobalDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "db unavailable"})
		return
	}
	count := GlobalDB.CountSessionsBySub(sub)
	refreshExpiry, hasRefresh := GlobalDB.GetRefreshExpiryBySub(sub)
	c.JSON(http.StatusOK, gin.H{
		"sub":                 sub,
		"session_count":       count,
		"has_refresh":         hasRefresh,
		"refresh_expires_unix": refreshExpiry,
	})
}

// handleLogoutAll — POST /api/auth/logout-all -> ออกจากระบบทุกเครื่อง:
// ลบ session ทั้งหมดของ sub นี้ (รวมเครื่องปัจจุบัน) + ลบ refresh token
// (token ที่ถูกขโมยไปใช้ต่อไม่ได้ทันที)
func handleLogoutAll(c *gin.Context) {
	sub := subFromCookie(c)
	if sub == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not logged in"})
		return
	}
	if GlobalDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "db unavailable"})
		return
	}
	removed := GlobalDB.DeleteUserSessionsBySub(sub)
	GlobalDB.DeleteRefreshTokenBySub(sub)
	clearSessionCookie(c)
	logInfof("🚪 logout-all sub=%s sessions_removed=%d", sub, removed)
	c.JSON(http.StatusOK, gin.H{"status": "ok", "sessions_removed": removed})
}
