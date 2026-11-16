package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/smtp"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

// Auth V2 (แผนใหม่ข้อ 1 — ส่วนเสริมระบบสมาชิก)
// เพิ่ม: register (email+password), verify-email, forgot/reset password,
// refresh token, role (user/admin), middleware requireAdmin
// ใช้ StoreData.UserProfiles + VerificationCodes + PasswordResets (JSONB รองรับ thiếuได้)

const (
	verifyExpirySec  = 600         // 10 นาที
	resetExpirySec   = 600         // 10 นาที
	refreshExpirySec = 7 * 24 * 3600 // 7 วัน

	// passwordMinLen — ขั้นต่ำ 8 ตัวอักษร (เดิม 6 อ่อนเกินไปสำหรับบัญชีที่มีข้อมูลผู้ใช้)
	passwordMinLen = 8
	// passwordMaxBytes — bcrypt ใช้ได้สูงสุด 72 bytes เกินกว่านี้จะถูก "ตัดเงียบ ๆ"
	// ทำให้ผู้ใช้คิดว่ารหัสยาว ๆ ปลอดภัยทั้งที่จริงถูกใช้แค่ 72 ไบต์แรก
	passwordMaxBytes = 72
)

// validatePassword ตรวจความแข็งของรหัสผ่านก่อน hash
func validatePassword(pw string) error {
	if utf8.RuneCountInString(pw) < passwordMinLen {
		return fmt.Errorf("รหัสผ่านต้องยาวอย่างน้อย %d ตัวอักษร", passwordMinLen)
	}
	if len(pw) > passwordMaxBytes {
		return fmt.Errorf("รหัสผ่านยาวเกิน %d ไบต์", passwordMaxBytes)
	}
	var hasLetter, hasDigit bool
	for _, r := range pw {
		if unicode.IsLetter(r) {
			hasLetter = true
		}
		if unicode.IsDigit(r) {
			hasDigit = true
		}
	}
	if !hasLetter || !hasDigit {
		return errors.New("รหัสผ่านต้องมีทั้งตัวอักษรและตัวเลข")
	}
	return nil
}

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

// errEmailNotConfigured — SMTP ยังไม่ตั้ง: ต้อง "ปิด" flow ที่ต้องส่งอีเมล
// ไม่ใช่ปล่อยผ่านแล้ว log รหัสลง log (ช่องโหว่ D1: ใครอ่าน log ได้ก็ยืนยัน/รีเซ็ตบัญชีคนอื่นได้)
var errEmailNotConfigured = errors.New("email service not configured (SMTP_HOST is empty)")

// emailSender — ชั้น indirection ให้เทสต์แทนที่ได้ (production = sendEmailReal)
var emailSender = sendEmailReal

// trySendEmail — ส่งอีเมลจริงถ้าตั้ง SMTP, คืน error ให้ผู้เรียกตัดสินใจ
// ห้าม log body เด็ดขาดเพราะมีรหัสยืนยัน/รีเซ็ตอยู่ในนั้น
func trySendEmail(to, subject, body string) error {
	return emailSender(to, subject, body)
}

func sendEmailReal(to, subject, body string) error {
	host := strings.TrimSpace(os.Getenv("SMTP_HOST"))
	if host == "" {
		// dev เท่านั้นที่ยอมให้เห็นรหัส (ต้องเปิดเองด้วย AUTH_LOG_CODES=true)
		if envBool("AUTH_LOG_CODES", false) {
			logInfof("✉️ [SMTP OFF] to=%s subj=%s body=%s", to, subject, body[:minInt(len(body), 80)])
		} else {
			logInfof("✉️ [SMTP OFF] to=%s subj=%s — ไม่ได้ส่ง (ตั้ง SMTP_HOST; เปิด AUTH_LOG_CODES=true เพื่อดูรหัสตอน dev)", to, subject)
		}
		return errEmailNotConfigured
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
	if err := smtp.SendMail(addr, auth, from, []string{to}, []byte(msg)); err != nil {
		// log เฉพาะ error ไม่ log body
		logInfof("❌ SMTP ส่งไม่สำเร็จ to=%s subj=%s: %v", to, subject, err)
		return err
	}
	return nil
}

// ----- per-account lockout (D5) -----
// rate limit ที่มีอยู่จำกัดต่อ IP แต่ credential stuffing กระจาย IP ได้
// ตัวนี้ล็อกที่ "บัญชี" (อีเมล) แทน จึงกันการยิงรหัสใส่เหยื่อรายคน

type authFailState struct {
	Fails       int
	WindowStart time.Time
	LockedUntil time.Time
}

var (
	authFailMu sync.Mutex
	authFails  = map[string]*authFailState{}
)

func authLimits() (maxFails int, lockMinutes int) {
	maxFails = envInt("AUTH_MAX_FAILS", 5)
	lockMinutes = envInt("AUTH_LOCK_MINUTES", 10)
	if maxFails <= 0 {
		maxFails = 5
	}
	if lockMinutes <= 0 {
		lockMinutes = 10
	}
	return
}

// authLocked — คืน (ล็อกอยู่ไหม, วินาทีที่ต้องรอ)
func authLocked(key string) (bool, int) {
	authFailMu.Lock()
	defer authFailMu.Unlock()

	st, ok := authFails[key]
	if !ok {
		return false, 0
	}
	now := time.Now()
	if !st.LockedUntil.IsZero() && now.Before(st.LockedUntil) {
		return true, int(st.LockedUntil.Sub(now).Seconds()) + 1
	}
	if st.LockedUntil.IsZero() && now.Sub(st.WindowStart) > 10*time.Minute {
		delete(authFails, key)
	}
	return false, 0
}

func authNoteFail(key string) {
	maxFails, lockMinutes := authLimits()
	authFailMu.Lock()
	defer authFailMu.Unlock()

	now := time.Now()
	st, ok := authFails[key]
	if !ok || (st.LockedUntil.IsZero() && now.Sub(st.WindowStart) > 10*time.Minute) {
		st = &authFailState{WindowStart: now}
		authFails[key] = st
	}
	st.Fails++
	if st.Fails >= maxFails {
		st.LockedUntil = now.Add(time.Duration(lockMinutes) * time.Minute)
	}
	// กัน map โตไม่จำกัด (key = อีเมลที่ถูกยิงมา)
	if len(authFails) > 1000 {
		for k, v := range authFails {
			if v.LockedUntil.IsZero() && now.Sub(v.WindowStart) > 10*time.Minute {
				delete(authFails, k)
			}
		}
	}
}

// authResetFails — login สำเร็จ = ล้างประวัติผิดของบัญชีนั้น
func authResetFails(key string) {
	authFailMu.Lock()
	delete(authFails, key)
	authFailMu.Unlock()
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
	if err := c.BindJSON(&body); err != nil || !strings.Contains(body.Email, "@") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email + password จำเป็น", "code": "invalid_body"})
		return
	}
	email := strings.ToLower(strings.TrimSpace(body.Email))
	if err := validatePassword(body.Password); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error(), "code": "weak_password"})
		return
	}
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

	// ส่งอีเมลก่อน "สร้างบัญชี" — ถ้าส่งไม่ได้จะได้ไม่มีบัญชีผีที่ยืนยันไม่ได้
	// (บังคับ verified อยู่แล้ว => บัญชีที่ไม่มีอีเมล = ใช้งานไม่ได้เลย)
	code := generateCode()
	if err := trySendEmail(email, "ScoreTrack — ยืนยันอีเมล", "รหัสยืนยัน: "+code+" (10 นาที)"); err != nil {
		logInfof("⛔ register ปฏิเสธ: ส่งอีเมลยืนยันไม่ได้ email=%s err=%v", email, err)
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "ระบบอีเมลยังไม่พร้อม — ติดต่อผู้ดูแลระบบ",
			"code":  "email_unavailable",
		})
		return
	}

	GlobalDB.SaveUserProfile(sub, prof, hash)
	GlobalDB.SaveVerificationCode(email, VerificationCode{Code: code, Expires: time.Now().Add(verifyExpirySec * time.Second).Unix(), Attempts: 0})
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
	// lockout ต่อบัญชี: กันการยิงรหัสใส่เหยื่อรายคนจากหลาย IP
	if locked, retrySec := authLocked(email); locked {
		c.Header("Retry-After", strconv.Itoa(retrySec))
		logInfof("🔒 login locked email=%s retry=%ds", email, retrySec)
		c.JSON(http.StatusTooManyRequests, gin.H{
			"error": "ลองผิดหลายครั้งเกินไป — รอสักครู่แล้วลองใหม่",
			"code":  "locked_out",
		})
		return
	}
	hashStr := GlobalDB.GetPasswordHashByEmail(email)
	// ข้อความเดียวกันทั้ง "ไม่พบอีเมล" และ "รหัสผิด" — กัน user enumeration
	if hashStr == "" || !verifyPassword(body.Password, hashStr) {
		authNoteFail(email)
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "อีเมลหรือรหัสผ่านไม่ถูกต้อง",
			"code":  "invalid_credentials",
		})
		return
	}
	authResetFails(email)

	sub := "email:" + email
	prof, _ := profileBySub(sub)
	if prof.Banned {
		logInfof("🚫 login blocked banned=%s", email)
		c.JSON(http.StatusForbidden, gin.H{"error": "บัญชีถูกระงับการใช้งาน — ติดต่อทีมงาน"})
		return
	}
	// บังคับยืนยันอีเมล: ไม่ยืนยัน = พิสูจน์ไม่ได้ว่าเป็นเจ้าของอีเมล
	// (ปิดได้ด้วย AUTH_REQUIRE_VERIFIED=false สำหรับ environment ทดสอบ)
	if !prof.Verified && envBool("AUTH_REQUIRE_VERIFIED", true) {
		logInfof("⏳ login blocked unverified=%s", email)
		c.JSON(http.StatusForbidden, gin.H{
			"error": "ต้องยืนยันอีเมลก่อนเข้าสู่ระบบ",
			"code":  "email_not_verified",
			"email": email,
		})
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
	if !ok || time.Now().Unix() > vc.Expires {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "รหัสไม่ถูกต้องหรือหมดอายุ"})
		return
	}
	// จำกัดจำนวนครั้งที่เดา (D6) — เดาผิดครบเพดาน = ทิ้งรหัส ต้องขอยืนยันใหม่
	maxAttempts := envInt("AUTH_MAX_CODE_ATTEMPTS", 5)
	if vc.Attempts >= maxAttempts {
		GlobalDB.DeleteVerificationCode(email)
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "เดารหัสผิดหลายครั้งเกินไป — ขอรหัสใหม่", "code": "too_many_attempts"})
		return
	}
	if vc.Code != body.Code {
		GlobalDB.SaveVerificationCode(email, VerificationCode{Code: vc.Code, Expires: vc.Expires, Attempts: vc.Attempts + 1})
		c.JSON(http.StatusUnauthorized, gin.H{"error": "รหัสไม่ถูกต้องหรือหมดอายุ", "code": "invalid_code"})
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
	// ตอบ 200 เหมือนกันทั้งกรณีมี/ไม่มีบัญชี และทั้งกรณีส่งอีเมลไม่ได้ (D7)
	// endpoint นี้ไม่ต้องใช้รหัสผ่าน จึงต้องไม่เปิดช่องให้ไล่ถามว่าอีเมลใดมีในระบบ
	const resendOK = "ถ้าอีเมลนี้มีอยู่ในระบบและยังไม่ยืนยัน เราได้ส่งรหัสใหม่ไปแล้ว"
	if GlobalDB == nil {
		c.JSON(http.StatusOK, gin.H{"status": "resent", "message": resendOK})
		return
	}
	prof := GlobalDB.GetUserProfileByEmail(email)
	if prof == nil || prof.Verified {
		c.JSON(http.StatusOK, gin.H{"status": "resent", "message": resendOK})
		return
	}
	code := generateCode()
	if err := trySendEmail(email, "ScoreTrack — รหัสยืนยันใหม่", "รหัสใหม่: "+code); err != nil {
		logInfof("⛔ resend-verify: ส่งอีเมลไม่ได้ email=%s err=%v", email, err)
		c.JSON(http.StatusOK, gin.H{"status": "resent", "message": resendOK})
		return
	}
	GlobalDB.SaveVerificationCode(email, VerificationCode{Code: code, Expires: time.Now().Add(verifyExpirySec * time.Second).Unix(), Attempts: 0})
	c.JSON(http.StatusOK, gin.H{"status": "resent", "message": resendOK})
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
	// ตอบ 200 เหมือนกันทั้งกรณีมี/ไม่มีบัญชี (D7: กัน user enumeration)
	const genericOK = "ถ้าอีเมลนี้มีอยู่ในระบบ เราได้ส่งรหัสรีเซ็ตไปแล้ว"
	if GlobalDB == nil || GlobalDB.GetPasswordHashByEmail(email) == "" {
		c.JSON(http.StatusOK, gin.H{"status": "reset requested"})
		return
	}
	// token ต้องมาจาก crypto/rand เต็ม 32 bytes (D2: เดิมเป็น 6 หลัก + hex ของ zero bytes)
	token := generateRefreshToken()
	if err := trySendEmail(email, "ScoreTrack — รีเซ็ตรหัสผ่าน", "รหัสรีเซ็ต: "+token+" (10 นาที)"); err != nil {
		logInfof("⛔ forgot-password: ส่งอีเมลไม่ได้ email=%s err=%v", email, err)
		// ไม่บอกว่าเป็นเพราะระบบอีเมลขัดข้อง — ตอบกว้าง ๆ เหมือนเดิม
		c.JSON(http.StatusOK, gin.H{"status": "reset requested"})
		return
	}
	GlobalDB.SavePasswordReset(email, PasswordReset{Token: token, Expires: time.Now().Add(resetExpirySec * time.Second).Unix(), Attempts: 0})
	logInfof("🔑 forgot-password: ออกโทเคนรีเซ็ต email=%s", email)
	c.JSON(http.StatusOK, gin.H{"status": "reset requested", "message": genericOK})
}

func handleResetPassword(c *gin.Context) {
	var body struct {
		Email       string `json:"email"`
		Code        string `json:"code"`
		NewPassword string `json:"new_password"`
	}
	if err := c.BindJSON(&body); err != nil || body.Email == "" || body.Code == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email + code + password จำเป็น"})
		return
	}
	if err := validatePassword(body.NewPassword); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error(), "code": "weak_password"})
		return
	}
	email := strings.ToLower(strings.TrimSpace(body.Email))
	if GlobalDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "db unavailable"})
		return
	}
	res, ok := GlobalDB.GetPasswordReset(email)
	if !ok || time.Now().Unix() > res.Expires {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "รหัสรีเซ็ตไม่ถูกต้องหรือหมดอายุ"})
		return
	}
	maxAttempts := envInt("AUTH_MAX_CODE_ATTEMPTS", 5)
	if res.Attempts >= maxAttempts {
		GlobalDB.DeletePasswordReset(email)
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "เดารหัสผิดหลายครั้งเกินไป — ขอรหัสใหม่", "code": "too_many_attempts"})
		return
	}
	if res.Token != body.Code {
		GlobalDB.SavePasswordReset(email, PasswordReset{Token: res.Token, Expires: res.Expires, Attempts: res.Attempts + 1})
		c.JSON(http.StatusUnauthorized, gin.H{"error": "รหัสรีเซ็ตไม่ถูกต้องหรือหมดอายุ", "code": "invalid_code"})
		return
	}
	hash, err := hashPassword(body.NewPassword)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "hash error"})
		return
	}
	GlobalDB.UpdatePasswordHash(email, hash)
	GlobalDB.DeletePasswordReset(email)
	// รีเซ็ตรหัสแล้วต้องตัด session/refresh ที่ค้างอยู่ทั้งหมด (กัน session ที่ถูกขโมย)
	sub := "email:" + email
	removedSessions := GlobalDB.DeleteUserSessionsBySub(sub)
	GlobalDB.DeleteRefreshTokenBySub(sub)
	clearSessionCookie(c)
	authResetFails(email)
	logInfof("🔑 reset-password สำเร็จ email=%s sessions_removed=%d", email, removedSessions)
	c.JSON(http.StatusOK, gin.H{"status": "password reset", "sessions_removed": removedSessions})
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
