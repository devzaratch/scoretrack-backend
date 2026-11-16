package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// stubEmailSender แทน emailSender ด้วยตัวที่ไม่ยิงเน็ตเวิร์ก
// (คืน nil = "ส่งสำเร็จ" ให้ flow ดำเนินต่อได้)
func stubEmailSender(t *testing.T) {
	t.Helper()
	prev := emailSender
	emailSender = func(to, subject, body string) error { return nil }
	t.Cleanup(func() { emailSender = prev })
}

// resetAuthFailState ล้าง lockout ที่เป็น global (กัน state รั่วข้ามเทสต์)
func resetAuthFailState() {
	authFailMu.Lock()
	authFails = map[string]*authFailState{}
	authFailMu.Unlock()
}

func postJSON(t *testing.T, handler gin.HandlerFunc, body string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req, _ := http.NewRequest(http.MethodPost, "/test", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req
	handler(c)
	return w
}

func TestValidatePassword(t *testing.T) {
	cases := []struct {
		name string
		pw   string
		ok   bool
	}{
		{"too short", "test123", false},
		{"letters only", "abcdefgh", false},
		{"digits only", "12345678", false},
		{"over bcrypt limit", strings.Repeat("a1", 40), false},
		{"acceptable", "pass1234", true},
		{"thai with digits", "รหัสผ่านดี1", true},
	}
	for _, tc := range cases {
		err := validatePassword(tc.pw)
		if tc.ok && err != nil {
			t.Errorf("%s: expected ok, got %v", tc.name, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("%s: expected error for %q", tc.name, tc.pw)
		}
	}
}

func TestRegisterWeakPasswordRejected(t *testing.T) {
	setupAuthV2Test(t)
	defer func() { GlobalDB = nil }()

	w := postJSON(t, handleRegister, `{"email":"weak@test.local","password":"abc123"}`)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "weak_password") {
		t.Fatalf("expected 400 weak_password, got %d %s", w.Code, w.Body.String())
	}
	if GlobalDB.GetUserProfileByEmail("weak@test.local") != nil {
		t.Fatal("weak password must not create an account")
	}
}

// เมื่อ SMTP ไม่พร้อม ต้องไม่สร้างบัญชีที่ยืนยันไม่ได้ และต้องไม่หลุดรหัสลง log
func TestRegisterFailsClosedWhenEmailUnavailable(t *testing.T) {
	setupAuthV2Test(t)
	defer func() { GlobalDB = nil }()
	emailSender = func(to, subject, body string) error { return errEmailNotConfigured }
	t.Cleanup(func() { emailSender = sendEmailReal })

	w := postJSON(t, handleRegister, `{"email":"nosmtp@test.local","password":"pass1234"}`)
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "email_unavailable") {
		t.Fatalf("expected 503 email_unavailable, got %d %s", w.Code, w.Body.String())
	}
	if GlobalDB.GetUserProfileByEmail("nosmtp@test.local") != nil {
		t.Fatal("account must not exist when the verification email could not be sent")
	}
	if _, ok := GlobalDB.GetVerificationCode("nosmtp@test.local"); ok {
		t.Fatal("verification code must not be stored when the email failed")
	}
}

// lockout ต่อบัญชี: ยิงรหัสผิดครบเพดานแล้วต้องได้ 429 แม้รหัสจะถูก
func TestLoginLockoutPerAccount(t *testing.T) {
	setupAuthV2Test(t)
	defer func() { GlobalDB = nil }()
	resetAuthFailState()
	t.Cleanup(resetAuthFailState)

	sub := "email:lock@test.local"
	hash, _ := hashPassword("pass1234")
	GlobalDB.SaveUserProfile(sub, UserProfile{Sub: sub, Email: "lock@test.local", Role: "user", Verified: true}, hash)

	maxFails, _ := authLimits()
	for i := 0; i < maxFails; i++ {
		w := postJSON(t, handleLogin, `{"email":"lock@test.local","password":"wrongpass1"}`)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d: expected 401, got %d %s", i+1, w.Code, w.Body.String())
		}
	}

	w := postJSON(t, handleLogin, `{"email":"lock@test.local","password":"pass1234"}`)
	if w.Code != http.StatusTooManyRequests || !strings.Contains(w.Body.String(), "locked_out") {
		t.Fatalf("expected 429 locked_out after %d fails, got %d %s", maxFails, w.Code, w.Body.String())
	}
	if w.Header().Get("Retry-After") == "" {
		t.Error("429 ต้องมี Retry-After header")
	}
}

// login ต้องไม่บอกว่า "อีเมลไม่มีในระบบ" ต่างจาก "รหัสผิด"
func TestLoginDoesNotEnumerateEmails(t *testing.T) {
	setupAuthV2Test(t)
	defer func() { GlobalDB = nil }()
	resetAuthFailState()
	t.Cleanup(resetAuthFailState)

	wUnknown := postJSON(t, handleLogin, `{"email":"nobody@test.local","password":"pass1234"}`)
	if wUnknown.Code != http.StatusUnauthorized {
		t.Fatalf("unknown email should be 401, got %d", wUnknown.Code)
	}
	if strings.Contains(wUnknown.Body.String(), "ไม่พบอีเมล") {
		t.Fatalf("response leaks whether the email exists: %s", wUnknown.Body.String())
	}
}

func TestVerifyEmailAttemptsCap(t *testing.T) {
	setupAuthV2Test(t)
	defer func() { GlobalDB = nil }()

	email := "cap@test.local"
	GlobalDB.SaveVerificationCode(email, VerificationCode{
		Code:     "123456",
		Expires:  time.Now().Add(600 * time.Second).Unix(),
		Attempts: envInt("AUTH_MAX_CODE_ATTEMPTS", 5),
	})

	w := postJSON(t, handleVerifyEmail, `{"email":"cap@test.local","code":"123456"}`)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after attempts cap, got %d %s", w.Code, w.Body.String())
	}
	if _, ok := GlobalDB.GetVerificationCode(email); ok {
		t.Fatal("code must be dropped once the attempt cap is reached")
	}
}

func TestVerifyEmailWrongCodeIncrementsAttempts(t *testing.T) {
	setupAuthV2Test(t)
	defer func() { GlobalDB = nil }()

	email := "wrong@test.local"
	GlobalDB.SaveVerificationCode(email, VerificationCode{Code: "424242", Expires: time.Now().Add(600 * time.Second).Unix()})

	w := postJSON(t, handleVerifyEmail, `{"email":"wrong@test.local","code":"000000"}`)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
	vc, ok := GlobalDB.GetVerificationCode(email)
	if !ok || vc.Attempts != 1 {
		t.Fatalf("attempts should be 1, got %+v ok=%v", vc, ok)
	}
}

func TestResendVerifyDoesNotEnumerate(t *testing.T) {
	setupAuthV2Test(t)
	defer func() { GlobalDB = nil }()

	// ไม่มีบัญชี -> ต้องยัง 200 (ไม่บอกว่าไม่มีอีเมลนี้)
	wUnknown := postJSON(t, handleResendVerify, `{"email":"ghost2@test.local"}`)
	if wUnknown.Code != http.StatusOK {
		t.Fatalf("unknown email should still be 200, got %d %s", wUnknown.Code, wUnknown.Body.String())
	}

	// มีบัญชีแต่ยังไม่ยืนยัน -> 200 และได้รหัสใหม่
	sub := "email:pending@test.local"
	GlobalDB.SaveUserProfile(sub, UserProfile{Sub: sub, Email: "pending@test.local", Role: "user", Verified: false}, "dummyhash")
	w := postJSON(t, handleResendVerify, `{"email":"pending@test.local"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("resend for unverified should be 200, got %d %s", w.Code, w.Body.String())
	}
	if vc, ok := GlobalDB.GetVerificationCode("pending@test.local"); !ok || vc.Code == "" {
		t.Fatal("resend must store a fresh verification code")
	}
}

func TestForgotPasswordAlways200AndStrongToken(t *testing.T) {
	setupAuthV2Test(t)
	defer func() { GlobalDB = nil }()

	// ไม่มีบัญชี -> ต้องยังตอบ 200 (กัน enumeration)
	wUnknown := postJSON(t, handleForgotPassword, `{"email":"ghost@test.local"}`)
	if wUnknown.Code != http.StatusOK {
		t.Fatalf("unknown email should still be 200, got %d %s", wUnknown.Code, wUnknown.Body.String())
	}

	GlobalDB.SaveUserProfile("email:reset2@test.local", UserProfile{Sub: "email:reset2@test.local", Email: "reset2@test.local", Role: "user", Verified: true}, "dummyhash")

	w := postJSON(t, handleForgotPassword, `{"email":"reset2@test.local"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("forgot fail code=%d body=%s", w.Code, w.Body.String())
	}
	res, ok := GlobalDB.GetPasswordReset("reset2@test.local")
	if !ok || res.Token == "" {
		t.Fatal("reset token missing")
	}
	// เดิม: 6 หลัก + "0000000000000000" (คงที่) -> ตอนนี้ต้องเป็น 32-byte hex
	if len(res.Token) != 64 {
		t.Fatalf("reset token should be 64 hex chars, got %q (len=%d)", res.Token, len(res.Token))
	}
	if strings.HasSuffix(res.Token, "0000000000000000") {
		t.Fatalf("reset token must not have a constant tail: %q", res.Token)
	}
}

func TestResetPasswordInvalidatesSessions(t *testing.T) {
	setupAuthV2Test(t)
	defer func() { GlobalDB = nil }()

	sub := "email:reset3@test.local"
	GlobalDB.SaveUserProfile(sub, UserProfile{Sub: sub, Email: "reset3@test.local", Role: "user", Verified: true}, "dummyhash")
	GlobalDB.SaveUserSession("old-session", sub)
	GlobalDB.SaveRefreshToken(sub, "old-refresh", time.Now().Add(3600*time.Second).Unix())

	GlobalDB.SavePasswordReset("reset3@test.local", PasswordReset{Token: "tok-abc", Expires: time.Now().Add(600 * time.Second).Unix()})

	w := postJSON(t, handleResetPassword, `{"email":"reset3@test.local","code":"tok-abc","new_password":"newpass123"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("reset fail code=%d body=%s", w.Code, w.Body.String())
	}
	if _, ok := GlobalDB.GetRefreshExpiryBySub(sub); ok {
		t.Fatal("refresh token must be revoked after a password reset")
	}
	// session ต้องถูกตัด (sub ตรงกับที่เก็บใน session)
	if GlobalDB.GetSessionSub("old-session") != "" {
		t.Fatal("sessions must be revoked after a password reset")
	}
}

func TestResetPasswordWeakPasswordRejected(t *testing.T) {
	setupAuthV2Test(t)
	defer func() { GlobalDB = nil }()

	GlobalDB.SaveUserProfile("email:weak2@test.local", UserProfile{Sub: "email:weak2@test.local", Email: "weak2@test.local", Role: "user", Verified: true}, "dummyhash")
	GlobalDB.SavePasswordReset("weak2@test.local", PasswordReset{Token: "tok-weak", Expires: time.Now().Add(600 * time.Second).Unix()})

	w := postJSON(t, handleResetPassword, `{"email":"weak2@test.local","code":"tok-weak","new_password":"short1"}`)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "weak_password") {
		t.Fatalf("expected 400 weak_password, got %d %s", w.Code, w.Body.String())
	}
}

func TestGoogleClaimsValidation(t *testing.T) {
	valid := googleClaims{Sub: "s1", Aud: "client-1", Iss: "https://accounts.google.com", EmailVerified: "true"}
	if err := validateGoogleClaims(valid, "client-1"); err != nil {
		t.Fatalf("valid claims rejected: %v", err)
	}

	cases := []struct {
		name     string
		claims   googleClaims
		clientID string
		want     string
	}{
		{"wrong issuer", googleClaims{Sub: "s", Aud: "client-1", Iss: "https://evil.example", EmailVerified: "true"}, "client-1", "issuer"},
		{"aud mismatch", googleClaims{Sub: "s", Aud: "other-app", Iss: "https://accounts.google.com", EmailVerified: "true"}, "client-1", "aud"},
		{"aud missing but required", googleClaims{Sub: "s", Aud: "", Iss: "https://accounts.google.com"}, "client-1", "aud"},
		{"email not verified", googleClaims{Sub: "s", Aud: "client-1", Iss: "https://accounts.google.com", Email: "a@b.c", EmailVerified: "false"}, "client-1", "email_verified"},
		{"no sub", googleClaims{Aud: "client-1", Iss: "https://accounts.google.com"}, "client-1", "sub"},
	}
	for _, tc := range cases {
		err := validateGoogleClaims(tc.claims, tc.clientID)
		if err == nil {
			t.Errorf("%s: expected error", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %q should mention %q", tc.name, err, tc.want)
		}
	}

	// dev/test: ไม่ได้ตั้ง GOOGLE_CLIENT_ID -> ยังยอมรับ (แค่ log warning ตอน boot)
	if err := validateGoogleClaims(googleClaims{Sub: "s", Aud: "any", Iss: "accounts.google.com"}, ""); err != nil {
		t.Errorf("empty client id should not reject: %v", err)
	}
}
