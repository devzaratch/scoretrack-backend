package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func setupAuthV2Test(t *testing.T) {
	t.Helper()
	GlobalDB = newTestDB(t)
	GlobalDB.EnsureAuthDefaults()
}

func TestAuthV2RegisterVerifyLoginFlow(t *testing.T) {
	setupAuthV2Test(t)
	defer func() { GlobalDB = nil }()

	// Register
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/api/auth/register", strings.NewReader(`{"email":"test@example.com","password":"test123"}`))
	req.Header.Set("Content-Type", "application/json")
	c, _ := gin.CreateTestContext(w)
	c.Request = req
	handleRegister(c)
	if w.Code != 200 {
		t.Fatalf("register fail code=%d body=%s", w.Code, w.Body.String())
	}

	// Login (before verify should succeed but verified=false)
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest(http.MethodPost, "/api/auth/login", strings.NewReader(`{"email":"test@example.com","password":"test123"}`))
	req2.Header.Set("Content-Type", "application/json")
	c2, _ := gin.CreateTestContext(w2)
	c2.Request = req2
	handleLogin(c2)
	if w2.Code != 200 {
		t.Fatalf("login fail code=%d body=%s", w2.Code, w2.Body.String())
	}
	var loginRes map[string]interface{}
	json.Unmarshal(w2.Body.Bytes(), &loginRes)
	if loginRes["verified"].(bool) != false {
		t.Fatal("email not verified yet")
	}

	// Verify
	vc, ok := GlobalDB.GetVerificationCode("test@example.com")
	if !ok || vc.Code == "" {
		t.Fatal("verification code missing")
	}
	w3 := httptest.NewRecorder()
	req3, _ := http.NewRequest(http.MethodPost, "/api/auth/verify-email", strings.NewReader(`{"email":"test@example.com","code":"`+vc.Code+`"}`))
	req3.Header.Set("Content-Type", "application/json")
	c3, _ := gin.CreateTestContext(w3)
	c3.Request = req3
	handleVerifyEmail(c3)
	if w3.Code != 200 {
		t.Fatalf("verify fail code=%d body=%s", w3.Code, w3.Body.String())
	}

	prof := GlobalDB.GetUserProfileByEmail("test@example.com")
	if prof == nil || !prof.Verified {
		t.Fatal("profile not verified after verify")
	}
}

func TestResetPasswordFlow(t *testing.T) {
	setupAuthV2Test(t)
	defer func() { GlobalDB = nil }()

	// Pre-register user
	GlobalDB.SaveUserProfile("email:test@reset.local", UserProfile{Sub: "email:test@reset.local", Email: "test@reset.local", Role: "user", Verified: true}, "dummyhash")

	// Forgot
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/api/auth/forgot-password", strings.NewReader(`{"email":"test@reset.local"}`))
	req.Header.Set("Content-Type", "application/json")
	c, _ := gin.CreateTestContext(w)
	c.Request = req
	handleForgotPassword(c)
	if w.Code != 200 {
		t.Fatalf("forgot fail code=%d body=%s", w.Code, w.Body.String())
	}

	res, ok := GlobalDB.GetPasswordReset("test@reset.local")
	if !ok || res.Token == "" {
		t.Fatal("reset token missing")
	}

	// Reset
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest(http.MethodPost, "/api/auth/reset-password", bytes.NewBufferString(`{"email":"test@reset.local","code":"`+res.Token+`","new_password":"newpass123"}`))
	req2.Header.Set("Content-Type", "application/json")
	c2, _ := gin.CreateTestContext(w2)
	c2.Request = req2
	handleResetPassword(c2)
	if w2.Code != 200 {
		t.Fatalf("reset fail code=%d body=%s", w2.Code, w2.Body.String())
	}
}

func TestRefreshTokenFlow(t *testing.T) {
	setupAuthV2Test(t)
	defer func() { GlobalDB = nil }()

	GlobalDB.SaveUserProfile("email:refresh@test.local", UserProfile{Sub: "email:refresh@test.local", Email: "refresh@test.local", Role: "user", Verified: true}, "dummyhash")

	reftoken := "test-refresh-token"
	GlobalDB.SaveRefreshToken("email:refresh@test.local", reftoken, time.Now().Add(3600*time.Second).Unix())

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/api/auth/refresh", strings.NewReader(`{"refresh_token":"`+reftoken+`"}`))
	req.Header.Set("Content-Type", "application/json")
	c, _ := gin.CreateTestContext(w)
	c.Request = req
	handleRefresh(c)
	if w.Code != 200 {
		t.Fatalf("refresh fail code=%d body=%s", w.Code, w.Body.String())
	}
}

// Phase A ข้อ 3: sessions summary + logout-all
func TestSessionsAndLogoutAll(t *testing.T) {
	setupAuthV2Test(t)
	defer func() { GlobalDB = nil }()

	sub := "email:sess@test.local"
	GlobalDB.SaveUserProfile(sub, UserProfile{Sub: sub, Email: "sess@test.local", Role: "user", Verified: true}, "dummyhash")

	// สร้าง session 2 เครื่อง + refresh token 1 ตัว
	GlobalDB.SaveUserSession("sess-token-a", sub)
	GlobalDB.SaveUserSession("sess-token-b", sub)
	GlobalDB.SaveRefreshToken(sub, "sess-refresh", time.Now().Add(3600*time.Second).Unix())

	// หมายเหตุ: DeleteUserSessionsBySub หา session ผ่าน sub = <sub> (จาก map ตรง ๆ)
	// -> เรียก handler ด้วย cookie ของ token-a
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/auth/sessions", http.NoBody)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "sess-token-a"})
	c, _ := gin.CreateTestContext(w)
	c.Request = req
	handleAuthSessions(c)
	if w.Code != 200 {
		t.Fatalf("sessions fail code=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"has_refresh":true`) || !strings.Contains(w.Body.String(), `"session_count":2`) {
		t.Fatalf("sessions body wrong: %s", w.Body.String())
	}

	// logout-all ด้วย cookie token-a -> ลบ session ทั้งหมดของ sub นี้ + refresh
	// (session ผู้ใช้อื่นตั้งไว้ล่วงหน้า: ต้องไม่ควรโดนลบ = isolation)
	GlobalDB.SaveUserSession("sess-token-other", "other-user")
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest(http.MethodPost, "/api/auth/logout-all", http.NoBody)
	req2.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "sess-token-a"})
	c2, _ := gin.CreateTestContext(w2)
	c2.Request = req2
	handleLogoutAll(c2)
	if w2.Code != 200 {
		t.Fatalf("logout-all fail code=%d body=%s", w2.Code, w2.Body.String())
	}

	if cnt := GlobalDB.CountSessionsBySub(sub); cnt != 0 {
		t.Fatalf("expected 0 sessions after logout-all, got %d", cnt)
	}
	if _, ok := GlobalDB.GetRefreshExpiryBySub(sub); ok {
		t.Fatal("refresh token should be gone after logout-all")
	}
	if GlobalDB.GetSessionSub("sess-token-other") != "other-user" {
		t.Fatal("other user's session must remain untouched")
	}
}
