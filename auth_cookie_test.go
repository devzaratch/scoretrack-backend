package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// ---------------------------------------------------------------------------
// แผนข้อ 10 — auth cookie: ตั้ง/ล้าง cookie, อ่าน token จาก cookie ก่อน Bearer,
// handler ยกระดับ session + logout
// ---------------------------------------------------------------------------

func newCookieTestEngine() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/me", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"key": resolveUserKey(c)})
	})
	return r
}

// setSessionCookie ต้องเป็น HttpOnly (JS อ่านไม่ได้) + SameSite=Lax + Path=/
func TestSetSessionCookie_Flags(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("GET", "/api/auth/google", nil)

	setSessionCookie(c, "tok-123")

	cookies := rec.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("ควรมี 1 cookie แต่ได้ %d", len(cookies))
	}
	ck := cookies[0]
	if ck.Name != sessionCookieName {
		t.Errorf("ชื่อ cookie ผิด: %s", ck.Name)
	}
	if ck.Value != "tok-123" {
		t.Errorf("ค่า cookie ผิด: %s", ck.Value)
	}
	if !ck.HttpOnly {
		t.Error("cookie ต้องเป็น HttpOnly (กัน XSS อ่าน session)")
	}
	if ck.SameSite != http.SameSiteLaxMode {
		t.Errorf("SameSite ต้องเป็น Lax ได้ %v", ck.SameSite)
	}
	if ck.Path != "/" {
		t.Errorf("Path ต้องเป็น / ได้ %s", ck.Path)
	}
	if ck.MaxAge <= 0 {
		t.Errorf("MaxAge ต้องมากกว่า 0 ได้ %d", ck.MaxAge)
	}
	if ck.Secure {
		t.Error("request http (ไม่มี X-Forwarded-Proto) ต้องไม่ตั้ง Secure")
	}
}

// https (ผ่าน proxy ที่ใส่ X-Forwarded-Proto) ต้องตั้ง Secure
func TestSetSessionCookie_SecureOnHTTPS(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("GET", "/api/auth/google", nil)
	c.Request.Header.Set("X-Forwarded-Proto", "https")

	setSessionCookie(c, "tok-abc")

	cookies := rec.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].Secure {
		t.Error("request ผ่าน https ต้องตั้ง Secure=true")
	}
}

// ล้าง cookie = MaxAge -1 (browser ลบทิ้งทันที)
func TestClearSessionCookie(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("GET", "/api/auth/logout", nil)

	clearSessionCookie(c)

	cookies := rec.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("ควรมี 1 cookie แต่ได้ %d", len(cookies))
	}
	if cookies[0].MaxAge >= 0 {
		t.Errorf("MaxAge ต้องเป็น -1 (ลบทิ้ง) ได้ %d", cookies[0].MaxAge)
	}
	if cookies[0].Value != "" {
		t.Errorf("ค่า cookie ต้องว่าง ได้ %q", cookies[0].Value)
	}
}

// cookie มาก่อน Bearer (ระบบใหม่คือ cookie)
func TestSessionTokenFromRequest_CookieFirst(t *testing.T) {
	r := newCookieTestEngine()

	req := httptest.NewRequest("GET", "/api/me", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "cookie-token"})
	req.Header.Set("Authorization", "Bearer bearer-token")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if !strings.Contains(rec.Body.String(), "cookie-token") {
		t.Errorf("ต้องใช้ token จาก cookie ก่อน ได้ %s", rec.Body.String())
	}
}

// ไม่มี cookie -> fallback ไป Bearer (compat client รุ่นเก่า)
func TestSessionTokenFromRequest_BearerFallback(t *testing.T) {
	r := newCookieTestEngine()

	req := httptest.NewRequest("GET", "/api/me", nil)
	req.Header.Set("Authorization", "Bearer bearer-token")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if !strings.Contains(rec.Body.String(), "bearer-token") {
		t.Errorf("ต้อง fallback ไป Bearer ได้ %s", rec.Body.String())
	}
}

// ไม่มีทั้ง cookie และ Bearer -> ไม่มี token (ค่าว่าง/null/undefined ต้องถือว่าไม่มี)
func TestSessionTokenFromRequest_EmptyVariants(t *testing.T) {
	for _, v := range []string{"", "null", "undefined"} {
		req := httptest.NewRequest("GET", "/api/me", nil)
		req.Header.Set("Authorization", "Bearer "+v)
		if got := sessionTokenFromRequest(mustCtx(req)); got != "" {
			t.Errorf("Bearer %q ต้องถือว่าไม่มี token ได้ %q", v, got)
		}
	}
}

func mustCtx(req *http.Request) *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = req
	return c
}

// resolveUserKey ต้องผูก session token -> google sub ได้ (ผ่าน cookie)
func TestResolveUserKey_FromCookieSession(t *testing.T) {
	if GlobalDB == nil {
		GlobalDB = newTestDB(t)
	}
	defer func() { GlobalDB = nil }()

	token := "test-session-token-cookie"
	GlobalDB.SaveUserSession(token, "google-sub-123")

	r := newCookieTestEngine()
	req := httptest.NewRequest("GET", "/api/me", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if !strings.Contains(rec.Body.String(), "user:g:google-sub-123") {
		t.Errorf("ต้อง resolve เป็น user:g:<sub> ได้ %s", rec.Body.String())
	}
}

// logout: ลบ session จริง (token เดิมใช้ต่อไม่ได้)
func TestHandleAuthLogout_DeletesSession(t *testing.T) {
	if GlobalDB == nil {
		GlobalDB = newTestDB(t)
	}
	defer func() { GlobalDB = nil }()

	token := "test-session-token-logout"
	GlobalDB.SaveUserSession(token, "google-sub-456")

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/auth/logout", handleAuthLogout)

	req := httptest.NewRequest("POST", "/api/auth/logout", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ควรได้ 200 ได้ %d", rec.Code)
	}
	if sub := GlobalDB.GetSessionSub(token); sub != "" {
		t.Errorf("session ต้องถูกลบ แต่ยังเจอ %q", sub)
	}
	// ต้องมี Set-Cookie ที่ล้าง cookie ด้วย
	found := false
	for _, ck := range rec.Result().Cookies() {
		if ck.Name == sessionCookieName && ck.MaxAge < 0 {
			found = true
		}
	}
	if !found {
		t.Error("ต้องส่ง Set-Cookie ที่ล้าง session cookie")
	}
}

// ยกระดับ session เดิมเป็น cookie: มี Bearer ที่ถูกต้อง -> ได้ cookie + 200
func TestHandleAuthSession_UpgradesToCookie(t *testing.T) {
	if GlobalDB == nil {
		GlobalDB = newTestDB(t)
	}
	defer func() { GlobalDB = nil }()

	token := "test-session-token-upgrade"
	GlobalDB.SaveUserSession(token, "google-sub-789")

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/auth/session", handleAuthSession)

	req := httptest.NewRequest("POST", "/api/auth/session", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ควรได้ 200 ได้ %d (%s)", rec.Code, rec.Body.String())
	}
	found := false
	for _, ck := range rec.Result().Cookies() {
		if ck.Name == sessionCookieName && ck.Value == token && ck.HttpOnly {
			found = true
		}
	}
	if !found {
		t.Error("ต้องออก session cookie (HttpOnly) ที่มีค่า token เดิม")
	}
}

// token ไม่มีในระบบ -> 401 (ไม่ออก cookie)
func TestHandleAuthSession_InvalidToken401(t *testing.T) {
	if GlobalDB == nil {
		GlobalDB = newTestDB(t)
	}
	defer func() { GlobalDB = nil }()

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/auth/session", handleAuthSession)

	req := httptest.NewRequest("POST", "/api/auth/session", nil)
	req.Header.Set("Authorization", "Bearer not-a-real-session")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("ควรได้ 401 ได้ %d", rec.Code)
	}
	if len(rec.Result().Cookies()) != 0 {
		t.Error("token ไม่ถูกต้อง ต้องไม่ออก cookie")
	}
}
