package main

// tests สำหรับ Phase B (ข้อ 4/5):
// - API key self-service: สร้าง -> รายการเห็น -> เพิกถอน (ป้องกันลบ key ของคนอื่น)

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// helper: สร้าง request พร้อม session cookie
func reqWithSession(token string, method, target, body string) (*httptest.ResponseRecorder, *gin.Context) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	c, _ := gin.CreateTestContext(w)
	c.Request = req
	return w, c
}

// saveSessionForTest — บันทึก session token ของ sub (tok-<email>)
func saveSessionForTest(sub string) string {
	tok := "tok-" + strings.TrimPrefix(sub, "email:")
	GlobalDB.SaveUserSession(tok, sub)
	return tok
}

func TestUserSelfServeAPIKeys(t *testing.T) {
	setupAuthV2Test(t)
	defer func() { GlobalDB = nil }()

	sub := "email:dev@test.local"
	GlobalDB.SaveUserProfile(sub, UserProfile{Sub: sub, Email: "dev@test.local", Role: "user", Verified: true}, "dummyhash")
	tok := saveSessionForTest(sub)

	// 1) สร้าง key — คืนค่าเต็ม 1 ครั้ง
	w, c := reqWithSession(tok, http.MethodPost, "/api/auth/api-keys", `{"name":"my tool"}`)
	handleUserAPIKeyCreate(c)
	if w.Code != http.StatusCreated {
		t.Fatalf("create=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "sk_live_") {
		t.Fatalf("expected full key in creation response: %s", w.Body.String())
	}

	// 2) รายการต้องเห็น key ของเอง
	w2, c2 := reqWithSession(tok, http.MethodGet, "/api/auth/api-keys", "")
	handleUserAPIKeyList(c2)
	if w2.Code != 200 || !strings.Contains(w2.Body.String(), "my tool") {
		t.Fatalf("list=%d body=%s", w2.Code, w2.Body.String())
	}

	// 3) คนอื่นลบ key ของเราไม่ได้ (สิทธิ์ตาม owner)
	sub3 := "email:dev3@test.local"
	GlobalDB.SaveUserProfile(sub3, UserProfile{Sub: sub3, Email: "dev3@test.local", Role: "user", Verified: true}, "dummyhash")
	GlobalDB.SaveUserSession("tok-dev3", sub3)

	views := selfServeKeyViews(sub)
	if len(views) != 1 {
		t.Fatalf("expected 1 view, got %d", len(views))
	}
	prefix := views[0].KeyID

	w3, c3 := reqWithSession("tok-dev3", http.MethodDelete, "/api/auth/api-keys/"+prefix, "")
	c3.Params = gin.Params{{Key: "key", Value: prefix}}
	handleUserAPIKeyRevoke(c3)
	if w3.Code != http.StatusForbidden {
		t.Fatalf("other-user revoke should be 403, got %d body=%s", w3.Code, w3.Body.String())
	}

	// 4) เจ้าของลบเองได้
	w4, c4 := reqWithSession(tok, http.MethodDelete, "/api/auth/api-keys/"+prefix, "")
	c4.Params = gin.Params{{Key: "key", Value: prefix}}
	handleUserAPIKeyRevoke(c4)
	if w4.Code != 200 {
		t.Fatalf("owner revoke=%d body=%s", w4.Code, w4.Body.String())
	}
}

func TestBanBlocksLogin(t *testing.T) {
	setupAuthV2Test(t)
	defer func() { GlobalDB = nil }()

	email := "banned@test.local"
	sub := "email:" + email
	GlobalDB.SaveUserProfile(sub, UserProfile{Sub: sub, Email: email, Role: "user", Verified: true}, "hash-from-test")
	GlobalDB.UpdatePasswordHash(email, "hash-from-test")
	// hash จริงจาก bcrypt เพื่อผ่าน verifyPassword
	hash, _ := hashPassword("pass123")
	GlobalDB.UpdatePasswordHash(email, hash)

	// ban แล้ว login ต้องโดน 403
	if !GlobalDB.SetUserBanned(sub, true) {
		t.Fatal("SetUserBanned returned false")
	}
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/api/auth/login", strings.NewReader(`{"email":"`+email+`","password":"pass123"}`))
	req.Header.Set("Content-Type", "application/json")
	c, _ := gin.CreateTestContext(w)
	c.Request = req
	handleLogin(c)
	if w.Code != http.StatusForbidden {
		t.Fatalf("banned login should be 403, got %d body=%s", w.Code, w.Body.String())
	}

	// ปลดแบน -> login ได้
	if !GlobalDB.SetUserBanned(sub, false) {
		t.Fatal("unban returned false")
	}
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest(http.MethodPost, "/api/auth/login", strings.NewReader(`{"email":"`+email+`","password":"pass123"}`))
	req2.Header.Set("Content-Type", "application/json")
	c2, _ := gin.CreateTestContext(w2)
	c2.Request = req2
	handleLogin(c2)
	if w2.Code != 200 {
		t.Fatalf("unbanned login should be 200, got %d body=%s", w2.Code, w2.Body.String())
	}
}
