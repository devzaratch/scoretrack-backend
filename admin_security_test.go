package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// ---------------------------------------------------------------------------
// แผนข้อ 2 — ความปลอดภัย: API key เก็บเป็น sha256 + admin lockout/audit
// ---------------------------------------------------------------------------

// resetAdminAuthFails — ล้าง state ระหว่างเทสต์ (global map ของ middleware)
func resetAdminAuthFails() {
	adminAuthMu.Lock()
	defer adminAuthMu.Unlock()
	adminAuthFails = map[string]*adminAuthFailState{}
}

// SaveAPIKey ต้องเก็บใน DB เป็น sha256 — ห้ามมีค่าเต็มเป็น key ของ map
func TestAPIKeyStoredHashed(t *testing.T) {
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = nil }()

	key, _ := generateAPIKey()
	GlobalDB.SaveAPIKey(key, APIKeyRecord{Name: "hashed", Enabled: true, DailyLimit: 100})

	GlobalDB.mu.RLock()
	_, plaintextExists := GlobalDB.data.APIKeys[key]
	_, hashExists := GlobalDB.data.APIKeys[hashAPIKey(key)]
	GlobalDB.mu.RUnlock()

	if plaintextExists {
		t.Error("ห้ามเก็บ key เต็มเป็น key ของ map (ต้องเป็น sha256)")
	}
	if !hashExists {
		t.Error("ต้องเก็บภายใต้ sha256(key)")
	}

	// ค้นด้วยค่าเต็มต้องยังใช้ได้ (hash ให้ภายใน)
	rec, ok := GlobalDB.GetAPIKey(key)
	if !ok || rec.Name != "hashed" {
		t.Errorf("GetAPIKey ด้วยค่าเต็มต้องใช้ได้: ok=%v rec=%+v", ok, rec)
	}
	if rec.KeyPrefix != apiKeyPrefix(key) {
		t.Errorf("KeyPrefix ต้องเป็น 16 ตัวแรก ได้ %q", rec.KeyPrefix)
	}
}

// key เก่าที่ยังเก็บ plaintext (map key = sk_live_...) ต้องถูกย้ายเป็น hash ตอน migrate
func TestAPIKeyMigratePlaintextToHashed(t *testing.T) {
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = nil }()

	oldKey := apiKeyIDPrefix + strings.Repeat("ab", 24)
	GlobalDB.mu.Lock()
	GlobalDB.data.APIKeys = map[string]APIKeyRecord{} // newTestDB ไม่ได้สร้าง map นี้
	GlobalDB.data.APIKeys[oldKey] = APIKeyRecord{Name: "legacy", Enabled: true}
	// ข้อมูลทดสอบที่ไม่ใช่รูปแบบ key ต้องไม่ถูกแตะ
	GlobalDB.data.APIKeys["sk"] = APIKeyRecord{Name: "test-row"}
	GlobalDB.mu.Unlock()

	moved := GlobalDB.migrateAPIKeysToHashed()
	if moved != 1 {
		t.Fatalf("ต้องย้าย 1 รายการ ได้ %d", moved)
	}

	GlobalDB.mu.RLock()
	_, stillPlain := GlobalDB.data.APIKeys[oldKey]
	_, hashExists := GlobalDB.data.APIKeys[hashAPIKey(oldKey)]
	_, testRow := GlobalDB.data.APIKeys["sk"]
	GlobalDB.mu.RUnlock()

	if stillPlain {
		t.Error("key plaintext เก่าต้องถูกย้ายออกจาก map แล้ว")
	}
	if !hashExists {
		t.Error("ต้องมีรายการภายใต้ sha256 หลังย้าย")
	}
	if !testRow {
		t.Error("ข้อมูลที่ไม่ใช่ key (ไม่ขึ้น sk_live_) ห้ามถูกย้าย/ลบ")
	}

	// ค้นด้วยค่าเต็มหลังย้ายต้องใช้ได้ + KeyPrefix เติมให้แล้ว
	rec, ok := GlobalDB.GetAPIKey(oldKey)
	if !ok || rec.Name != "legacy" {
		t.Errorf("หลังย้ายต้องค้นเจอ: ok=%v rec=%+v", ok, rec)
	}
	if rec.KeyPrefix != apiKeyPrefix(oldKey) {
		t.Errorf("KeyPrefix หลังย้ายผิด: %q", rec.KeyPrefix)
	}
}

// เพิกถอนผ่าน prefix (แบบที่หน้า admin ส่งมา) ต้องใช้ได้กับ key ที่เก็บแบบ hash
func TestAPIKeyRevokeByPrefixHashed(t *testing.T) {
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = nil }()

	key, _ := generateAPIKey()
	GlobalDB.SaveAPIKey(key, APIKeyRecord{Name: "revoke-me", Enabled: true})
	prefix := apiKeyPrefix(key)

	hash := GlobalDB.FindAPIKeyHashByPrefix(prefix)
	if hash == "" {
		t.Fatal("FindAPIKeyHashByPrefix ต้องค้นเจอจาก KeyPrefix")
	}
	if hash == key {
		t.Error("ค่าที่คืนต้องเป็น hash ไม่ใช่ค่าเต็ม")
	}
	if !GlobalDB.DeleteAPIKeyByHash(hash) {
		t.Fatal("ลบด้วย hash ต้องสำเร็จ")
	}
	if _, ok := GlobalDB.GetAPIKey(key); ok {
		t.Error("หลังเพิกถอนต้องใช้ key ไม่ได้แล้ว")
	}
	if GlobalDB.FindAPIKeyHashByPrefix(prefix) != "" {
		t.Error("ค้นซ้ำหลังลบต้องไม่เจอ")
	}
}

// ListAPIKeyViews ต้องแสดง key_id จาก KeyPrefix (ไม่ใช่ hash)
func TestAPIKeyViewsUsePrefixNotHash(t *testing.T) {
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = nil }()

	key, _ := generateAPIKey()
	GlobalDB.SaveAPIKey(key, APIKeyRecord{Name: "view", Enabled: true})

	views := GlobalDB.ListAPIKeyViews()
	if len(views) != 1 {
		t.Fatalf("ต้องมี 1 รายการ ได้ %d", len(views))
	}
	if views[0].KeyID != apiKeyPrefix(key) {
		t.Errorf("KeyID ต้องเป็น 16 ตัวแรกของ key ได้ %q", views[0].KeyID)
	}
	if strings.Contains(views[0].KeyID, " ") || len(views[0].KeyID) != 16 {
		t.Errorf("KeyID รูปผิด: %q", views[0].KeyID)
	}
}

// ---------------------------------------------------------------------------
// Admin: lockout + audit
// ---------------------------------------------------------------------------

func adminTestEngine() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	adm := r.Group("/api/admin", adminAuthMiddleware())
	adm.GET("/stats", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"ok": true}) })
	adm.POST("/api-keys", func(c *gin.Context) { c.JSON(http.StatusCreated, gin.H{"created": true}) })
	return r
}

// ยิง token ผิดต่อเนื่อง -> ต้องถูกล็อก 429 (ไม่ใช่ 401 ตลอดกาล = brute-force ได้)
func TestAdminAuth_LockoutAfterFails(t *testing.T) {
	resetAdminAuthFails()
	defer resetAdminAuthFails()
	t.Setenv("ADMIN_TOKEN", "correct-token-123")
	t.Setenv("ADMIN_AUTH_MAX_FAILS", "3")
	t.Setenv("ADMIN_AUTH_LOCK_MINUTES", "10")

	r := adminTestEngine()

	// ยิงผิด 3 ครั้ง (IP เดียวกัน — httptest ไม่มี XFF -> ClientIP = 192.0.2.1)
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest("GET", "/api/admin/stats", nil)
		req.Header.Set("X-Admin-Token", "wrong")
		req.Header.Set("X-Forwarded-For", "203.0.113.9")
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("ครั้งที่ %d ควรได้ 401 ได้ %d", i+1, rec.Code)
		}
	}

	// ครั้งที่ 4 ต่อให้ token ถูกก็ต้องโดน 429 (ถูกล็อกแล้ว)
	req := httptest.NewRequest("GET", "/api/admin/stats", nil)
	req.Header.Set("X-Admin-Token", "correct-token-123")
	req.Header.Set("X-Forwarded-For", "203.0.113.9")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("หลังล็อกต้องได้ 429 ได้ %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("429 ต้องมี Retry-After")
	}

	// IP อื่นไม่ได้รับผลกระทบ
	req2 := httptest.NewRequest("GET", "/api/admin/stats", nil)
	req2.Header.Set("X-Admin-Token", "correct-token-123")
	req2.Header.Set("X-Forwarded-For", "198.51.100.7")
	rec2 := httptest.NewRecorder()
	r.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("IP อื่นต้องใช้ได้ตามปกติ ได้ %d", rec2.Code)
	}
}

// token ถูกแล้วต้องล้างประวัติความผิด (ครั้งถัดไปแม้ผิด 2 ครั้งก็ยังไม่ล็อก)
func TestAdminAuth_SuccessResetsFails(t *testing.T) {
	resetAdminAuthFails()
	defer resetAdminAuthFails()
	t.Setenv("ADMIN_TOKEN", "correct-token-123")
	t.Setenv("ADMIN_AUTH_MAX_FAILS", "3")

	r := adminTestEngine()
	xff := func() string { return "203.0.113.50" }

	// ผิด 2 ครั้ง
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/api/admin/stats", nil)
		req.Header.Set("X-Admin-Token", "wrong")
		req.Header.Set("X-Forwarded-For", xff())
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
	}
	// สำเร็จ 1 ครั้ง -> รีเซ็ต
	req := httptest.NewRequest("GET", "/api/admin/stats", nil)
	req.Header.Set("X-Admin-Token", "correct-token-123")
	req.Header.Set("X-Forwarded-For", xff())
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("token ถูกต้องได้ 200 ได้ %d", rec.Code)
	}

	// ผิดอีก 2 ครั้ง -> ยังไม่ครบเกณฑ์ ต้องได้ 401 (ไม่ใช่ 429)
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/api/admin/stats", nil)
		req.Header.Set("X-Admin-Token", "wrong")
		req.Header.Set("X-Forwarded-For", xff())
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("หลังรีเซ็ต ผิดครั้งที่ %d ต้องได้ 401 ได้ %d", i+1, rec.Code)
		}
	}
}

// คำขอที่เปลี่ยนแปลงข้อมูล (POST) ต้องถูกบันทึก audit / GET ไม่ต้อง
func TestAdminAuditLog_WritesOnMutation(t *testing.T) {
	resetAdminAuthFails()
	defer resetAdminAuthFails()
	t.Setenv("ADMIN_TOKEN", "correct-token-123")
	auditFile := filepath.Join(t.TempDir(), "audit.log")
	t.Setenv("ADMIN_AUDIT_LOG", auditFile)

	r := adminTestEngine()

	// GET -> ไม่บันทึก
	reqGet := httptest.NewRequest("GET", "/api/admin/stats", nil)
	reqGet.Header.Set("X-Admin-Token", "correct-token-123")
	reqGet.Header.Set("X-Forwarded-For", "203.0.113.80")
	r.ServeHTTP(httptest.NewRecorder(), reqGet)

	if _, err := os.Stat(auditFile); err == nil {
		t.Error("GET ไม่ต้องสร้างไฟล์ audit")
	}

	// POST -> บันทึก 1 แถว
	reqPost := httptest.NewRequest("POST", "/api/admin/api-keys", strings.NewReader(`{}`))
	reqPost.Header.Set("X-Admin-Token", "correct-token-123")
	reqPost.Header.Set("X-Forwarded-For", "203.0.113.80")
	recPost := httptest.NewRecorder()
	r.ServeHTTP(recPost, reqPost)

	raw, err := os.ReadFile(auditFile)
	if err != nil {
		t.Fatalf("ต้องมีไฟล์ audit หลัง POST: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 1 {
		t.Fatalf("ต้องมี 1 แถว ได้ %d", len(lines))
	}
	var entry map[string]interface{}
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("audit ต้องเป็น JSON: %v (%s)", err, lines[0])
	}
	if entry["method"] != "POST" || entry["path"] != "/api/admin/api-keys" {
		t.Errorf("audit ผิด: %+v", entry)
	}
	if entry["ip"] != "203.0.113.80" {
		t.Errorf("audit ต้องบันทึก IP จาก XFF: %+v", entry)
	}
	if entry["status"].(float64) != float64(http.StatusCreated) {
		t.Errorf("audit status ผิด: %+v", entry)
	}
	if _, ok := entry["time"]; !ok {
		t.Error("audit ต้องมีเวลา")
	}
}

// XFF หลายชั้น: ต้องใช้ตัวสุดท้าย (ตัวที่ proxy มองเห็นจริง)
func TestAdminClientIP_UsesRightmostXFF(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("GET", "/", nil)
	c.Request.Header.Set("X-Forwarded-For", "1.2.3.4, 5.6.7.8, 9.9.9.9")
	if got := adminClientIP(c); got != "9.9.9.9" {
		t.Errorf("ต้องใช้ XFF ตัวสุดท้าย ได้ %q", got)
	}
	c.Request.Header.Del("X-Forwarded-For")
	if got := adminClientIP(c); got == "" {
		t.Error("ไม่มี XFF ต้องใช้ ClientIP ของ gin")
	}
}

// ล็อกหมดอายุ (ครบ lockMinutes) -> ใช้ได้ใหม่
func TestAdminAuth_LockExpires(t *testing.T) {
	resetAdminAuthFails()
	defer resetAdminAuthFails()
	t.Setenv("ADMIN_TOKEN", "correct-token-123")
	t.Setenv("ADMIN_AUTH_MAX_FAILS", "1")
	t.Setenv("ADMIN_AUTH_LOCK_MINUTES", "1")

	// ยิงผิด 1 ครั้ง -> ล็อกทันที
	adminAuthNoteFail("198.51.100.99")
	locked, _ := adminAuthLocked("198.51.100.99")
	if !locked {
		t.Fatal("ต้องถูกล็อกหลังครบเกณฑ์")
	}

	// ปลอมล็อกให้หมดอายุ (ย้อนเวลา)
	adminAuthMu.Lock()
	adminAuthFails["198.51.100.99"].LockedUntil = time.Now().Add(-time.Second)
	adminAuthMu.Unlock()

	locked, _ = adminAuthLocked("198.51.100.99")
	if locked {
		t.Error("ล็อกหมดอายุต้องปลดแล้ว")
	}
}
