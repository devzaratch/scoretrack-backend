package main

import (
	"bufio"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// ============================================================
// Admin Panel — token-based auth (env ADMIN_TOKEN)
// Route: /api/admin/* (ป้องกันด้วย X-Admin-Token header)
//
// แผนข้อ 2 (ความปลอดภัย):
//   - เทียบ token แบบ constant-time (กัน timing attack)
//   - ล็อก IP ที่ยิงผิดต่อเนื่อง (ADMIN_AUTH_MAX_FAILS ครั้งใน 10 นาที
//     -> 429 นาน ADMIN_AUTH_LOCK_MINUTES นาที) — กัน brute-force token
//   - audit log ทุกคำขอที่เปลี่ยนแปลงข้อมูล (POST/PUT/DELETE)
//     ลง logs/admin-audit.log (เปลี่ยน path ได้ด้วย env ADMIN_AUDIT_LOG)
// ============================================================

// adminAuthFailState — นับความผิดต่อ IP
type adminAuthFailState struct {
	Fails       int
	WindowStart time.Time
	LockedUntil time.Time
}

var (
	adminAuthMu    sync.Mutex
	adminAuthFails = map[string]*adminAuthFailState{}
)

// adminAuthLimits — อ่านค่าจาก env (default: 5 ครั้ง / ล็อก 10 นาที)
func adminAuthLimits() (maxFails int, lockMinutes int) {
	maxFails = envInt("ADMIN_AUTH_MAX_FAILS", 5)
	lockMinutes = envInt("ADMIN_AUTH_LOCK_MINUTES", 10)
	if maxFails <= 0 {
		maxFails = 5
	}
	if lockMinutes <= 0 {
		lockMinutes = 10
	}
	return
}

// adminClientIP — IP ของแอดมิน (XFF ตัวสุดท้าย = ที่ proxy มองเห็น
// ตาม convention ของโปรเจคนี้ ไม่มี req.ip ฝั่ง Next)
func adminClientIP(c *gin.Context) string {
	xff := strings.TrimSpace(c.GetHeader("X-Forwarded-For"))
	if xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[len(parts)-1])
	}
	return c.ClientIP()
}

// adminAuthLocked — เช็คว่า IP นี้ถูกล็อกอยู่ไหม
func adminAuthLocked(ip string) (bool, int) {
	adminAuthMu.Lock()
	defer adminAuthMu.Unlock()

	st, ok := adminAuthFails[ip]
	if !ok {
		return false, 0
	}
	now := time.Now()
	if !st.LockedUntil.IsZero() && now.Before(st.LockedUntil) {
		return true, int(st.LockedUntil.Sub(now).Seconds()) + 1
	}
	// หมดช่วงเวลาแล้ว -> ล้างประวัติ
	if st.LockedUntil.IsZero() && now.Sub(st.WindowStart) > 10*time.Minute {
		delete(adminAuthFails, ip)
	}
	return false, 0
}

// adminAuthNoteFail — บันทึก 1 ความผิด (ครบเกณฑ์ -> ล็อก IP)
func adminAuthNoteFail(ip string) {
	maxFails, lockMinutes := adminAuthLimits()
	adminAuthMu.Lock()
	defer adminAuthMu.Unlock()

	now := time.Now()
	st, ok := adminAuthFails[ip]
	if !ok || (st.LockedUntil.IsZero() && now.Sub(st.WindowStart) > 10*time.Minute) {
		st = &adminAuthFailState{WindowStart: now}
		adminAuthFails[ip] = st
	}
	st.Fails++
	if st.Fails >= maxFails {
		st.LockedUntil = now.Add(time.Duration(lockMinutes) * time.Minute)
	}

	// กันหน่วยความจำบวมถ้ามี IP ยิงผิดเยอะ (ตัดรายการเก่าที่หมดอายุ)
	if len(adminAuthFails) > 1000 {
		for k, v := range adminAuthFails {
			if v.LockedUntil.IsZero() && now.Sub(v.WindowStart) > 10*time.Minute {
				delete(adminAuthFails, k)
			}
		}
	}
}

// adminAuthResetFails — สำเร็จ -> ล้างประวัติความผิดของ IP
func adminAuthResetFails(ip string) {
	adminAuthMu.Lock()
	defer adminAuthMu.Unlock()
	delete(adminAuthFails, ip)
}

// writeAdminAudit — บันทึกคำขอที่เปลี่ยนแปลงข้อมูล (JSON line ต่อแถว)
func writeAdminAudit(ip, method, path string, status int) {
	entry := map[string]interface{}{
		"time":   time.Now().UTC().Format(time.RFC3339),
		"ip":     ip,
		"method": method,
		"path":   path,
		"status": status,
	}
	line, _ := json.Marshal(entry)

	logPath := getEnv("ADMIN_AUDIT_LOG", "logs/admin-audit.log")
	if err := os.MkdirAll("logs", 0755); err == nil {
		if f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600); err == nil {
			_, _ = f.Write(append(line, '\n'))
			_ = f.Close()
		}
	}
	logInfof("📝 admin audit: %s %s -> %d (ip=%s)", method, path, status, ip)
}

// adminAuthMiddleware ตรวจสอบ X-Admin-Token กับ env ADMIN_TOKEN
func adminAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		ip := adminClientIP(c)

		// ถูกล็อกจากยิงผิดก่อนหน้า -> ไม่ต้องเช็ค token
		if locked, retrySec := adminAuthLocked(ip); locked {
			c.Header("Retry-After", strconv.Itoa(retrySec))
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error": "ยิงผิดต่อเนื่องเกินกำหนด -> ถูกระงับชั่วคราว",
			})
			c.Abort()
			return
		}

		token := c.GetHeader("X-Admin-Token")
		expected := getEnv("ADMIN_TOKEN", "")
		if expected == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "ADMIN_TOKEN ยังไม่ได้ตั้งค่าใน env"})
			c.Abort()
			return
		}
		// constant-time compare (กันเดา token จากระยะเวลาตอบ)
		if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(expected)) != 1 {
			adminAuthNoteFail(ip)
			logInfof("⚠️ admin auth ผิดจาก ip=%s %s %s", ip, c.Request.Method, c.Request.URL.Path)
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
			c.Abort()
			return
		}
		adminAuthResetFails(ip)
		c.Next()

		// audit ทุกคำขอที่เปลี่ยนแปลงข้อมูล (ของ GET ไม่บันทึก กันไฟล์บวม)
		if c.Request.Method != http.MethodGet {
			writeAdminAudit(ip, c.Request.Method, c.Request.URL.Path, c.Writer.Status())
		}
	}
}

// GET /admin/stats — ภาพรวมทั้งระบบ
func handleAdminStats(c *gin.Context) {
	stats := gin.H{
		"total_users":        0,
		"total_sessions":     0,
		"total_fcm_tokens":   0,
		"total_predictions":  0,
		"total_match_subs":   0,
		"total_achievements": 0,
		"cache_backend":      cacheBackendName(),
	}
	if GlobalDB != nil {
		stats["total_users"] = GlobalDB.CountUsers()
		stats["total_sessions"] = GlobalDB.CountSessions()
		stats["total_fcm_tokens"] = GlobalDB.CountFCMTokens()
		stats["total_predictions"] = GlobalDB.CountPredictions()
		stats["total_match_subs"] = GlobalDB.CountMatchSubscriptions()
		stats["total_achievements"] = GlobalDB.CountAchievements()
	}
	c.JSON(http.StatusOK, stats)
}

// GET /admin/users — รายชื่อผู้ใช้ทั้งหมด
func handleAdminUsers(c *gin.Context) {
	users := []map[string]interface{}{}
	if GlobalDB != nil {
		users = GlobalDB.GetAllUsers()
	}
	c.JSON(http.StatusOK, gin.H{"users": users, "count": len(users)})
}

// GET /admin/predictions — คำทำนายทั้งหมด
func handleAdminPredictions(c *gin.Context) {
	preds := map[string]map[string]interface{}{}
	if GlobalDB != nil {
		preds = GlobalDB.GetAllPredictions()
	}
	c.JSON(http.StatusOK, gin.H{"predictions": preds})
}

// GET /admin/client-errors — อ่าน log ล่าสุด 100 บรรทัด
func handleAdminClientErrors(c *gin.Context) {
	file, err := os.Open("logs/client-errors.log")
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"errors": []string{}, "message": "ยังไม่มี error log"})
		return
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() && len(lines) < 100 {
		lines = append(lines, scanner.Text())
	}
	// Reverse: ล่าสุดก่อน
	for i, j := 0, len(lines)-1; i < j; i, j = i+1, j-1 {
		lines[i], lines[j] = lines[j], lines[i]
	}
	c.JSON(http.StatusOK, gin.H{"errors": lines, "count": len(lines)})
}

// GET /admin/db-size — ขนาดไฟล์ DB
func handleAdminDBSize(c *gin.Context) {
	info, err := os.Stat("data/store.json")
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"size_bytes": 0, "message": "ไม่พบไฟล์ DB"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"size_bytes": info.Size()})
}

// --- Phase B ข้อ 4: จัดการบัญชี Auth V2 (email/Google) ---

// GET /admin/accounts — รายการบัญชี (profile) ทั้งหมด
func handleAdminAccounts(c *gin.Context) {
	accounts := []UserProfile{}
	if GlobalDB != nil {
		accounts = GlobalDB.ListUserProfiles()
	}
	c.JSON(http.StatusOK, gin.H{"accounts": accounts, "count": len(accounts)})
}

// POST /admin/accounts/:sub — action: ban | unban | role {"role":"user"|"admin"}
func handleAdminAccountAction(c *gin.Context) {
	sub := strings.TrimSpace(c.Param("sub"))
	if sub == "" || GlobalDB == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ไม่พบ sub"})
		return
	}
	var body struct {
		Action string `json:"action"` // ban | unban | role
		Role   string `json:"role"`
	}
	_ = c.BindJSON(&body)

	switch body.Action {
	case "ban":
		if !GlobalDB.SetUserBanned(sub, true) {
			c.JSON(http.StatusNotFound, gin.H{"error": "ไม่พบบัญชี"})
			return
		}
		// ออกจากระบบทุกเครื่องทันที (session + refresh หมด)
		GlobalDB.DeleteUserSessionsBySub(sub)
		GlobalDB.DeleteRefreshTokenBySub(sub)
		logInfof("🚫 admin แบนบัญชี: %s", sub)
	case "unban":
		if !GlobalDB.SetUserBanned(sub, false) {
			c.JSON(http.StatusNotFound, gin.H{"error": "ไม่พบบัญชี"})
			return
		}
		logInfof("✅ admin ปลดแบนบัญชี: %s", sub)
	case "role":
		if !GlobalDB.SetUserRole(sub, body.Role) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "action=role ต้องมี role = user หรือ admin และไม่พบบัญชี"})
			return
		}
		logInfof("🔧 admin เปลี่ยน role: %s -> %s", sub, body.Role)
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "action เป็น ban | unban | role เท่านั้น"})
		return
	}
	prof, _ := profileBySub(sub)
	c.JSON(http.StatusOK, gin.H{"status": "ok", "sub": sub, "account": prof})
}

// --- Phase B ข้อ 10: รายงานแชทฝั่ง admin ---

// GET /admin/chat-reports
func handleAdminChatReports(c *gin.Context) {
	reports := []ChatReport{}
	if GlobalDB != nil {
		reports = GlobalDB.ListChatReports()
	}
	c.JSON(http.StatusOK, gin.H{"reports": reports, "count": len(reports)})
}

// DELETE /admin/chat-reports/:idx — ปิด/ลบรายงาน (index จากตาราง newest-first)
func handleAdminChatReportDelete(c *gin.Context) {
	i, err := strconv.Atoi(c.Param("idx"))
	if err != nil || GlobalDB == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "idx ไม่ถูกต้อง"})
		return
	}
	// ListChatReports คืน newest-first — หา index เก่าใน DB = len-1-i
	dbIdx := len(GlobalDB.ListChatReports()) - 1 - i
	if !GlobalDB.RemoveChatReport(dbIdx) {
		c.JSON(http.StatusNotFound, gin.H{"error": "ไม่พบรายงาน"})
		return
	}
	logInfof("🗑️ admin ลบรายงานแชท idx=%d", i)
	c.JSON(http.StatusOK, gin.H{"status": "removed"})
}

// setupAdminRoutes ตั้งค่า admin routes ทั้งหมด
func setupAdminRoutes(rg *gin.RouterGroup) {
	admin := rg.Group("/admin", adminAuthMiddleware())
	{
		admin.GET("/stats", handleAdminStats)
		admin.GET("/users", handleAdminUsers)
		admin.GET("/accounts", handleAdminAccounts)
		admin.POST("/accounts/:sub", handleAdminAccountAction)
		admin.GET("/predictions", handleAdminPredictions)
		admin.GET("/client-errors", handleAdminClientErrors)
		admin.GET("/db-size", handleAdminDBSize)
		admin.GET("/metrics", handleAdminMetrics)
		admin.GET("/chat-reports", handleAdminChatReports)
		admin.DELETE("/chat-reports/:idx", handleAdminChatReportDelete)
		// Public API keys (แผนข้อ 7)
		admin.GET("/api-keys", handleAPIKeyList)
		admin.POST("/api-keys", handleAPIKeyCreate)
		admin.DELETE("/api-keys/:key", handleAPIKeyRevoke)
	}
}
