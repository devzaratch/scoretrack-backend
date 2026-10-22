package main

// apikey — Public API + API key (แผนข้อ 7)
//
// เปิด REST API ให้คนภายนอกใช้ (นักพัฒนา/สื่อ/แอปอื่น) ด้วย API key ที่ต้องขอจากแอดมิน
//   - key เก็บใน PersistentDB (เหมือนข้อมูลอื่น) พร้อมโควตาราย key/วัน + ต่อนาที
//   - ยืนยันด้วย Authorization: Bearer <key> หรือ X-API-Key: <key>
//   - ตอบ 401 (ไม่มี/ผิด) · 403 (ถูกปิดใช้) · 429 (โควตาหมด) + header X-RateLimit-*
//   - ทุกครั้งที่ใช้ key จะบันทึก last_used_at + calls_today (เห็นในหน้าแอดมิน)
//
// ข้อควรระวัง: endpoint /api/v1/* เป็นของสาธารณะภายนอก — ห้ามเอา endpoint ที่แตะข้อมูล
// ส่วนบุคคล (favorites/predictions ของผู้ใช้) มาไว้ใต้ /v1 เด็ดขาด

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// APIKeyRecord — ข้อมูลของ API key 1 ตัว
type APIKeyRecord struct {
	Name        string `json:"name"`
	Owner       string `json:"owner"`
	CreatedAt   int64  `json:"created_at"`
	LastUsedAt  int64  `json:"last_used_at"`
	CallsToday  int    `json:"calls_today"`
	DayKey      string `json:"day_key"`
	DailyLimit  int    `json:"daily_limit"`
	MinuteLimit int    `json:"minute_limit"`
	Enabled     bool   `json:"enabled"`
	// นับต่อนาที (ในหน่วยความจริง ไม่ต้อง persist ทุกครั้ง)
	MinuteCalls  int   `json:"minute_calls"`
	MinuteWindow int64 `json:"minute_window"`
}

// apiKeyPrefix — id สั้นของ key (ใช้แสดงในหน้า admin + ใช้เพิกถอน)
func apiKeyPrefix(key string) string {
	if len(key) <= 16 {
		return key
	}
	return key[:16]
}

// apiKeyIDPrefix — ขึ้นต้นด้วย sk_ เพื่อให้รู้ว่าเป็น key ของเรา (และไม่ไปชนกับ bearer ของ user)
const apiKeyIDPrefix = "sk_live_"

// generateAPIKey — สร้าง key ใหม่ (สุ่ม 24 ไบต์ = 48 ตัว hex)
func generateAPIKey() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return apiKeyIDPrefix + hex.EncodeToString(buf), nil
}

// apiKeyFromRequest — ดึง key จาก Authorization: Bearer หรือ X-API-Key
func apiKeyFromRequest(c *gin.Context) string {
	if v := strings.TrimSpace(c.GetHeader("X-API-Key")); v != "" {
		return v
	}
	auth := strings.TrimSpace(c.GetHeader("Authorization"))
	if auth == "" {
		return ""
	}
	lower := strings.ToLower(auth)
	if strings.HasPrefix(lower, "bearer ") {
		return strings.TrimSpace(auth[len("bearer "):])
	}
	return ""
}

// apiKeyAuth — middleware ตรวจ API key + โควตา
func apiKeyAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		key := apiKeyFromRequest(c)
		if key == "" {
			c.Header("WWW-Authenticate", `Bearer realm="scoretrack-api"`)
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": "missing api key",
				"hint":  "ส่ง Authorization: Bearer <api key> หรือ X-API-Key: <api key>",
			})
			c.Abort()
			return
		}

		rec, ok := GlobalDB.GetAPIKey(key)
		if !ok {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid api key"})
			c.Abort()
			return
		}
		if !rec.Enabled {
			c.JSON(http.StatusForbidden, gin.H{"error": "api key ถูกปิดใช้งาน"})
			c.Abort()
			return
		}

		now := time.Now()
		// รีเซ็ตตัวนับวัน/นาทีถ้าข้ามช่วงเวลา
		today := now.Format("2006-01-02")
		if rec.DayKey != today {
			rec.DayKey = today
			rec.CallsToday = 0
		}
		if now.Unix()-rec.MinuteWindow >= 60 {
			rec.MinuteWindow = now.Unix()
			rec.MinuteCalls = 0
		}

		dailyLimit := rec.DailyLimit
		if dailyLimit <= 0 {
			dailyLimit = 1000
		}
		minuteLimit := rec.MinuteLimit
		if minuteLimit <= 0 {
			minuteLimit = 60
		}

		c.Header("X-RateLimit-Limit", strconv.Itoa(dailyLimit))
		c.Header("X-RateLimit-Minute-Limit", strconv.Itoa(minuteLimit))

		if rec.CallsToday >= dailyLimit {
			c.Header("Retry-After", "3600")
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":      "daily quota หมดแล้ว",
				"limit":      dailyLimit,
				"resets_at":  time.Now().UTC().Truncate(24 * time.Hour).Add(24 * time.Hour).Format(time.RFC3339),
				"used_today": rec.CallsToday,
			})
			c.Abort()
			return
		}
		if rec.MinuteCalls >= minuteLimit {
			c.Header("Retry-After", "60")
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error": "เรียกถี่เกินโควตาต่อนาที",
				"limit": minuteLimit,
				"used":  rec.MinuteCalls,
			})
			c.Abort()
			return
		}

		rec.CallsToday++
		rec.MinuteCalls++
		rec.LastUsedAt = now.Unix()
		GlobalDB.SaveAPIKey(key, rec)

		// remaining สะท้อนหลังนับคำขอนี้แล้ว (เหมือน convention ของ API ส่วนใหญ่)
		c.Header("X-RateLimit-Remaining", strconv.Itoa(maxInt(dailyLimit-rec.CallsToday, 0)))

		c.Next()
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ---------------------------------------------------------------------------
// Admin: จัดการ key
// ---------------------------------------------------------------------------

// APIKeyView — ข้อมูล key สำหรับแสดงผลในหน้าแอดมิน
// (ไม่คืนค่า key เต็ม เพื่อไม่ให้ key หลุดในหน้า admin — ใช้ key_id (prefix) แทน)
type APIKeyView struct {
	APIKeyRecord
	KeyID string `json:"key_id"`
}

// handleAPIStatus — GET /api/v1/status (ไม่ต้อง key) = service รันอยู่ไหม + เวอร์ชัน
func handleAPIStatus(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"service":   "scoretrack-api",
		"version":   "v1",
		"status":    "ok",
		"time":      time.Now().UTC().Format(time.RFC3339),
		"auth":      "ส่ง Authorization: Bearer <api key> หรือ X-API-Key: <api key>",
		"endpoints": []string{"/api/v1/matches", "/api/v1/match/:id", "/api/v1/league/:id", "/api/v1/league/:id/table", "/api/v1/team/:id", "/api/v1/team/:id/squad", "/api/v1/player/:id", "/api/v1/news", "/api/v1/calendar"},
		"docs":      "/api/v1/openapi.json",
	})
}

// handleAPIKeyCreate — POST /api/admin/api-keys -> สร้าง key ใหม่ (คืน key เต็ม 1 ครั้ง)
func handleAPIKeyCreate(c *gin.Context) {
	var body struct {
		Name        string `json:"name"`
		Owner       string `json:"owner"`
		DailyLimit  int    `json:"daily_limit"`
		MinuteLimit int    `json:"minute_limit"`
	}
	_ = c.BindJSON(&body)
	name := strings.TrimSpace(body.Name)
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ต้องระบุชื่อ key"})
		return
	}
	if GlobalDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "ฐานข้อมูลไม่พร้อม"})
		return
	}

	key, err := generateAPIKey()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "สร้าง key ไม่สำเร็จ"})
		return
	}
	daily := body.DailyLimit
	if daily <= 0 {
		daily = 1000
	}
	minute := body.MinuteLimit
	if minute <= 0 {
		minute = 60
	}
	rec := APIKeyRecord{
		Name:        name,
		Owner:       strings.TrimSpace(body.Owner),
		CreatedAt:   time.Now().Unix(),
		DayKey:      time.Now().Format("2006-01-02"),
		DailyLimit:  daily,
		MinuteLimit: minute,
		Enabled:     true,
	}
	GlobalDB.SaveAPIKey(key, rec)

	logInfof("🔑 สร้าง API key: %s (owner=%s, %d/วัน)", name, rec.Owner, daily)
	c.JSON(http.StatusCreated, gin.H{
		"key":    key,
		"record": rec,
		"note":   "เก็บ key นี้ไว้เลย — แสดงค่าเต็มครั้งเดียวเท่านั้น",
	})
}

// handleAPIKeyList — GET /api/admin/api-keys
func handleAPIKeyList(c *gin.Context) {
	if GlobalDB == nil {
		c.JSON(http.StatusOK, gin.H{"keys": []APIKeyView{}})
		return
	}
	c.JSON(http.StatusOK, gin.H{"keys": GlobalDB.ListAPIKeyViews()})
}

// handleAPIKeyRevoke — DELETE /api/admin/api-keys/:key_id (ใช้ prefix ของ key)
// ค้นจาก prefix เพราะในรายการไม่แสดงค่า key เต็ม (กัน key หลุดในหน้า admin)
func handleAPIKeyRevoke(c *gin.Context) {
	keyID := strings.TrimSpace(c.Param("key"))
	if keyID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ไม่พบ key"})
		return
	}
	if GlobalDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "ฐานข้อมูลไม่พร้อม"})
		return
	}
	full := GlobalDB.FindAPIKeyByPrefix(keyID)
	if full == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "ไม่พบ key"})
		return
	}
	if !GlobalDB.DeleteAPIKey(full) {
		c.JSON(http.StatusNotFound, gin.H{"error": "ไม่พบ key"})
		return
	}
	logInfof("🔑 เพิกถอน API key: %s...", keyID)
	c.JSON(http.StatusOK, gin.H{"status": "revoked", "key_id": keyID})
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
