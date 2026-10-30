package main

// user_selfserve — Phase B ข้อ 5 + ข้อ 10
//
// ข้อ 5: API key self-service — ผู้ใช้ (session cookie) สร้าง/ดู/เพิกถอน key ของตัวเองได้
//   โดยไม่ต้องยุ่งกับ token แอดมิน (ทางเดียวกับ apikey.go แต่ผูกกับผู้ใช้)
//   - key เก็บ hash (sha256) เหมือนระบบเดิม — คืนค่า "เต็ม" ครั้งเดียวตอนสร้าง
//   - จำกัด 3 key ที่ยังเปิดใช้ต่อผู้ใช้ 1 คน (กันสร้างกองทิ้ง)
//   - โควตาเริ่มต้น 1,000/วัน และ 60/นาที (เหมือนทาง admin)
//
// ข้อ 10: รายงานข้อความแชท — POST /api/chat/report (log แบบเปิดได้ จำกัดจำนวน)

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// userKeyViews — key ของผู้ใช้เอง (ใช้ APIKeyView ของ apikey.go) grep ว่าอยู่ถาดีร์
func selfServeKeyViews(sub string) []APIKeyView {
	out := []APIKeyView{}
	if GlobalDB == nil {
		return out
	}
	for _, v := range GlobalDB.ListAPIKeyViews() {
		if strings.TrimSpace(v.Owner) == sub {
			out = append(out, v)
		}
	}
	return out
}

// handleUserAPIKeyList — GET /api/auth/api-keys
func handleUserAPIKeyList(c *gin.Context) {
	sub := subFromCookie(c)
	if sub == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not logged in"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"keys": selfServeKeyViews(sub)})
}

// handleUserAPIKeyCreate — POST /api/auth/api-keys {name}
func handleUserAPIKeyCreate(c *gin.Context) {
	sub := subFromCookie(c)
	if sub == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not logged in"})
		return
	}
	if GlobalDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "db unavailable"})
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	_ = c.BindJSON(&body)
	name := strings.TrimSpace(body.Name)
	if name == "" || len(name) > 60 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ต้องระบุชื่อ key (ไม่เกิน 60 ตัวอักษร)"})
		return
	}

	mine := selfServeKeyViews(sub)
	active := 0
	for _, k := range mine {
		if k.Enabled {
			active++
		}
	}
	if active >= 3 {
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "มี key ที่เปิดใช้ครบ 3 ตัวแล้ว — ปิดหรือเพิกถอนก่อนสร้างใหม่"})
		return
	}

	key, err := generateAPIKey()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "สร้าง key ไม่สำเร็จ"})
		return
	}
	rec := APIKeyRecord{
		Name:        name,
		Owner:       sub,
		CreatedAt:   time.Now().Unix(),
		DayKey:      time.Now().Format("2006-01-02"),
		DailyLimit:  1000,
		MinuteLimit: 60,
		Enabled:     true,
	}
	GlobalDB.SaveAPIKey(key, rec)
	logInfof("🔑 self-service key: %s owner=%s", name, sub)
	c.JSON(http.StatusCreated, gin.H{
		"key": key,
		"record": rec,
		"note": "เก็บ key นี้ไว้เลย — แสดงค่าเต็มครั้งเดียวเท่านั้น",
	})
}

// handleUserAPIKeyRevoke — DELETE /api/auth/api-keys/:key_id (prefix)
func handleUserAPIKeyRevoke(c *gin.Context) {
	sub := subFromCookie(c)
	if sub == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not logged in"})
		return
	}
	if GlobalDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "db unavailable"})
		return
	}
	keyID := strings.TrimSpace(c.Param("key"))
	hash := GlobalDB.FindAPIKeyHashByPrefix(keyID)
	if hash == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "ไม่พบ key"})
		return
	}
	// ป้องกันาด้านสิทธิ์: ลบได้เฉพาะ key ของตัวเอง (owner == session sub)
	if rec, ok := GlobalDB.GetAPIKeyByHash(hash); ok {
		if strings.TrimSpace(rec.Owner) != sub {
			c.JSON(http.StatusForbidden, gin.H{"error": "key นี้ไม่ใช่ของคุณ"})
			return
		}
	}
	if !GlobalDB.DeleteAPIKeyByHash(hash) {
		c.JSON(http.StatusNotFound, gin.H{"error": "ไม่พบ key"})
		return
	}
	logInfof("🔑 ผู้ใช้เพิกถอน key ของตัวเอง sub=%s %s...", sub, keyID[:minInt(len(keyID), 12)])
	c.JSON(http.StatusOK, gin.H{"status": "revoked", "key_id": keyID})
}

// --- ข้อ 10: รายงานข้อความแชท ---

// handleChatReport — POST /api/chat/report
// body: { match_id, user, text, reason } — ผู้รายงานได้จาก IP ให้แอดมินติดตาม
func handleChatReport(c *gin.Context) {
	var body struct {
		MatchID string `json:"match_id"`
		User    string `json:"user"`
		Text    string `json:"text"`
		Reason  string `json:"reason"`
	}
	if err := c.BindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "match_id และเหตุผลจำเป็น"})
		return
	}
	reason := strings.TrimSpace(body.Reason)
	if reason == "" || len(reason) > 200 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ระบุเหตุผลรายงาน (ไม่เกิน 200 ตัวอักษร)"})
		return
	}
	if GlobalDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "db unavailable"})
		return
	}

	text := strings.TrimSpace(body.Text)
	if len(text) > 300 {
		text = text[:300]
	}
	rep := ChatReport{
		MatchID:    strings.TrimSpace(body.MatchID),
		User:       strings.TrimSpace(body.User),
		Text:       text,
		Reason:     reason,
		ReporterIP: adminClientIP(c),
		ReportedAt: time.Now().Unix(),
	}
	GlobalDB.AddChatReport(rep)
	logInfof("🚨 chat report: room=%s user=%q reason=%q", rep.MatchID, rep.User, reason)
	c.JSON(http.StatusOK, gin.H{"status": "reported"})
}

// handleChatHistory — GET /api/chat/history/:matchId?limit=50
// คืนประวัติแชทในห้อง (จาก PersistentDB ChatMessages ที่ SaveChatMessage เก็บ
// ตอน broadcast — แหล่งเดียวกับที่ WS ส่งเข้ามา จึงเห็นเก่าครบถึง 100 ข้อความล่าสุด)
// ใช้เมื่อ WS ยังไม่ส่งต่อสด หรือผู้ใช้ reload ห้องแชทข้าม session
func handleChatHistory(c *gin.Context) {
	matchID := c.Param("matchId")
	if matchID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "match_id จำเป็น"})
		return
	}
	// SaveChatMessage เก็บสูงสุด 100 — clamp ให้อยู่ช่วงที่มีข้อมูลจริง
	limit := 50
	if v, err := strconv.Atoi(c.DefaultQuery("limit", "50")); err == nil {
		limit = v
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	if GlobalDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "db unavailable"})
		return
	}
	history := GlobalDB.GetChatHistory(matchID, limit)
	if history == nil {
		history = []map[string]interface{}{}
	}
	c.JSON(http.StatusOK, gin.H{
		"match_id": matchID,
		"count":    len(history),
		"messages": history,
	})
}
