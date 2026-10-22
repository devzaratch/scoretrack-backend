package main

// interests — ความสนใจของผู้ใช้ (ติดตามทีม/ลีก) ใช้ทำหน้าแรกให้เป็นของเขา (แผนข้อ 4)
//
// ทำไม: หน้าแรกเดิมเหมือนกันทุกคน = "แมตช์ทั้งวัน" 48 คู่ ไม่มีอะไรให้คนกลับมา
// -> ให้ผู้ใช้กด "ติดตาม" ทีม/ลีกที่ชอบ แล้วหน้าแรกจัดลำดับให้คู่ที่ติดตามขึ้นก่อน
//    (ยังไม่ต้องไปแตะระบบแจ้งเตือนซับซ้อน — ทำทีละก้าว)
//
// key ที่เก็บ: "league:<id>" หรือ "team:<ชื่อทีมค้นอักษรเล็ก>"
// (payload แมตช์ของเราไม่มี team id มีแค่ชื่อ -> ใช้ชื่อเทียบแบบ case-insensitive)

import (
	"net/http"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
)

// normalizeInterestKey — แปลง kind+id เป็น key มาตรฐาน
func normalizeInterestKey(kind, id string) string {
	k := strings.ToLower(strings.TrimSpace(kind))
	i := strings.ToLower(strings.TrimSpace(id))
	if k == "" || i == "" {
		return ""
	}
	if k != "team" && k != "league" {
		return ""
	}
	return k + ":" + i
}

// handleUserInterests — GET /api/user/interests -> รายการที่ผู้ใช้ติดตาม
func handleUserInterests(c *gin.Context) {
	userKey := resolveUserKey(c)
	keys := []string{}
	if GlobalDB != nil {
		keys = GlobalDB.GetUserInterests(userKey)
	}
	leagues := []string{}
	teams := []string{}
	for _, k := range keys {
		if strings.HasPrefix(k, "league:") {
			leagues = append(leagues, strings.TrimPrefix(k, "league:"))
		} else if strings.HasPrefix(k, "team:") {
			teams = append(teams, strings.TrimPrefix(k, "team:"))
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"interests": keys,
		"leagues":   leagues,
		"teams":     teams,
	})
}

// handleUserInterestToggle — POST /api/user/interests {kind, id, follow}
// follow = true -> ติดตาม, false -> เลิกติดตาม
func handleUserInterestToggle(c *gin.Context) {
	var body struct {
		Kind   string `json:"kind"`
		ID     string `json:"id"`
		Follow bool   `json:"follow"`
	}
	if err := c.BindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ข้อมูลไม่ถูกต้อง"})
		return
	}
	key := normalizeInterestKey(body.Kind, body.ID)
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "kind ต้องเป็น team หรือ league"})
		return
	}
	if GlobalDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "ฐานข้อมูลไม่พร้อม"})
		return
	}

	userKey := resolveUserKey(c)
	interests := GlobalDB.SetUserInterest(userKey, key, body.Follow)
	logInfof("👀 ผู้ใช้ [%s] %s ทีม/ลีก: %s (ตอนนี้ %d รายการ)", userKey,
		map[bool]string{true: "ติดตาม", false: "เลิกติดตาม"}[body.Follow], key, len(interests))

	c.JSON(http.StatusOK, gin.H{
		"status":    "ok",
		"following": body.Follow,
		"interests": interests,
	})
}

// sortedInterests — เรียง key ให้คงที่ (กันลำดับสุ่มตอนอ่านจาก map)
func sortedInterests(keys map[string]bool) []string {
	out := make([]string, 0, len(keys))
	for k := range keys {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
