package main

import (
	"bufio"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

// ============================================================
// Admin Panel — token-based auth (env ADMIN_TOKEN)
// Route: /api/admin/* (ป้องกันด้วย X-Admin-Token header)
// ============================================================

// adminAuthMiddleware ตรวจสอบ X-Admin-Token กับ env ADMIN_TOKEN
func adminAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.GetHeader("X-Admin-Token")
		expected := getEnv("ADMIN_TOKEN", "")
		if expected == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "ADMIN_TOKEN ยังไม่ได้ตั้งค่าใน env"})
			c.Abort()
			return
		}
		if token == "" || token != expected {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
			c.Abort()
			return
		}
		c.Next()
	}
}

// GET /admin/stats — ภาพรวมทั้งระบบ
func handleAdminStats(c *gin.Context) {
	stats := gin.H{
		"total_users":          0,
		"total_sessions":       0,
		"total_fcm_tokens":     0,
		"total_predictions":    0,
		"total_match_subs":     0,
		"total_achievements":   0,
		"cache_backend":        cacheBackendName(),
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

// setupAdminRoutes ตั้งค่า admin routes ทั้งหมด
func setupAdminRoutes(rg *gin.RouterGroup) {
	admin := rg.Group("/admin", adminAuthMiddleware())
	{
		admin.GET("/stats", handleAdminStats)
		admin.GET("/users", handleAdminUsers)
		admin.GET("/predictions", handleAdminPredictions)
		admin.GET("/client-errors", handleAdminClientErrors)
		admin.GET("/db-size", handleAdminDBSize)
		admin.GET("/metrics", handleAdminMetrics)
	}
}
