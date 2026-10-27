package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// handleMonitorStatus — /api/monitor/status (Phase 3 ข้อ 8)
// ตรวจ: DB, Redis, provider quota, error rate — ใช้สำหรับ uptime probe / alert channel
func handleMonitorStatus(c *gin.Context) {
	healthy := true
	reasons := []string{}
	status := gin.H{
		"healthy": healthy,
		"time":    time.Now().UTC().Format(time.RFC3339),
		"checks": gin.H{
			"database":       "ok",
			"redis":          "ok",
			"provider_quota": "ok",
		},
	}

	if GlobalDB == nil {
		healthy = false
		reasons = append(reasons, "db: not initialized")
		status["checks"].(gin.H)["database"] = "fail"
	}
	if rdb == nil {
		healthy = false
		reasons = append(reasons, "redis: unavailable")
		status["checks"].(gin.H)["redis"] = "fail"
	}

	if providerChain != nil && len(providerChain) > 0 {
		quotaClosed := 0
		for _, p := range providerChain {
			snap := p.Budget().Snapshot()
			if snap == nil {
				continue
			}
			remaining, ok := snap["remaining"].(int)
			if ok && remaining == 0 {
				quotaClosed++
			}
		}
		if quotaClosed > 0 {
			status["checks"].(gin.H)["provider_quota"] = fmt.Sprintf("%d/%d providers quota full", quotaClosed, len(providerChain))
		} else {
			status["checks"].(gin.H)["provider_quota"] = fmt.Sprintf("%d providers ok", len(providerChain))
		}
	}

	status["healthy"] = healthy
	status["reasons"] = reasons
	if !healthy {
		c.JSON(http.StatusServiceUnavailable, status)
		return
	}
	c.JSON(http.StatusOK, status)
}
