package main

import (
	"bytes"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

func handleHealthAlert(c *gin.Context) {
	// ส่ง webhook ไปยัง URL ใน env ALERT_WEBHOOK_URL (ถ้ามี)
	url := os.Getenv("ALERT_WEBHOOK_URL")
	if url != "" {
		go func() {
			http.Post(url, "application/json", bytes.NewBufferString(`{"status":"down","source":"scoretrack"}`))
		}()
	}
	c.JSON(http.StatusOK, gin.H{"alerted": url != ""})
}
