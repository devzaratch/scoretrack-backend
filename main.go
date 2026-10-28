package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var (
	ctx = context.Background()
	rdb *redis.Client

	// WebSocket upgrader
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	// WebSocket Clients ถูกนับผ่าน GlobalLiveHub.ClientCount() แล้ว
	// (ตัวแปร wsClients เดิมถูกลบ เพราะไม่เคยถูกเติมค่า -> ticker ไม่เคยทำงาน)

	// In-Memory Favorites & FCM tokens
	userFavorites   = make(map[string][]int)
	userFavoritesMu sync.Mutex
)

// In-Memory Cache with TTL for fallback when Redis is absent
type CacheItem struct {
	Data      []byte
	ExpiresAt time.Time
}

type MemoryCache struct {
	mu    sync.RWMutex
	items map[string]CacheItem
}

var memoryCache = &MemoryCache{
	items: make(map[string]CacheItem),
}

func (c *MemoryCache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, found := c.items[key]
	if !found {
		return nil, false
	}
	if time.Now().After(item.ExpiresAt) {
		return nil, false
	}
	return item.Data, true
}

func (c *MemoryCache) Set(key string, data []byte, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items[key] = CacheItem{
		Data:      data,
		ExpiresAt: time.Now().Add(ttl),
	}
}

const (
	DefaultRapidAPIHost = "fotmob4.p.rapidapi.com"
)

func getRapidAPIKey() string {
	key := os.Getenv("RAPIDAPI_KEY")
	if key == "" {
		key = getEnv("RAPIDAPI_KEY", "")
	}
	if key == "" {
		logInfoln("⚠️ RAPIDAPI_KEY is not set in environment or .env.local")
	}
	return key
}

// Main entry point for Go Backend API server
func main() {
	loadEnvFile(".env.local")
	loadEnvFile("../.env.local")

	// 0. เตรียม Data Provider layer (GOAL API เป็นหลัก + คุมโควตา + ID registry)
	initProviders()

	// 1. ตรวจสอบบริการ Redis (รองรับ Upstash: REDIS_PASSWORD + REDIS_TLS)
	redisAddr := getEnv("REDIS_ADDR", "127.0.0.1:6379")
	redisPassword := getEnv("REDIS_PASSWORD", "")
	useRedisTLS := strings.EqualFold(getEnv("REDIS_TLS", "false"), "true")
	conn, err := net.DialTimeout("tcp", redisAddr, 500*time.Millisecond)
	if err != nil {
		logInfoln("ℹ️ Info: ไม่พบ Redis Server -> สลับไปใช้ Direct Proxy & Mock Fallback (ปกติ หากไม่ได้เปิด Redis)")
		rdb = nil
	} else {
		conn.Close()
		opts := &redis.Options{
			Addr:     redisAddr,
			Password: redisPassword,
			DB:       0,
		}
		if useRedisTLS {
			opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		client := redis.NewClient(opts)
		// PING ยืนยันก่อนใช้จริง — Upstash ต้องผ่าน TLS + AUTH ก่อน (TCP connect อย่างเดียวไม่พอ)
		pingCtx, cancelPing := context.WithTimeout(ctx, 3*time.Second)
		pingErr := client.Ping(pingCtx).Err()
		cancelPing()
		if pingErr != nil {
			logInfof("ℹ️ Info: Redis PING ไม่สำเร็จ (%v) -> ใช้ in-memory cache แทน", pingErr)
			client.Close()
			rdb = nil
		} else {
			rdb = client
			logInfoln("✅ เชื่อมต่อ Redis สำเร็จ!")
		}
	}

	// เริ่ม Janitor สำหรับ in-memory cache (ถ้าไม่มี Redis)
	startJanitor()

	// เริ่มต้นหมุน Background Routine สำหรับยิง WebSocket Live Tick ทุก 5 วินาที
	// 0. Initialize Persistent Database
	InitDB()

	// 1. Start Non-blocking WebSocket Hubs
	go GlobalLiveHub.Run()
	go GlobalChatHub.Run()

	// 2. Start Live Ticker Engine
	go startLiveTicker()

	// 3. SLO + alerting + synthetic monitoring (แผนข้อ 9)
	//    - ประเมิน availability/latency/freshness/provider/โควตา ทุก 60 วิ
	//    - แจ้งเตือน Telegram เมื่อสถานะเปลี่ยน (ยังไม่ได้ตั้ง env = log อย่างเดียว)
	//    - ยิงตรวจ endpoint ของตัวเองเป็นระยะ (healthz + sources)
	go startSLOLoop()
	go startSyntheticMonitor()

	// 4. Cache warmer (แผนข้อ 2) — เติม cache เป็นระยะ + กัน Render cold start
	go startCacheWarmer()

	// 5. Pre-match reminder (แผนใหม่ข้อ 3) — เตือนก่อนบอลเตะให้คนที่กด 🔔 ไว้
	go startPreMatchReminder()

	// 6. Backup restore drill (แผนใหม่ข้อ 10) — พิสูจน์ว่า snapshot กู้ได้จริง
	go startBackupDrillLoop()

	// 7. Season archive (แผนใหม่ข้อ 4) — เก็บผลแมตช์จบลง Redis/memory
	go startArchiveSweeper()

	// gin.New + middleware ของเราเอง (แผนข้อ 9): gin.Default() = [GIN] free text
	// logger + Recovery ไม่มี field — ตอนนี้ request log เป็น JSON ทุกบรรทัด
	// (observability.go) + panic เก็บ stack ลง structured log
	r := gin.New()
	// requestIDMiddleware ต้องมาก่อนสุด — recovery/log/rate-limit ทุกตัวใช้ ID เดียวกัน
	r.Use(requestIDMiddleware(), recoveryMiddleware(), requestLogMiddleware())

	// หน้าแรก Root / สำหรับตรวจสอบสถานะ Server
	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "online",
			"message": "🚀 Score-track Go Backend is running!",
			"endpoints": gin.H{
				"matches":   "/api/matches",
				"sources":   "/api/_sources",
				"match":     "/api/match/:id",
				"league":    "/api/league/:id",
				"team":      "/api/team/:id",
				"player":    "/api/player/:id",
				"websocket": "/ws/live",
			},
		})
	})

	// /healthz — ตรวจ dependency จริง (Redis/DB/WS) ไม่ใช่แค่ "process ยังอยู่"
	// ใช้กับ uptime monitor + Render health check:
	// - 200 = ทุก component พร้อมใช้งาน
	// - 503 = มี component สำคัญล่ม (ใช้ alert ได้ ต่างจาก / ที่ตอบ 200 เสมอ)
	r.GET("/healthz", handleHealthz)

	// 2. ตั้งค่า CORS รองรับทั้ง Localhost และ Production บน Vercel
	r.Use(cors.New(cors.Config{
		AllowOriginFunc: func(origin string) bool {
			// โดเมน production ตั้งค่าผ่าน env ALLOWED_ORIGINS (คั่นด้วย comma)
			// ตัวอย่าง: ALLOWED_ORIGINS=https://doballlaos.com,https://www.doballlaos.com
			for _, allowed := range strings.Split(getEnv("ALLOWED_ORIGINS", ""), ",") {
				allowed = strings.TrimSpace(allowed)
				if allowed != "" && strings.EqualFold(allowed, origin) {
					return true
				}
			}

			// อนุญาต localhost เสมอสำหรับการพัฒนา
			if strings.HasPrefix(origin, "http://localhost:") ||
				strings.HasPrefix(origin, "http://127.0.0.1:") ||
				origin == "http://localhost" ||
				origin == "http://127.0.0.1" {
				return true
			}

			// เปิด preview บน Vercel ได้เมื่อตั้ง ALLOW_VERCEL_PREVIEW=true เท่านั้น
			// (อันตราย: ใครก็สร้างเว็บ *.vercel.app มาเรียก API เราได้)
			if getEnv("ALLOW_VERCEL_PREVIEW", "false") == "true" && strings.HasSuffix(origin, ".vercel.app") {
				return true
			}

			return false
		},
		AllowMethods:     []string{"GET", "POST", "OPTIONS", "PUT", "DELETE"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization", "X-Requested-With", "X-Admin-Token"},
		ExposeHeaders:    []string{"Content-Length", "X-Cache", "X-Cache-Backend", "X-Data-Source", "X-Data-Fetched-At", "X-Data-Age-Seconds", "X-Data-Stale"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	// Rate Limiter Middleware
	r.Use(rateLimiterMiddleware())

	// WebSocket Endpoints สำหรับ Live Score & Live Chat real-time updates
	r.GET("/ws/live", handleWebSocket)
	r.GET("/ws/chat/:id", handleChatWebSocket)

	// Handlers with optimized cache TTLs to conserve API Quota
	matchesHandler := handleProxy("matches:%s", 60*time.Second, func(c *gin.Context) string {
		date := c.DefaultQuery("date", time.Now().Format("20060102"))
		return fmt.Sprintf("https://www.fotmob.com/api/matches?date=%s", date)
	})

	matchDetailsHandler := handleProxy("match:%s", 2*time.Minute, func(c *gin.Context) string {
		matchID := c.Param("id")
		if matchID == "" {
			matchID = c.Query("matchId")
		}
		if matchID == "" {
			matchID = c.Query("id")
		}
		return fmt.Sprintf("https://www.fotmob.com/api/matchDetails?matchId=%s", matchID)
	})

	leagueURLBuilder := func(c *gin.Context) string {
		leagueID := c.Param("id")
		if leagueID == "" {
			leagueID = c.Query("id")
		}
		season := c.Query("season")
		if season != "" {
			return fmt.Sprintf("https://www.fotmob.com/api/leagues?id=%s&season=%s", leagueID, season)
		}
		return fmt.Sprintf("https://www.fotmob.com/api/leagues?id=%s", leagueID)
	}
	// แยก cache key ต่อ sub-route (ก่อนหน้านี้ /league/:id/table ชนกับ /league/:id)
	leagueHandler := handleProxy("league:%s", 30*time.Minute, leagueURLBuilder)
	leagueTableHandler := handleProxy("league:table:%s", 10*time.Minute, leagueURLBuilder)
	leagueFixturesHandler := handleProxy("league:fixtures:%s", 10*time.Minute, leagueURLBuilder)
	leagueStatsHandler := handleProxy("league:stats:%s", 30*time.Minute, leagueURLBuilder)

	teamHandler := handleProxy("team:%s", 30*time.Minute, func(c *gin.Context) string {
		teamID := c.Param("id")
		if teamID == "" {
			teamID = c.Query("id")
		}
		return fmt.Sprintf("https://www.fotmob.com/api/teams?id=%s", teamID)
	})

	teamSquadHandler := handleProxy("team:squad:%s", 2*time.Hour, func(c *gin.Context) string {
		teamID := c.Param("id")
		if teamID == "" {
			teamID = c.Query("id")
		}
		return fmt.Sprintf("https://www.fotmob.com/api/teams?id=%s&squad=true", teamID)
	})

	teamFixturesHandler := handleProxy("team:fixtures:%s", 1*time.Hour, func(c *gin.Context) string {
		teamID := c.Param("id")
		if teamID == "" {
			teamID = c.Query("id")
		}
		return fmt.Sprintf("https://www.fotmob.com/api/teams?id=%s&fixtures=true", teamID)
	})

	playerHandler := handleProxy("player:%s", 2*time.Hour, func(c *gin.Context) string {
		playerID := c.Param("id")
		if playerID == "" {
			playerID = c.Query("id")
		}
		return fmt.Sprintf("https://www.fotmob.com/api/playerData?id=%s", playerID)
	})

	// กล่องค้นหา: GOAL 3 endpoint (FotMob suggest ตายแล้ว 404) — ดู phase1b_search.go
	searchHandler := searchSuggestHandler

	// Register Routes under /api and /api/api (Safety Alias)
	setupRoutes := func(rg *gin.RouterGroup) {
		// Sources & Quota status (Phase 1: Data Layer)
		rg.GET("/_sources", handleSourcesStatus)

		// Search (limit 30/min — แพง GOAL API)
		rg.GET("/search", rateLimitMiddleware("search", RLSearch, time.Minute), searchHandler)
		rg.GET("/search/suggest", rateLimitMiddleware("search", RLSearch, time.Minute), searchHandler)

		// News (limit 30/min — RSS)
		rg.GET("/news", rateLimitMiddleware("news", RLNews, time.Minute), newsHandler)

		// Matches (limit 60/min — proxy)
		rg.GET("/matches", rateLimitMiddleware("proxy", RLProxy, time.Minute), matchesHandler)
		rg.GET("/matchesDay", rateLimitMiddleware("proxy", RLProxy, time.Minute), matchesHandler)
		rg.GET("/live", rateLimitMiddleware("proxy", RLProxy, time.Minute), matchesHandler)
		rg.GET("/allMatches", rateLimitMiddleware("proxy", RLProxy, time.Minute), matchesHandler)
		rg.GET("/match/live", rateLimitMiddleware("proxy", RLProxy, time.Minute), matchesHandler)

		// Match Details (limit 60/min)
		rg.GET("/match/:id", rateLimitMiddleware("proxy", RLProxy, time.Minute), matchDetailsHandler)
		rg.GET("/match/:id/commentary", rateLimitMiddleware("proxy", RLProxy, time.Minute), matchDetailsHandler)
		rg.GET("/match", rateLimitMiddleware("proxy", RLProxy, time.Minute), matchDetailsHandler)
		rg.GET("/matchDetails", rateLimitMiddleware("proxy", RLProxy, time.Minute), matchDetailsHandler)

		// League (limit 60/min)
		rg.GET("/league/:id", rateLimitMiddleware("proxy", RLProxy, time.Minute), leagueHandler)
		rg.GET("/league/:id/table", rateLimitMiddleware("proxy", RLProxy, time.Minute), leagueTableHandler)
		rg.GET("/league/:id/fixtures", rateLimitMiddleware("proxy", RLProxy, time.Minute), leagueFixturesHandler)
		rg.GET("/league/:id/stats", rateLimitMiddleware("proxy", RLProxy, time.Minute), leagueStatsHandler)
		rg.GET("/league", rateLimitMiddleware("proxy", RLProxy, time.Minute), leagueHandler)
		rg.GET("/leagues", rateLimitMiddleware("proxy", RLProxy, time.Minute), leagueHandler)

		// Team (limit 60/min)
		rg.GET("/team/:id/squad", rateLimitMiddleware("proxy", RLProxy, time.Minute), teamSquadHandler)
		rg.GET("/team/:id/fixtures", rateLimitMiddleware("proxy", RLProxy, time.Minute), teamFixturesHandler)
		rg.GET("/team/:id", rateLimitMiddleware("proxy", RLProxy, time.Minute), teamHandler)
		rg.GET("/team", rateLimitMiddleware("proxy", RLProxy, time.Minute), teamHandler)
		rg.GET("/teams", rateLimitMiddleware("proxy", RLProxy, time.Minute), teamHandler)

		// Player (limit 60/min)
		rg.GET("/player/:id", rateLimitMiddleware("proxy", RLProxy, time.Minute), playerHandler)
		rg.GET("/player", rateLimitMiddleware("proxy", RLProxy, time.Minute), playerHandler)
		rg.GET("/playerData", rateLimitMiddleware("proxy", RLProxy, time.Minute), playerHandler)

		// Auth & User Endpoints (auth 10/min, user writes 30/min)
		rg.POST("/auth/google", rateLimitMiddleware("auth", RLAuth, time.Minute), handleGoogleAuth)
		// แผนข้อ 10 — session cookie: ยกระดับ session เดิมเป็น cookie + ออกจากระบบ
		rg.POST("/auth/session", rateLimitMiddleware("auth", RLAuth, time.Minute), handleAuthSession)
		rg.POST("/auth/logout", rateLimitMiddleware("auth", RLAuth, time.Minute), handleAuthLogout)
		rg.GET("/user/favorites", handleGetFavorites)
		rg.POST("/user/favorites", rateLimitMiddleware("user-write", RLUserWrite, time.Minute), handleSaveFavorites)
		rg.POST("/user/fcm", rateLimitMiddleware("user-write", RLUserWrite, time.Minute), handleFCMRegister)
		rg.POST("/user/match-subscribe", rateLimitMiddleware("user-write", RLUserWrite, time.Minute), handleMatchSubscribe)

		// ความสนใจ (ติดตามทีม/ลีก) — ใช้จัดลำดับหน้าแรกให้เป็นของผู้ใช้ (แผนข้อ 4)
		rg.GET("/user/interests", handleUserInterests)
		rg.POST("/user/interests", rateLimitMiddleware("user-write", RLUserWrite, time.Minute), handleUserInterestToggle)

		// PDPA (แผนข้อ 10): สิทธิเข้าถึง/ลบข้อมูลส่วนบุคคลของตัวเอง
		rg.GET("/user/data", handleUserDataExport)
		rg.DELETE("/user/account", handleDeleteAccount)

		// Predictions (leaderboard cached 2min, save 30/min)
		rg.GET("/predictions/leaderboard", handlePredictionsLeaderboard)
		rg.POST("/predictions/save", rateLimitMiddleware("prediction", RLPrediction, time.Minute), handleSavePrediction)
		rg.GET("/predictions/my", handleGetMyPredictions)
		rg.GET("/predictions/my/points", handleMyPoints)

		// Notification Preferences
		rg.GET("/user/notifications", handleGetNotificationPrefs)
		rg.POST("/user/notifications", rateLimitMiddleware("user-write", RLUserWrite, time.Minute), handleSaveNotificationPrefs)
		rg.GET("/calendar", handleCalendar)
		rg.GET("/calendar/ical", handleCalendarIcal)

		// Auth V2 (แผนใหม่ข้อ 1) — register / login / verify / forgot / reset / refresh / me
		rg.POST("/auth/register", rateLimitMiddleware("auth", RLAuth, time.Minute), handleRegister)
		rg.POST("/auth/login", rateLimitMiddleware("auth", RLAuth, time.Minute), handleLogin)
		rg.POST("/auth/verify-email", rateLimitMiddleware("auth", RLAuth, time.Minute), handleVerifyEmail)
		rg.POST("/auth/resend-verify", rateLimitMiddleware("auth", RLAuth, time.Minute), handleResendVerify)
		rg.POST("/auth/forgot-password", rateLimitMiddleware("auth", RLAuth, time.Minute), handleForgotPassword)
		rg.POST("/auth/reset-password", rateLimitMiddleware("auth", RLAuth, time.Minute), handleResetPassword)
		rg.POST("/auth/refresh", rateLimitMiddleware("auth", RLAuth, time.Minute), handleRefresh)
		rg.GET("/auth/me", handleMe)
		rg.GET("/auth/sessions", rateLimitMiddleware("auth", RLAuth, time.Minute), handleAuthSessions)
		rg.POST("/auth/logout-all", rateLimitMiddleware("auth", RLAuth, time.Minute), handleLogoutAll)
		// Phase B ข้อ 5: API key self-service
		rg.GET("/auth/api-keys", handleUserAPIKeyList)
		rg.POST("/auth/api-keys", rateLimitMiddleware("user-write", RLUserWrite, time.Minute), handleUserAPIKeyCreate)
		rg.DELETE("/auth/api-keys/:key", rateLimitMiddleware("user-write", RLUserWrite, time.Minute), handleUserAPIKeyRevoke)
		// Phase B ข้อ 10: รายงานข้อความแชท
		rg.POST("/chat/report", rateLimitMiddleware("user-write", RLUserWrite, time.Minute), handleChatReport)

		// Season Archive (แผนใหม่ข้อ 4) — ผลแมตช์ที่จบแล้วจาก Redis/memory ของเราเอง
		rg.GET("/archive", rateLimitMiddleware("proxy", RLProxy, time.Minute), handleArchiveList)
		rg.GET("/archive/:id", rateLimitMiddleware("proxy", RLProxy, time.Minute), handleArchiveMatch)

		// Image CDN proxy (แผนใหม่ข้อ 6) — /api/img?u=<encoded url> (memory+Redis cache 7 วัน)
		rg.GET("/img", rateLimitMiddleware("proxy", RLImage, time.Minute), handleImageProxy)

		// Error reporting (frontend monitoring)
		rg.POST("/client-errors", rateLimitMiddleware("error-report", RLErrorReport, time.Minute), handleClientError)

		// Monitor / Uptime (แผนใหม่ข้อ 8) — health check + provider quota + error rate
		rg.GET("/monitor/status", rateLimitMiddleware("proxy", RLProxy, time.Minute), handleMonitorStatus)
		rg.POST("/metrics", rateLimitMiddleware("metrics", RLMetrics, time.Minute), handleMetrics)

		// Admin Panel
		setupAdminRoutes(rg)
	}

	apiGroup := r.Group("/api")
	setupRoutes(apiGroup)

	nestedApiGroup := r.Group("/api/api")
	setupRoutes(nestedApiGroup)

	// Public API v1 (แผนข้อ 7) — ใช้ API key + โควตาแยกต่อ key
	// endpoint เดียวกับของเว็บ (แต่คนละ path + ต้องยืนยันด้วย key)
	v1 := r.Group("/api/v1")
	{
		v1.GET("/status", handleAPIStatus) // ไม่ต้อง key (เช็คว่า service ใครไหน) — แต่จะไม่บอกอะไรมาก
	}
	v1Auth := v1.Group("", apiKeyAuth())
	{
		v1Auth.GET("/matches", matchesHandler)
		v1Auth.GET("/match/:id", matchDetailsHandler)
		v1Auth.GET("/league/:id", leagueHandler)
		v1Auth.GET("/league/:id/table", leagueTableHandler)
		v1Auth.GET("/team/:id", teamHandler)
		v1Auth.GET("/team/:id/squad", teamSquadHandler)
		v1Auth.GET("/player/:id", playerHandler)
		v1Auth.GET("/news", newsHandler)
		v1Auth.GET("/calendar", handleCalendar)
	}

	port := getEnv("PORT", "8080")
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second, // กัน slowloris — header ที่ค้างไม่ถูกอ่านทันเวลา
	}
	logInfof("🚀 Go Backend (พร้อม WebSocket Live Server & Dynamic Engine) เริ่มทำงานที่ http://localhost:%s", port)

	// Serve ใน goroutine เพื่อให้ main รอสัญญาณจบได้
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.ListenAndServe()
	}()

	// Graceful shutdown: Render ส่ง SIGTERM ก่อน kill →
	// รับสัญญาณ → หยุดรับ connection ใหม่ → flush ข้อมูลลงดิสก์ + Redis snapshot
	// (เดิม r.Run() ตรง ๆ = ถูก kill ปุ๊บ ข้อมูล 3 วิท้ายหาย + snapshot สุดท้ายไม่ถูก push)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serveErr:
		if err != nil && err != http.ErrServerClosed {
			logFatalf("❌ Server ล่ม: %v", err)
		}
	case sig := <-quit:
		logInfof("🛑 ได้รับสัญญาณ %v — กำลังปิดเซิร์ฟเวอร์อย่างปลอดภัย...", sig)

		// ตัด timeout กันค้าง: connection ที่ยัง open (เช่น WS) ถูก force-close เมื่อหมดเวลา
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logInfof("⚠️ Shutdown ไม่สมบูรณ์: %v (บังคับปิด)", err)
			srv.Close()
		}

		// FlushNow เขียนไฟล์ล่าสุด (รวมข้อมูล 3 วิท้าย) + ดัน snapshot ขึ้น Redis
		if GlobalDB != nil {
			if err := GlobalDB.FlushNow(); err != nil {
				logInfof("⚠️ Flush ก่อนปิดล้มเหลว: %v", err)
			}
		}
		logInfof("✅ ปิดเซิร์ฟเวอร์เรียบร้อย")
	}
}

// ---------------------------------------------------------
// WebSocket & Real-time Live Handler
// ---------------------------------------------------------

func handleWebSocket(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		logInfof("WebSocket Upgrade Error: %v", err)
		return
	}

	client := &LiveClient{
		hub:  GlobalLiveHub,
		conn: conn,
		send: make(chan []byte, sendChannelCap),
	}
	client.hub.register <- client

	go client.writePump()
	go client.readPump()
}

func startLiveTicker() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var lastREST time.Time
	var lastInvalidate time.Time

	for range ticker.C {
		// นับ client จริงจาก Hub (ตัวแปร wsClients เดิมไม่เคยถูกเติมค่า
		// ทำให้ ticker ข้ามทุกครั้ง -> หน้าแรกไม่เคยได้คะแนนสดอัตโนมัติ)
		clientCount := GlobalLiveHub.ClientCount()

		// active = มีคนดูอยู่ หรือมีคนกด 🔔 subscribe แมตช์ใดไว้
		hasSubs := GlobalDB != nil && GlobalDB.HasAnyMatchSubscriptions()
		active := clientCount > 0 || hasSubs

		// prune สถานะแมตช์เก่า (จบ/หายไปเกิน 2 ชม.) — ทำทุกรอบ ไม่ขึ้นกับคนดู
		// regression เดิม: อยู่หลัง gate "ไม่มีคนดู -> continue" -> state เก่าค้าง
		// memory leak ถาวรตอนไม่มีใครเปิดเว็บ prune ฟรี = ไม่ยิง upstream
		if removed := GlobalGoalDetector.pruneOlderThan(2 * time.Hour); removed > 0 {
			logInfof("🧹 goal detector: prune %d match states เก่าแล้ว", removed)
		}

		// เลือกแหล่ง live ตัวแรกที่ยังใช้ได้ (GOAL ก่อน ตัวสำรอง API-Football ทีหลัง)
		src := activeLiveSource()
		if src == nil {
			continue
		}

		// 1. REST sweep ตามรอบของ provider แต่ละตัว
		//    (GOAL = 180 วิ เพราะมี WebSocket push ช่วย realtime / API-Football = 60 วิ
		//     เพราะไม่มี push ต้องพึ่ง sweep อย่างเดียว — ทุกการ sweep = 1 call เท่านั้น)
		//
		// regression "deadlock เชิงพฤติกรรม" เดิม: ไม่มีคนดู -> continue ข้าม sweep เลย
		// -> liveState ว่าง -> goal WS subscribe 0 คู่ -> subscriptions=0 ถาวร
		// -> ระบบ live ไม่ตื่นเองจนกว่าจะมีคนเปิดหน้าแรก (health log เคยโชว์ rest_swept=false)
		// ตอนนี้: ไม่มีคนดู -> ช้าลงเป็น IDLE_SWEEP_SEC (default 15 นาที) ยังคงวนอยู่
		// ระบบรู้ตัวเองว่ามีบอล live -> WS subscribe เอง -> พร้อมทันทีที่คนเปิดเว็บ/กด 🔔
		// (96 calls/วัน ตอน idle vs 480 ถ้า sweep เร็วตลอด — ยังกันโควตาได้)
		sweepInterval := liveSweepInterval(src.SweepInterval(), active)
		if time.Since(lastREST) >= sweepInterval {
			if err := src.LiveSweep(); err == nil {
				lastREST = time.Now()
			}

			// ถ้ามีคู่แข่งใหม่ที่ยังไม่อยู่ในรายการวันนี้ -> สั่งโหลดรายการใหม่ (จำกัด 1 ครั้ง/2 นาที)
			if time.Since(lastInvalidate) > 2*time.Minute && src.HasUnknownLiveMatches() {
				src.InvalidateTodayCache()
				lastInvalidate = time.Now()
				logInfof("🔄 live ticker: พบคู่แข่งใหม่ -> โหลดรายการวันนี้ใหม่")
			}
		}

		// 2. อ่านสถานะรวมจาก memory (ไม่ยิง upstream / ไม่มี mock fallback แล้ว)
		liveUpdates := src.LiveUpdatesSnapshot()
		if len(liveUpdates) == 0 {
			continue
		}

		// หมายเหตุ: ถอด fallback ข้อมูลปลอม (match_id 4200001) ออกแล้ว
		// ถ้าไม่มีคู่แข่งจริงในระบบ ก็ไม่ส่งอะไรเลย ห้ามส่งข้อมูลจำลองเข้า WS

		// 2. Process Goal Events & FCM Alerts
		for _, update := range liveUpdates {
			if mID, ok := update["match_id"].(int); ok {
				hScore, _ := update["home_score"].(int)
				aScore, _ := update["away_score"].(int)
				stat, _ := update["status"].(string)
				hName, _ := update["home_name"].(string)
				aName, _ := update["away_name"].(string)
				GlobalGoalDetector.ProcessMatchUpdate(mID, hScore, aScore, stat, hName, aName)
			}
		}

		// 4. Non-blocking Broadcast Live Updates via LiveHub
		GlobalLiveHub.BroadcastJSON(liveUpdates)
	}
}

// liveSweepInterval เลือกความถี่ sweep: มีคนดู/มี sub -> รอบปกติ (เร็ว)
// ไม่มีใครเลย -> ช้าลง (IDLE_SWEEP_SEC) เพื่อให้ระบบยัง "ตื่น" อยู่เอง
// (liveState มีข้อมูล -> goal WS subscribe เองได้ ไม่ต้องรอคนเปิดหน้าแรก)
// โดยไม่กินโควตาเต็มอัตราตอนไม่มีใครใช้งาน
func liveSweepInterval(base time.Duration, active bool) time.Duration {
	if active {
		return base
	}
	idle := time.Duration(envInt("IDLE_SWEEP_SEC", 900)) * time.Second
	if idle < base {
		return base
	}
	return idle
}

var (
	chatRoomClients   = make(map[string]map[*websocket.Conn]bool)
	chatRoomClientsMu sync.Mutex

	chatRoomHistory   = make(map[string][]map[string]interface{})
	chatRoomHistoryMu sync.Mutex
)

func handleChatWebSocket(c *gin.Context) {
	matchID := c.Param("id")
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}

	client := &ChatClient{
		hub:     GlobalChatHub,
		conn:    conn,
		send:    make(chan []byte, sendChannelCap),
		matchID: matchID,
		ip:      c.ClientIP(), // rate limit ต่อ user ใน readPump
	}
	client.hub.register <- client

	// 1. Send recent chat history
	history := GlobalChatHub.GetRoomHistory(matchID)
	if historyBytes, err := json.Marshal(history); err == nil {
		client.send <- historyBytes
	}

	go client.writePump()
	go client.readPump()
}

// ---------------------------------------------------------
// Auth, User & Push Notification Handlers
// ---------------------------------------------------------

func getUserIdentifier(c *gin.Context) string {
	// cookie มาก่อน (ระบบใหม่) แล้วค่อย Bearer (compat กับ client รุ่นเก่า)
	if token := sessionTokenFromRequest(c); token != "" {
		return "user:" + token
	}
	deviceID := strings.TrimSpace(c.GetHeader("X-Device-ID"))
	if deviceID == "" {
		deviceID = strings.TrimSpace(c.GetHeader("X-Device-UUID"))
	}
	if deviceID != "" && deviceID != "null" && deviceID != "undefined" {
		return "device:" + deviceID
	}
	clientIP := c.ClientIP()
	if clientIP != "" {
		return "ip:" + clientIP
	}
	return "anonymous"
}

func handleGoogleAuth(c *gin.Context) {
	var body struct {
		Token string `json:"token"`
	}
	if err := c.BindJSON(&body); err != nil || strings.TrimSpace(body.Token) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ข้อมูลไม่ถูกต้อง"})
		return
	}

	// ตรวจ Google ID token จริง (ไม่เชื่อ token ดิบจาก client)
	sub, name, err := verifyGoogleIDToken(c.Request.Context(), strings.TrimSpace(body.Token))
	if err != nil {
		logInfof("❌ Google auth verify ล้มเหลว: %v", err)
		c.JSON(http.StatusUnauthorized, gin.H{"error": "ยืนยันตัวตน Google ไม่สำเร็จ"})
		return
	}

	// ออก session token ของเราเอง ผูกกับ google sub
	sess := make([]byte, 32)
	if _, err := rand.Read(sess); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "สร้าง session ไม่สำเร็จ"})
		return
	}
	sessionToken := hex.EncodeToString(sess)
	userKey := "user:g:" + sub
	if GlobalDB != nil {
		GlobalDB.SaveUserSession(sessionToken, sub)
	}

	var favs []int
	if GlobalDB != nil {
		favs = GlobalDB.GetUserFavorites(userKey)
	} else {
		favs = []int{}
	}

	logInfof("✅ Google login สำเร็จ [%s]: %s", userKey, name)
	// แผนข้อ 10: ออก session เป็น cookie (HttpOnly) แทนการคืน token ใน body
	// -> XSS อ่าน token ไม่ได้ และ server รู้ว่าใคร login อยู่จาก cookie เอง
	setSessionCookie(c, sessionToken)
	c.JSON(http.StatusOK, gin.H{
		"username":      name,
		"favs":          favs,
		"authenticated": true,
	})
}

// handleAuthSession — ยกระดับ session เดิม (ที่เก็บใน localStorage) เป็น cookie
// ใช้ตอน deploy ระบบ cookie: ผู้ใช้ที่ login ค้างจากรุ่นก่อนยังมี Bearer token
// -> ให้ยิง endpoint นี้ 1 ครั้ง แล้วลบ token ออกจาก localStorage ฝั่ง client
func handleAuthSession(c *gin.Context) {
	token := sessionTokenFromRequest(c)
	if token == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "ยังไม่มี session"})
		return
	}
	if GlobalDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "ฐานข้อมูลไม่พร้อม"})
		return
	}
	sub := GlobalDB.GetSessionSub(token)
	if sub == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "session ไม่ถูกต้องหรือหมดอายุ"})
		return
	}
	setSessionCookie(c, token)
	logInfof("🔄 ยกระดับ session เป็น cookie สำเร็จ [%s]", "user:g:"+sub)
	c.JSON(http.StatusOK, gin.H{
		"authenticated": true,
		"sub":           sub,
	})
}

// handleAuthLogout — ลบ session + ล้าง cookie (ฝั่ง client แค่ล้าง localStorage
// เดิมทำให้ token ยังใช้ได้ต่อ = ออกจากระบบไม่สมบูรณ์)
func handleAuthLogout(c *gin.Context) {
	token := sessionTokenFromRequest(c)
	if token != "" && GlobalDB != nil {
		GlobalDB.DeleteUserSession(token)
	}
	clearSessionCookie(c)
	logInfof("👋 ออกจากระบบ (ลบ session แล้ว)")
	c.JSON(http.StatusOK, gin.H{"status": "logged_out"})
}

// verifyGoogleIDToken ตรวจ ID token กับ Google แล้วคืน sub + ชื่อ
func verifyGoogleIDToken(ctx context.Context, idToken string) (sub, name string, err error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://oauth2.googleapis.com/tokeninfo?id_token="+idToken, nil)
	if err != nil {
		return "", "", err
	}
	client := &http.Client{Timeout: 10 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("google tokeninfo status %d", res.StatusCode)
	}
	var info struct {
		Sub   string `json:"sub"`
		Aud   string `json:"aud"`
		Name  string `json:"name"`
		Email string `json:"email"`
	}
	if err := json.NewDecoder(res.Body).Decode(&info); err != nil {
		return "", "", err
	}
	if info.Sub == "" {
		return "", "", fmt.Errorf("token ไม่มี sub")
	}
	// กัน token จากแอปอื่น: aud ต้องตรง client id ของเรา (ถ้าตั้งไว้)
	if want := strings.TrimSpace(os.Getenv("GOOGLE_CLIENT_ID")); want != "" && info.Aud != want {
		return "", "", fmt.Errorf("aud ไม่ตรง client id ของเรา")
	}
	display := strings.TrimSpace(info.Name)
	if display == "" {
		display = strings.TrimSpace(info.Email)
	}
	if display == "" {
		display = "FootballFan"
	}
	return info.Sub, display, nil
}

// resolveUserKey ผูก session (cookie หรือ Bearer) -> google sub ก่อน fallback แบบเดิม
func resolveUserKey(c *gin.Context) string {
	if token := sessionTokenFromRequest(c); token != "" && GlobalDB != nil {
		if sub := GlobalDB.GetSessionSub(token); sub != "" {
			return "user:g:" + sub
		}
	}
	return getUserIdentifier(c)
}

func handleGetFavorites(c *gin.Context) {
	userKey := resolveUserKey(c)
	var favs []int
	if GlobalDB != nil {
		favs = GlobalDB.GetUserFavorites(userKey)
	} else {
		favs = []int{}
	}

	c.JSON(http.StatusOK, gin.H{
		"favorites": favs,
		"user_key":  userKey,
	})
}

func handleSaveFavorites(c *gin.Context) {
	var body struct {
		Favorites []int `json:"favorites"`
	}
	if err := c.BindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ข้อมูลไม่ถูกต้อง"})
		return
	}

	userKey := resolveUserKey(c)
	if GlobalDB != nil {
		GlobalDB.SaveUserFavorites(userKey, body.Favorites)
	}

	logInfof("⭐ บันทึกรายการโปรดสำหรับผู้ใช้ [%s]: %v", userKey, body.Favorites)
	c.JSON(http.StatusOK, gin.H{"status": "saved", "favorites": body.Favorites, "user_key": userKey})
}

func handleFCMRegister(c *gin.Context) {
	var body struct {
		FCMToken string `json:"fcm_token"`
	}
	if err := c.BindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Token ไม่ถูกต้อง"})
		return
	}
	if strings.TrimSpace(body.FCMToken) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Token ว่าง"})
		return
	}

	// บันทึก token ลงฐานจริงๆ (เดิมแค่ log ทิ้ง ทำให้ push ใช้ไม่ได้)
	if GlobalDB != nil {
		GlobalDB.SaveFCMToken(body.FCMToken)
	}

	logInfof("📲 ลงทะเบียน FCM Push Token สำเร็จ (ท้าย token: ...%s)", tailOf(body.FCMToken, 8))
	c.JSON(http.StatusOK, gin.H{"status": "registered"})
}

func handleMatchSubscribe(c *gin.Context) {
	var body struct {
		MatchID  int    `json:"match_id"`
		Action   string `json:"action"` // "subscribe" or "unsubscribe"
		FCMToken string `json:"fcm_token"`
	}
	if err := c.BindJSON(&body); err != nil || body.MatchID == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ข้อมูลไม่ถูกต้อง"})
		return
	}

	token := body.FCMToken
	if token == "" {
		token = "session_token_" + c.ClientIP()
	}

	// เก็บลง PersistentDB ที่เดียวกับที่ DispatchFCMPush อ่าน
	// (ของเดิมเขียนลง map ใน memory อีกตัว ส่งจริงเลยไม่เคยเจอคนตาม)
	var subCount int
	if GlobalDB != nil {
		if body.Action == "unsubscribe" {
			subCount = GlobalDB.UnsubscribeMatch(body.MatchID, token)
			logInfof("🔔 ยกเลิกการติดตามการแจ้งเตือนแมตช์ #%d (Token: %s)", body.MatchID, token)
		} else {
			subCount = GlobalDB.SubscribeMatch(body.MatchID, token)
			logInfof("🔔 ลงทะเบียนการแจ้งเตือนประตูแมตช์ #%d สำเร็จ (Token: %s)", body.MatchID, token)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"status":      "success",
		"match_id":    body.MatchID,
		"subscribed":  body.Action != "unsubscribe",
		"subscribers": subCount,
	})
}

// --- Predictions Leaderboard ---
func handlePredictionsLeaderboard(c *gin.Context) {
	cacheKey := "predictions:leaderboard"
	if cached, source, found := cacheGet(cacheKey); found {
		c.Header("X-Cache", "HIT")
		c.Header("X-Data-Source", source)
		c.Data(http.StatusOK, "application/json; charset=utf-8", cached)
		return
	}

	if GlobalDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "DB not ready"})
		return
	}

	leaderboard := computeLeaderboard()

	payload := gin.H{
		"leaderboard": leaderboard,
		"count":       len(leaderboard),
		"scoring":     "ถูกสกอร์เป๊ะ +3 · ถูกผล +1 · ผิด 0",
	}
	raw, _ := json.Marshal(payload)
	cacheSet(cacheKey, raw, TTLLeaderboard)

	c.Header("X-Cache", "MISS")
	c.Data(http.StatusOK, "application/json; charset=utf-8", raw)
}

// handleMyPoints คะแนนของฉัน (ใช้หน้า /predictions สรุปแต้ม)
func handleMyPoints(c *gin.Context) {
	userKey := resolveUserKey(c)
	c.JSON(http.StatusOK, gin.H{"user": userKey, "score": userPoints(userKey)})
}

func handleSavePrediction(c *gin.Context) {
	var body struct {
		MatchID    string                 `json:"match_id"`
		Prediction map[string]interface{} `json:"prediction"`
	}
	if err := c.BindJSON(&body); err != nil || body.MatchID == "" || body.Prediction == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ข้อมูลไม่ถูกต้อง"})
		return
	}

	// แผนข้อ 5: ปิดรอบทาย — ถ้าแมตช์เริ่มแล้วทายไม่ได้ (กันคนแก้ทายหลังเขียนไม่ได้)
	if started, status := matchHasStarted(body.MatchID); started {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":        "ปิดรอบทายผลแล้ว — แมตช์เริ่มไปแล้ว",
			"match_status": status,
			"closed":       true,
		})
		return
	}

	userKey := resolveUserKey(c)
	GlobalDB.SavePrediction(userKey, body.MatchID, body.Prediction)

	c.JSON(http.StatusOK, gin.H{"status": "saved", "match_id": body.MatchID, "user": userKey})
}

func handleGetMyPredictions(c *gin.Context) {
	userKey := resolveUserKey(c)
	preds := GlobalDB.GetPredictionsByUser(userKey)
	c.JSON(http.StatusOK, gin.H{"predictions": preds, "user": userKey})
}

func handleGetNotificationPrefs(c *gin.Context) {
	userKey := resolveUserKey(c)
	prefs := GlobalDB.GetNotificationPrefs(userKey)
	c.JSON(http.StatusOK, gin.H{"preferences": prefs, "user": userKey})
}

func handleSaveNotificationPrefs(c *gin.Context) {
	var body struct {
		Preferences map[string]interface{} `json:"preferences"`
	}
	if err := c.BindJSON(&body); err != nil || body.Preferences == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ข้อมูลไม่ถูกต้อง"})
		return
	}

	userKey := resolveUserKey(c)
	GlobalDB.SaveNotificationPrefs(userKey, body.Preferences)
	c.JSON(http.StatusOK, gin.H{"status": "saved", "user": userKey})
}

// Calendar Handlers
func handleCalendar(c *gin.Context) {
	// For now, return mock/fallback calendar events
	events := []map[string]interface{}{
		{
			"match_id":   "999999",
			"home_name":  "ททีม A",
			"away_name":  "ทีม B",
			"start_time": time.Now().Add(24 * time.Hour).Format(time.RFC3339),
			"status":     "scheduled",
		},
	}
	c.JSON(http.StatusOK, gin.H{"calendar": events})
}

func handleCalendarIcal(c *gin.Context) {
	events := []map[string]interface{}{
		{
			"match_id":   "999999",
			"home_name":  "ทีม A",
			"away_name":  "ทีม B",
			"start_time": time.Now().Add(24 * time.Hour).Format(time.RFC3339),
			"status":     "scheduled",
		},
	}

	var lines []string
	lines = append(lines, "BEGIN:VCALENDAR")
	lines = append(lines, "VERSION:2.0")
	lines = append(lines, "PRODID:-//Score-track//Football Calendar//EN")
	now := time.Now().Format("20060102T150405Z")
	lines = append(lines, "CREATED:"+now)
	lines = append(lines, "END:VCALENDAR")
	lines = append(lines, "")

	for _, e := range events {
		lines = append(lines, "BEGIN:VEVENT")
		lines = append(lines, "UID:"+fmt.Sprintf("scoretrack-%s", e["match_id"]))
		st := e["start_time"].(string)
		lines = append(lines, "DTSTART:"+st)
		lines = append(lines, "SUMMARY:"+fmt.Sprintf("%v vs %v", e["home_name"], e["away_name"]))
		lines = append(lines, "END:VEVENT")
		lines = append(lines, "")
	}

	c.Header("Content-Type", "text/calendar; charset=utf-8")
	c.Header("Content-Disposition", "attachment; filename=scoretrack-calendar.ics")
	c.Data(http.StatusOK, "text/calendar; charset=utf-8", []byte(strings.Join(lines, "\r\n")))
}

// handleClientError รับ error report จาก frontend แล้วเขียนลง log ไฟล์
func handleClientError(c *gin.Context) {
	var body struct {
		Message string `json:"message"`
		Source  string `json:"source"`
		Page    string `json:"page"`
	}
	if err := c.BindJSON(&body); err != nil || strings.TrimSpace(body.Message) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ข้อมูลไม่ถูกต้อง"})
		return
	}

	// จำกัดความยาวกัน log ถูก spam
	msg := body.Message
	if len(msg) > 500 {
		msg = msg[:500]
	}
	src := body.Source
	if src == "" {
		src = "unknown"
	}
	page := body.Page

	// เขียนลงไฟล์ log
	logEntry := fmt.Sprintf("[%s] page=%s source=%s msg=%q\n", time.Now().Format(time.RFC3339), page, src, msg)
	if err := os.MkdirAll("logs", 0755); err == nil {
		if f, err := os.OpenFile("logs/client-errors.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
			f.WriteString(logEntry)
			f.Close()
		}
	}
	logInfof("⚠️ Client Error [%s] %s: %s", src, page, msg)

	c.JSON(http.StatusOK, gin.H{"status": "logged"})
}

// rateLimitScale — ตัวคูณโควตา rate limit ทุกตัว: E2E/CI ยิงจาก IP เดียวหลาย
// browser พร้อมกัน เกินโควตาปกติ (120/min global + per-route) จน SSR โดน 429
// เป็นบางรอบ — ตั้ง RATE_LIMIT_SCALE=1000 เฉพาะตอนรัน test เท่านั้น
var rateLimitScale = func() int {
	v := strings.TrimSpace(os.Getenv("RATE_LIMIT_SCALE"))
	if v == "" {
		return 1
	}
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return n
	}
	return 1
}()

// Rate Limiter Middleware (120 reqs/min per IP — global safety net)
func rateLimiterMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		ip := c.ClientIP()
		if !GlobalRateLimiter.Allow(ip, 120*rateLimitScale, time.Minute) {
			rateLimitedTotal.Add(1)
			logWarn("rate limited", "ip", ip, "scope", "global", "route", c.FullPath(), "request_id", requestIDFrom(c))
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error": "คำขอมากเกินไป กรุณารอ 1 นาทีก่อนลองใหม่",
			})
			c.Abort()
			return
		}
		c.Next()
	}
}

// rateLimitMiddleware สร้าง per-endpoint limiter (composite key = ip|route)
// ใช้กับ endpoint ที่แพง/敏感 เช่น auth, search, proxy
func rateLimitMiddleware(route string, limit int, window time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := c.ClientIP() + "|" + route
		if !GlobalRateLimiter.AllowKey(key, limit*rateLimitScale, window) {
			rateLimitedTotal.Add(1)
			logWarn("rate limited", "ip", c.ClientIP(), "scope", route, "route", c.FullPath(), "request_id", requestIDFrom(c))
			c.Header("Retry-After", fmt.Sprintf("%d", int(window.Seconds())))
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error": fmt.Sprintf("คำขอมากเกินไปสำหรับ %s กรุณารอ %v ก่อนลองใหม่", route, window),
			})
			c.Abort()
			return
		}
		c.Next()
	}
}

// Rate limit profiles (per-endpoint)
const (
	RLAuth        = 10  // auth/google — ป้องกัน token spam
	RLSearch      = 30  // search suggest — แพง (GOAL API)
	RLProxy       = 60  // proxy endpoints (matches/league/team/player)
	RLNews        = 30  // news RSS
	RLErrorReport = 20  // client-errors — กัน log spam
	RLMetrics     = 60  // metrics beacon (vitals+pageview)
	RLPrediction  = 30  // predictions save
	RLUserWrite   = 30  // favorites/fcm/subscribe writes
	RLImage       = 300 // image CDN proxy (/api/img — proxy ไฟล์รูปภาพ, 300/min)
)

// ---------------------------------------------------------
// Helper Functions & Caching Logic
// ---------------------------------------------------------

// handleHealthz — deep health check สำหรับ uptime monitor / Render
// ต่างจาก / ที่ตอบ 200 เสมอ: ตัวนี้เช็ค dependency จริงแล้วคืน 503 ถ้าพัง
// components:
//   - db:        PersistentDB โหลดเสร็จ + เขียนไฟล์ได้
//   - redis:     ต่อกับ Upstash/Redis สำเร็จ (ถ้า config ไว้) — degraded ถ้าหลุด
//   - live_ws:   hub goroutine ตอบสนอง (ClientCount คืนได้ = Run() ยังรันอยู่)
//   - providers: มี data snapshot ใน memory อย่างน้อย 1 ตัว (กัน cold start ปลอม)
func handleHealthz(c *gin.Context) {
	checks := gin.H{}
	healthy := true

	// 1. DB — GlobalDB nil = InitDB ล้มเหลว (ข้อมูลผู้ใช้ทั้งหมดใช้ไม่ได้)
	if GlobalDB != nil {
		checks["db"] = "ok"
	} else {
		checks["db"] = "unavailable"
		healthy = false
	}

	// 2. Redis — ถ้าตั้งค่าไว้แล้วหลุด = snapshot/restore + cache ใช้ไม่ได้
	//    (nil = ไม่ได้ตั้งค่า → in-memory mode ถือว่า ok ไม่ใช่ error)
	if rdb != nil {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		if err := rdb.Ping(pingCtx).Err(); err != nil {
			checks["redis"] = fmt.Sprintf("error: %v", err)
			healthy = false
		} else {
			checks["redis"] = "ok"
		}
		cancel()
	} else {
		checks["redis"] = "memory-mode"
	}

	// 3. WS hub — ClientCount คืนได้พิสูจน์ว่า Run() goroutine ยัง process channel อยู่
	checks["live_ws"] = fmt.Sprintf("ok (%d clients)", GlobalLiveHub.ClientCount())

	// 4. Providers — มี live data ในหน่วยความจำไหม (cold start ใหม่ = ยังว่าง = degraded)
	checks["providers"] = gin.H{
		"active":     activeProviderName(),
		"registry":   registry.Counts(),
		"dropped_ws": GlobalLiveHub.DroppedBroadcasts(),
	}

	status := http.StatusOK
	state := "healthy"
	if !healthy {
		status = http.StatusServiceUnavailable
		state = "unhealthy"
	}
	c.JSON(status, gin.H{
		"status":       state,
		"checks":       checks,
		"generated_at": time.Now().Format(time.RFC3339),
	})
}

// activeProviderName — คืนชื่อ provider ตัวแรกที่ยังใช้ได้ (หรือ "")
func activeProviderName() string {
	for _, p := range providerChain {
		if p.Enabled() && p.Budget().Remaining() > 0 {
			return p.Name()
		}
	}
	return ""
}

// handleSourcesStatus รายงานสถานะแหล่งข้อมูล + โควตาคงเหลือ (ไม่เปิดเผย key)
func handleSourcesStatus(c *gin.Context) {
	active := ""
	for _, p := range providerChain {
		if p.Enabled() && p.Budget().Remaining() > 0 {
			active = p.Name()
			break
		}
	}

	var goalWS interface{}
	if g := goalProvider(); g != nil {
		goalWS = g.WSStatus()
	}

	c.JSON(http.StatusOK, gin.H{
		"status":          "ok",
		"active_provider": active,
		"providers":       snapshotProviders(),
		"goal_ws":         goalWS,
		"registry":        registry.Counts(),
		"live_ws": gin.H{
			"clients":           GlobalLiveHub.ClientCount(),
			"dropped_broadcast": GlobalLiveHub.DroppedBroadcasts(),
		},
		"generated_at": time.Now().Format(time.RFC3339),
	})
}

// leagueKindFromPattern แปลง cache key pattern ของลีกเป็น kind ที่ LeagueProvider รู้จัก
//
//	"league:%s"          -> ""        (รายละเอียดลีก)
//	"league:table:%s"    -> "table"
//	"league:fixtures:%s" -> "fixtures"
//	"league:stats:%s"    -> "stats"   (GOAL ไม่มี -> provider คืน false เอง)
func leagueKindFromPattern(pattern string) string {
	switch {
	case strings.HasPrefix(pattern, "league:table:"):
		return "table"
	case strings.HasPrefix(pattern, "league:fixtures:"):
		return "fixtures"
	case strings.HasPrefix(pattern, "league:stats:"):
		return "stats"
	default:
		return ""
	}
}

func handleProxy(cacheKeyPattern string, ttl time.Duration, targetURLBuilder func(c *gin.Context) string) gin.HandlerFunc {
	return func(c *gin.Context) {
		paramOrQuery := c.Param("id")
		if paramOrQuery == "" {
			paramOrQuery = c.Query("matchId")
		}
		if paramOrQuery == "" {
			paramOrQuery = c.Query("id")
		}
		if paramOrQuery == "" {
			paramOrQuery = c.Query("date")
		}
		if paramOrQuery == "" {
			paramOrQuery = time.Now().Format("20060102")
		}

		cacheKey := fmt.Sprintf(cacheKeyPattern, paramOrQuery)
		targetURL := targetURLBuilder(c)

		// 1. Check Redis Cache or In-Memory Cache Fallback
		if cachedData, cacheSource, found := cacheGet(cacheKey); found {
			c.Header("X-Cache", "HIT")
			// เดิมส่งชื่อ cache backend ใส่ X-Data-Source (ทำให้เข้าใจผิดว่าเป็นแหล่งข้อมูล)
			// -> แยกเป็น X-Cache-Backend + X-Data-Source (แหล่งข้อมูลจริง) + อายุข้อมูล
			c.Header("X-Cache-Backend", cacheSource)
			setProvenanceHeadersForKey(c, cacheKey, "cache")
			c.Data(http.StatusOK, "application/json; charset=utf-8", cachedData)
			return
		}

		// 2. Fetch from Upstream API (Data Provider ก่อน แล้วค่อย FotMob/Engine)
		dataType := ResolveDataType(cacheKeyPattern)

		var data []byte
		var err error
		sourceTag := "fotmob"

		// singleflight: cache-miss พร้อมกัน 20 คำขอคีย์เดียวกัน -> ยิง upstream ครั้งเดียว
		// ที่เหลือรอแล้วใช้ผลเดียวกัน (กัน stampede ตอน cache หมดอายุพร้อมกัน
		// -> minute-limit 60/min โดนทันที -> 429 -> circuit เปิดทั้งระบบ)
		// หมายเหตุ: closure ไม่แตะตัวแปร c ที่เปลี่ยน state เพราะตัวรอจะใช้ผลของคำขอแรก
		flightKey := cacheKey + "|" + c.Request.URL.RawQuery
		data, sourceTag, err = proxyFlights.Do(flightKey, func() ([]byte, string, error) {
			// เช็ค cache ซ้ำหลังเข้าคิว — คำขอแรกอาจเติม cache ไปแล้วระหว่างรอ
			if cachedData, _, found := cacheGet(cacheKey); found {
				return cachedData, "cache-recheck", nil
			}

			var d []byte
			var e error
			tag := "fotmob"

			if dataType == "matches" {
				if raw, src, ok := fetchMatchesFromProviders(paramOrQuery); ok {
					d, tag = raw, src
				} else {
					d, e = fetchFromFotmob(targetURL)
				}
			} else if dataType == "match" {
				if raw, src, ok := fetchMatchDetailsFromProviders(paramOrQuery); ok {
					d, tag = raw, src
				} else {
					d, e = fetchCompleteMatchDetails(paramOrQuery)
				}
			} else if dataType == "team" {
				if raw, src, ok := fetchTeamFromProviders("", paramOrQuery); ok {
					d, tag = raw, src
				} else {
					d, e = fetchCompleteTeamDetails(paramOrQuery)
				}
			} else if dataType == "team-squad" {
				if raw, src, ok := fetchTeamFromProviders("squad", paramOrQuery); ok {
					d, tag = raw, src
				} else {
					d, e = fetchTeamSquad(paramOrQuery)
				}
			} else if dataType == "team-fixtures" {
				if raw, src, ok := fetchTeamFromProviders("fixtures", paramOrQuery); ok {
					d, tag = raw, src
				} else {
					d, e = fetchTeamFixtures(paramOrQuery)
				}
			} else if dataType == "league" {
				kind := leagueKindFromPattern(cacheKeyPattern)
				if raw, src, ok := fetchLeagueFromProviders(kind, paramOrQuery); ok {
					d, tag = raw, src
				} else {
					d, e = fetchCompleteLeagueDetails(paramOrQuery)
				}
			} else if dataType == "player" {
				// Phase 1c: หน้าโปรไฟล์นักเตะ -> GOAL ก่อน ถ้า resolve ไม่ได้/ล้ม ถอยไป FotMob เหมือนเดิม
				if raw, src, ok := fetchPlayerFromProviders(paramOrQuery); ok {
					d, tag = raw, src
				} else {
					d, e = fetchFromFotmob(targetURL)
				}
			} else {
				d, e = fetchFromFotmob(targetURL)
			}
			return d, tag, e
		})

		// ผลจากคำขอแรกที่วิ่งอยู่ระหว่างที่เราเข้าคิว — cache มีของใหม่แล้ว
		// ตอบ HIT ตรง ๆ ไม่ต้อง validate/transform ซ้ำ (ข้อมูลผ่านขั้นตอนพวกนี้ตอนถูก cache แล้ว)
		if sourceTag == "cache-recheck" {
			c.Header("X-Cache", "HIT")
			c.Header("X-Data-Source", "singleflight")
			setProvenanceHeadersForKey(c, cacheKey, "singleflight")
			c.Data(http.StatusOK, "application/json; charset=utf-8", data)
			return
		}

		if err == nil && len(data) > 0 {
			if dataType == "matches" && sourceTag == "fotmob" {
				if transformed, transformErr := TransformRapidAPIMatchesToRealMatches(data); transformErr == nil && len(transformed) > 0 {
					data, _ = json.Marshal(transformed)
				}
			}

			// Validate if league data is incomplete (e.g. RapidAPI metadata-only JSON)
			// ข้ามเมื่อได้ข้อมูลจาก Data Provider (GOAL) เพราะ shape ต่างกัน
			var rawMap map[string]interface{}
			if errJson := json.Unmarshal(data, &rawMap); errJson == nil {
				_, hasTable := rawMap["table"]
				_, hasDetails := rawMap["details"]
				if dataType == "league" && sourceTag == "fotmob" && (!hasTable || !hasDetails) {
					realData := GetRealData(dataType, paramOrQuery)
					if len(realData) > 0 {
						var realMap map[string]interface{}
						if errReal := json.Unmarshal(realData, &realMap); errReal == nil {
							leagueName := rawMap["name"]
							if leagueName == nil || leagueName == "" {
								leagueName = rawMap["shortName"]
							}
							country := rawMap["country"]
							reqSeason := c.Query("season")
							seasonVal := reqSeason
							if seasonVal == "" {
								seasonVal = fmt.Sprintf("%v", rawMap["selectedSeason"])
							}
							if seasonVal == "" || seasonVal == "<nil>" {
								seasonVal = "2024/2025"
							}

							detailsMap := map[string]interface{}{
								"id":             paramOrQuery,
								"name":           leagueName,
								"country":        country,
								"selectedSeason": seasonVal,
							}
							realMap["details"] = detailsMap
							seasonsList := []string{"2026/2027", "2025/2026", "2024/2025", "2023/2024", "2022/2023"}
							realMap["allAvailableSeasons"] = seasonsList
							realMap["seasonsWithLinks"] = seasonsList
							data, _ = json.Marshal(realMap)
						} else {
							data = realData
						}
					}
				}
			}

			// 3. Save to Cache on Success
			//    หน้า detail จาก Data Provider: แมตช์จบแล้วผลไม่เปลี่ยน -> เก็บ 6 ชม.
			//    ถ้ายังแข่งอยู่ -> เก็บสั้น 90 วิ (ไม่กินโควตาเมื่อมีคนดูซ้ำ)
			cacheTTL := ttl
			if dataType == "match" && sourceTag != "fotmob" {
				if matchDetailsFinished(data) {
					cacheTTL = 6 * time.Hour
				} else {
					cacheTTL = 90 * time.Second
				}
			}
			// ข้อมูลลีกที่เป็นตาราง hardcode ฤดูกาลเก่า (tableSource=hardcoded-stale)
			// หรือตาราง TSDB ที่ล็อกฤดูกาล 2024-2025 (tableSource=thesportsdb)
			// -> cache สั้น 3 นาทีเท่านั้น: ข้อมูลปีก่อนถูกเสิร์ฟเป็นปีนี้อยู่แล้ว
			// ยิ่งค้างนานยิ่งผิดนาน — สั้นเพื่อให้ retry เจอของจริงทันทีที่ provider ฟื้น
			// ("computed-live"/"rapidapi" = ข้อมูลจริง -> cache เต็ม TTL ปกติ)
			if dataType == "league" && (bytes.Contains(data, []byte(`"hardcoded-stale"`)) || bytes.Contains(data, []byte(`"thesportsdb"`))) {
				cacheTTL = 3 * time.Minute
				logInfof("⚠️ league %s: ตารางคะแนนเป็นข้อมูลเก่า (hardcode/TSDB ปีก่อน) -> cache สั้น 3 นาที ( provider จริงล่ม )", paramOrQuery)
			}
			cacheSet(cacheKey, data, cacheTTL)

			// แผนข้อ 3: จำที่มาของข้อมูล (แหล่ง + เวลาที่ดึง) แล้วส่งเป็น header
			// ให้ผู้ใช้/ฝั่ง web รู้ว่าข้อมูลสดแค่ไหน มาจาก provider ไหน
			noteProvenance(cacheKey, sourceTag)
			c.Header("X-Cache", "MISS")
			setProvenanceHeaders(c, sourceTag, time.Now())
			c.Data(http.StatusOK, "application/json; charset=utf-8", data)
			return
		}

		dataType = ResolveDataType(cacheKeyPattern)
		realData := GetRealData(dataType, paramOrQuery)
		// ทุกแหล่งล้ม + ไม่มีข้อมูลสำรองจริง -> ตอบ 404 จริงสำหรับหน้า detail
		// ให้หน้าเว็บเรียก notFound() ได้ (HTTP 404 จริง + noindex + not-found.tsx)
		// เดิมตอบ 200 ร่างเปล่า (JSON.parse หน้าเว็บพัง) หรือ 200 + ข้อมูลปลอม
		if shouldNotFound(dataType, realData) {
			logInfof("ℹ️ 404 %s %s: ทุกแหล่งข้อมูลล้มและไม่มีข้อมูลสำรองจริง", dataType, paramOrQuery)
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		if len(realData) > 0 {
			// ข้อมูล hardcode ฤดูกาลเก่า / TSDB ปีก่อน -> cache สั้น 3 นาที (เหตุผลเดียวกับบล็อกด้านบน)
			realTTL := ttl
			if dataType == "league" && (bytes.Contains(realData, []byte(`"hardcoded-stale"`)) || bytes.Contains(realData, []byte(`"thesportsdb"`))) {
				realTTL = 3 * time.Minute
				logInfof("⚠️ league %s: Real-Data-Engine คืนตารางข้อมูลเก่า (hardcode/TSDB ปีก่อน) -> cache สั้น 3 นาที", paramOrQuery)
			}
			cacheSet(cacheKey, realData, realTTL)
		}

		c.Header("X-Cache", "MISS")
		c.Header("X-Data-Source", "Real-Data-Engine")
		c.Data(http.StatusOK, "application/json; charset=utf-8", realData)
	}
}

func fetchFromFotmob(targetURL string) ([]byte, error) {
	client := &http.Client{Timeout: 6 * time.Second}

	// 1. Try Direct FotMob URL first (Free, 0 RapidAPI quota used)
	directURL := targetURL
	if !strings.HasPrefix(directURL, "http") {
		directURL = "https://www.fotmob.com/api" + directURL
	}

	reqDirect, errDirect := http.NewRequest("GET", directURL, nil)
	if errDirect == nil {
		reqDirect.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
		reqDirect.Header.Set("Accept", "application/json, text/plain, */*")
		reqDirect.Header.Set("Accept-Language", "th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7")
		reqDirect.Header.Set("Referer", "https://www.fotmob.com/")

		respDirect, errResp := client.Do(reqDirect)
		if errResp == nil && respDirect.StatusCode == http.StatusOK {
			defer respDirect.Body.Close()
			bodyDirect, errRead := io.ReadAll(respDirect.Body)
			if errRead == nil && len(bodyDirect) > 0 {
				return bodyDirect, nil
			}
		} else if respDirect != nil {
			respDirect.Body.Close()
		}
	}

	// 2. Fallback to RapidAPI if Direct FotMob fails or is rate-limited
	//    (โควตาฟรีแค่ 500 req/เดือน -> ต้องผ่าน budget governor เสมอ)
	if ok, reason := rapidBudgetInstance().Allow(); !ok {
		return nil, fmt.Errorf("rapidapi budget: %s", reason)
	}

	rapidKey := getRapidAPIKey()
	rapidHost := getEnv("RAPIDAPI_HOST", DefaultRapidAPIHost)
	if rapidKey == "" {
		return nil, fmt.Errorf("direct fotmob failed and no rapidapi key configured")
	}

	urlToFetch := targetURL
	if strings.Contains(targetURL, "www.fotmob.com/api") {
		transformed := targetURL
		if strings.Contains(transformed, "/api/matches?date=") {
			parts := strings.Split(transformed, "date=")
			if len(parts) == 2 {
				rawDate := parts[1]
				formattedDate := rawDate
				if len(rawDate) == 8 {
					formattedDate = fmt.Sprintf("%s-%s-%s", rawDate[0:4], rawDate[4:6], rawDate[6:8])
				}
				transformed = fmt.Sprintf("https://%s/api/fotmob/v1/match/list?date=%s&timezone=Asia/Bangkok", rapidHost, formattedDate)
			}
		}

		transformed = strings.Replace(transformed, "https://www.fotmob.com/api/leagues?id=", "https://"+rapidHost+"/api/fotmob/v1/league/details?league_id=", 1)
		transformed = strings.Replace(transformed, "https://www.fotmob.com/api/matchDetails?matchId=", "https://"+rapidHost+"/api/fotmob/v1/match/details?match_id=", 1)
		transformed = strings.Replace(transformed, "https://www.fotmob.com/api/teams?id=", "https://"+rapidHost+"/api/fotmob/v1/team/details?team_id=", 1)
		transformed = strings.Replace(transformed, "https://www.fotmob.com/api/playerData?id=", "https://"+rapidHost+"/api/fotmob/v1/player/details?player_id=", 1)

		if transformed == targetURL {
			transformed = strings.Replace(targetURL, "https://www.fotmob.com/api", "https://"+rapidHost+"/api/fotmob/v1", 1)
		}
		urlToFetch = transformed
	}

	req, err := http.NewRequest("GET", urlToFetch, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("x-rapidapi-key", rapidKey)
	req.Header.Set("x-rapidapi-host", rapidHost)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close()
		}
		if err != nil {
			rapidBudgetInstance().Failure(err)
			return nil, err
		}
		failErr := fmt.Errorf("fotmob returned status: %d", resp.StatusCode)
		rapidBudgetInstance().Failure(failErr)
		return nil, failErr
	}
	defer resp.Body.Close()

	body, errRead := io.ReadAll(resp.Body)
	if errRead != nil {
		rapidBudgetInstance().Failure(errRead)
		return nil, errRead
	}
	rapidBudgetInstance().Success(1, nil)
	return body, nil
}

// looksLikeMatchDetails — ยืนยันว่า payload เป็น matchDetails จริง (มี header/general/content)
// RapidAPI/Proxy บางตัวตอบ JSON error ด้วย HTTP 200 สำหรับแมตช์ที่ไม่มีอยู่จริง
// ถ้าผ่านเฉย ๆ จะได้ 200 + payload เปล่า -> หน้าเว็บโชว์ Home/Away ปลอม แทน 404 จริง
func looksLikeMatchDetails(m map[string]interface{}) bool {
	if m == nil {
		return false
	}
	if _, hasHeader := m["header"]; hasHeader {
		return true
	}
	return m["general"] != nil || m["content"] != nil
}

// shouldNotFound — เมื่อทุกแหล่งข้อมูลล้มและไม่มีข้อมูลสำรองจริง:
// หน้า detail (match/team/player) ตอบ 404 จริงให้หน้าเว็บเรียก notFound() ได้
// ส่วนรายการ/ลีก (matches ตามวันที่, league, ...) คง 200 เสมอ —
// วันไม่มีบอล = ว่างเปล่า ไม่ใช่ 404
func shouldNotFound(dataType string, realData []byte) bool {
	if len(realData) > 0 {
		return false
	}
	return dataType == "match" || dataType == "team" || dataType == "player"
}

func fetchCompleteMatchDetails(matchID string) ([]byte, error) {
	// 1. Try Direct Single Request first (0 RapidAPI quota used)
	directURL := fmt.Sprintf("https://www.fotmob.com/api/matchDetails?matchId=%s", matchID)
	if data, err := fetchFromFotmob(directURL); err == nil && len(data) > 0 {
		var testMap map[string]interface{}
		if errJson := json.Unmarshal(data, &testMap); errJson == nil {
			if looksLikeMatchDetails(testMap) {
				return data, nil
			}
		}
	}

	// 2. Fallback to RapidAPI sub-requests if direct call fails
	rapidKey := getRapidAPIKey()
	rapidHost := getEnv("RAPIDAPI_HOST", DefaultRapidAPIHost)

	client := &http.Client{Timeout: 6 * time.Second}

	fetchSub := func(subPath string) []byte {
		url := fmt.Sprintf("https://%s/api/fotmob/v1/match/%s?match_id=%s", rapidHost, subPath, matchID)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil
		}
		if rapidKey != "" {
			req.Header.Set("x-rapidapi-key", rapidKey)
			req.Header.Set("x-rapidapi-host", rapidHost)
			req.Header.Set("Content-Type", "application/json")
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

		resp, err := client.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				resp.Body.Close()
			}
			return nil
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return body
	}

	var detailsRaw, lineupRaw, factsRaw, tableRaw, statsRaw []byte
	var wg sync.WaitGroup

	wg.Add(5)
	go func() {
		defer wg.Done()
		detailsRaw = fetchSub("details")
	}()
	go func() {
		defer wg.Done()
		lineupRaw = fetchSub("details/lineup")
	}()
	go func() {
		defer wg.Done()
		factsRaw = fetchSub("details/facts")
	}()
	go func() {
		defer wg.Done()
		tableRaw = fetchSub("details/table")
	}()
	go func() {
		defer wg.Done()
		statsRaw = fetchSub("details/stats")
	}()
	wg.Wait()

	if len(detailsRaw) == 0 {
		return nil, fmt.Errorf("failed to fetch base match details")
	}

	var baseMap map[string]interface{}
	if err := json.Unmarshal(detailsRaw, &baseMap); err != nil {
		return nil, err
	}

	// ยืนยันว่าเป็น matchDetails จริง (มี header/general/content)
	// — RapidAPI/Proxy บางตัวตอบ JSON error ด้วย HTTP 200 สำหรับแมตช์ที่ไม่มีจริง
	// ถ้าผ่านเฉย ๆ จะได้ 200 + payload เปล่า -> หน้าเว็บโชว์ Home/Away ปลอมแทน 404
	if !looksLikeMatchDetails(baseMap) {
		return nil, fmt.Errorf("match details จาก sub-request ไม่มี header/general/content (matchId=%s ไม่มีอยู่จริงหรือ upstream ตอบ error)", matchID)
	}

	contentMap := make(map[string]interface{})

	// 1. Lineup
	if len(lineupRaw) > 0 {
		var lineupObj interface{}
		if err := json.Unmarshal(lineupRaw, &lineupObj); err == nil {
			contentMap["lineup"] = lineupObj
		}
	}

	// 2. Facts & H2H Extraction
	if len(factsRaw) > 0 {
		var factsObj map[string]interface{}
		if err := json.Unmarshal(factsRaw, &factsObj); err == nil {
			contentMap["matchFacts"] = factsObj

			// Extract H2H from teamForm if available
			if tf, ok := factsObj["teamForm"].([]interface{}); ok {
				var h2hMatches []map[string]interface{}
				for _, teamGroup := range tf {
					if groupList, ok := teamGroup.([]interface{}); ok {
						for _, item := range groupList {
							if matchItem, ok := item.(map[string]interface{}); ok {
								if tt, ok := matchItem["tooltipText"].(map[string]interface{}); ok {
									hScore, _ := strconv.Atoi(fmt.Sprintf("%v", tt["homeScore"]))
									aScore, _ := strconv.Atoi(fmt.Sprintf("%v", tt["awayScore"]))
									dateStr := fmt.Sprintf("%v", tt["utcTime"])
									if len(dateStr) >= 10 {
										dateStr = dateStr[:10]
									}
									h2hMatches = append(h2hMatches, map[string]interface{}{
										"date": dateStr,
										"time": dateStr,
										"home": map[string]interface{}{
											"name":  tt["homeTeam"],
											"score": hScore,
										},
										"away": map[string]interface{}{
											"name":  tt["awayTeam"],
											"score": aScore,
										},
									})
								}
							}
						}
					}
				}
				if len(h2hMatches) > 0 {
					contentMap["h2h"] = map[string]interface{}{
						"matches": h2hMatches,
					}
				}
			}
		}
	}

	// 3. Table
	if len(tableRaw) > 0 {
		var tableObj interface{}
		if err := json.Unmarshal(tableRaw, &tableObj); err == nil {
			contentMap["table"] = tableObj
		}
	}

	// 4. Stats
	if len(statsRaw) > 0 {
		var statsObj interface{}
		if err := json.Unmarshal(statsRaw, &statsObj); err == nil {
			contentMap["stats"] = statsObj
		}
	}

	baseMap["content"] = contentMap

	return json.Marshal(baseMap)
}

func fetchTeamSquad(teamID string) ([]byte, error) {
	rapidKey := getRapidAPIKey()
	rapidHost := getEnv("RAPIDAPI_HOST", DefaultRapidAPIHost)

	url := fmt.Sprintf("https://%s/api/fotmob/v1/team/details/squad?team_id=%s", rapidHost, teamID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	if rapidKey != "" {
		req.Header.Set("x-rapidapi-key", rapidKey)
		req.Header.Set("x-rapidapi-host", rapidHost)
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

	client := &http.Client{Timeout: 6 * time.Second}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close()
		}
		return nil, fmt.Errorf("failed to fetch squad")
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	var squadList interface{}
	if err := json.Unmarshal(raw, &squadList); err == nil {
		wrap := map[string]interface{}{
			"squad": squadList,
		}
		return json.Marshal(wrap)
	}
	return raw, nil
}

func fetchTeamFixtures(teamID string) ([]byte, error) {
	rapidKey := getRapidAPIKey()
	rapidHost := getEnv("RAPIDAPI_HOST", DefaultRapidAPIHost)

	url := fmt.Sprintf("https://%s/api/fotmob/v1/team/details/fixtures?team_id=%s", rapidHost, teamID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	if rapidKey != "" {
		req.Header.Set("x-rapidapi-key", rapidKey)
		req.Header.Set("x-rapidapi-host", rapidHost)
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

	client := &http.Client{Timeout: 6 * time.Second}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close()
		}
		return nil, fmt.Errorf("failed to fetch fixtures")
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	var fixList interface{}
	if err := json.Unmarshal(raw, &fixList); err == nil {
		wrap := map[string]interface{}{
			"fixtures":    fixList,
			"allFixtures": fixList,
		}
		return json.Marshal(wrap)
	}
	return raw, nil
}

func fetchCompleteTeamDetails(teamID string) ([]byte, error) {
	// 1. Try Direct Single Request first (0 RapidAPI quota used)
	directURL := fmt.Sprintf("https://www.fotmob.com/api/teams?id=%s", teamID)
	if data, err := fetchFromFotmob(directURL); err == nil && len(data) > 0 {
		var testMap map[string]interface{}
		if errJson := json.Unmarshal(data, &testMap); errJson == nil {
			if _, hasDetails := testMap["details"]; hasDetails || testMap["overview"] != nil || testMap["name"] != nil {
				return data, nil
			}
		}
	}

	// 2. Fallback to RapidAPI sub-requests if direct call fails
	rapidKey := getRapidAPIKey()
	rapidHost := getEnv("RAPIDAPI_HOST", DefaultRapidAPIHost)

	client := &http.Client{Timeout: 6 * time.Second}

	fetchSub := func(subPath string) []byte {
		url := fmt.Sprintf("https://%s/api/fotmob/v1/team/%s?team_id=%s", rapidHost, subPath, teamID)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil
		}
		if rapidKey != "" {
			req.Header.Set("x-rapidapi-key", rapidKey)
			req.Header.Set("x-rapidapi-host", rapidHost)
			req.Header.Set("Content-Type", "application/json")
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

		resp, err := client.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				resp.Body.Close()
			}
			return nil
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return body
	}

	var detailsRaw, overviewRaw, squadRaw, fixRaw []byte
	var wg sync.WaitGroup

	wg.Add(4)
	go func() {
		defer wg.Done()
		detailsRaw = fetchSub("details")
	}()
	go func() {
		defer wg.Done()
		overviewRaw = fetchSub("details/overview")
	}()
	go func() {
		defer wg.Done()
		squadRaw = fetchSub("details/squad")
	}()
	go func() {
		defer wg.Done()
		fixRaw = fetchSub("details/fixtures")
	}()
	wg.Wait()

	if len(detailsRaw) == 0 && len(overviewRaw) == 0 {
		return nil, fmt.Errorf("failed to fetch team info")
	}

	resultMap := make(map[string]interface{})

	// 1. Details
	var detMap map[string]interface{}
	if len(detailsRaw) > 0 {
		if err := json.Unmarshal(detailsRaw, &detMap); err == nil {
			resultMap["details"] = detMap
			resultMap["name"] = detMap["name"]
			resultMap["country"] = detMap["country"]
		}
	}

	// 2. Overview
	if len(overviewRaw) > 0 {
		var overMap map[string]interface{}
		if err := json.Unmarshal(overviewRaw, &overMap); err == nil {
			resultMap["overview"] = overMap
			if tf, ok := overMap["teamForm"].([]interface{}); ok {
				var formList []string
				for _, item := range tf {
					if m, ok := item.(map[string]interface{}); ok {
						if rs, ok := m["resultString"].(string); ok && rs != "" {
							formList = append(formList, rs)
						}
					}
				}
				if len(formList) > 0 {
					overMap["form"] = formList
					resultMap["teamForm"] = formList
				}
			}
			if nm, ok := overMap["nextMatch"]; ok {
				resultMap["nextMatch"] = nm
			}
			if tp, ok := overMap["topPlayers"]; ok {
				resultMap["topPlayers"] = tp
			}
			if ven, ok := overMap["venue"]; ok {
				resultMap["venue"] = ven
			}
			if tab, ok := overMap["table"]; ok {
				resultMap["table"] = tab
			}
			if tr, ok := overMap["transfers"]; ok {
				resultMap["transfers"] = tr
			}
		}
	}

	// 3. Squad
	if len(squadRaw) > 0 {
		var squadList interface{}
		if err := json.Unmarshal(squadRaw, &squadList); err == nil {
			resultMap["squad"] = squadList
		}
	}

	// 4. Fixtures
	if len(fixRaw) > 0 {
		var fixList interface{}
		if err := json.Unmarshal(fixRaw, &fixList); err == nil {
			resultMap["fixtures"] = map[string]interface{}{
				"fixtures":    fixList,
				"allFixtures": fixList,
			}
		}
	}

	return json.Marshal(resultMap)
}

func fetchCompleteLeagueDetails(leagueID string) ([]byte, error) {
	// 1. Try Direct Single Request first (0 RapidAPI quota used)
	directURL := fmt.Sprintf("https://www.fotmob.com/api/leagues?id=%s", leagueID)
	if data, err := fetchFromFotmob(directURL); err == nil && len(data) > 0 {
		var testMap map[string]interface{}
		if errJson := json.Unmarshal(data, &testMap); errJson == nil {
			if _, hasTable := testMap["table"]; hasTable || testMap["details"] != nil {
				return data, nil
			}
		}
	}

	// 2. Fallback to RapidAPI sub-requests if direct call fails
	rapidKey := getRapidAPIKey()
	rapidHost := getEnv("RAPIDAPI_HOST", DefaultRapidAPIHost)

	client := &http.Client{Timeout: 6 * time.Second}

	fetchSub := func(subPath string) []byte {
		url := fmt.Sprintf("https://%s/api/fotmob/v1/league/%s?league_id=%s", rapidHost, subPath, leagueID)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil
		}
		if rapidKey != "" {
			req.Header.Set("x-rapidapi-key", rapidKey)
			req.Header.Set("x-rapidapi-host", rapidHost)
			req.Header.Set("Content-Type", "application/json")
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

		resp, err := client.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				resp.Body.Close()
			}
			return nil
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return body
	}

	var detailsRaw, tableRaw, fixRaw, playerStatsRaw, teamStatsRaw, transfersRaw []byte
	var wg sync.WaitGroup

	wg.Add(6)
	go func() {
		defer wg.Done()
		detailsRaw = fetchSub("details")
	}()
	go func() {
		defer wg.Done()
		tableRaw = fetchSub("details/table")
	}()
	go func() {
		defer wg.Done()
		fixRaw = fetchSub("details/fixtures")
	}()
	go func() {
		defer wg.Done()
		playerStatsRaw = fetchSub("details/player-stats")
	}()
	go func() {
		defer wg.Done()
		teamStatsRaw = fetchSub("details/team-stats")
	}()
	go func() {
		defer wg.Done()
		transfersRaw = fetchSub("details/transfers")
	}()
	wg.Wait()

	if len(detailsRaw) == 0 && len(tableRaw) == 0 {
		return nil, fmt.Errorf("failed to fetch league info")
	}

	resultMap := make(map[string]interface{})

	// 1. Details
	var detMap map[string]interface{}
	if len(detailsRaw) > 0 {
		if err := json.Unmarshal(detailsRaw, &detMap); err == nil {
			resultMap["details"] = detMap
			if name, ok := detMap["name"]; ok {
				resultMap["name"] = name
			}
			if country, ok := detMap["country"]; ok {
				resultMap["country"] = country
			}
			if season, ok := detMap["selectedSeason"]; ok {
				resultMap["selectedSeason"] = season
			}
		}
	}
	if detMap == nil {
		detMap = map[string]interface{}{
			"id":             leagueID,
			"name":           "League",
			"country":        "INT",
			"selectedSeason": "2024/2025",
		}
		resultMap["details"] = detMap
	}

	// 2. Table
	if len(tableRaw) > 0 {
		var tableObj interface{}
		if err := json.Unmarshal(tableRaw, &tableObj); err == nil {
			resultMap["table"] = tableObj
		}
	}

	// 3. Fixtures
	if len(fixRaw) > 0 {
		var fixList interface{}
		if err := json.Unmarshal(fixRaw, &fixList); err == nil {
			resultMap["fixtures"] = fixList
			resultMap["matches"] = map[string]interface{}{
				"allMatches": fixList,
			}
		}
	}

	// 4. Stats (Players & Teams)
	statsMap := make(map[string]interface{})
	if len(playerStatsRaw) > 0 {
		var pStats interface{}
		if err := json.Unmarshal(playerStatsRaw, &pStats); err == nil {
			statsMap["players"] = pStats
			if pList, ok := pStats.([]interface{}); ok && len(pList) > 0 {
				if firstCat, ok := pList[0].(map[string]interface{}); ok {
					if topThree, ok := firstCat["topThree"]; ok {
						statsMap["topScorers"] = topThree
					}
				}
			}
		}
	}
	if len(teamStatsRaw) > 0 {
		var tStats interface{}
		if err := json.Unmarshal(teamStatsRaw, &tStats); err == nil {
			statsMap["teams"] = tStats
		}
	}
	resultMap["stats"] = statsMap

	// 5. Transfers
	if len(transfersRaw) > 0 {
		var transList interface{}
		if err := json.Unmarshal(transfersRaw, &transList); err == nil {
			resultMap["transfers"] = transList
		}
	}

	seasonsList := []string{"2026/2027", "2025/2026", "2024/2025", "2023/2024", "2022/2023"}
	resultMap["allAvailableSeasons"] = seasonsList
	resultMap["seasonsWithLinks"] = seasonsList

	return json.Marshal(resultMap)
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

func loadEnvFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			k := strings.TrimSpace(parts[0])
			v := strings.TrimSpace(parts[1])
			if _, exists := os.LookupEnv(k); !exists {
				os.Setenv(k, v)
			}
		}
	}
}
