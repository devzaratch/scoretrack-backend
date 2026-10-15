package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
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
		log.Println("⚠️ RAPIDAPI_KEY is not set in environment or .env.local")
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
		log.Println("ℹ️ Info: ไม่พบ Redis Server -> สลับไปใช้ Direct Proxy & Mock Fallback (ปกติ หากไม่ได้เปิด Redis)")
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
			log.Printf("ℹ️ Info: Redis PING ไม่สำเร็จ (%v) -> ใช้ in-memory cache แทน", pingErr)
			client.Close()
			rdb = nil
		} else {
			rdb = client
			log.Println("✅ เชื่อมต่อ Redis สำเร็จ!")
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

	r := gin.Default()

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
		ExposeHeaders:    []string{"Content-Length", "X-Cache", "X-Data-Source"},
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
		rg.GET("/user/favorites", handleGetFavorites)
		rg.POST("/user/favorites", rateLimitMiddleware("user-write", RLUserWrite, time.Minute), handleSaveFavorites)
		rg.POST("/user/fcm", rateLimitMiddleware("user-write", RLUserWrite, time.Minute), handleFCMRegister)
		rg.POST("/user/match-subscribe", rateLimitMiddleware("user-write", RLUserWrite, time.Minute), handleMatchSubscribe)

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

		// Error reporting (frontend monitoring)
		rg.POST("/client-errors", rateLimitMiddleware("error-report", RLErrorReport, time.Minute), handleClientError)

		// Admin Panel
		setupAdminRoutes(rg)
	}

	apiGroup := r.Group("/api")
	setupRoutes(apiGroup)

	nestedApiGroup := r.Group("/api/api")
	setupRoutes(nestedApiGroup)

	port := getEnv("PORT", "8080")
	log.Printf("🚀 Go Backend (พร้อม WebSocket Live Server & Dynamic Engine) เริ่มทำงานที่ http://localhost:%s", port)
	r.Run(":" + port)
}

// ---------------------------------------------------------
// WebSocket & Real-time Live Handler
// ---------------------------------------------------------

func handleWebSocket(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("WebSocket Upgrade Error: %v", err)
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

		// ไม่มีคนดูเว็บ AND ไม่มีแมตช์ไหนถูก subscribe push -> ข้ามรอบนี้ (ไม่กินโควตา)
		// แต่ถ้ามีคนกด 🔔 subscribe แมตช์ใดไว้ -> ต้องทำงานต่อแม้ไม่มีคนเปิดเว็บ
		// ไม่งั้น push เริ่มแข่ง/ประตู/จบเกม ไม่มีทางถึงมือคนที่รอ
		hasSubs := GlobalDB != nil && GlobalDB.HasAnyMatchSubscriptions()
		if clientCount == 0 && !hasSubs {
			continue
		}

		// เลือกแหล่ง live ตัวแรกที่ยังใช้ได้ (GOAL ก่อน ตัวสำรอง API-Football ทีหลัง)
		src := activeLiveSource()
		if src == nil {
			continue
		}

		// 1. REST sweep ตามรอบของ provider แต่ละตัว
		//    (GOAL = 180 วิ เพราะมี WebSocket push ช่วย realtime / API-Football = 60 วิ
		//     เพราะไม่มี push ต้องพึ่ง sweep อย่างเดียว — ทุกการ sweep = 1 call เท่านั้น)
		if time.Since(lastREST) >= src.SweepInterval() {
			if err := src.LiveSweep(); err == nil {
				lastREST = time.Now()
			}

			// ถ้ามีคู่แข่งใหม่ที่ยังไม่อยู่ในรายการวันนี้ -> สั่งโหลดรายการใหม่ (จำกัด 1 ครั้ง/2 นาที)
			if time.Since(lastInvalidate) > 2*time.Minute && src.HasUnknownLiveMatches() {
				src.InvalidateTodayCache()
				lastInvalidate = time.Now()
				log.Printf("🔄 live ticker: พบคู่แข่งใหม่ -> โหลดรายการวันนี้ใหม่")
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

		// 3. Non-blocking Broadcast Live Updates via LiveHub
		GlobalLiveHub.BroadcastJSON(liveUpdates)

		// 4. prune สถานะแมตช์เก่า (จบ/หายไปเกิน 2 ชม.) กัน memory leak
		if removed := GlobalGoalDetector.pruneOlderThan(2 * time.Hour); removed > 0 {
			log.Printf("🧹 goal detector: prune %d match states เก่าแล้ว", removed)
		}
	}
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
	authHeader := c.GetHeader("Authorization")
	userKey := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
	if userKey != "" && userKey != "null" && userKey != "undefined" {
		return "user:" + userKey
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
		log.Printf("❌ Google auth verify ล้มเหลว: %v", err)
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

	log.Printf("✅ Google login สำเร็จ [%s]: %s", userKey, name)
	c.JSON(http.StatusOK, gin.H{
		"username": name,
		"token":    sessionToken,
		"favs":     favs,
	})
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

// resolveUserKey ผูก Bearer session -> google sub ก่อน fallback แบบเดิม
func resolveUserKey(c *gin.Context) string {
	authHeader := c.GetHeader("Authorization")
	token := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
	if token != "" && token != "null" && token != "undefined" && GlobalDB != nil {
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

	log.Printf("⭐ บันทึกรายการโปรดสำหรับผู้ใช้ [%s]: %v", userKey, body.Favorites)
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

	log.Printf("📲 ลงทะเบียน FCM Push Token สำเร็จ (ท้าย token: ...%s)", tailOf(body.FCMToken, 8))
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
			log.Printf("🔔 ยกเลิกการติดตามการแจ้งเตือนแมตช์ #%d (Token: %s)", body.MatchID, token)
		} else {
			subCount = GlobalDB.SubscribeMatch(body.MatchID, token)
			log.Printf("🔔 ลงทะเบียนการแจ้งเตือนประตูแมตช์ #%d สำเร็จ (Token: %s)", body.MatchID, token)
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
		"count":      len(leaderboard),
		"scoring":    "ถูกสกอร์เป๊ะ +3 · ถูกผล +1 · ผิด 0",
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
		MatchID     string                 `json:"match_id"`
		Prediction map[string]interface{} `json:"prediction"`
	}
	if err := c.BindJSON(&body); err != nil || body.MatchID == "" || body.Prediction == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ข้อมูลไม่ถูกต้อง"})
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
			"match_id": "999999",
			"home_name": "ททีม A",
			"away_name": "ทีม B",
			"start_time": time.Now().Add(24 * time.Hour).Format(time.RFC3339),
			"status": "scheduled",
		},
	}
	c.JSON(http.StatusOK, gin.H{"calendar": events})
}

func handleCalendarIcal(c *gin.Context) {
	events := []map[string]interface{}{
		{
			"match_id": "999999",
			"home_name": "ทีม A",
			"away_name": "ทีม B",
			"start_time": time.Now().Add(24 * time.Hour).Format(time.RFC3339),
			"status": "scheduled",
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
	log.Printf("⚠️ Client Error [%s] %s: %s", src, page, msg)

	c.JSON(http.StatusOK, gin.H{"status": "logged"})
}

// Rate Limiter Middleware (120 reqs/min per IP — global safety net)
func rateLimiterMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		ip := c.ClientIP()
		if !GlobalRateLimiter.Allow(ip, 120, time.Minute) {
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
		if !GlobalRateLimiter.AllowKey(key, limit, window) {
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
	RLAuth          = 10    // auth/google — ป้องกัน token spam
	RLSearch        = 30    // search suggest — แพง (GOAL API)
	RLProxy         = 60    // proxy endpoints (matches/league/team/player)
	RLNews          = 30    // news RSS
	RLErrorReport   = 20    // client-errors — กัน log spam
	RLPrediction    = 30    // predictions save
	RLUserWrite     = 30    // favorites/fcm/subscribe writes
)

// ---------------------------------------------------------
// Helper Functions & Caching Logic
// ---------------------------------------------------------

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
		"generated_at":    time.Now().Format(time.RFC3339),
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
			c.Header("X-Data-Source", cacheSource)
			c.Data(http.StatusOK, "application/json; charset=utf-8", cachedData)
			return
		}

		// 2. Fetch from Upstream API (Data Provider ก่อน แล้วค่อย FotMob/Engine)
		dataType := ResolveDataType(cacheKeyPattern)

		var data []byte
		var err error
		sourceTag := "fotmob"

		if dataType == "matches" {
			if raw, src, ok := fetchMatchesFromProviders(paramOrQuery); ok {
				data, sourceTag = raw, src
			} else {
				data, err = fetchFromFotmob(targetURL)
			}
		} else if dataType == "match" {
			if raw, src, ok := fetchMatchDetailsFromProviders(paramOrQuery); ok {
				data, sourceTag = raw, src
			} else {
				data, err = fetchCompleteMatchDetails(paramOrQuery)
			}
		} else if dataType == "team" {
			if raw, src, ok := fetchTeamFromProviders("", paramOrQuery); ok {
				data, sourceTag = raw, src
			} else {
				data, err = fetchCompleteTeamDetails(paramOrQuery)
			}
		} else if dataType == "team-squad" {
			if raw, src, ok := fetchTeamFromProviders("squad", paramOrQuery); ok {
				data, sourceTag = raw, src
			} else {
				data, err = fetchTeamSquad(paramOrQuery)
			}
		} else if dataType == "team-fixtures" {
			if raw, src, ok := fetchTeamFromProviders("fixtures", paramOrQuery); ok {
				data, sourceTag = raw, src
			} else {
				data, err = fetchTeamFixtures(paramOrQuery)
			}
		} else if dataType == "league" {
			kind := leagueKindFromPattern(cacheKeyPattern)
			if raw, src, ok := fetchLeagueFromProviders(kind, paramOrQuery); ok {
				data, sourceTag = raw, src
			} else {
				data, err = fetchCompleteLeagueDetails(paramOrQuery)
			}
		} else if dataType == "player" {
			// Phase 1c: หน้าโปรไฟล์นักเตะ -> GOAL ก่อน ถ้า resolve ไม่ได้/ล้ม ถอยไป FotMob เหมือนเดิม
			if raw, src, ok := fetchPlayerFromProviders(paramOrQuery); ok {
				data, sourceTag = raw, src
			} else {
				data, err = fetchFromFotmob(targetURL)
			}
		} else {
			data, err = fetchFromFotmob(targetURL)
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
			cacheSet(cacheKey, data, cacheTTL)

			c.Header("X-Cache", "MISS")
			c.Header("X-Data-Source", sourceTag)
			c.Data(http.StatusOK, "application/json; charset=utf-8", data)
			return
		}

		dataType = ResolveDataType(cacheKeyPattern)
		realData := GetRealData(dataType, paramOrQuery)
		if len(realData) > 0 {
			cacheSet(cacheKey, realData, ttl)
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

func fetchCompleteMatchDetails(matchID string) ([]byte, error) {
	// 1. Try Direct Single Request first (0 RapidAPI quota used)
	directURL := fmt.Sprintf("https://www.fotmob.com/api/matchDetails?matchId=%s", matchID)
	if data, err := fetchFromFotmob(directURL); err == nil && len(data) > 0 {
		var testMap map[string]interface{}
		if errJson := json.Unmarshal(data, &testMap); errJson == nil {
			if _, hasHeader := testMap["header"]; hasHeader || testMap["general"] != nil || testMap["content"] != nil {
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
