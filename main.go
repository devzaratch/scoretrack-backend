package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
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

	// WebSocket Clients Manager
	wsClients   = make(map[*websocket.Conn]bool)
	wsClientsMu sync.Mutex

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
	return key
}

// Main entry point for Go Backend API server
func main() {
	loadEnvFile(".env.local")
	loadEnvFile("../.env.local")
	// 1. ตรวจสอบบริการ Redis ก่อนอย่างเงียบๆ (ใช้ 127.0.0.1 บน Windows)
	redisAddr := getEnv("REDIS_ADDR", "127.0.0.1:6379")
	conn, err := net.DialTimeout("tcp", redisAddr, 500*time.Millisecond)
	if err != nil {
		log.Println("ℹ️ Info: ไม่พบ Redis Server -> สลับไปใช้ Direct Proxy & Mock Fallback (ปกติ หากไม่ได้เปิด Redis)")
		rdb = nil
	} else {
		conn.Close()
		rdb = redis.NewClient(&redis.Options{
			Addr:     redisAddr,
			Password: "",
			DB:       0,
		})
		log.Println("✅ เชื่อมต่อ Redis สำเร็จ!")
	}

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

	// Handlers
	matchesHandler := handleProxy("matches:%s", 30*time.Second, func(c *gin.Context) string {
		date := c.DefaultQuery("date", time.Now().Format("20060102"))
		return fmt.Sprintf("https://www.fotmob.com/api/matches?date=%s", date)
	})

	matchDetailsHandler := handleProxy("match:%s", 15*time.Second, func(c *gin.Context) string {
		matchID := c.Param("id")
		if matchID == "" {
			matchID = c.Query("matchId")
		}
		if matchID == "" {
			matchID = c.Query("id")
		}
		return fmt.Sprintf("https://www.fotmob.com/api/matchDetails?matchId=%s", matchID)
	})

	leagueHandler := handleProxy("league:%s", 10*time.Minute, func(c *gin.Context) string {
		leagueID := c.Param("id")
		if leagueID == "" {
			leagueID = c.Query("id")
		}
		season := c.Query("season")
		if season != "" {
			return fmt.Sprintf("https://www.fotmob.com/api/leagues?id=%s&season=%s", leagueID, season)
		}
		return fmt.Sprintf("https://www.fotmob.com/api/leagues?id=%s", leagueID)
	})

	teamHandler := handleProxy("team:%s", 15*time.Minute, func(c *gin.Context) string {
		teamID := c.Param("id")
		if teamID == "" {
			teamID = c.Query("id")
		}
		return fmt.Sprintf("https://www.fotmob.com/api/teams?id=%s", teamID)
	})

	teamSquadHandler := handleProxy("team:squad:%s", 30*time.Minute, func(c *gin.Context) string {
		teamID := c.Param("id")
		if teamID == "" {
			teamID = c.Query("id")
		}
		return fmt.Sprintf("https://www.fotmob.com/api/teams?id=%s&squad=true", teamID)
	})

	teamFixturesHandler := handleProxy("team:fixtures:%s", 15*time.Minute, func(c *gin.Context) string {
		teamID := c.Param("id")
		if teamID == "" {
			teamID = c.Query("id")
		}
		return fmt.Sprintf("https://www.fotmob.com/api/teams?id=%s&fixtures=true", teamID)
	})

	playerHandler := handleProxy("player:%s", 30*time.Minute, func(c *gin.Context) string {
		playerID := c.Param("id")
		if playerID == "" {
			playerID = c.Query("id")
		}
		return fmt.Sprintf("https://www.fotmob.com/api/playerData?id=%s", playerID)
	})

	searchHandler := handleProxy("search:%s", 15*time.Minute, func(c *gin.Context) string {
		term := c.Query("q")
		if term == "" {
			term = c.Query("term")
		}
		return fmt.Sprintf("https://www.fotmob.com/api/search/suggest?term=%s", url.QueryEscape(term))
	})

	// Register Routes under /api and /api/api (Safety Alias)
	setupRoutes := func(rg *gin.RouterGroup) {
		// Search
		rg.GET("/search", searchHandler)
		rg.GET("/search/suggest", searchHandler)

		// Matches
		rg.GET("/matches", matchesHandler)
		rg.GET("/matchesDay", matchesHandler)
		rg.GET("/live", matchesHandler)
		rg.GET("/allMatches", matchesHandler)
		rg.GET("/match/live", matchesHandler)

		// Match Details
		rg.GET("/match/:id", matchDetailsHandler)
		rg.GET("/match/:id/stream", handleGetMatchStream)
		// ❗ POST /match/:id/stream ย้ายไป adminGroup ด้านล่าง (ต้องมี ADMIN_TOKEN)
		rg.GET("/match/:id/commentary", matchDetailsHandler)
		rg.GET("/match", matchDetailsHandler)
		rg.GET("/matchDetails", matchDetailsHandler)

		// League
		rg.GET("/league/:id", leagueHandler)
		rg.GET("/league/:id/table", leagueHandler)
		rg.GET("/league/:id/fixtures", leagueHandler)
		rg.GET("/league/:id/stats", leagueHandler)
		rg.GET("/league", leagueHandler)
		rg.GET("/leagues", leagueHandler)

		// Team
		rg.GET("/team/:id/squad", teamSquadHandler)
		rg.GET("/team/:id/fixtures", teamFixturesHandler)
		rg.GET("/team/:id", teamHandler)
		rg.GET("/team", teamHandler)
		rg.GET("/teams", teamHandler)

		// Player
		rg.GET("/player/:id", playerHandler)
		rg.GET("/player", playerHandler)
		rg.GET("/playerData", playerHandler)

		// Auth & User Endpoints
		rg.POST("/auth/google", handleGoogleAuth)
		rg.GET("/user/favorites", handleGetFavorites)
		rg.POST("/user/favorites", handleSaveFavorites)
		rg.POST("/user/fcm", handleFCMRegister)
		rg.POST("/user/match-subscribe", handleMatchSubscribe)
	}

	apiGroup := r.Group("/api")
	setupRoutes(apiGroup)

	nestedApiGroup := r.Group("/api/api")
	setupRoutes(nestedApiGroup)

	// ---------------------------------------------------------
	// Admin-only routes (ต้องส่ง header X-Admin-Token)
	// ---------------------------------------------------------
	adminRoutes := func(rg *gin.RouterGroup) {
		rg.POST("/match/:id/stream", handleSaveMatchStream)
		rg.GET("/admin/verify", handleAdminVerify)
	}
	adminRoutes(r.Group("/api", AdminAuthMiddleware()))
	adminRoutes(r.Group("/api/api", AdminAuthMiddleware()))

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

var (
	liveTickerCacheData []byte
	liveTickerLastFetch time.Time
	liveTickerCacheMu   sync.Mutex
)

func startLiveTicker() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		wsClientsMu.Lock()
		clientCount := len(wsClients)
		wsClientsMu.Unlock()

		if clientCount == 0 {
			continue
		}

		// 1. Polling Real Live Matches Data from FotMob with Smart 35s In-Memory Debouncing
		todayStr := time.Now().Format("20060102")
		targetURL := fmt.Sprintf("https://www.fotmob.com/api/matches?date=%s", todayStr)

		liveTickerCacheMu.Lock()
		useCached := time.Since(liveTickerLastFetch) < 35*time.Second && len(liveTickerCacheData) > 0
		var data []byte
		var err error

		if useCached {
			data = liveTickerCacheData
		} else {
			data, err = fetchFromFotmob(targetURL)
			if err == nil && len(data) > 0 {
				liveTickerCacheData = data
				liveTickerLastFetch = time.Now()
			} else if len(liveTickerCacheData) > 0 {
				data = liveTickerCacheData
				err = nil
			}
		}
		liveTickerCacheMu.Unlock()

		var liveUpdates []map[string]interface{}

		if err == nil && len(data) > 0 {
			var result struct {
				Leagues []struct {
					Matches []struct {
						ID     int `json:"id"`
						Status struct {
							LiveTime struct {
								Short string `json:"short"`
							} `json:"liveTime"`
							Reason struct {
								Short string `json:"short"`
							} `json:"reason"`
							Finished bool   `json:"finished"`
							Started  bool   `json:"started"`
							ScoreStr string `json:"scoreStr"`
						} `json:"status"`
						Home struct {
							Score int `json:"score"`
						} `json:"home"`
						Away struct {
							Score int `json:"score"`
						} `json:"away"`
					} `json:"matches"`
				} `json:"leagues"`
			}

			if realMatches, transformErr := TransformRapidAPIMatchesToRealMatches(data); transformErr == nil && len(realMatches) > 0 {
				for _, rm := range realMatches {
					liveUpdates = append(liveUpdates, map[string]interface{}{
						"match_id":   rm.MatchID,
						"status":     rm.Status,
						"home_score": rm.HomeTeam.Score,
						"away_score": rm.AwayTeam.Score,
					})
				}
			} else if err := json.Unmarshal(data, &result); err == nil {
				for _, league := range result.Leagues {
					for _, m := range league.Matches {
						statusStr := m.Status.LiveTime.Short
						if statusStr == "" {
							statusStr = m.Status.Reason.Short
						}
						if statusStr == "" && m.Status.Finished {
							statusStr = "FT"
						}

						liveUpdates = append(liveUpdates, map[string]interface{}{
							"match_id":   m.ID,
							"status":     statusStr,
							"home_score": m.Home.Score,
							"away_score": m.Away.Score,
						})
					}
				}
			}
		}

		// Fallback Mock update if no live match data found
		if len(liveUpdates) == 0 {
			liveUpdates = append(liveUpdates, map[string]interface{}{
				"match_id":   4200001,
				"status":     "Live",
				"home_score": 2,
				"away_score": 1,
			})
		}

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

var (
	// Match Subscribers Map: matchID -> set of FCM tokens
	matchSubscribers   = make(map[int]map[string]bool)
	matchSubscribersMu sync.Mutex
)

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
	if err := c.BindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ข้อมูลไม่ถูกต้อง"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"username": "FootballFan",
		"token":    "demo_jwt_token_scoretrack",
		"favs":     []int{4200001, 4200002},
	})
}

func handleGetFavorites(c *gin.Context) {
	userKey := getUserIdentifier(c)
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

	userKey := getUserIdentifier(c)
	if GlobalDB != nil {
		GlobalDB.SaveUserFavorites(userKey, body.Favorites)
	}

	log.Printf("⭐ บันทึกรายการโปรดสำหรับผู้ใช้ [%s]: %v", userKey, body.Favorites)
	c.JSON(http.StatusOK, gin.H{"status": "saved", "favorites": body.Favorites, "user_key": userKey})
}

// ---------------------------------------------------------
// Live Stream Handlers
// ---------------------------------------------------------

func handleGetMatchStream(c *gin.Context) {
	matchID := c.Param("id")
	if matchID == "" {
		matchID = c.Query("id")
	}

	var customStreams []StreamServerOption
	if GlobalDB != nil && matchID != "" {
		customStreams = GlobalDB.GetMatchStreams(matchID)
	}

	if len(customStreams) > 0 {
		c.JSON(http.StatusOK, gin.H{
			"match_id": matchID,
			"source":   "custom",
			"servers":  customStreams,
		})
		return
	}

	// Default broadcast channels
	defaultServers := []StreamServerOption{
		{
			ID:      "srv-1",
			Name:    "Server 1 (สัญญาณหลัก HD 1080p)",
			URL:     "https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8",
			Quality: "1080p",
		},
		{
			ID:      "srv-2",
			Name:    "Server 2 (สัญญาณสำรอง 720p)",
			URL:     "https://test-streams.mux.dev/issue664_0/prog_index.m3u8",
			Quality: "720p",
		},
		{
			ID:      "srv-3",
			Name:    "Server 3 (สัญญาณบรรยายไทย)",
			URL:     "https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8",
			Quality: "720p",
		},
	}

	c.JSON(http.StatusOK, gin.H{
		"match_id": matchID,
		"source":   "default",
		"servers":  defaultServers,
	})
}

func handleSaveMatchStream(c *gin.Context) {
	matchID := c.Param("id")
	if matchID == "" {
		matchID = c.Query("id")
	}

	var body struct {
		Servers []StreamServerOption `json:"servers"`
	}
	if err := c.BindJSON(&body); err != nil || matchID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ข้อมูลไม่ถูกต้อง"})
		return
	}

	if GlobalDB != nil {
		GlobalDB.SaveMatchStreams(matchID, body.Servers)
	}

	c.JSON(http.StatusOK, gin.H{
		"status":   "saved",
		"match_id": matchID,
		"servers":  body.Servers,
	})
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

	matchSubscribersMu.Lock()
	if matchSubscribers[body.MatchID] == nil {
		matchSubscribers[body.MatchID] = make(map[string]bool)
	}

	if body.Action == "unsubscribe" {
		delete(matchSubscribers[body.MatchID], token)
		log.Printf("🔔 ยกเลิกการติดตามการแจ้งเตือนแมตช์ #%d (Token: %s)", body.MatchID, token)
	} else {
		matchSubscribers[body.MatchID][token] = true
		log.Printf("🔔 ลงทะเบียนการแจ้งเตือนประตูแมตช์ #%d สำเร็จ (Token: %s)", body.MatchID, token)
	}
	subCount := len(matchSubscribers[body.MatchID])
	matchSubscribersMu.Unlock()

	c.JSON(http.StatusOK, gin.H{
		"status":      "success",
		"match_id":    body.MatchID,
		"subscribed":  body.Action != "unsubscribe",
		"subscribers": subCount,
	})
}

// Rate Limiter Middleware (120 reqs/min per IP)
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

// ---------------------------------------------------------
// Helper Functions & Caching Logic
// ---------------------------------------------------------

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
		if rdb != nil {
			cachedData, err := rdb.Get(ctx, cacheKey).Result()
			if err == nil && cachedData != "" {
				c.Header("X-Cache", "HIT")
				c.Header("X-Data-Source", "Redis-Cache")
				c.Data(http.StatusOK, "application/json; charset=utf-8", []byte(cachedData))
				return
			}
		} else {
			if cachedData, found := memoryCache.Get(cacheKey); found {
				c.Header("X-Cache", "HIT")
				c.Header("X-Data-Source", "In-Memory-Cache")
				c.Data(http.StatusOK, "application/json; charset=utf-8", cachedData)
				return
			}
		}

		// 2. Fetch from Upstream API (FotMob or Real Football Engine)
		dataType := ResolveDataType(cacheKeyPattern)

		var data []byte
		var err error

		if dataType == "match" {
			data, err = fetchCompleteMatchDetails(paramOrQuery)
		} else if dataType == "team" {
			data, err = fetchCompleteTeamDetails(paramOrQuery)
		} else if dataType == "team-squad" {
			data, err = fetchTeamSquad(paramOrQuery)
		} else if dataType == "team-fixtures" {
			data, err = fetchTeamFixtures(paramOrQuery)
		} else if dataType == "league" {
			data, err = fetchCompleteLeagueDetails(paramOrQuery)
		} else {
			data, err = fetchFromFotmob(targetURL)
		}

		if err == nil && len(data) > 0 {
			if dataType == "matches" {
				if transformed, transformErr := TransformRapidAPIMatchesToRealMatches(data); transformErr == nil && len(transformed) > 0 {
					data, _ = json.Marshal(transformed)
				}
			}

			// Validate if league data is incomplete (e.g. RapidAPI metadata-only JSON)
			var rawMap map[string]interface{}
			if errJson := json.Unmarshal(data, &rawMap); errJson == nil {
				_, hasTable := rawMap["table"]
				_, hasDetails := rawMap["details"]
				if dataType == "league" && (!hasTable || !hasDetails) {
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
			if rdb != nil {
				rdb.Set(ctx, cacheKey, string(data), ttl)
			} else {
				memoryCache.Set(cacheKey, data, ttl)
			}

			c.Header("X-Cache", "MISS")
			c.Header("X-Data-Source", "FotMob-Live")
			c.Data(http.StatusOK, "application/json; charset=utf-8", data)
			return
		}

		dataType = ResolveDataType(cacheKeyPattern)
		realData := GetRealData(dataType, paramOrQuery)
		if len(realData) > 0 {
			if rdb != nil {
				rdb.Set(ctx, cacheKey, string(realData), ttl)
			} else {
				memoryCache.Set(cacheKey, realData, ttl)
			}
		}

		c.Header("X-Cache", "MISS")
		c.Header("X-Data-Source", "Real-Data-Engine")
		c.Data(http.StatusOK, "application/json; charset=utf-8", realData)
	}
}

func fetchFromFotmob(targetURL string) ([]byte, error) {
	rapidKey := getRapidAPIKey()
	rapidHost := getEnv("RAPIDAPI_HOST", DefaultRapidAPIHost)
	client := &http.Client{Timeout: 6 * time.Second}

	// 1. If no RAPIDAPI_KEY is set, fetch directly from FotMob Web API
	if rapidKey == "" {
		reqDirect, errDirect := http.NewRequest("GET", targetURL, nil)
		if errDirect != nil {
			return nil, errDirect
		}
		reqDirect.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
		reqDirect.Header.Set("Accept", "application/json, text/plain, */*")
		reqDirect.Header.Set("Accept-Language", "th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7")
		reqDirect.Header.Set("Referer", "https://www.fotmob.com/")

		respDirect, errResp := client.Do(reqDirect)
		if errResp == nil && respDirect.StatusCode == http.StatusOK {
			defer respDirect.Body.Close()
			return io.ReadAll(respDirect.Body)
		}
		if respDirect != nil {
			respDirect.Body.Close()
			return nil, fmt.Errorf("direct fotmob returned status: %d", respDirect.StatusCode)
		}
		return nil, errResp
	}

	// 2. Construct RapidAPI URL v1 mapping when rapidKey is set
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

	// 3. Fallback to Direct URL if RapidAPI fails or returns non-200
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close()
		}

		reqDirect, errDirect := http.NewRequest("GET", targetURL, nil)
		if errDirect == nil {
			reqDirect.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
			reqDirect.Header.Set("Accept", "application/json, text/plain, */*")
			reqDirect.Header.Set("Accept-Language", "th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7")
			reqDirect.Header.Set("Referer", "https://www.fotmob.com/")

			respDirect, errResp := client.Do(reqDirect)
			if errResp == nil && respDirect.StatusCode == http.StatusOK {
				defer respDirect.Body.Close()
				return io.ReadAll(respDirect.Body)
			} else if respDirect != nil {
				respDirect.Body.Close()
			}
		}
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("fotmob returned status: %d", resp.StatusCode)
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func fetchCompleteMatchDetails(matchID string) ([]byte, error) {
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
