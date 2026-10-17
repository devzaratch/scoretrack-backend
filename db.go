package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type StoreData struct {
	ChatMessages       map[string][]map[string]interface{} `json:"chat_messages"`
	UserFavorites      map[string][]int                    `json:"user_favorites"`
	UserSessions       map[string]string                   `json:"user_sessions"` // session token -> google sub
	MatchSubscriptions map[int][]string                    `json:"match_subscriptions"`
	FCMTokens          map[string]int64                    `json:"fcm_tokens"`       // token -> unix timestamp ที่ลงทะเบียนล่าสุด
	Predictions               map[string]map[string]interface{} `json:"predictions"`      // userID -> matchID -> prediction data
	UserNotificationPrefs    map[string]map[string]interface{} `json:"notification_prefs"` // userID -> preferences
	Achievements           map[string][]string               `json:"achievements"`       // userID -> list of unlocked achievement IDs
}

type PersistentDB struct {
	data     StoreData
	filePath string
	mu       sync.RWMutex
	dirty    bool

	// ครั้งสุดท้ายที่ push snapshot ขึ้น Redis (throttle 60 วิ/ครั้ง)
	// เขียนจาก syncWorker goroutine ตัวเดียว ไม่ชนกัน
	lastSnapshot time.Time
}

var GlobalDB *PersistentDB

func InitDB() *PersistentDB {
	dataDir := "data"
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Printf("Warning: Failed to create data dir: %v", err)
	}

	dbPath := filepath.Join(dataDir, "store.json")
	db := &PersistentDB{
		filePath: dbPath,
		data: StoreData{
			ChatMessages:       make(map[string][]map[string]interface{}),
			UserFavorites:      make(map[string][]int),
			UserSessions:       make(map[string]string),
			MatchSubscriptions: make(map[int][]string),
			FCMTokens:          make(map[string]int64),
			Predictions:               make(map[string]map[string]interface{}),
			UserNotificationPrefs:    make(map[string]map[string]interface{}),
			Achievements:             make(map[string][]string),
		},
	}

	// Load existing data from file if present
	if fileData, err := os.ReadFile(dbPath); err == nil && len(fileData) > 0 {
		var loaded StoreData
		if err := json.Unmarshal(fileData, &loaded); err == nil {
			if loaded.ChatMessages != nil {
				db.data.ChatMessages = loaded.ChatMessages
			}
			if loaded.UserFavorites != nil {
				db.data.UserFavorites = loaded.UserFavorites
			}
			if loaded.UserSessions != nil {
				db.data.UserSessions = loaded.UserSessions
			}
			if loaded.MatchSubscriptions != nil {
				db.data.MatchSubscriptions = loaded.MatchSubscriptions
			}
			if loaded.FCMTokens != nil {
				db.data.FCMTokens = loaded.FCMTokens
			}
			if loaded.Predictions != nil {
				db.data.Predictions = loaded.Predictions
			}
			if loaded.UserNotificationPrefs != nil {
				db.data.UserNotificationPrefs = loaded.UserNotificationPrefs
			}
			if loaded.Achievements != nil {
				db.data.Achievements = loaded.Achievements
			}
			log.Printf("📦 Loaded persistent database from %s", dbPath)
		}
	}

	// กู้ข้อมูลจาก Redis ถ้า local ว่าง (Render ดิสก์ ephemeral — หายตอน restart)
	db.restoreFromSnapshot()

	// Start Background Async Sync Worker (Flush dirty changes to disk every 3 seconds)
	go db.syncWorker(3 * time.Second)

	GlobalDB = db
	return db
}

func (db *PersistentDB) syncWorker(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		db.mu.Lock()
		if !db.dirty {
			db.mu.Unlock()
			continue
		}

		jsonData, err := json.MarshalIndent(db.data, "", "  ")
		if err == nil {
			tmpPath := db.filePath + ".tmp"
			if err := os.WriteFile(tmpPath, jsonData, 0644); err == nil {
				os.Rename(tmpPath, db.filePath)
				db.dirty = false
				// สำรอง snapshot ขึ้น Redis (best-effort, throttle ในตัว)
				db.maybePushSnapshot(jsonData)
			}
		}
		db.mu.Unlock()
	}
}

// --------------------------------------------------
// Chat Messages Persistence
// --------------------------------------------------

func (db *PersistentDB) SaveChatMessage(matchID string, msg map[string]interface{}) {
	db.mu.Lock()
	defer db.mu.Unlock()

	history := db.data.ChatMessages[matchID]
	history = append(history, msg)
	if len(history) > 100 {
		history = history[len(history)-100:]
	}
	db.data.ChatMessages[matchID] = history
	db.dirty = true
}

func (db *PersistentDB) GetChatHistory(matchID string, limit int) []map[string]interface{} {
	db.mu.RLock()
	defer db.mu.RUnlock()

	history, ok := db.data.ChatMessages[matchID]
	if !ok || len(history) == 0 {
		return []map[string]interface{}{
			{
				"id":   1,
				"user": "System",
				"text": "ยินดีต้อนรับสู่ห้องแชทสด DoBallLaos!",
				"type": "text",
			},
		}
	}

	if limit > 0 && len(history) > limit {
		return history[len(history)-limit:]
	}
	return history
}

// --------------------------------------------------
// User Favorites Persistence
// --------------------------------------------------

func (db *PersistentDB) SaveUserFavorites(userID string, favs []int) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.data.UserFavorites[userID] = favs
	db.dirty = true
}

func (db *PersistentDB) GetUserFavorites(userID string) []int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	favs, ok := db.data.UserFavorites[userID]
	if !ok {
		return []int{}
	}
	return favs
}

// --------------------------------------------------
// User Sessions (Google sub <-> session token)
// --------------------------------------------------

func (db *PersistentDB) SaveUserSession(token, sub string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.data.UserSessions[token] = sub
	db.dirty = true
}

func (db *PersistentDB) GetSessionSub(token string) string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.data.UserSessions[token]
}

// --------------------------------------------------
// Match FCM Subscriptions Persistence
// --------------------------------------------------

func (db *PersistentDB) SubscribeMatch(matchID int, token string) int {
	db.mu.Lock()
	defer db.mu.Unlock()

	tokens := db.data.MatchSubscriptions[matchID]
	exists := false
	for _, t := range tokens {
		if t == token {
			exists = true
			break
		}
	}

	if !exists && token != "" {
		tokens = append(tokens, token)
		db.data.MatchSubscriptions[matchID] = tokens
		db.dirty = true
	}

	return len(tokens)
}

func (db *PersistentDB) UnsubscribeMatch(matchID int, token string) int {
	db.mu.Lock()
	defer db.mu.Unlock()

	tokens := db.data.MatchSubscriptions[matchID]
	newTokens := make([]string, 0, len(tokens))
	for _, t := range tokens {
		if t != token {
			newTokens = append(newTokens, t)
		}
	}

	db.data.MatchSubscriptions[matchID] = newTokens
	db.dirty = true
	return len(newTokens)
}

func (db *PersistentDB) GetMatchSubscribers(matchID int) []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	tokens, ok := db.data.MatchSubscriptions[matchID]
	if !ok {
		return []string{}
	}
	res := make([]string, len(tokens))
	copy(res, tokens)
	return res
}

// HasAnyMatchSubscriptions คืน true เมื่อมีแมตช์ใดแมตช์หนึ่งที่มีคน subscribe อยู่
// (ใช้โดย live ticker ตัดสินใจว่าต้องทำงานต่อแม้ไม่มีคนเปิดเว็บ เพื่อให้ push ถึงมือคนที่กด 🔔)
func (db *PersistentDB) HasAnyMatchSubscriptions() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for _, tokens := range db.data.MatchSubscriptions {
		if len(tokens) > 0 {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------
// FCM Push Token Storage
// ---------------------------------------------------------

// SaveFCMToken เก็บ token ไว้พร้อม timestamp (กัน token ซ้ำอัตโนมัติ)
func (db *PersistentDB) SaveFCMToken(token string) {
	if token == "" {
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.data.FCMTokens == nil {
		db.data.FCMTokens = make(map[string]int64)
	}
	db.data.FCMTokens[token] = time.Now().Unix()
	db.dirty = true
}

// GetFCMTokens คืน token ทั้งหมดที่ลงทะเบียนไว้
func (db *PersistentDB) GetFCMTokens() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	tokens := make([]string, 0, len(db.data.FCMTokens))
	for t := range db.data.FCMTokens {
		tokens = append(tokens, t)
	}
	return tokens
}

// RemoveFCMToken ลบ token ที่ใช้ไม่ได้ออก (เช่น FCM ตอบ unregistered)
func (db *PersistentDB) RemoveFCMToken(token string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.data.FCMTokens, token)
	db.dirty = true
}

// PruneFCMTokens ลบ token ที่เก่าเกิน maxAge (เรียกเป็นรอบๆ กันไฟล์บวม)
func (db *PersistentDB) PruneFCMTokens(maxAge time.Duration) int {
	cutoff := time.Now().Add(-maxAge).Unix()
	db.mu.Lock()
	defer db.mu.Unlock()
	removed := 0
	for token, ts := range db.data.FCMTokens {
		if ts < cutoff {
			delete(db.data.FCMTokens, token)
			removed++
		}
	}
	if removed > 0 {
		db.dirty = true
	}
	return removed
}

// --------------------------------------------------
// User Predictions Persistence
// --------------------------------------------------

func (db *PersistentDB) SavePrediction(userID, matchID string, pred map[string]interface{}) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.data.Predictions == nil {
		db.data.Predictions = make(map[string]map[string]interface{})
	}

	if db.data.Predictions[userID] == nil {
		db.data.Predictions[userID] = make(map[string]interface{})
	}
	db.data.Predictions[userID][matchID] = pred
	db.dirty = true
}

func (db *PersistentDB) GetPredictionsByUser(userID string) map[string]interface{} {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.data.Predictions == nil || db.data.Predictions[userID] == nil {
		return make(map[string]interface{})
	}

	result := make(map[string]interface{})
	for k, v := range db.data.Predictions[userID] {
		result[k] = v
	}
	return result
}

func (db *PersistentDB) GetAllPredictions() map[string]map[string]interface{} {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.data.Predictions == nil {
		return make(map[string]map[string]interface{})
	}

	result := make(map[string]map[string]interface{})
	for userID, predictions := range db.data.Predictions {
		result[userID] = make(map[string]interface{})
		for matchID, pred := range predictions {
			result[userID][matchID] = pred
		}
	}
	return result
}

// --------------------------------------------------
// User Notification Preferences
// --------------------------------------------------

func (db *PersistentDB) SaveNotificationPrefs(userID string, prefs map[string]interface{}) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.data.UserNotificationPrefs == nil {
		db.data.UserNotificationPrefs = make(map[string]map[string]interface{})
	}
	db.data.UserNotificationPrefs[userID] = prefs
	db.dirty = true
}

func (db *PersistentDB) GetNotificationPrefs(userID string) map[string]interface{} {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.data.UserNotificationPrefs == nil {
		return make(map[string]interface{})
	}
	prefs, ok := db.data.UserNotificationPrefs[userID]
	if !ok {
		return make(map[string]interface{})
	}
	result := make(map[string]interface{})
	for k, v := range prefs {
		result[k] = v
	}
	return result
}

// ---------------------------------------------------------
// Admin Stats Methods
// ---------------------------------------------------------

func (db *PersistentDB) CountUsers() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	seen := map[string]bool{}
	for k := range db.data.UserFavorites {
		seen[k] = true
	}
	for _, sub := range db.data.UserSessions {
		seen["user:g:"+sub] = true
	}
	for k := range db.data.Predictions {
		seen[k] = true
	}
	return len(seen)
}

func (db *PersistentDB) CountSessions() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.data.UserSessions)
}

func (db *PersistentDB) CountFCMTokens() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.data.FCMTokens)
}

func (db *PersistentDB) CountPredictions() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	total := 0
	for _, preds := range db.data.Predictions {
		total += len(preds)
	}
	return total
}

func (db *PersistentDB) CountMatchSubscriptions() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	total := 0
	for _, subs := range db.data.MatchSubscriptions {
		total += len(subs)
	}
	return total
}

func (db *PersistentDB) CountAchievements() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	total := 0
	for _, ach := range db.data.Achievements {
		total += len(ach)
	}
	return total
}

func (db *PersistentDB) GetAllUsers() []map[string]interface{} {
	db.mu.RLock()
	defer db.mu.RUnlock()

	type userInfo struct {
		key           string
		favorites     int
		predictions   int
		achievements  int
		hasFCM        bool
	}
	users := map[string]*userInfo{}

	getUser := func(key string) *userInfo {
		if u, ok := users[key]; ok {
			return u
		}
		u := &userInfo{key: key}
		users[key] = u
		return u
	}

	for k, favs := range db.data.UserFavorites {
		u := getUser(k)
		u.favorites = len(favs)
	}
	for k, preds := range db.data.Predictions {
		u := getUser(k)
		u.predictions = len(preds)
	}
	for k, ach := range db.data.Achievements {
		u := getUser(k)
		u.achievements = len(ach)
	}
	for _, sub := range db.data.UserSessions {
		u := getUser("user:g:" + sub)
		u.hasFCM = true
	}

	result := make([]map[string]interface{}, 0, len(users))
	for _, u := range users {
		result = append(result, map[string]interface{}{
			"key":          u.key,
			"favorites":    u.favorites,
			"predictions":  u.predictions,
			"achievements": u.achievements,
			"has_session":  u.hasFCM,
		})
	}
	return result
}

// ---------------------------------------------------------
// Graceful Shutdown Flush
// ---------------------------------------------------------

// FlushNow บันทึกลงไฟล์ทันที ไม่รอ sync worker
// เรียกตอนโปรเกรมกำลังปิด เพื่อไม่ให้ข้อมูล 3 วินาทีสุดท้ายหาย
func (db *PersistentDB) FlushNow() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	jsonData, err := json.MarshalIndent(db.data, "", "  ")
	if err != nil {
		return err
	}

	tmpPath := db.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, jsonData, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, db.filePath); err != nil {
		return err
	}

	db.dirty = false
	log.Printf("💾 บันทึกข้อมูลลงดิสก์ครบก่อนปิดเซิร์ฟเวอร์แล้ว (%s)", db.filePath)
	return nil
}
