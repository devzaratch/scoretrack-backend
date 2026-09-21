package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type StreamServerOption struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	URL     string `json:"url"`
	Quality string `json:"quality"`
}

type StoreData struct {
	ChatMessages       map[string][]map[string]interface{} `json:"chat_messages"`
	UserFavorites      map[string][]int                    `json:"user_favorites"`
	MatchSubscriptions map[int][]string                    `json:"match_subscriptions"`
	MatchStreams       map[string][]StreamServerOption     `json:"match_streams"`
	FCMTokens          map[string]int64                    `json:"fcm_tokens"` // token -> unix timestamp ที่ลงทะเบียนล่าสุด
}

type PersistentDB struct {
	data     StoreData
	filePath string
	mu       sync.RWMutex
	dirty    bool
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
			MatchSubscriptions: make(map[int][]string),
			MatchStreams:       make(map[string][]StreamServerOption),
			FCMTokens:          make(map[string]int64),
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
			if loaded.MatchSubscriptions != nil {
				db.data.MatchSubscriptions = loaded.MatchSubscriptions
			}
			if loaded.MatchStreams != nil {
				db.data.MatchStreams = loaded.MatchStreams
			}
			if loaded.FCMTokens != nil {
				db.data.FCMTokens = loaded.FCMTokens
			}
			log.Printf("📦 Loaded persistent database from %s", dbPath)
		}
	}

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

// --------------------------------------------------
// Match Live Streams Persistence
// --------------------------------------------------

func (db *PersistentDB) GetMatchStreams(matchID string) []StreamServerOption {
	db.mu.RLock()
	defer db.mu.RUnlock()

	streams, ok := db.data.MatchStreams[matchID]
	if !ok || len(streams) == 0 {
		return nil
	}
	res := make([]StreamServerOption, len(streams))
	copy(res, streams)
	return res
}

func (db *PersistentDB) SaveMatchStreams(matchID string, streams []StreamServerOption) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.data.MatchStreams == nil {
		db.data.MatchStreams = make(map[string][]StreamServerOption)
	}
	db.data.MatchStreams[matchID] = streams
	db.dirty = true
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
