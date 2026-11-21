package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type AdminAuthFailRecord struct {
	Fails       int   `json:"fails"`
	WindowStart int64 `json:"window_start_unix,omitempty"`
	LockedUntil int64 `json:"locked_until_unix,omitempty"`
}

type AdminAuditRecord struct {
	Time   string `json:"time"`
	IP     string `json:"ip"`
	Method string `json:"method"`
	Path   string `json:"path"`
	Status int    `json:"status"`
}

type StoreData struct {
	ChatMessages          map[string][]map[string]interface{} `json:"chat_messages"`
	UserFavorites         map[string][]int                    `json:"user_favorites"`
	UserSessions          UserSessionStore                    `json:"user_sessions"` // session token -> server-side session record
	MatchSubscriptions    map[int][]string                    `json:"match_subscriptions"`
	FCMTokens             map[string]int64                    `json:"fcm_tokens"`         // token -> unix timestamp ที่ลงทะเบียนล่าสุด
	Predictions           map[string]map[string]interface{}   `json:"predictions"`        // userID -> matchID -> prediction data
	UserNotificationPrefs map[string]map[string]interface{}   `json:"notification_prefs"` // userID -> preferences
	Achievements          map[string][]string                 `json:"achievements"`       // userID -> list of unlocked achievement IDs
	APIKeys               map[string]APIKeyRecord             `json:"api_keys"`           // api key -> record (แผนข้อ 7: public API)
	UserInterests         map[string][]string                 `json:"user_interests"`     // userID -> ["league:47", "team:arsenal fc"] (แผนข้อ 4)
	// --- Auth V2 (ข้อ 1 รอบใหม่) ---
	UserProfiles      map[string]UserProfile          `json:"user_profiles"`      // sub -> profile
	EmailToSub        map[string]string               `json:"email_to_sub"`       // email -> sub
	UserPasswords     map[string]string               `json:"user_passwords"`     // email -> bcrypt hash
	VerificationCodes map[string]VerificationCode     `json:"verification_codes"` // email -> code + expiry
	PasswordResets    map[string]PasswordReset        `json:"password_resets"`    // email -> token + expiry
	RefreshTokens     map[string]string               `json:"refresh_tokens"`     // sub -> refresh token
	RefreshExpires    map[string]int64                `json:"refresh_expires"`    // sub -> unix expiry
	ChatReports       []ChatReport                    `json:"chat_reports"`       // (Phase B ข้อ 10) รายงานข้อความแชทที่ผู้ใช้กดรายงาน
	// --- Hardening set4 task 6: admin lockout/audit durability ---
	AdminAuthFails map[string]AdminAuthFailRecord `json:"admin_auth_fails,omitempty"`
	AdminAuditLogs []AdminAuditRecord             `json:"admin_audit_logs,omitempty"`
}

type PersistentDB struct {
	data     StoreData
	filePath string
	store    Storage // ชั้นเก็บข้อมูลถาวร (ไฟล์/Postgres) — แผนข้อ 1
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
		logInfof("Warning: Failed to create data dir: %v", err)
	}

	dbPath := filepath.Join(dataDir, "store.json")
	db := &PersistentDB{
		filePath: dbPath,
		data:     newEmptyStoreData(),
	}

	// แผนข้อ 1: เลือกที่เก็บข้อมูล (Postgres ถ้าตั้ง DATA_BACKEND=postgres, ไม่งั้นไฟล์แบบเดิม)
	store, storeErr := newStorage()
	if storeErr != nil {
		logInfof("⚠️ เปิด storage ไม่สำเร็จ (%v) — ใช้ไฟล์แทน", storeErr)
		store = newFileStorage(dbPath)
	}
	db.store = store

	// Load existing data from storage
	if loaded, err := db.storage().Load(); err == nil {
		db.data = loaded
		logInfof("📦 Loaded persistent database from %s", db.storage().Name())

		// แผนข้อ 1: ครั้งแรกที่ย้ายมาใช้ DB — ถ้า DB ยังว่างแต่มีไฟล์เดิมอยู่ ให้นำเข้าไป
		if isEmptyStore(db.data) {
			if legacy, lerr := os.ReadFile(dbPath); lerr == nil && len(legacy) > 0 {
				var old StoreData
				if json.Unmarshal(legacy, &old) == nil {
					db.data = mergeLoaded(old)
					db.dirty = true
					logInfof("📥 นำเข้าข้อมูลจาก %s เข้า %s (ครั้งเดียว)", dbPath, db.storage().Name())
				}
			}
		}
	} else {
		logInfof("⚠️ อ่านข้อมูลไม่สำเร็จ: %v", err)
	}

	// กู้ข้อมูลจาก Redis ถ้า local ว่าง (Render ดิสก์ ephemeral — หายตอน restart)
	db.restoreFromSnapshot()

	// ความปลอดภัย (แผนข้อ 2): ย้าย API key ที่ยังเก็บ plaintext ใน map
	// ไปเป็น sha256 (รวมของที่เพิ่งกู้จาก snapshot ด้วย)
	if n := db.migrateAPIKeysToHashed(); n > 0 {
		logInfof("🔒 ย้าย API key %d รายการเป็น sha256 (ไม่เก็บค่าเต็มใน DB แล้ว)", n)
	}

	// บังคับ dirty ตั้งแต่ boot (เมื่อมี Redis) — syncWorker จะ flush + push
	// snapshot แรกภายใน 3 วิ กันช่องว่างที่ไม่มีกิจกรรมผู้ใช้แล้วไม่เคยมี
	// snapshot เกิดขึ้นเลย (ถ้าดิสก์หายทีหลังจะไม่มีอะไรให้กู้)
	if rdb != nil {
		db.dirty = true
	}

	// Auth V2: เตรียม map สำหรับระบบสมาชิกใหม่
	db.EnsureAuthDefaults()

	// Start Background Async Sync Worker (Flush dirty changes to disk every 3 seconds)
	go db.syncWorker(3 * time.Second)

	GlobalDB = db
	return db
}

// storage — คืน storage ที่ใช้จริง (ถ้ายังไม่ได้กำหนด = ใช้ไฟล์ตาม filePath)
// ป้องกัน nil pointer เวลาโค้ดสร้าง PersistentDB เอง (เช่นในเทสต์)
func (db *PersistentDB) storage() Storage {
	if db.store != nil {
		return db.store
	}
	return newFileStorage(db.filePath)
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

		// แผนข้อ 1: เขียนผ่าน Storage (ไฟล์/Postgres) — Redis snapshot ยังทำเป็น fallback
		if err := db.storage().Save(db.data); err == nil {
			db.dirty = false
			if jsonData, err := json.Marshal(db.data); err == nil {
				db.maybePushSnapshot(jsonData)
			}
		} else {
			logInfof("⚠️ flush ข้อมูลไม่สำเร็จ: %v", err)
		}
		db.mu.Unlock()
	}
}

// --------------------------------------------------
// Chat Messages Persistence
// --------------------------------------------------

func sanitizeChatStoredMessage(msg map[string]interface{}) map[string]interface{} {
	if msg == nil {
		return nil
	}
	out := map[string]interface{}{}
	if v, ok := msg["id"]; ok { out["id"] = v }
	if v, ok := msg["user"].(string); ok { out["user"] = trimToRuneLen(v, 40) }
	if v, ok := msg["text"].(string); ok { out["text"] = trimToRuneLen(v, 500) }
	if v, ok := msg["type"].(string); ok { out["type"] = trimToRuneLen(v, 20) }
	if v, ok := msg["time"]; ok { out["time"] = v }
	if len(out) == 0 { return nil }
	return out
}

func (db *PersistentDB) SaveChatMessage(matchID string, msg map[string]interface{}) {
	matchID = sanitizeMatchID(matchID)
	msg = sanitizeChatStoredMessage(msg)
	if matchID == "" || msg == nil {
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	history := db.data.ChatMessages[matchID]
	history = append(history, msg)
	if len(history) > 100 {
		history = history[len(history)-100:]
	}
	cutoff := time.Now().AddDate(0, 0, -30).Unix()
	trimmed := []map[string]interface{}{}
	for _, m := range history {
		if tVal, ok := m["time"]; ok {
			switch v := tVal.(type) {
			case float64:
				if v < float64(cutoff) { continue }
			case int:
				if int64(v) < cutoff { continue }
			case int64:
				if v < cutoff { continue }
			case string:
				if ts, err := strconv.ParseInt(v, 10, 64); err == nil && ts < cutoff { continue }
			}
		}
		trimmed = append(trimmed, m)
	}
	db.data.ChatMessages[matchID] = trimmed
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
	userID = strings.TrimSpace(userID)
	if userID == "" || len(userID) > 200 { return }
	favs = sanitizeFavorites(favs, maxFavoritesPerUser)
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

func userSessionMaxAge() time.Duration {
	days := envInt("SESSION_MAX_AGE_DAYS", 30)
	if days <= 0 {
		days = 30
	}
	return time.Duration(days) * 24 * time.Hour
}

func (db *PersistentDB) SaveUserSession(token, sub string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.data.UserSessions == nil {
		db.data.UserSessions = make(UserSessionStore)
	}
	now := time.Now().Unix()
	db.data.UserSessions[token] = UserSession{
		Sub:        sub,
		CreatedAt:  now,
		LastSeenAt: now,
		ExpiresAt:  time.Now().Add(userSessionMaxAge()).Unix(),
	}
	db.dirty = true
}

func (db *PersistentDB) GetSessionSub(token string) string {
	now := time.Now().Unix()

	db.mu.RLock()
	rec, ok := db.data.UserSessions[token]
	db.mu.RUnlock()
	if !ok || rec.Sub == "" {
		return ""
	}
	if rec.ExpiresAt > 0 && now >= rec.ExpiresAt {
		db.mu.Lock()
		if cur, ok := db.data.UserSessions[token]; ok {
			if cur.ExpiresAt > 0 && time.Now().Unix() >= cur.ExpiresAt {
				delete(db.data.UserSessions, token)
				db.dirty = true
			}
		}
		db.mu.Unlock()
		return ""
	}

	if rec.LastSeenAt == 0 || now-rec.LastSeenAt >= 300 {
		db.mu.Lock()
		if cur, ok := db.data.UserSessions[token]; ok && cur.Sub != "" && (cur.ExpiresAt == 0 || now < cur.ExpiresAt) {
			if cur.LastSeenAt == 0 || now-cur.LastSeenAt >= 300 {
				cur.LastSeenAt = now
				db.data.UserSessions[token] = cur
				db.dirty = true
			}
		}
		db.mu.Unlock()
	}
	return rec.Sub
}

// DeleteUserSession — ลบ session (logout) เพื่อให้ token เดิมใช้ไม่ได้อีก
func (db *PersistentDB) DeleteUserSession(token string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, ok := db.data.UserSessions[token]; ok {
		delete(db.data.UserSessions, token)
		db.dirty = true
	}
}

// PruneExpiredUserSessions removes expired server-side sessions and reports count.
func (db *PersistentDB) PruneExpiredUserSessions() int {
	now := time.Now().Unix()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.data.UserSessions == nil {
		return 0
	}
	removed := 0
	for token, rec := range db.data.UserSessions {
		if rec.ExpiresAt > 0 && now >= rec.ExpiresAt {
			delete(db.data.UserSessions, token)
			removed++
		}
	}
	if removed > 0 {
		db.dirty = true
	}
	return removed
}

// sessionSub returns the subject only for non-expired records.
func sessionSub(rec UserSession, now int64) string {
	if rec.Sub == "" {
		return ""
	}
	if rec.ExpiresAt > 0 && now >= rec.ExpiresAt {
		return ""
	}
	return rec.Sub
}

// SaveAPIKey — บันทึก/อัปเดต API key (แผนข้อ 7)
func (db *PersistentDB) SaveAPIKey(key string, rec APIKeyRecord) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.data.APIKeys == nil {
		db.data.APIKeys = make(map[string]APIKeyRecord)
	}
	rec.KeyPrefix = apiKeyPrefix(key)
	db.data.APIKeys[hashAPIKey(key)] = rec
	db.dirty = true
}

// GetAPIKey — ดึงข้อมูล key (มี = ใช้ได้)
// ค้นจาก sha256 เสมอ + รองรับ record เก่าที่ยังเก็บ plaintext เป็น map key
// (ค้นเจอแล้วย้ายเป็น hash ทันที = lazy migration)
func (db *PersistentDB) GetAPIKey(key string) (APIKeyRecord, bool) {
	hash := hashAPIKey(key)

	db.mu.RLock()
	rec, ok := db.data.APIKeys[hash]
	db.mu.RUnlock()
	if ok {
		return rec, true
	}

	// fallback: key เก่าก่อน migration (map key = plaintext ขึ้นต้น sk_live_)
	db.mu.Lock()
	defer db.mu.Unlock()
	if rec, ok := db.data.APIKeys[key]; ok && strings.HasPrefix(key, apiKeyIDPrefix) {
		delete(db.data.APIKeys, key)
		rec.KeyPrefix = apiKeyPrefix(key)
		db.data.APIKeys[hash] = rec
		db.dirty = true
		return rec, true
	}
	return APIKeyRecord{}, false
}

// migrateAPIKeysToHashed — ย้าย key ที่ยังเก็บ plaintext (map key = sk_live_...)
// ไปเป็น sha256 ตอนบูต (คืนจำนวนรายการที่ย้าย) — เรียกจาก InitDB หลังโหลด
func (db *PersistentDB) migrateAPIKeysToHashed() int {
	db.mu.Lock()
	defer db.mu.Unlock()

	moved := 0
	for k, rec := range db.data.APIKeys {
		if !strings.HasPrefix(k, apiKeyIDPrefix) {
			continue // ไม่ใช่ key รูปแบบ plaintext (เช่น ข้อมูลทดสอบ) -> ข้าม
		}
		delete(db.data.APIKeys, k)
		rec.KeyPrefix = apiKeyPrefix(k)
		db.data.APIKeys[hashAPIKey(k)] = rec
		moved++
	}
	if moved > 0 {
		db.dirty = true
	}
	return moved
}

// ListAPIKeys — รายการ key ทั้งหมด (เรียงตามเวลาที่ใช้ล่าสุด)
func (db *PersistentDB) ListAPIKeys() []APIKeyRecord {
	db.mu.RLock()
	defer db.mu.RUnlock()

	out := make([]APIKeyRecord, 0, len(db.data.APIKeys))
	for _, rec := range db.data.APIKeys {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LastUsedAt != out[j].LastUsedAt {
			return out[i].LastUsedAt > out[j].LastUsedAt
		}
		if out[i].CreatedAt != out[j].CreatedAt {
			return out[i].CreatedAt > out[j].CreatedAt
		}
		// map iteration สุ่ม -> tie-break ด้วย KeyPrefix ให้หน้า admin ไม่สลับแถว
		return out[i].KeyPrefix < out[j].KeyPrefix
	})
	return out
}

// ListAPIKeyViews — เหมือน ListAPIKeys แต่เติม key_id (prefix) และไม่คืนค่า key เต็ม
// (map key เป็น sha256 แล้ว -> ใช้ KeyPrefix ที่บันทึกไว้ใน record แสดงผล)
func (db *PersistentDB) ListAPIKeyViews() []APIKeyView {
	db.mu.RLock()
	defer db.mu.RUnlock()

	out := make([]APIKeyView, 0, len(db.data.APIKeys))
	for key, rec := range db.data.APIKeys {
		pid := rec.KeyPrefix
		if pid == "" {
			pid = apiKeyPrefix(key) // record เก่าที่ยังไม่มี KeyPrefix
		}
		out = append(out, APIKeyView{APIKeyRecord: rec, KeyID: pid})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LastUsedAt != out[j].LastUsedAt {
			return out[i].LastUsedAt > out[j].LastUsedAt
		}
		if out[i].CreatedAt != out[j].CreatedAt {
			return out[i].CreatedAt > out[j].CreatedAt
		}
		return out[i].KeyID < out[j].KeyID
	})
	return out
}

// FindAPIKeyHashByPrefix — หา key ใน map (sha256) จาก prefix ที่แสดงในหน้า admin
// (ค่าเต็มไม่ถูกเก็บใน DB แล้ว -> ค้นจาก KeyPrefix ที่บันทึกไว้ใน record)
func (db *PersistentDB) FindAPIKeyHashByPrefix(prefix string) string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for key, rec := range db.data.APIKeys {
		if rec.KeyPrefix != "" {
			// รับทั้ง prefix สั้น (sk_live_xxxx) และ key เต็ม (หน้า admin เคยส่ง key เต็มมาก่อน)
			if strings.HasPrefix(rec.KeyPrefix, prefix) || strings.HasPrefix(prefix, rec.KeyPrefix) {
				return key
			}
			continue
		}
		// record เก่าก่อน migration (ไม่มี KeyPrefix)
		if strings.HasPrefix(key, prefix) {
			return key
		}
	}
	return ""
}

// DeleteAPIKeyByHash — เพิกถอน key ด้วยค่า map key (sha256) ลบถาวร
func (db *PersistentDB) DeleteAPIKeyByHash(hash string) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, ok := db.data.APIKeys[hash]; !ok {
		return false
	}
	delete(db.data.APIKeys, hash)
	db.dirty = true
	return true
}

// GetAPIKeyByHash — ดึง record จาก hash (ใช้ตอนผู้ใช้เพิกถอน key ของตัวเอง: เช็ค owner)
func (db *PersistentDB) GetAPIKeyByHash(hash string) (APIKeyRecord, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	rec, ok := db.data.APIKeys[hash]
	return rec, ok
}

// GetUserInterests — รายการความสนใจของผู้ใช้ (เรียงคงที่)
func (db *PersistentDB) GetUserInterests(userID string) []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	src := db.data.UserInterests[userID]
	out := make([]string, 0, len(src))
	for _, k := range src {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// SetUserInterest — ติดตาม/เลิกติดตาม (คืนรายการล่าสุดของผู้ใช้)
func (db *PersistentDB) SetUserInterest(userID string, key string, follow bool) []string {
	userID = strings.TrimSpace(userID)
	key = normalizeInterestKey(strings.SplitN(key, ":", 2)[0], strings.TrimPrefix(key, strings.SplitN(key, ":", 2)[0]+":"))
	if key == "" { key = strings.ToLower(strings.TrimSpace(key)) }
	if userID == "" || len(userID) > 200 || !validInterestID(strings.TrimPrefix(strings.TrimPrefix(key, "league:"), "team:")) || ( !strings.HasPrefix(key, "league:") && !strings.HasPrefix(key, "team:") ) {
		return db.GetUserInterests(userID)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.data.UserInterests == nil { db.data.UserInterests = make(map[string][]string) }
	current := db.data.UserInterests[userID]
	set := make(map[string]bool, len(current))
	for _, k := range current { set[k] = true }
	if follow {
		if len(set) < maxInterestsPerUser || set[key] { set[key] = true }
	} else {
		delete(set, key)
	}
	next := sortedInterests(set)
	db.data.UserInterests[userID] = next
	db.dirty = true
	return next
}

// GetUserAchievements — รายการ achievement ที่ปลดล็อกของผู้ใช้
func (db *PersistentDB) GetUserAchievements(userID string) []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.data.Achievements[userID]
}

// GetSessionSubByUser — หา google sub ของ user key (user:g:<sub>) จาก session ที่ยังมีและไม่หมดอายุ
func (db *PersistentDB) GetSessionSubByUser(userKey string) string {
	if len(userKey) < len("user:g:") || userKey[:len("user:g:")] != "user:g:" {
		return ""
	}
	sub := userKey[len("user:g:"):]
	now := time.Now().Unix()
	db.mu.RLock()
	defer db.mu.RUnlock()
	for _, rec := range db.data.UserSessions {
		if sessionSub(rec, now) == sub {
			return sub
		}
	}
	return ""
}

// DeleteUserSessionsBySub — ลบทุก session ที่ผูกกับ google sub นี้
func (db *PersistentDB) DeleteUserSessionsBySub(sub string) int {
	db.mu.Lock()
	defer db.mu.Unlock()

	n := 0
	for token, rec := range db.data.UserSessions {
		if rec.Sub == sub {
			delete(db.data.UserSessions, token)
			n++
		}
	}
	if n > 0 {
		db.dirty = true
	}
	return n
}

// DeleteUserData — ลบข้อมูลทั้งหมดของผู้ใช้ 1 คน (PDPA: right to erasure)
// คืนจำนวนรายการที่ลบในแต่ละประเภท เพื่อรายงานกลับให้ผู้ใช้
func (db *PersistentDB) DeleteUserData(userID string) map[string]interface{} {
	db.mu.Lock()
	defer db.mu.Unlock()

	removed := map[string]interface{}{
		"favorites":          0,
		"predictions":        0,
		"notification_prefs": 0,
		"achievements":       0,
	}

	if favs, ok := db.data.UserFavorites[userID]; ok {
		removed["favorites"] = len(favs)
		delete(db.data.UserFavorites, userID)
	}
	if preds, ok := db.data.Predictions[userID]; ok {
		removed["predictions"] = len(preds)
		delete(db.data.Predictions, userID)
	}
	if _, ok := db.data.UserNotificationPrefs[userID]; ok {
		removed["notification_prefs"] = 1
		delete(db.data.UserNotificationPrefs, userID)
	}
	if ach, ok := db.data.Achievements[userID]; ok {
		removed["achievements"] = len(ach)
		delete(db.data.Achievements, userID)
	}

	db.dirty = true
	return removed
}

// --------------------------------------------------
// Match FCM Subscriptions Persistence
// --------------------------------------------------

func (db *PersistentDB) SubscribeMatch(matchID int, token string) int {
	token = sanitizeFCMToken(token)
	if token == "" || matchID <= 0 || matchID > 1_000_000_000 { return 0 }
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.data.MatchSubscriptions == nil { db.data.MatchSubscriptions = make(map[int][]string) }
	tokens := db.data.MatchSubscriptions[matchID]
	for _, tkn := range tokens { if tkn == token { return len(tokens) } }
	if len(tokens) >= maxSubscriptionsPerMatch { return len(tokens) }
	totalSubs := 0
	for _, v := range db.data.MatchSubscriptions { totalSubs += len(v) }
	if totalSubs >= maxMatchSubscriptions { return len(tokens) }
	tokens = append(tokens, token)
	db.data.MatchSubscriptions[matchID] = tokens
	db.dirty = true
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
	token = sanitizeFCMToken(token)
	if token == "" { return }
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.data.FCMTokens == nil { db.data.FCMTokens = make(map[string]int64) }
	db.data.FCMTokens[token] = time.Now().Unix()
	db.dirty = true
}

// GetFCMTokens// GetFCMTokens คืน token ทั้งหมดที่ลงทะเบียนไว้
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

func (db *PersistentDB) SavePrediction(userID, matchID string, pred map[string]interface{}) error {
	userID = strings.TrimSpace(userID)
	matchID = sanitizeMatchID(matchID)
	if userID == "" || len(userID) > 200 || matchID == "" {
		return fmt.Errorf("invalid prediction key")
	}
	if err := validateJSONObjectSize(pred, maxPredictionJSONBytes, "prediction"); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.data.Predictions == nil { db.data.Predictions = make(map[string]map[string]interface{}) }
	if db.data.Predictions[userID] == nil { db.data.Predictions[userID] = make(map[string]interface{}) }
	db.data.Predictions[userID][matchID] = pred
	db.dirty = true
	return nil
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

func (db *PersistentDB) SaveNotificationPrefs(userID string, prefs map[string]interface{}) error {
	userID = strings.TrimSpace(userID)
	if userID == "" || len(userID) > 200 {
		return fmt.Errorf("invalid user")
	}
	if err := validateJSONObjectSize(prefs, maxNotificationPrefsBytes, "preferences"); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.data.UserNotificationPrefs == nil { db.data.UserNotificationPrefs = make(map[string]map[string]interface{}) }
	db.data.UserNotificationPrefs[userID] = prefs
	db.dirty = true
	return nil
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
	now := time.Now().Unix()
	for _, rec := range db.data.UserSessions {
		if sub := sessionSub(rec, now); sub != "" {
			seen["user:g:"+sub] = true
		}
	}
	for k := range db.data.Predictions {
		seen[k] = true
	}
	return len(seen)
}

func (db *PersistentDB) CountSessions() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	now := time.Now().Unix()
	n := 0
	for _, rec := range db.data.UserSessions {
		if sessionSub(rec, now) != "" {
			n++
		}
	}
	return n
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
		key          string
		favorites    int
		predictions  int
		achievements int
		hasFCM       bool
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
	now := time.Now().Unix()
	for _, rec := range db.data.UserSessions {
		if sub := sessionSub(rec, now); sub != "" {
			u := getUser("user:g:" + sub)
			u.hasFCM = true
		}
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

	// แผนข้อ 1: เขียนผ่าน Storage (ไฟล์/Postgres)
	if err := db.storage().Save(db.data); err != nil {
		return err
	}
	db.dirty = false
	logInfof("💾 บันทึกข้อมูลลงดิสก์ครบก่อนปิดเซิร์ฟเวอร์แล้ว (%s)", db.filePath)

	// ดัน snapshot ขึ้น Redis แบบ synchronous (forced) —
	// เรียกตอน shutdown เท่านั้น ต่างจาก maybePushSnapshot ที่ throttle + async
	// (Render บูต instance ใหม่กู้จาก snapshot ได้ แม้ instance เก่าตายก่อน syncWorker รอบถัดไป)
	if rdb != nil {
		pushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := rdb.Set(pushCtx, dbSnapshotKey, string(jsonData), 0).Err(); err != nil {
			logInfof("⚠️ สำรอง snapshot ขึ้น Redis ตอนปิดไม่สำเร็จ: %v", err)
		} else {
			logInfoln("✅ ดัน snapshot ขึ้น Redis ตอนปิดครบแล้ว")
		}
	}
	return nil
}
