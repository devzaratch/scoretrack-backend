package main

// storage — ชั้นเก็บข้อมูลถาวร (แผนข้อ 1: ย้ายจากไฟล์ JSON ไป PostgreSQL)
//
// เดิม: ทุกอย่างอยู่ใน data/store.json ไฟล์เดียว (ฝั่ง instance เดียว)
//   - Render ฟรี tier = ดิสก์ ephemeral -> restart แล้วข้อมูลหาย (ต้องพึ่ง Redis snapshot)
//   - scale เป็น 2 instance = ข้อมูลแยกกัน (ผู้ใช้คนละเครื่องเห็นคนละอย่าง)
//   - backup = copy ไฟล์, query ซับซ้อนไม่ได้
//
// ตอนนี้: Storage interface + 2 implementation
//   - fileStorage   : ของเดิม (default) — ใช้ตอน dev/ไม่มี DB
//   - postgresStorage: เก็บเป็น JSONB document ใน Postgres (DATA_BACKEND=postgres)
//     ต่อยอดเป็นตารางจริงได้ทีละตารางโดยไม่ต้องแตะ business logic
//
// เลือกด้วย env: DATA_BACKEND=postgres + DATABASE_URL=postgres://user:pass@host:5432/db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/lib/pq" // driver ของ PostgreSQL
)

// Storage — ชั้นเก็บข้อมูลถาวร (ถอดออกได้ผ่าน interface นี้)
type Storage interface {
	// Load — อ่านข้อมูลทั้งชุด (ถ้ายังไม่มี = คืน StoreData ว่าง ไม่ error)
	Load() (StoreData, error)
	// Save — เขียนข้อมูลทั้งชุด (atomic)
	Save(data StoreData) error
	// Name — ชื่อสำหรับ log
	Name() string
}

// newEmptyStoreData — โครงสร้างว่างที่ map ครบ (กัน nil map ตอนเขียน)
func newEmptyStoreData() StoreData {
	return StoreData{
		ChatMessages:          make(map[string][]map[string]interface{}),
		UserFavorites:         make(map[string][]int),
		UserSessions:          make(map[string]string),
		MatchSubscriptions:    make(map[int][]string),
		FCMTokens:             make(map[string]int64),
		Predictions:           make(map[string]map[string]interface{}),
		UserNotificationPrefs: make(map[string]map[string]interface{}),
		Achievements:          make(map[string][]string),
		APIKeys:               make(map[string]APIKeyRecord),
		UserInterests:         make(map[string][]string),
	}
}

// mergeLoaded — รวมข้อมูลที่อ่านมากับโครงสร้างว่าง (คง field ที่ไม่มีในไฟล์เก่า)
func mergeLoaded(loaded StoreData) StoreData {
	base := newEmptyStoreData()
	if loaded.ChatMessages != nil {
		base.ChatMessages = loaded.ChatMessages
	}
	if loaded.UserFavorites != nil {
		base.UserFavorites = loaded.UserFavorites
	}
	if loaded.UserSessions != nil {
		base.UserSessions = loaded.UserSessions
	}
	if loaded.MatchSubscriptions != nil {
		base.MatchSubscriptions = loaded.MatchSubscriptions
	}
	if loaded.FCMTokens != nil {
		base.FCMTokens = loaded.FCMTokens
	}
	if loaded.Predictions != nil {
		base.Predictions = loaded.Predictions
	}
	if loaded.UserNotificationPrefs != nil {
		base.UserNotificationPrefs = loaded.UserNotificationPrefs
	}
	if loaded.Achievements != nil {
		base.Achievements = loaded.Achievements
	}
	if loaded.APIKeys != nil {
		base.APIKeys = loaded.APIKeys
	}
	if loaded.UserInterests != nil {
		base.UserInterests = loaded.UserInterests
	}
	return base
}

// isEmptyStore — ข้อมูลว่างจริงไหม (ใช้ตัดสินว่าต้อง import ข้อมูลเดิมเข้า DB ไหม)
func isEmptyStore(data StoreData) bool {
	return len(data.UserSessions) == 0 &&
		len(data.UserFavorites) == 0 &&
		len(data.Predictions) == 0 &&
		len(data.APIKeys) == 0 &&
		len(data.UserInterests) == 0 &&
		len(data.Achievements) == 0 &&
		len(data.ChatMessages) == 0
}

// ---------------------------------------------------------------------------
// fileStorage — ของเดิม (data/store.json, เขียนแบบ atomic)
// ---------------------------------------------------------------------------

type fileStorage struct {
	path string
}

func newFileStorage(path string) *fileStorage {
	return &fileStorage{path: path}
}

func (s *fileStorage) Name() string { return "file(" + s.path + ")" }

func (s *fileStorage) Load() (StoreData, error) {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return newEmptyStoreData(), nil
		}
		return newEmptyStoreData(), err
	}
	if len(raw) == 0 {
		return newEmptyStoreData(), nil
	}
	var loaded StoreData
	if err := json.Unmarshal(raw, &loaded); err != nil {
		return newEmptyStoreData(), err
	}
	return mergeLoaded(loaded), nil
}

func (s *fileStorage) Save(data StoreData) error {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0755); err != nil {
		return err
	}
	tmpPath := s.path + ".tmp"
	if err := os.WriteFile(tmpPath, jsonData, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, s.path)
}

// ---------------------------------------------------------------------------
// postgresStorage — เก็บเป็น JSONB document (แผนข้อ 1 เฟส 1)
// ---------------------------------------------------------------------------

type postgresStorage struct {
	conn  *sql.DB
	table string
}

func newPostgresStorage(dsn string) (*postgresStorage, error) {
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	// pool เล็กนิด: เราเก็บทั้งชุดใน document เดียว
	conn.SetMaxOpenConns(4)
	conn.SetMaxIdleConns(2)
	conn.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		return nil, err
	}
	return &postgresStorage{conn: conn, table: "scoretrack_store"}, nil
}

func (s *postgresStorage) Name() string { return "postgres(" + s.table + ")" }

// ensureTable — สร้างตารางถ้ายังไม่มี (idempotent)
func (s *postgresStorage) ensureTable() error {
	stmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id          INTEGER PRIMARY KEY,
		doc         JSONB NOT NULL,
		updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
	)`, s.table)
	_, err := s.conn.Exec(stmt)
	return err
}

func (s *postgresStorage) Load() (StoreData, error) {
	if err := s.ensureTable(); err != nil {
		return newEmptyStoreData(), err
	}
	var raw []byte
	err := s.conn.QueryRow(fmt.Sprintf(`SELECT doc FROM %s WHERE id = 1`, s.table)).Scan(&raw)
	if err != nil {
		if err == sql.ErrNoRows {
			return newEmptyStoreData(), nil
		}
		return newEmptyStoreData(), err
	}
	var loaded StoreData
	if err := json.Unmarshal(raw, &loaded); err != nil {
		return newEmptyStoreData(), err
	}
	return mergeLoaded(loaded), nil
}

func (s *postgresStorage) Save(data StoreData) error {
	if err := s.ensureTable(); err != nil {
		return err
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	// upsert แถวเดียว (id=1) — ข้อมูลทั้งชุดอยู่ใน document เดียว
	stmt := fmt.Sprintf(`INSERT INTO %s (id, doc, updated_at)
		VALUES (1, $1, now())
		ON CONFLICT (id) DO UPDATE SET doc = EXCLUDED.doc, updated_at = now()`, s.table)
	_, err = s.conn.Exec(stmt, string(jsonData))
	return err
}

// Close — ปิด connection
func (s *postgresStorage) Close() error {
	if s.conn == nil {
		return nil
	}
	return s.conn.Close()
}

// newStorage — เลือก storage จาก env
//
//	DATA_BACKEND=postgres + DATABASE_URL -> postgres
//	อย่างอื่น -> ไฟล์ (default, เหมือนเดิม)
func newStorage() (Storage, error) {
	backend := strings.ToLower(strings.TrimSpace(os.Getenv("DATA_BACKEND")))
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))

	if backend == "postgres" || (backend == "" && dsn != "" && os.Getenv("DATA_BACKEND") == "postgres") {
		if dsn == "" {
			return nil, fmt.Errorf("DATA_BACKEND=postgres แต่ไม่ได้ตั้ง DATABASE_URL")
		}
		store, err := newPostgresStorage(dsn)
		if err != nil {
			return nil, err
		}
		logInfof("🗄️ ใช้ PostgreSQL เป็นที่เก็บข้อมูล (%s)", store.Name())
		return store, nil
	}

	path := filepath.Join("data", "store.json")
	store := newFileStorage(path)
	logInfof("📁 ใช้ไฟล์เป็นที่เก็บข้อมูล (%s) — ตั้ง DATA_BACKEND=postgres เพื่อใช้ DB", store.Name())
	return store, nil
}
