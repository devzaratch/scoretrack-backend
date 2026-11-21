package main

// storage â€” à¸Šà¸±à¹‰à¸™à¹€à¸à¹‡à¸šà¸‚à¹‰à¸­à¸¡à¸¹à¸¥à¸–à¸²à¸§à¸£ (à¹à¸œà¸™à¸‚à¹‰à¸­ 1: à¸¢à¹‰à¸²à¸¢à¸ˆà¸²à¸à¹„à¸Ÿà¸¥à¹Œ JSON à¹„à¸› PostgreSQL)
//
// à¹€à¸”à¸´à¸¡: à¸—à¸¸à¸à¸­à¸¢à¹ˆà¸²à¸‡à¸­à¸¢à¸¹à¹ˆà¹ƒà¸™ data/store.json à¹„à¸Ÿà¸¥à¹Œà¹€à¸”à¸µà¸¢à¸§ (à¸à¸±à¹ˆà¸‡ instance à¹€à¸”à¸µà¸¢à¸§)
//   - Render à¸Ÿà¸£à¸µ tier = à¸”à¸´à¸ªà¸à¹Œ ephemeral -> restart à¹à¸¥à¹‰à¸§à¸‚à¹‰à¸­à¸¡à¸¹à¸¥à¸«à¸²à¸¢ (à¸•à¹‰à¸­à¸‡à¸žà¸¶à¹ˆà¸‡ Redis snapshot)
//   - scale à¹€à¸›à¹‡à¸™ 2 instance = à¸‚à¹‰à¸­à¸¡à¸¹à¸¥à¹à¸¢à¸à¸à¸±à¸™ (à¸œà¸¹à¹‰à¹ƒà¸Šà¹‰à¸„à¸™à¸¥à¸°à¹€à¸„à¸£à¸·à¹ˆà¸­à¸‡à¹€à¸«à¹‡à¸™à¸„à¸™à¸¥à¸°à¸­à¸¢à¹ˆà¸²à¸‡)
//   - backup = copy à¹„à¸Ÿà¸¥à¹Œ, query à¸‹à¸±à¸šà¸‹à¹‰à¸­à¸™à¹„à¸¡à¹ˆà¹„à¸”à¹‰
//
// à¸•à¸­à¸™à¸™à¸µà¹‰: Storage interface + 2 implementation
//   - fileStorage   : à¸‚à¸­à¸‡à¹€à¸”à¸´à¸¡ (default) â€” à¹ƒà¸Šà¹‰à¸•à¸­à¸™ dev/à¹„à¸¡à¹ˆà¸¡à¸µ DB
//   - postgresStorage: à¹€à¸à¹‡à¸šà¹€à¸›à¹‡à¸™ JSONB document à¹ƒà¸™ Postgres (DATA_BACKEND=postgres)
//     à¸•à¹ˆà¸­à¸¢à¸­à¸”à¹€à¸›à¹‡à¸™à¸•à¸²à¸£à¸²à¸‡à¸ˆà¸£à¸´à¸‡à¹„à¸”à¹‰à¸—à¸µà¸¥à¸°à¸•à¸²à¸£à¸²à¸‡à¹‚à¸”à¸¢à¹„à¸¡à¹ˆà¸•à¹‰à¸­à¸‡à¹à¸•à¸° business logic
//
// à¹€à¸¥à¸·à¸­à¸à¸”à¹‰à¸§à¸¢ env: DATA_BACKEND=postgres + DATABASE_URL=postgres://user:pass@host:5432/db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/lib/pq" // driver à¸‚à¸­à¸‡ PostgreSQL
)

// Storage â€” à¸Šà¸±à¹‰à¸™à¹€à¸à¹‡à¸šà¸‚à¹‰à¸­à¸¡à¸¹à¸¥à¸–à¸²à¸§à¸£ (à¸–à¸­à¸”à¸­à¸­à¸à¹„à¸”à¹‰à¸œà¹ˆà¸²à¸™ interface à¸™à¸µà¹‰)
type Storage interface {
	// Load â€” à¸­à¹ˆà¸²à¸™à¸‚à¹‰à¸­à¸¡à¸¹à¸¥à¸—à¸±à¹‰à¸‡à¸Šà¸¸à¸” (à¸–à¹‰à¸²à¸¢à¸±à¸‡à¹„à¸¡à¹ˆà¸¡à¸µ = à¸„à¸·à¸™ StoreData à¸§à¹ˆà¸²à¸‡ à¹„à¸¡à¹ˆ error)
	Load() (StoreData, error)
	// Save â€” à¹€à¸‚à¸µà¸¢à¸™à¸‚à¹‰à¸­à¸¡à¸¹à¸¥à¸—à¸±à¹‰à¸‡à¸Šà¸¸à¸” (atomic)
	Save(data StoreData) error
	// Name â€” à¸Šà¸·à¹ˆà¸­à¸ªà¸³à¸«à¸£à¸±à¸š log
	Name() string
}

// newEmptyStoreData â€” à¹‚à¸„à¸£à¸‡à¸ªà¸£à¹‰à¸²à¸‡à¸§à¹ˆà¸²à¸‡à¸—à¸µà¹ˆ map à¸„à¸£à¸š (à¸à¸±à¸™ nil map à¸•à¸­à¸™à¹€à¸‚à¸µà¸¢à¸™)
func newEmptyStoreData() StoreData {
	return StoreData{
		ChatMessages:          make(map[string][]map[string]interface{}),
		UserFavorites:         make(map[string][]int),
		UserSessions:          make(UserSessionStore),
		MatchSubscriptions:    make(map[int][]string),
		FCMTokens:             make(map[string]int64),
		Predictions:           make(map[string]map[string]interface{}),
		UserNotificationPrefs: make(map[string]map[string]interface{}),
		Achievements:          make(map[string][]string),
		APIKeys:               make(map[string]APIKeyRecord),
		UserInterests:         make(map[string][]string),
	}
}

// mergeLoaded â€” à¸£à¸§à¸¡à¸‚à¹‰à¸­à¸¡à¸¹à¸¥à¸—à¸µà¹ˆà¸­à¹ˆà¸²à¸™à¸¡à¸²à¸à¸±à¸šà¹‚à¸„à¸£à¸‡à¸ªà¸£à¹‰à¸²à¸‡à¸§à¹ˆà¸²à¸‡ (à¸„à¸‡ field à¸—à¸µà¹ˆà¹„à¸¡à¹ˆà¸¡à¸µà¹ƒà¸™à¹„à¸Ÿà¸¥à¹Œà¹€à¸à¹ˆà¸²)
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

// isEmptyStore â€” à¸‚à¹‰à¸­à¸¡à¸¹à¸¥à¸§à¹ˆà¸²à¸‡à¸ˆà¸£à¸´à¸‡à¹„à¸«à¸¡ (à¹ƒà¸Šà¹‰à¸•à¸±à¸”à¸ªà¸´à¸™à¸§à¹ˆà¸²à¸•à¹‰à¸­à¸‡ import à¸‚à¹‰à¸­à¸¡à¸¹à¸¥à¹€à¸”à¸´à¸¡à¹€à¸‚à¹‰à¸² DB à¹„à¸«à¸¡)
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
// fileStorage â€” à¸‚à¸­à¸‡à¹€à¸”à¸´à¸¡ (data/store.json, à¹€à¸‚à¸µà¸¢à¸™à¹à¸šà¸š atomic)
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
// postgresStorage â€” à¹€à¸à¹‡à¸šà¹€à¸›à¹‡à¸™ JSONB document (à¹à¸œà¸™à¸‚à¹‰à¸­ 1 à¹€à¸Ÿà¸ª 1)
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
	// pool à¹€à¸¥à¹‡à¸à¸™à¸´à¸”: à¹€à¸£à¸²à¹€à¸à¹‡à¸šà¸—à¸±à¹‰à¸‡à¸Šà¸¸à¸”à¹ƒà¸™ document à¹€à¸”à¸µà¸¢à¸§
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

// ensureTable â€” à¸ªà¸£à¹‰à¸²à¸‡à¸•à¸²à¸£à¸²à¸‡à¸–à¹‰à¸²à¸¢à¸±à¸‡à¹„à¸¡à¹ˆà¸¡à¸µ (idempotent)
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
	// upsert à¹à¸–à¸§à¹€à¸”à¸µà¸¢à¸§ (id=1) â€” à¸‚à¹‰à¸­à¸¡à¸¹à¸¥à¸—à¸±à¹‰à¸‡à¸Šà¸¸à¸”à¸­à¸¢à¸¹à¹ˆà¹ƒà¸™ document à¹€à¸”à¸µà¸¢à¸§
	stmt := fmt.Sprintf(`INSERT INTO %s (id, doc, updated_at)
		VALUES (1, $1, now())
		ON CONFLICT (id) DO UPDATE SET doc = EXCLUDED.doc, updated_at = now()`, s.table)
	_, err = s.conn.Exec(stmt, string(jsonData))
	return err
}

// Close â€” à¸›à¸´à¸” connection
func (s *postgresStorage) Close() error {
	if s.conn == nil {
		return nil
	}
	return s.conn.Close()
}

// newStorage â€” à¹€à¸¥à¸·à¸­à¸ storage à¸ˆà¸²à¸ env
//
//	DATA_BACKEND=postgres + DATABASE_URL -> postgres
//	à¸­à¸¢à¹ˆà¸²à¸‡à¸­à¸·à¹ˆà¸™ -> à¹„à¸Ÿà¸¥à¹Œ (default, à¹€à¸«à¸¡à¸·à¸­à¸™à¹€à¸”à¸´à¸¡)
func newStorage() (Storage, error) {
	backend := strings.ToLower(strings.TrimSpace(os.Getenv("DATA_BACKEND")))
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))

	if backend == "postgres" || (backend == "" && dsn != "" && os.Getenv("DATA_BACKEND") == "postgres") {
		if dsn == "" {
			return nil, fmt.Errorf("DATA_BACKEND=postgres à¹à¸•à¹ˆà¹„à¸¡à¹ˆà¹„à¸”à¹‰à¸•à¸±à¹‰à¸‡ DATABASE_URL")
		}
		store, err := newPostgresStorage(dsn)
		if err != nil {
			return nil, err
		}
		logInfof("ðŸ—„ï¸ à¹ƒà¸Šà¹‰ PostgreSQL à¹€à¸›à¹‡à¸™à¸—à¸µà¹ˆà¹€à¸à¹‡à¸šà¸‚à¹‰à¸­à¸¡à¸¹à¸¥ (%s)", store.Name())
		return store, nil
	}

	path := filepath.Join("data", "store.json")
	store := newFileStorage(path)
	logInfof("ðŸ“ à¹ƒà¸Šà¹‰à¹„à¸Ÿà¸¥à¹Œà¹€à¸›à¹‡à¸™à¸—à¸µà¹ˆà¹€à¸à¹‡à¸šà¸‚à¹‰à¸­à¸¡à¸¹à¸¥ (%s) â€” à¸•à¸±à¹‰à¸‡ DATA_BACKEND=postgres à¹€à¸žà¸·à¹ˆà¸­à¹ƒà¸Šà¹‰ DB", store.Name())
	return store, nil
}

