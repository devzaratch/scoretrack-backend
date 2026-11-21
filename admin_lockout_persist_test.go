package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

)

func TestAdminAuthFailPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "store.json")
	t.Setenv("ADMIN_TOKEN", "correct-token-123")
	t.Setenv("ADMIN_AUTH_MAX_FAILS", "2")
	t.Setenv("ADMIN_AUTH_LOCK_MINUTES", "10")

	old := GlobalDB
	t.Cleanup(func() { GlobalDB = old })

	db := &PersistentDB{filePath: dbPath, data: newEmptyStoreData(), store: newFileStorage(dbPath)}
	GlobalDB = db
	r := adminTestEngine()
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/api/admin/stats", nil)
		req.Header.Set("X-Admin-Token", "wrong")
		req.Header.Set("X-Forwarded-For", "203.0.113.44")
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
	}
	if err := db.storage().Save(db.data); err != nil { t.Fatal(err) }

	loaded, err := newFileStorage(dbPath).Load()
	if err != nil { t.Fatal(err) }
	GlobalDB = &PersistentDB{filePath: dbPath, data: loaded, store: newFileStorage(dbPath)}

	req := httptest.NewRequest("GET", "/api/admin/stats", nil)
	req.Header.Set("X-Admin-Token", "correct-token-123")
	req.Header.Set("X-Forwarded-For", "203.0.113.44")
	rec := httptest.NewRecorder()
	adminTestEngine().ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after restart, got %d", rec.Code)
	}
}

func TestAdminAuditPersistsAndCaps(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "store.json")
	old := GlobalDB
	t.Cleanup(func() { GlobalDB = old })
	db := &PersistentDB{filePath: dbPath, data: newEmptyStoreData(), store: newFileStorage(dbPath)}
	GlobalDB = db
	for i := 0; i < adminAuditMaxRecords+10; i++ {
		db.AppendAdminAudit(AdminAuditRecord{Time: time.Now().Add(time.Duration(i) * time.Second).UTC().Format(time.RFC3339), IP: "1.2.3.4", Method: "POST", Path: "/api/admin/x", Status: 200})
	}
	if len(db.data.AdminAuditLogs) != adminAuditMaxRecords {
		t.Fatalf("audit cap got %d", len(db.data.AdminAuditLogs))
	}
	if err := db.storage().Save(db.data); err != nil { t.Fatal(err) }
	loaded, err := newFileStorage(dbPath).Load()
	if err != nil { t.Fatal(err) }
	GlobalDB = &PersistentDB{filePath: dbPath, data: loaded, store: newFileStorage(dbPath)}
	logs := GlobalDB.AdminAuditLogs(5)
	if len(logs) != 5 { t.Fatalf("want 5 got %d", len(logs)) }
	if logs[0].Path != "/api/admin/x" { t.Fatalf("unexpected log: %+v", logs[0]) }
}

func TestAdminLockoutExpiresAndIsPruned(t *testing.T) {
	db := &PersistentDB{data: newEmptyStoreData()}
	GlobalDB = db
	t.Cleanup(func() { GlobalDB = nil })
	now := time.Now()
	db.NoteAdminAuthFail("1.1.1.1", now.Add(-11*time.Minute), 1, 1)
	db.data.AdminAuthFails["1.1.1.1"] = AdminAuthFailRecord{Fails: 1, WindowStart: now.Add(-2 * time.Hour).Unix(), LockedUntil: now.Add(-time.Minute).Unix()}
	locked, _, changed := db.GetAdminAuthLock("1.1.1.1", now)
	if locked || !changed { t.Fatalf("locked=%v changed=%v", locked, changed) }
	if _, ok := db.data.AdminAuthFails["1.1.1.1"]; ok { t.Fatal("expected stale lock pruned") }
}

func TestStoreDataAdminStateRoundTripJSON(t *testing.T) {
	s := newEmptyStoreData()
	s.AdminAuthFails["1.2.3.4"] = AdminAuthFailRecord{Fails: 2, WindowStart: 1, LockedUntil: 2}
	s.AdminAuditLogs = []AdminAuditRecord{{Time: "2026-11-21T00:00:00Z", IP: "1.2.3.4", Method: "POST", Path: "/x", Status: 201}}
	b, err := json.Marshal(s)
	if err != nil { t.Fatal(err) }
	var got StoreData
	if err := json.Unmarshal(b, &got); err != nil { t.Fatal(err) }
	if got.AdminAuthFails["1.2.3.4"].Fails != 2 { t.Fatalf("fail mismatch: %+v", got.AdminAuthFails["1.2.3.4"]) }
	if len(got.AdminAuditLogs) != 1 || got.AdminAuditLogs[0].Status != 201 { t.Fatalf("audit mismatch: %+v", got.AdminAuditLogs) }
}
