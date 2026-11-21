package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestUserSessionLegacyStringUnmarshal(t *testing.T) {
	// legacy format: token -> sub (string)
	raw := []byte(`{"user_sessions":{"tok-legacy":"sub-legacy"}}`)
	var data StoreData
	if err := json.Unmarshal(raw, &data); err != nil {
		t.Fatalf("unmarshal legacy user_sessions: %v", err)
	}
	db := &PersistentDB{data: mergeLoaded(data)}
	if got := db.GetSessionSub("tok-legacy"); got != "sub-legacy" {
		t.Fatalf("expected legacy session sub preserved, got %q", got)
	}
}

func TestSaveUserSessionSetsExpiryAndRoundTrip(t *testing.T) {
	db := newTestDB(t)
	before := time.Now().Unix()
	db.SaveUserSession("tok1", "sub1")

	rec, ok := db.data.UserSessions["tok1"]
	if !ok {
		t.Fatal("expected session record stored")
	}
	if rec.Sub != "sub1" {
		t.Fatalf("expected sub1, got %q", rec.Sub)
	}
	if rec.ExpiresAt <= before {
		t.Fatalf("expected future expires_at, got %d (before=%d)", rec.ExpiresAt, before)
	}
	if rec.CreatedAt <= 0 {
		t.Fatalf("expected created_at set, got %d", rec.CreatedAt)
	}
	// round-trip through JSON should keep structured form
	b, err := json.Marshal(db.data)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var loaded StoreData
	if err := json.Unmarshal(b, &loaded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	db2 := &PersistentDB{data: mergeLoaded(loaded)}
	if got := db2.GetSessionSub("tok1"); got != "sub1" {
		t.Fatalf("expected sub after round-trip, got %q", got)
	}
}

func TestGetSessionSubRejectsExpiredAndPrunes(t *testing.T) {
	db := newTestDB(t)
	db.data.UserSessions["expired"] = UserSession{
		Sub:       "sub-exp",
		ExpiresAt: time.Now().Add(-time.Minute).Unix(),
		CreatedAt: time.Now().Add(-2 * time.Hour).Unix(),
	}
	if got := db.GetSessionSub("expired"); got != "" {
		t.Fatalf("expected expired session rejected, got %q", got)
	}
	if _, ok := db.data.UserSessions["expired"]; ok {
		t.Fatal("expected expired session pruned on read")
	}
}

func TestPruneExpiredUserSessions(t *testing.T) {
	db := newTestDB(t)
	db.data.UserSessions["old"] = UserSession{Sub: "a", ExpiresAt: time.Now().Add(-time.Hour).Unix()}
	db.data.UserSessions["new"] = UserSession{Sub: "b", ExpiresAt: time.Now().Add(time.Hour).Unix()}
	removed := db.PruneExpiredUserSessions()
	if removed != 1 {
		t.Fatalf("expected 1 removed, got %d", removed)
	}
	if _, ok := db.data.UserSessions["old"]; ok {
		t.Fatal("expected old session removed")
	}
	if _, ok := db.data.UserSessions["new"]; !ok {
		t.Fatal("expected active session kept")
	}
}
