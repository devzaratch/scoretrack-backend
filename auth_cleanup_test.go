package main

import (
	"testing"
	"time"
)

func TestPruneExpiredAuthTransientData(t *testing.T) {
	db := newTestDB(t)
	if db.data.VerificationCodes == nil { db.data.VerificationCodes = map[string]VerificationCode{} }
	if db.data.PasswordResets == nil { db.data.PasswordResets = map[string]PasswordReset{} }
	if db.data.RefreshTokens == nil { db.data.RefreshTokens = map[string]string{} }
	if db.data.RefreshExpires == nil { db.data.RefreshExpires = map[string]int64{} }
	now := time.Now().Unix()
	db.data.VerificationCodes["old@example.com"] = VerificationCode{Code: "1", Expires: now - 10}
	db.data.VerificationCodes["new@example.com"] = VerificationCode{Code: "2", Expires: now + 600}
	db.data.PasswordResets["old@example.com"] = PasswordReset{Token: "a", Expires: now - 10}
	db.data.PasswordResets["new@example.com"] = PasswordReset{Token: "b", Expires: now + 600}
	db.data.RefreshTokens["sub-old"] = "rt-old"
	db.data.RefreshExpires["sub-old"] = now - 10
	db.data.RefreshTokens["sub-new"] = "rt-new"
	db.data.RefreshExpires["sub-new"] = now + 600

	removed := db.PruneExpiredAuthTransientData()
	if removed < 3 {
		t.Fatalf("expected >=3 removed, got %d", removed)
	}
	if _, ok := db.data.VerificationCodes["old@example.com"]; ok { t.Fatal("old verification not pruned") }
	if _, ok := db.data.VerificationCodes["new@example.com"]; !ok { t.Fatal("new verification pruned") }
	if _, ok := db.data.PasswordResets["old@example.com"]; ok { t.Fatal("old reset not pruned") }
	if _, ok := db.data.PasswordResets["new@example.com"]; !ok { t.Fatal("new reset pruned") }
	if _, ok := db.data.RefreshTokens["sub-old"]; ok { t.Fatal("old refresh not pruned") }
	if _, ok := db.data.RefreshExpires["sub-old"]; ok { t.Fatal("old refresh expires not pruned") }
	if _, ok := db.data.RefreshTokens["sub-new"]; !ok { t.Fatal("new refresh pruned") }
}
