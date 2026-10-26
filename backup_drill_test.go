package main

import (
	"encoding/json"
	"testing"
)

func TestVerifySnapshotValid(t *testing.T) {
	snap := StoreData{
		UserSessions:       map[string]string{"s1": "u1", "s2": "u2"},
		UserFavorites:      map[string][]int{"u1": {1}},
		Predictions:        map[string]map[string]interface{}{"u1": {"m1": "2-1"}},
		FCMTokens:          map[string]int64{"tok": 123},
		MatchSubscriptions: map[int][]string{9: {"tok"}},
	}
	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	res := verifySnapshotBytes(b)
	if !res.OK {
		t.Fatalf("expected OK, got error: %s", res.Error)
	}
	if res.Bytes != len(b) {
		t.Fatalf("bytes=%d want %d", res.Bytes, len(b))
	}
	if res.Sessions != 2 || res.Favorites != 1 || res.Predictions != 1 || res.FCMTokens != 1 || res.Subs != 1 {
		t.Fatalf("unexpected counts: %+v", res)
	}
	if res.UserRecords != 6 {
		t.Fatalf("userRecordCount=%d want 6", res.UserRecords)
	}
}

func TestVerifySnapshotEmptyBytes(t *testing.T) {
	res := verifySnapshotBytes(nil)
	if res.OK {
		t.Fatal("empty snapshot must fail")
	}
	if res.Error == "" {
		t.Fatal("expected error message")
	}
}

func TestVerifySnapshotInvalidJSON(t *testing.T) {
	res := verifySnapshotBytes([]byte("{not json"))
	if res.OK {
		t.Fatal("invalid JSON must fail")
	}
	if res.Bytes == 0 {
		t.Fatal("should still report size")
	}
}

func TestVerifySnapshotEmptyValidJSON(t *testing.T) {
	res := verifySnapshotBytes([]byte(`{}`))
	if !res.OK {
		t.Fatal("valid empty JSON should pass parse")
	}
	if res.UserRecords != 0 {
		t.Fatalf("expected 0 records, got %d", res.UserRecords)
	}
}
