package main

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestUserRecordCount(t *testing.T) {
	if got := userRecordCount(nil); got != 0 {
		t.Fatalf("nil -> %d, want 0", got)
	}
	empty := &StoreData{}
	if got := userRecordCount(empty); got != 0 {
		t.Fatalf("empty -> %d, want 0", got)
	}
	// แชทไม่นับเป็นข้อมูลผู้ใช้
	chatOnly := &StoreData{
		ChatMessages: map[string][]map[string]interface{}{
			"999999": {{"text": "hi"}},
		},
	}
	if got := userRecordCount(chatOnly); got != 0 {
		t.Fatalf("chat-only -> %d, want 0 (แชทไม่นับ)", got)
	}
	filled := &StoreData{
		UserSessions:       map[string]string{"tok1": "google-sub-1"},
		UserFavorites:      map[string][]int{"google-sub-1": {1, 2}},
		Predictions:        map[string]map[string]interface{}{"google-sub-1": {"100": nil}},
		Achievements:       map[string][]string{"google-sub-1": {"first_login"}},
		FCMTokens:          map[string]int64{"fcm-1": 1760000000},
		MatchSubscriptions: map[int][]string{1: {"google-sub-1"}},
	}
	if got := userRecordCount(filled); got != 6 {
		t.Fatalf("filled -> %d, want 6", got)
	}
}

func TestShouldRestoreFromSnapshot(t *testing.T) {
	dataSnap := &StoreData{
		UserSessions:  map[string]string{"tok": "sub"},
		UserFavorites: map[string][]int{"sub": {5}},
	}
	empty := &StoreData{}
	chatLocal := &StoreData{
		ChatMessages: map[string][]map[string]interface{}{"1": {{"text": "x"}}},
	}

	cases := []struct {
		name        string
		local, snap *StoreData
		want        bool
	}{
		{"ดิสก์ว่าง + snapshot มีข้อมูล -> กู้", empty, dataSnap, true},
		{"ดิสก์ว่าง + snapshot ว่าง -> ไม่กู้", empty, empty, false},
		{"ดิสก์มีข้อมูล + snapshot มีข้อมูล -> ใช้ของดิสก์", dataSnap, dataSnap, false},
		{"ดิสก์มีข้อมูล + ไม่มี snapshot -> ไม่กู้", dataSnap, empty, false},
		{"ดิสก์มีแค่แชท + snapshot มีผู้ใช้ -> กู้ (แชทไม่นับ)", chatLocal, dataSnap, true},
		{"nil snapshot -> ไม่กู้", empty, nil, false},
	}
	for _, c := range cases {
		if got := shouldRestoreFromSnapshot(c.local, c.snap); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

// TestSnapshotRoundTrip ประกันว่า JSON ที่ push ขึ้น Redis
// marshal/unmarshal กลับได้ครบ ทุก field สำคัญ
func TestSnapshotRoundTrip(t *testing.T) {
	orig := &StoreData{
		ChatMessages: map[string][]map[string]interface{}{
			// ตัวเลขใน interface{} ต้องเป็น float64 ตรงแบบที่ json.Unmarshal
			// คืนกลับมา (int ใน test = DeepEqual fail — พฤติกรรม JSON ปกติ)
			"123": {{"id": float64(1), "user": "A", "text": "สวัสดี", "type": "text"}},
		},
		UserFavorites:      map[string][]int{"sub-a": {10, 20}},
		UserSessions:       map[string]string{"tok-a": "sub-a"},
		MatchSubscriptions: map[int][]string{55: {"sub-a"}},
		FCMTokens:          map[string]int64{"fcm-a": 1760000000},
		Predictions: map[string]map[string]interface{}{
			"sub-a": {"100": map[string]interface{}{"score": "2-1", "points": float64(3)}},
		},
		UserNotificationPrefs: map[string]map[string]interface{}{
			"sub-a": {"goals": true},
		},
		Achievements: map[string][]string{"sub-a": {"first_prediction"}},
	}

	raw, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back StoreData
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(orig.ChatMessages, back.ChatMessages) {
		t.Errorf("ChatMessages ไม่ตรงหลัง round-trip")
	}
	if !reflect.DeepEqual(orig.UserFavorites, back.UserFavorites) {
		t.Errorf("UserFavorites ไม่ตรง")
	}
	if !reflect.DeepEqual(orig.UserSessions, back.UserSessions) {
		t.Errorf("UserSessions ไม่ตรง")
	}
	if !reflect.DeepEqual(orig.Predictions, back.Predictions) {
		t.Errorf("Predictions ไม่ตรง")
	}
	if !reflect.DeepEqual(orig.Achievements, back.Achievements) {
		t.Errorf("Achievements ไม่ตรง")
	}
	if !reflect.DeepEqual(orig.MatchSubscriptions, back.MatchSubscriptions) {
		t.Errorf("MatchSubscriptions ไม่ตรง")
	}
	if userRecordCount(&back) != userRecordCount(orig) {
		t.Errorf("userRecordCount ไม่ตรงหลัง round-trip")
	}
}
