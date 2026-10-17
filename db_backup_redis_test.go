package main

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// withTestRedis ตั้ง package-level rdb ชั่วคราวเป็น client ต่อ miniredis
// แล้วคืนค่าเดิมตอนจบ (test ใน package นี้ไม่มีตัวอื่นแตะ rdb)
func withTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	old := rdb
	rdb = client
	t.Cleanup(func() {
		rdb = old
		client.Close()
		mr.Close()
	})
	return mr, client
}

func newTestDB(t *testing.T) *PersistentDB {
	t.Helper()
	return &PersistentDB{
		filePath: filepath.Join(t.TempDir(), "store.json"),
		data: StoreData{
			ChatMessages:          make(map[string][]map[string]interface{}),
			UserFavorites:         make(map[string][]int),
			UserSessions:          make(map[string]string),
			MatchSubscriptions:    make(map[int][]string),
			FCMTokens:             make(map[string]int64),
			Predictions:           make(map[string]map[string]interface{}),
			UserNotificationPrefs: make(map[string]map[string]interface{}),
			Achievements:          make(map[string][]string),
		},
	}
}

// TestRestoreFromSnapshot_RealRedis — กู้ข้อมูลจาก snapshot จริง (miniredis)
// เมื่อ local ว่าง (จำลองดิสก์ Render ถูกล้างตอน restart)
func TestRestoreFromSnapshot_RealRedis(t *testing.T) {
	mr, _ := withTestRedis(t)

	snap := StoreData{
		UserSessions:  map[string]string{"tok-real": "sub-real"},
		UserFavorites: map[string][]int{"sub-real": {10, 20}},
		Predictions: map[string]map[string]interface{}{
			"sub-real": {"100": map[string]interface{}{"score": "2-1"}},
		},
		FCMTokens: map[string]int64{"fcm-real": 1760000000},
	}
	raw, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	mr.Set(dbSnapshotKey, string(raw))

	db := newTestDB(t) // local ว่าง
	if !db.restoreFromSnapshot() {
		t.Fatal("ควร restore สำเร็จเมื่อ local ว่าง + snapshot มีข้อมูล")
	}
	if got := db.data.UserSessions["tok-real"]; got != "sub-real" {
		t.Errorf("UserSessions ไม่ถูกกู้: %q", got)
	}
	if favs := db.data.UserFavorites["sub-real"]; len(favs) != 2 || favs[0] != 10 {
		t.Errorf("UserFavorites ไม่ถูกกู้: %v", favs)
	}
	if !db.dirty {
		t.Error("restore แล้วต้อง mark dirty ให้ syncWorker เขียนไฟล์ local")
	}
}

// TestRestoreFromSnapshot_SkipsWhenLocalHasData — ดิสก์ยังมีข้อมูล -> ห้ามทับ
func TestRestoreFromSnapshot_SkipsWhenLocalHasData(t *testing.T) {
	mr, _ := withTestRedis(t)
	snap := StoreData{UserSessions: map[string]string{"tok-old": "sub-old"}}
	raw, _ := json.Marshal(snap)
	mr.Set(dbSnapshotKey, string(raw))

	db := newTestDB(t)
	db.data.UserSessions["tok-local"] = "sub-local" // local มีข้อมูลอยู่แล้ว

	if db.restoreFromSnapshot() {
		t.Fatal("local มีข้อมูลแล้ว ห้าม restore ทับ")
	}
	if _, ok := db.data.UserSessions["tok-old"]; ok {
		t.Error("snapshot ไม่ควรถูกดึงมา")
	}
	if db.dirty {
		t.Error("ไม่ restore = ไม่ต้อง mark dirty")
	}
}

// TestRestoreFromSnapshot_NoRedisOrNoKey — nil rdb / ไม่มี key -> เงียบ ไม่ panic
func TestRestoreFromSnapshot_NoRedisOrNoKey(t *testing.T) {
	old := rdb
	rdb = nil
	defer func() { rdb = old }()

	db := newTestDB(t)
	if db.restoreFromSnapshot() {
		t.Error("rdb=nil ต้องคืน false")
	}

	_, _ = withTestRedis(t) // rdb ต่อแล้วแต่ยังไม่มี key
	db2 := newTestDB(t)
	if db2.restoreFromSnapshot() {
		t.Error("ไม่มี key ต้องคืน false")
	}
}

// TestMaybePushSnapshot_WritesToRedis — flush แรก push snapshot ขึ้น Redis ทันที
// (lastSnapshot เป็น zero value -> ผ่านเงื่อนไข throttle) แล้วกันซ้ำภายใน 60 วิ
func TestMaybePushSnapshot_WritesToRedis(t *testing.T) {
	mr, _ := withTestRedis(t)

	db := newTestDB(t)
	payload := []byte(`{"user_sessions":{"tok":"sub"}}`)
	db.maybePushSnapshot(payload)
	if db.lastSnapshot.IsZero() {
		t.Fatal("ต้องอัปเดต lastSnapshot หลัง push")
	}

	// push อยู่ใน goroutine — poll รอจน key โผล่ (สูงสุด 2 วิ)
	deadline := time.Now().Add(2 * time.Second)
	for {
		v, err := mr.Get(dbSnapshotKey)
		if err == nil {
			if v != string(payload) {
				t.Fatalf("snapshot ไม่ตรง: got %q", v)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("snapshot ไม่ถูกเขียนขึ้น Redis ภายใน 2 วิ")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// เรียกซ้ำทันที -> throttle ข้าม ห้ามเขียนทับ (goroutine เก่า sync แล้ว)
	db.maybePushSnapshot([]byte(`{"x":1}`))
	time.Sleep(100 * time.Millisecond)
	v, _ := mr.Get(dbSnapshotKey)
	if v != string(payload) {
		t.Errorf("throttle พัง — เขียนทับก่อน 60 วิ: %q", v)
	}
}
