package main

import (
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// แผนข้อ 1 — ชั้น Storage (ไฟล์/Postgres)
// ---------------------------------------------------------------------------

func TestFileStorage_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.json")
	store := newFileStorage(path)

	// ยังไม่มีไฟล์ -> ข้อมูลว่าง ไม่ error
	data, err := store.Load()
	if err != nil {
		t.Fatalf("โหลดไฟล์ที่ยังไม่มีไม่ควร error: %v", err)
	}
	if len(data.UserSessions) != 0 {
		t.Errorf("ข้อมูลควรว่าง ได้ %v", data.UserSessions)
	}

	// เขียน -> อ่านกลับได้ของเดิม
	data.UserSessions["tok"] = "sub-1"
	data.UserFavorites["user:g:sub-1"] = []int{1, 2, 3}
	if err := store.Save(data); err != nil {
		t.Fatalf("บันทึกไม่สำเร็จ: %v", err)
	}
	back, err := store.Load()
	if err != nil {
		t.Fatalf("อ่านกลับไม่สำเร็จ: %v", err)
	}
	if back.UserSessions["tok"] != "sub-1" {
		t.Errorf("session หายหลัง round-trip: %v", back.UserSessions)
	}
	if len(back.UserFavorites["user:g:sub-1"]) != 3 {
		t.Errorf("favorites หายหลัง round-trip: %v", back.UserFavorites)
	}

	// ไฟล์ .tmp ต้องถูกลบ (เขียนแบบ atomic)
	if _, err := os.Stat(path + ".tmp"); err == nil {
		t.Error("ไฟล์ .tmp ควรถูก rename ไปแล้ว")
	}
}

func TestMergeLoaded_KeepsAllFields(t *testing.T) {
	loaded := StoreData{
		UserSessions:  map[string]string{"tok": "sub"},
		APIKeys:       map[string]APIKeyRecord{"sk": {Name: "x"}},
		UserInterests: map[string][]string{"user:g:sub": {"league:47"}},
	}
	merged := mergeLoaded(loaded)
	if merged.UserSessions["tok"] != "sub" {
		t.Error("UserSessions หาย")
	}
	if merged.APIKeys["sk"].Name != "x" {
		t.Error("APIKeys หาย")
	}
	if len(merged.UserInterests["user:g:sub"]) != 1 {
		t.Error("UserInterests หาย")
	}
	// field ที่ไม่มีในไฟล์เก่าต้องเป็น map ว่าง (ไม่ใช่ nil)
	if merged.FCMTokens == nil || merged.Achievements == nil {
		t.Error("map ที่ไม่มีข้อมูลต้องไม่เป็น nil")
	}
}

func TestIsEmptyStore(t *testing.T) {
	if !isEmptyStore(newEmptyStoreData()) {
		t.Error("ข้อมูลว่างต้องเป็น empty")
	}
	withData := newEmptyStoreData()
	withData.UserSessions["tok"] = "sub"
	if isEmptyStore(withData) {
		t.Error("มี session แล้วต้องไม่ว่าง")
	}
}

// Postgres — ข้ามถ้าไม่ได้ตั้ง DATABASE_URL (ไม่มี DB ในเครื่อง/CI)
func TestPostgresStorage_RoundTrip(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("ไม่ได้ตั้ง DATABASE_URL — ข้ามการทดสอบ Postgres")
	}
	store, err := newPostgresStorage(dsn)
	if err != nil {
		t.Fatalf("เชื่อมต่อ Postgres ไม่สำเร็จ: %v", err)
	}
	defer store.Close()

	data, err := store.Load()
	if err != nil {
		t.Fatalf("โหลดไม่สำเร็จ: %v", err)
	}
	data.UserSessions["e2e-token"] = "e2e-sub"
	if err := store.Save(data); err != nil {
		t.Fatalf("บันทึกไม่สำเร็จ: %v", err)
	}
	back, err := store.Load()
	if err != nil {
		t.Fatalf("อ่านกลับไม่สำเร็จ: %v", err)
	}
	if back.UserSessions["e2e-token"] != "e2e-sub" {
		t.Errorf("session หายหลัง round-trip: %v", back.UserSessions)
	}
}
