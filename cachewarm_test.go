package main

import (
	"os"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// แผนข้อ 2 — cache warmer: ตั้งค่า + ตัวนับ + snapshot
// ---------------------------------------------------------------------------

func TestWarmupLeagues_DefaultAndOverride(t *testing.T) {
	// ค่าเริ่มต้น
	os.Unsetenv("WARMUP_LEAGUES")
	defaults := warmupLeagues()
	if len(defaults) == 0 {
		t.Fatal("ต้องมีลีกค่าเริ่มต้นอย่างน้อย 1 ลีก")
	}

	// override จาก env
	t.Setenv("WARMUP_LEAGUES", " 47 , 87 ,")
	got := warmupLeagues()
	if len(got) != 2 || got[0] != "47" || got[1] != "87" {
		t.Errorf("parse ลีกจาก env ไม่ถูก: %v", got)
	}
}

func TestWarmupEnabled(t *testing.T) {
	os.Unsetenv("WARMUP_ENABLED")
	if !warmupEnabled() {
		t.Error("ค่าเริ่มต้นควรเปิดอยู่")
	}
	t.Setenv("WARMUP_ENABLED", "0")
	if warmupEnabled() {
		t.Error("WARMUP_ENABLED=0 ต้องปิด")
	}
	t.Setenv("WARMUP_ENABLED", "1")
	if !warmupEnabled() {
		t.Error("WARMUP_ENABLED=1 ต้องเปิด")
	}
}

func TestBangkokDate_UsesThaiTimezone(t *testing.T) {
	d := bangkokDate(0)
	now := time.Now().In(time.FixedZone("ICT", 7*3600))
	if d != now.Format("20060102") {
		t.Errorf("วันที่ต้องเป็นเวลาไทย: %s != %s", d, now.Format("20060102"))
	}
	tomorrow := bangkokDate(1)
	expected := now.AddDate(0, 0, 1).Format("20060102")
	if tomorrow != expected {
		t.Errorf("วันที่พรุ่งนี้ผิด: %s != %s", tomorrow, expected)
	}
}

func TestSnapshotWarmup_HasFields(t *testing.T) {
	snap := snapshotWarmup()
	for _, key := range []string{"enabled", "runs", "ok", "failed", "last_run_at", "last_ok", "last_error", "interval_sec", "leagues"} {
		if _, ok := snap[key]; !ok {
			t.Errorf("snapshotWarmup ขาดคีย์ %s", key)
		}
	}
}

// warmMatches ต้องข้ามถ้า cache ยังสด (ไม่ดึง provider ใหม่ = ไม่กินโควตาเปล่า)
func TestWarmMatches_SkipsFreshCache(t *testing.T) {
	date := bangkokDate(0)
	key := "matches:" + date
	cacheSet(key, []byte(`[]`), 60*time.Second)

	source, ok := warmMatches(date)
	if !ok {
		t.Error("cache ที่ยังสดต้องถือว่าสำเร็จ")
	}
	if source != "cache-fresh" {
		t.Errorf("ต้องข้ามเพราะ cache ยังสด ได้ %s", source)
	}
}
