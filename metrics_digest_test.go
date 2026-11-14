package main

// metrics_digest_test.go — แผน-v2 ข้อ 4: digest รายวัน
// ทดสอบ pure builder + snapshot แบบ in-memory (rdb=nil) โดยไม่แตะ Redis จริง

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBuildDailyDigestTopPathsSorted(t *testing.T) {
	agg := metricsAgg{
		PVTotal: 42,
		Paths:   map[string]int64{"/a": 5, "/b": 9, "/c": 5, "/d": 1, "/e": 3, "/f": 2, "/": 7},
		Actions: map[string]int64{"search_submit": 4},
		Vitals:  map[string]vitalStat{"LCP": {Count: 3, Avg: 1200.5}},
	}
	now := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	d := buildDailyDigest("2026-11-14", now, agg)

	if d.Pageviews != 42 {
		t.Fatalf("pageviews = %d", d.Pageviews)
	}
	if len(d.TopPaths) != 5 {
		t.Fatalf("top paths = %d (ต้องตัดเหลือ 5)", len(d.TopPaths))
	}
	// เรียงจากมากไปน้อย; คะแนนเท่ากันเรียง path อักษร
	if d.TopPaths[0].Path != "/b" || d.TopPaths[0].Count != 9 {
		t.Fatalf("อันดับ 1 = %+v (ต้องเป็น /b:9)", d.TopPaths[0])
	}
	if d.TopPaths[1].Path != "/" || d.TopPaths[2].Path != "/a" {
		t.Fatalf("tie-break ผิด: %+v", d.TopPaths[1:3])
	}
	if d.Vitals["LCP"].Avg != 1200.5 {
		t.Fatalf("vitals LCP = %+v", d.Vitals["LCP"])
	}
	if d.Date != "2026-11-14" || d.TakenAt == "" {
		t.Fatalf("date/taken_at = %s / %s", d.Date, d.TakenAt)
	}
}

func TestSnapshotDailyDigestInMemory(t *testing.T) {
	// ยืม in-memory state หลัก (เทียบเท่า rdb=nil ใน prod dev) — เคลียร์ก่อน/หลัง
	metricsMu.Lock()
	oldPV, oldPaths := memPageviewsTotl, memPageviews
	memPageviewsTotl, memPageviews = 3, map[string]int64{"/": 3}
	oldDig := memDigests
	memDigests = map[string]DailyDigest{}
	metricsMu.Unlock()
	defer func() {
		metricsMu.Lock()
		memPageviewsTotl, memPageviews = oldPV, oldPaths
		memDigests = oldDig
		metricsMu.Unlock()
	}()

	d, err := snapshotDailyDigest(time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if d.Pageviews != 3 {
		t.Fatalf("pageviews = %d", d.Pageviews)
	}
	stored, ok := memDigests["2026-11-14"]
	if !ok {
		t.Fatal("ต้องเก็บ digest ต่อวันที่ในหน่วยความจำ")
	}
	raw, _ := json.Marshal(stored)
	if len(raw) == 0 {
		t.Fatal("digest serializable")
	}

	hist, err := loadDigests(30)
	if err != nil || len(hist) != 1 || hist[0].Date != "2026-11-14" {
		t.Fatalf("loadDigests = %+v err=%v", hist, err)
	}
}

func TestSnapshotOverwritesSameDay(t *testing.T) {
	metricsMu.Lock()
	oldDig := memDigests
	memDigests = map[string]DailyDigest{}
	metricsMu.Unlock()
	defer func() {
		metricsMu.Lock()
		memDigests = oldDig
		metricsMu.Unlock()
	}()

	day := time.Date(2026, 11, 14, 0, 30, 0, 0, time.UTC)
	if _, err := snapshotDailyDigest(day); err != nil {
		t.Fatalf("snap1: %v", err)
	}
	metricsMu.Lock()
	memPageviewsTotl = memPageviewsTotl + 10
	metricsMu.Unlock()
	d2, err := snapshotDailyDigest(day.Add(time.Hour))
	if err != nil {
		t.Fatalf("snap2: %v", err)
	}
	metricsMu.Lock()
	got := memDigests["2026-11-14"]
	metricsMu.Unlock()
	if got.Pageviews != d2.Pageviews || got.TakenAt != d2.TakenAt {
		t.Fatalf("snapshot ซ้ำในวันเดียวกันต้องแทนที่: %+v vs %+v", got, d2)
	}
}
