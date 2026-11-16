package main

// leaderboard_tie_test.go — อันดับที่คะแนนเท่ากันต้องคงที่
//
// เดิม computeLeaderboard / computePeriodLeaderboard เรียงด้วย sort.Slice ที่
// tie-break ไม่ครบทุกตัวชี้วัด และ entries มาจาก map (Go iterate แบบสุ่ม)
// ผลคือสองคนที่คะแนนเท่ากันจะสลับอันดับไปมาทุก request และ CI แดงแบบสุ่ม
// (เจอจริง: run #73/#75 แดงที่ commit ที่แก้แค่ docs, run #74 ผ่าน)

import (
	"fmt"
	"testing"
	"time"
)

func TestComputeLeaderboardTieIsDeterministic(t *testing.T) {
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = nil }()

	// สองคนทายเหมือนกันเป๊ะ -> คะแนน/exact/predictions เท่ากันหมด
	for _, user := range []string{"user:b", "user:a"} {
		GlobalDB.SavePrediction(user, "m-tie", map[string]interface{}{
			"home": 2, "away": 1, "createdAt": time.Now().Format(time.RFC3339),
		})
	}
	cacheSet("matchresult:m-tie", []byte(`{"home":2,"away":1,"finished":true}`), time.Hour)

	var first string
	for i := 0; i < 50; i++ {
		entries := computeLeaderboard()
		if len(entries) != 2 {
			t.Fatalf("ต้องได้ 2 entries, got %v", entries)
		}
		got := fmt.Sprintf("%s,%s", entries[0].UserID, entries[1].UserID)
		if i == 0 {
			first = got
			continue
		}
		if got != first {
			t.Fatalf("อันดับไม่คงที่: ครั้งแรก %s ครั้งที่ %d ได้ %s", first, i+1, got)
		}
	}
	// tie-break ที่ตกลงไว้ = UserID น้อยกว่าขึ้นก่อน
	if want := "user:a,user:b"; first != want {
		t.Fatalf("tie-break ต้องเรียงตาม UserID: got %s want %s", first, want)
	}
}

func TestPeriodLeaderboardTieIsDeterministic(t *testing.T) {
	GlobalDB = newTestDB(t)
	defer func() { GlobalDB = nil }()

	now := time.Now()
	for _, user := range []string{"user:b", "user:a"} {
		GlobalDB.SavePrediction(user, "m-ptie", map[string]interface{}{
			"home": 2, "away": 1, "createdAt": now.Add(-time.Hour).Format(time.RFC3339),
		})
	}
	cacheSet("matchresult:m-ptie", []byte(`{"home":2,"away":1,"finished":true}`), time.Hour)

	var first string
	for i := 0; i < 50; i++ {
		entries := computePeriodLeaderboard("week")
		if len(entries) != 2 {
			t.Fatalf("ต้องได้ 2 entries, got %v", entries)
		}
		got := fmt.Sprintf("%s,%s", entries[0].UserID, entries[1].UserID)
		if i == 0 {
			first = got
			continue
		}
		if got != first {
			t.Fatalf("อันดับไม่คงที่: ครั้งแรก %s ครั้งที่ %d ได้ %s", first, i+1, got)
		}
	}
	if want := "user:a,user:b"; first != want {
		t.Fatalf("tie-break ต้องเรียงตาม UserID: got %s want %s", first, want)
	}
}
