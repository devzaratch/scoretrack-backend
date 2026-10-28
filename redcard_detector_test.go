package main

// tests RedCardDetector (Phase C ข้อ 6):
// - extractRedCards: กรองเฉพาะ type=card + card=red (yellow/ไม่รู้จัก = ข้าม)
// - diffRedCards: คืนเฉพาะการ์ดใหม่ตามจำนวนที่เพิ่ม (นาทีท้ายสุดก่อน)
// - Process: ครั้งแรก seed ไม่ push / จำนวนเพิ่ม = broadcast WS RED_CARD

import (
	"testing"
)

func mkCard(minute, kind string, isHome bool) map[string]interface{} {
	return map[string]interface{}{
		"time":   minute,
		"type":   "card",
		"card":   kind,
		"isHome": isHome,
		"player": "Test Player",
	}
}

func TestExtractRedCards(t *testing.T) {
	events := []interface{}{
		mkCard("10'", "yellow", true),
		mkCard("23'", "red", true),
		mkCard("40'", "yellowred", false), // เหลืองสองใบ — ไม่ใช่แดงตรง ๆ
		mkCard("55'", "red", false),
		map[string]interface{}{"type": "goal", "time": "60'"},
		"garbage",
	}
	reds := extractRedCards(events)
	if len(reds) != 2 {
		t.Fatalf("expected 2 reds, got %d (%v)", len(reds), reds)
	}
	if reds[0].Team != "home" || reds[1].Team != "away" {
		t.Fatalf("team wrong: %v", reds)
	}
}

func TestDiffRedCards(t *testing.T) {
	all := []RedCardEvent{
		{Player: "A", Team: "home", Minute: "23'"},
		{Player: "B", Team: "away", Minute: "78'"},
		{Player: "C", Team: "home", Minute: "85'"},
	}
	// เดิมเห็น 1 ใบ -> ต้องได้ 2 ใบใหม่ (78, 85 — เรียงนาทีก่อน)
	fresh := diffRedCards(all, 1)
	if len(fresh) != 2 || fresh[0].Minute != "78'" || fresh[1].Minute != "85'" {
		t.Fatalf("expected [78,85], got %v", fresh)
	}
	// จำนวนไม่เปลี่ยน/ลด -> ไม่มีอะไรใหม่
	if diffRedCards(all, 3) != nil {
		t.Fatal("no change must be nil")
	}
	if diffRedCards(all[:1], 2) != nil {
		t.Fatal("shrink must be nil")
	}
}

func TestRedCardDetectorFirstSightSeeds(t *testing.T) {
	rc := &RedCardDetector{states: make(map[string]int)}
	events := []interface{}{mkCard("23'", "red", true)}
	// ครั้งแรก = seed ไม่ push (ไม่ panic, state ต้องเท่ากับจำนวนการ์ด)
	rc.Process("101", "Home FC", "Away FC", events)
	if rc.states["101"] != 1 {
		t.Fatalf("seed state = %d, want 1", rc.states["101"])
	}
	// เห็นซ้ำจำนวนเดิม = เงียบ (state คงเดิม)
	rc.Process("101", "Home FC", "Away FC", events)
	if rc.states["101"] != 1 {
		t.Fatalf("repeat state = %d, want 1", rc.states["101"])
	}
	// เพิ่มการ์ดใหม่ = state อัปเดต + broadcast WS RED_CARD
	// (BroadcastJSON ไม่มี client ใน test = ไม่พัง; ตรวจจาก state + log)
	events2 := append(events, mkCard("88'", "red", false))
	rc.Process("101", "Home FC", "Away FC", events2)
	if rc.states["101"] != 2 {
		t.Fatalf("after new card state = %d, want 2", rc.states["101"])
	}
}
