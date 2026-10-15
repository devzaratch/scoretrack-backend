package main

import (
	"testing"
	"time"
)

// กฎ event detection:
// - kickoff: เห็นครั้งแรกนาที 1-5 หรือ LIVE (ไม่ push ตอน restart กลางแมตช์)
// - goal: สกอร์เพิ่ม
// - ft: status เป็นจบเกม — ครั้งเดียวเท่านั้น

func kinds(events []matchEvent) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.Kind
	}
	return out
}

func TestDetectorFirstSightEarlyKickoff(t *testing.T) {
	gd := NewGoalDetector()
	ev := gd.computeEvents(1, 0, 0, "1'", "A", "B")
	if len(ev) != 1 || ev[0].Kind != "kickoff" {
		t.Fatalf("expected [kickoff] for first sight at 1', got %v", kinds(ev))
	}
}

func TestDetectorFirstSightLIVEKickoff(t *testing.T) {
	gd := NewGoalDetector()
	ev := gd.computeEvents(2, 0, 0, "LIVE", "A", "B")
	if len(ev) != 1 || ev[0].Kind != "kickoff" {
		t.Fatalf("expected [kickoff] for first sight LIVE, got %v", kinds(ev))
	}
}

func TestDetectorFirstSightMidMatchNoKickoff(t *testing.T) {
	// restart กลางแมตช์ (นาที 60) -> ห้าม push kickoff (กัน spam ทุกคู่ตอนบูต)
	gd := NewGoalDetector()
	ev := gd.computeEvents(3, 1, 0, "63'", "A", "B")
	if len(ev) != 0 {
		t.Fatalf("expected [] for first sight at 63', got %v", kinds(ev))
	}
}

func TestDetectorFirstSightFinishedNoEvents(t *testing.T) {
	// restart หลังแมตช์จบ -> ห้าม push อะไร (กัน spam ทุกคู่ตอนบูต)
	gd := NewGoalDetector()
	ev := gd.computeEvents(4, 2, 1, "FT", "A", "B")
	if len(ev) != 0 {
		t.Fatalf("expected [] for first sight FT, got %v", kinds(ev))
	}
}

func TestDetectorGoalDetection(t *testing.T) {
	gd := NewGoalDetector()
	gd.computeEvents(5, 0, 0, "10'", "A", "B")
	ev := gd.computeEvents(5, 1, 0, "25'", "A", "B")
	if len(ev) != 1 || ev[0].Kind != "goal" {
		t.Fatalf("expected [goal], got %v", kinds(ev))
	}
	if ev[0].Scorer != "A" {
		t.Fatalf("expected scorer A, got %q", ev[0].Scorer)
	}
}

func TestDetectorGoalAway(t *testing.T) {
	gd := NewGoalDetector()
	gd.computeEvents(6, 0, 0, "10'", "A", "B")
	ev := gd.computeEvents(6, 0, 1, "40'", "A", "B")
	if len(ev) != 1 || ev[0].Kind != "goal" || ev[0].Scorer != "B" {
		t.Fatalf("expected [goal B], got %v", kinds(ev))
	}
}

func TestDetectorNoGoalOnSameScore(t *testing.T) {
	gd := NewGoalDetector()
	gd.computeEvents(7, 1, 0, "30'", "A", "B")
	ev := gd.computeEvents(7, 1, 0, "35'", "A", "B")
	if len(ev) != 0 {
		t.Fatalf("expected [] for same score, got %v", kinds(ev))
	}
}

func TestDetectorFinishedOnce(t *testing.T) {
	gd := NewGoalDetector()
	gd.computeEvents(8, 0, 0, "1'", "A", "B")
	gd.computeEvents(8, 1, 0, "90'", "A", "B")

	ev := gd.computeEvents(8, 1, 0, "FT", "A", "B")
	if len(ev) != 1 || ev[0].Kind != "ft" {
		t.Fatalf("expected [ft], got %v", kinds(ev))
	}

	// update หลัง FT -> ห้ามมี event อะไรอีก
	ev2 := gd.computeEvents(8, 1, 0, "FT", "A", "B")
	if len(ev2) != 0 {
		t.Fatalf("expected [] after FT, got %v", kinds(ev2))
	}
	ev3 := gd.computeEvents(8, 2, 0, "FT", "A", "B")
	if len(ev3) != 0 {
		t.Fatalf("expected [] after FT (score change), got %v", kinds(ev3))
	}
}

func TestDetectorGoalWithFTInOneUpdate(t *testing.T) {
	// ประตูชัยท้ายเกม + จบในรอบเดียว -> ได้ทั้ง goal และ ft
	gd := NewGoalDetector()
	gd.computeEvents(9, 0, 0, "89'", "A", "B")
	ev := gd.computeEvents(9, 1, 0, "FT", "A", "B")
	got := kinds(ev)
	if len(ev) != 2 || ev[0].Kind != "ft" || ev[1].Kind != "goal" {
		t.Fatalf("expected [ft goal], got %v", got)
	}
}

func TestDetectorKickoffTransition(t *testing.T) {
	// เห็น NS ก่อนแล้วค่อย live -> push kickoff ตอน transition
	gd := NewGoalDetector()
	gd.computeEvents(10, 0, 0, "NS", "A", "B")
	ev := gd.computeEvents(10, 0, 0, "1'", "A", "B")
	if len(ev) != 1 || ev[0].Kind != "kickoff" {
		t.Fatalf("expected [kickoff] on NS->live transition, got %v", kinds(ev))
	}
}

func TestIsFinishedStatus(t *testing.T) {
	finished := []string{"FT", "ft", "FT PEN", "FINISHED", "AET", "PEN", "ABANDONED", "AWARDED"}
	for _, s := range finished {
		if !isFinishedStatus(s) {
			t.Errorf("isFinishedStatus(%q) = false, expected true", s)
		}
	}
	notFinished := []string{"NS", "LIVE", "45'", "HT", "TBD", "POSTP", "CANC", ""}
	for _, s := range notFinished {
		if isFinishedStatus(s) {
			t.Errorf("isFinishedStatus(%q) = true, expected false", s)
		}
	}
}

func TestIsEarlyKickoffStatus(t *testing.T) {
	early := []string{"1'", "5'", "LIVE", "3'"}
	for _, s := range early {
		if !isEarlyKickoffStatus(s) {
			t.Errorf("isEarlyKickoffStatus(%q) = false, expected true", s)
		}
	}
	notEarly := []string{"6'", "45+2'", "63'", "HT", "FT", "NS", ""}
	for _, s := range notEarly {
		if isEarlyKickoffStatus(s) {
			t.Errorf("isEarlyKickoffStatus(%q) = true, expected false", s)
		}
	}
}

func TestPruneOlderThan(t *testing.T) {
	gd := NewGoalDetector()
	gd.computeEvents(11, 0, 0, "10'", "A", "B")

	// ยังไม่เก่า -> ไม่ถูก prune
	if n := gd.pruneOlderThan(time.Hour); n != 0 {
		t.Fatalf("expected 0 removed, got %d", n)
	}

	// แกล้งทำให้เก่า
	gd.mu.Lock()
	gd.states[11].LastUpdate = time.Now().Add(-3 * time.Hour)
	gd.mu.Unlock()

	if n := gd.pruneOlderThan(2 * time.Hour); n != 1 {
		t.Fatalf("expected 1 removed, got %d", n)
	}
	if len(gd.states) != 0 {
		t.Fatalf("expected empty states, got %d", len(gd.states))
	}
}
