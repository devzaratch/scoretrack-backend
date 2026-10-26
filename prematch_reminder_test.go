package main

import (
	"testing"
	"time"
)

func TestReadyToRemindWindow(t *testing.T) {
	now := time.Date(2026, 10, 26, 20, 0, 0, 0, time.UTC)

	cases := []struct {
		name       string
		inMinutes  int
		wantDue    bool
		wantLeftIs int // -1 = ไม่เช็ค
	}{
		{"30 นาทีก่อน", 30, true, 31},
		{"5 นาทีก่อน", 5, true, 6},
		{"1 นาทีก่อน", 1, true, 2},
		{"เตะแล้ว", 0, false, -1},
		{"เตะไป 5 นาที", -5, false, -1},
		{"เกิน window (31 นาที)", 31, false, -1},
	}
	for _, tc := range cases {
		due, left := readyToRemind(now.Add(time.Duration(tc.inMinutes)*time.Minute), now, 30)
		if due != tc.wantDue {
			t.Fatalf("%s: due=%v want %v", tc.name, due, tc.wantDue)
		}
		if tc.wantLeftIs >= 0 && left != tc.wantLeftIs {
			t.Fatalf("%s: left=%d want %d", tc.name, left, tc.wantLeftIs)
		}
	}
}

func TestPreMatchReminderSentOnce(t *testing.T) {
	r := &PreMatchReminder{sent: make(map[int]int64)}
	kickoff := time.Date(2026, 10, 26, 21, 0, 0, 0, time.UTC)

	if r.alreadySent(42, kickoff) {
		t.Fatal("must not be marked before sending")
	}
	r.markSent(42, kickoff)
	if !r.alreadySent(42, kickoff) {
		t.Fatal("must be marked after sending")
	}
	// เวลาเตะเปลี่ยน (เลื่อนเกม) -> ต้องแจ้งใหม่ได้
	if r.alreadySent(42, kickoff.Add(30*time.Minute)) {
		t.Fatal("rescheduled kickoff must be treated as new")
	}
}

func TestPreMatchReminderPrune(t *testing.T) {
	r := &PreMatchReminder{sent: make(map[int]int64)}
	oldKickoff := time.Now().Add(-72 * time.Hour)
	for i := 0; i < 2100; i++ {
		r.sent[i] = oldKickoff.Unix()
	}
	r.markSent(9999, time.Now().Add(time.Hour))

	if len(r.sent) >= 2100 {
		t.Fatalf("expected prune to shrink map, still %d entries", len(r.sent))
	}
	if _, ok := r.sent[9999]; !ok {
		t.Fatal("fresh entry must survive prune")
	}
}
