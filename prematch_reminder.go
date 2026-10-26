package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Pre-match Reminder (แผนใหม่ข้อ 3)
// kickoff/FT push มีอยู่แล้วใน goal_detector — ตัวนี้เพิ่ม "เตือนก่อนบอลเตะ"
// (default 30 นาที) ให้ผู้ใช้ที่กด 🔔 subscribe แมตช์นั้นไว้
//
// แหล่งข้อมูล: fetchMatchesFromProviders (day cache — ไม่กินโควต้าเพิ่มต่อรอบ)
// ปิดได้ด้วย env PREMATCH_REMINDER=off

const (
	prematchDefaultIntervalMin = 5
	prematchDefaultWindowMin   = 30
)

type PreMatchReminder struct {
	mu   sync.Mutex
	sent map[int]int64 // matchID -> kickoff unix ที่แจ้งไปแล้ว (กันส่งซ้ำ)
}

var GlobalPreMatchReminder = &PreMatchReminder{sent: make(map[int]int64)}

// startPreMatchReminder — เรียกจาก main() แบบ go routine
func startPreMatchReminder() {
	if !envBool("PREMATCH_REMINDER", true) {
		logInfof("⏰ pre-match reminder: ปิดด้วย PREMATCH_REMINDER=off")
		return
	}
	intervalMin := envInt("PREMATCH_INTERVAL_MIN", prematchDefaultIntervalMin)
	if intervalMin < 1 {
		intervalMin = 1
	}
	windowMin := envInt("PREMATCH_WINDOW_MIN", prematchDefaultWindowMin)
	if windowMin < 1 || windowMin > 180 {
		windowMin = prematchDefaultWindowMin
	}

	logInfof("⏰ pre-match reminder: ทำงานทุก %d นาที (แจ้งเตือนก่อน %d นาที)", intervalMin, windowMin)
	ticker := time.NewTicker(time.Duration(intervalMin) * time.Minute)
	defer ticker.Stop()

	// รอ 1 นาทีหลัง start — ให้ cache warmer/provider พร้อมก่อน
	time.Sleep(time.Minute)
	GlobalPreMatchReminder.runOnce(windowMin)

	for range ticker.C {
		GlobalPreMatchReminder.runOnce(windowMin)
	}
}

// runOnce — สแกนรายการแข่งวันนี้ หาคู่ที่กำลังจะเตะใน window แล้วส่ง FCM
func (r *PreMatchReminder) runOnce(windowMin int) {
	if GlobalDB == nil {
		return
	}
	raw, source, ok := fetchMatchesFromProviders(normalizeDateKey(""))
	if !ok || len(raw) == 0 {
		return
	}
	var matches []RealMatch
	if err := json.Unmarshal(raw, &matches); err != nil {
		logWarn("pre-match reminder: parse fixtures failed", "error", err.Error())
		return
	}

	now := time.Now()
	sentCount := 0
	for _, m := range matches {
		// เฉพาะคู่ที่ยังไม่เตะ
		if isLiveStatus(m.Status) || isFinishedStatus(m.Status) {
			continue
		}
		kickoff, err := time.Parse(time.RFC3339, m.MatchTime)
		if err != nil {
			continue // ไม่มีเวลาที่ parse ได้ = ข้าม (ไม่เดา)
		}
		due, minutesLeft := readyToRemind(kickoff, now, windowMin)
		if !due {
			continue
		}
		if r.alreadySent(m.MatchID, kickoff) {
			continue
		}
		// ส่งเฉพาะคู่ที่มีคนกด 🔔 ไว้ — ไม่มี subscriber = ไม่ยิง FCM เปล่า
		if len(GlobalDB.GetMatchSubscribers(m.MatchID)) == 0 {
			continue
		}

		title := "⏰ เกมกำลังจะเริ่ม!"
		body := fmt.Sprintf("%s vs %s • เริ่มใน %d นาที (%s)", m.HomeTeam.Name, m.AwayTeam.Name, minutesLeft, m.League)
		DispatchFCMPush(m.MatchID, title, body)
		r.markSent(m.MatchID, kickoff)
		sentCount++
		logInfof("⏰ pre-match reminder: match #%d %s (%d นาที)", m.MatchID, body, minutesLeft)
	}

	if sentCount > 0 {
		logInfof("⏰ pre-match reminder: ส่งแล้ว %d คู่ (source: %s)", sentCount, source)
	}
}

// readyToRemind — pure logic: คู่ควรแจ้งหรือไม่ + เหลือกี่นาที (ปัดขึ้น)
// แยกออกมาเพื่อ test ได้โดยไม่ต้องยิง provider
func readyToRemind(kickoff, now time.Time, windowMin int) (bool, int) {
	until := kickoff.Sub(now)
	if until <= 0 || until > time.Duration(windowMin)*time.Minute {
		return false, 0
	}
	return true, int(until.Minutes()) + 1
}

func (r *PreMatchReminder) alreadySent(matchID int, kickoff time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	prev, ok := r.sent[matchID]
	return ok && prev == kickoff.Unix()
}

func (r *PreMatchReminder) markSent(matchID int, kickoff time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent[matchID] = kickoff.Unix()
	// กัน map โตไม่จำกัด: เก็บเฉพาะรอบ ~2 วัน (แมตช์จบแล้วไม่ต้องจำ)
	if len(r.sent) > 2000 {
		cutoff := time.Now().Add(-48 * time.Hour).Unix()
		for id, ts := range r.sent {
			if ts < cutoff {
				delete(r.sent, id)
			}
		}
	}
}
