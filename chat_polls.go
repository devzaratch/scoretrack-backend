package main

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Chat Polls (แผนใหม่ข้อ 5)
// โพลมาตรฐาน 1 อันต่อแมตช์ ("ใครจะชนะ?") — โหวตผ่าน live WebSocket
// เก็บในหน่วยความจำ (ephemeral): restart/redeploy = รีเซ็ต (ยอมรับได้สำหรับโพลสด)
// ข้อดี: ไม่แตะ schema DB, ไม่กิน Redis commands, latency ต่ำ

const (
	pollQuestionDefault = "ใครจะชนะแมตช์นี้?"
	pollMaxVoterLen     = 64
	pollOptionHome      = "home"
	pollOptionDraw      = "draw"
	pollOptionAway      = "away"
	pollTTL             = 8 * time.Hour // เก็บโพลไม่เกิน 8 ชม. แล้วล้าง
)

// PollState — สถานะโพลที่ broadcast ให้ client
// voters เป็น field ปิด (ไม่ marshal) — client เห็นแค่ยอดรวม
type PollState struct {
	MatchID  string         `json:"match_id"`
	Question string         `json:"question"`
	Options  map[string]int `json:"options"`
	Total    int            `json:"total"`
	Updated  int64          `json:"updated"`
	voters   map[string]string
	created  time.Time
}

var (
	pollsMu        sync.Mutex
	matchPolls     = make(map[string]*PollState)
	pollsLastSweep time.Time
)

// validPollOption — รับเฉพาะ 3 ตัวเลือกมาตรฐาน
func validPollOption(opt string) bool {
	switch opt {
	case pollOptionHome, pollOptionDraw, pollOptionAway:
		return true
	}
	return false
}

// sanitizeVoter — ตัดความยาว + ตัดช่องว่าง; ว่าง = ใช้ "anon"
func sanitizeVoter(v string) string {
	v = strings.TrimSpace(v)
	if len(v) > pollMaxVoterLen {
		v = v[:pollMaxVoterLen]
	}
	if v == "" {
		return "anon"
	}
	return v
}

func newPollState(matchID string) *PollState {
	return &PollState{
		MatchID:  matchID,
		Question: pollQuestionDefault,
		Options: map[string]int{
			pollOptionHome: 0,
			pollOptionDraw: 0,
			pollOptionAway: 0,
		},
		voters:  make(map[string]string),
		created: time.Now(),
	}
}

// sweepPollsLocked — ล้างโพลเก่า (ต้องถือ pollsMu อยู่แล้ว)
func sweepPollsLocked(now time.Time) {
	if now.Sub(pollsLastSweep) < 30*time.Minute {
		return
	}
	pollsLastSweep = now
	for id, p := range matchPolls {
		if now.Sub(p.created) > pollTTL {
			delete(matchPolls, id)
		}
	}
}

// getOrCreatePoll คืนโพลของแมตช์ (สร้างใหม่ถ้ายังไม่มี)
func getOrCreatePoll(matchID string) *PollState {
	pollsMu.Lock()
	defer pollsMu.Unlock()
	sweepPollsLocked(time.Now())
	p, ok := matchPolls[matchID]
	if !ok {
		p = newPollState(matchID)
		matchPolls[matchID] = p
	}
	return p
}

// votePoll บันทึกโหวต (1 voter = 1 คะแนน, เปลี่ยนตัวเลือกได้ -> ย้ายคะแนน)
// คืน state ปัจจุบัน (copy) + ok=false เมื่อ option ไม่ถูกต้อง/ไม่มี matchID
func votePoll(matchID, voter, option string) (PollState, bool) {
	matchID = strings.TrimSpace(matchID)
	if matchID == "" || !validPollOption(option) {
		return PollState{}, false
	}
	voter = sanitizeVoter(voter)

	pollsMu.Lock()
	defer pollsMu.Unlock()
	sweepPollsLocked(time.Now())

	p, ok := matchPolls[matchID]
	if !ok {
		p = newPollState(matchID)
		matchPolls[matchID] = p
	}
	// ย้ายคะแนนถ้า voter เปลี่ยนใจ
	if prev, exists := p.voters[voter]; exists {
		if prev == option {
			return *p, true // โหวตเดิมซ้ำ — ไม่เปลี่ยนอะไร
		}
		if p.Options[prev] > 0 {
			p.Options[prev]--
			p.Total--
		}
	}
	p.voters[voter] = option
	p.Options[option]++
	p.Total++
	p.Updated = time.Now().Unix()
	return *p, true
}

// pollEnvelope — รูปแบบ message ที่ client คาดหวัง: {"type":"poll","poll":{...}}
func pollEnvelope(p PollState) map[string]interface{} {
	return map[string]interface{}{
		"type": "poll",
		"poll": p,
	}
}

// pollStateJSON — envelope แบบ bytes (ส่งเฉพาะ client เดียว /ws/live)
func pollStateJSON(p PollState) []byte {
	b, err := json.Marshal(pollEnvelope(p))
	if err != nil {
		return nil
	}
	return b
}

// currentPollJSON — state ล่าสุดของแมตช์ (สำหรับ poll_get)
func currentPollJSON(matchID string) []byte {
	p := getOrCreatePoll(matchID)
	return pollStateJSON(*p)
}

// handleLiveClientMessage — ประมวลผลข้อความจาก client บน /ws/live
// รองรับ: poll_vote (โหวต + broadcast), poll_get (ขอ state ปัจจุบันของตัวเอง)
// คืน payload ที่ต้องส่งกลับเฉพาะ client นี้ (nil = ไม่มี)
func handleLiveClientMessage(raw []byte, clientIP string) []byte {
	var msg map[string]interface{}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil
	}
	typ, _ := msg["type"].(string)
	matchID := ""
	if v, ok := msg["match_id"]; ok {
		switch t := v.(type) {
		case string:
			matchID = t
		case float64:
			matchID = strconv.FormatInt(int64(t), 10)
		}
	}
	switch typ {
	case "poll_get":
		if matchID == "" {
			return nil
		}
		return currentPollJSON(matchID)
	case "poll_vote":
		option, _ := msg["option"].(string)
		voter, _ := msg["voter"].(string)
		if voter == "" {
			voter = clientIP
		}
		state, ok := votePoll(matchID, voter, option)
		if !ok {
			return nil
		}
		// broadcast ให้ทุกคนใน live hub เห็นยอดใหม่ (BroadcastJSON แปลง interface{} เอง)
		if err := GlobalLiveHub.BroadcastJSON(pollEnvelope(state)); err == nil {
			logInfof("📊 poll: match %s vote %s (total %d)", matchID, option, state.Total)
		}
		return nil
	}
	return nil
}
