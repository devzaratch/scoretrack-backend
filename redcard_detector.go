package main

// RedCardDetector (Phase C ข้อ 6) — แจ้งเตือน "ใบแดงสด"
//
// ทำงานกับข้อมูลที่ backend โหลดอยู่แล้ว (match details มี events การ์ดอยู่ในตัว)
// จึงไม่กินโควตา provider เพิ่มเลย:
//   - ประตู: มีอยู่แล้วใน goal_detector (live sweep ตามสกอร์)
//   - ใบแดง: ตัวนี้ — เช็คทุกครั้งที่มีคนเปิดหน้าแมตช์ (detail fetch ทำแล้ว)
//     - ครั้งแรกที่เห็นแมตช์ = seed จำนวนใบแดงเดิม (ไม่ push ย้อนหลัง กัน spam ตอน restart)
//     - จำนวนใบแดงเพิ่มขึ้น = push FCM ให้คนกด 🔔 + broadcast WS (type: RED_CARD)
//
// โครงเทียบเคียง goal_detector.go — logic หลักแยกเป็น pure function เพื่อ test ได้

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

type RedCardDetector struct {
	mu     sync.Mutex
	states map[string]int // matchID(string) -> จำนวนใบแดงรวมที่เคยเห็น
}

var GlobalRedCardDetector = &RedCardDetector{states: make(map[string]int)}

// RedCardEvent — ใบแดง 1 ใบจาก events (รูปเดียวกับ buildGoalEvents/buildAPIFootEvents)
type RedCardEvent struct {
	Player string
	Team   string // "home" | "away"
	Minute string // ข้อความนาทีตามต้นทาง เช่น "45'" หรือ "45+2'"
}

// extractRedCards — pure: กรองการ์ดแดงจากรายการ events
func extractRedCards(events []interface{}) []RedCardEvent {
	out := make([]RedCardEvent, 0)
	for _, e := range events {
		m, ok := e.(map[string]interface{})
		if !ok {
			continue
		}
		typ, _ := m["type"].(string)
		card, _ := m["card"].(string)
		if typ != "card" || card != "red" {
			continue
		}
		team := "away"
		if isHome, _ := m["isHome"].(bool); isHome {
			team = "home"
		}
		out = append(out, RedCardEvent{
			Player: fmt.Sprint(m["player"]),
			Team:   team,
			Minute: fmt.Sprint(m["time"]),
		})
	}
	return out
}

// diffRedCards — pure: คืน "ใบแดงใหม่" เมื่อจำนวนรวมเพิ่มจาก prevCount
// events ต้นทางเรียงนาทีน้อย->มากแล้ว — ที่นี่เรียงซ้ำเชิงป้องกัน
// แล้วตัด k ใบนาทีท้ายสุดเป็นการ์ดที่เพิ่งเกิด
func diffRedCards(all []RedCardEvent, prevCount int) []RedCardEvent {
	if prevCount < 0 {
		prevCount = 0
	}
	if len(all) <= prevCount {
		return nil
	}
	sorted := make([]RedCardEvent, len(all))
	copy(sorted, all)
	sort.SliceStable(sorted, func(i, j int) bool {
		return eventMinute(sorted[i].Minute) < eventMinute(sorted[j].Minute)
	})
	return sorted[prevCount:]
}

// Process — เช็ค events ของแมตช์แล้ว push ใบแดงที่เพิ่งเกิด
// เรียกจาก provider MatchDetails หลัง build events (ไม่ยิง upstream เพิ่ม)
func (rc *RedCardDetector) Process(matchID, homeName, awayName string, events []interface{}) {
	reds := extractRedCards(events)

	rc.mu.Lock()
	prev, seen := rc.states[matchID]
	if !seen {
		// ครั้งแรกที่เห็น — seed แล้วจบ (ไม่ push ย้อนหลัง)
		rc.states[matchID] = len(reds)
		rc.mu.Unlock()
		return
	}
	fresh := diffRedCards(reds, prev)
	if len(fresh) == 0 {
		rc.mu.Unlock()
		return
	}
	rc.states[matchID] = len(reds)
	// กัน map โตไม่จำกัด: เกิน 3000 เคลียร์ทั้งก้อน
	// (ครั้งถัดไปจะ re-seed แบบเงียบ — ไม่ spam เพราะ first-sight ไม่ push)
	if len(rc.states) > 3000 {
		rc.states = make(map[string]int)
		rc.states[matchID] = len(reds)
	}
	rc.mu.Unlock()

	idInt, _ := strconv.Atoi(matchID)
	for _, r := range fresh {
		teamName := awayName
		if r.Team == "home" {
			teamName = homeName
		}
		title := "🟥 ใบแดง!"
		body := fmt.Sprintf("%s (%s) • นาที %s — %s vs %s", r.Player, teamName, r.Minute, homeName, awayName)
		logInfof("🟥 RED CARD #%d: %s", idInt, body)

		if idInt > 0 {
			go DispatchFCMPush(idInt, title, body)
		}
		// WS: คนที่เปิดหน้าเว็บอยู่เห็น toast ทันที (เทียบเคียง GOAL broadcast)
		payload := map[string]interface{}{
			"type":      "RED_CARD",
			"match_id":  idInt,
			"player":    r.Player,
			"team":      r.Team,
			"minute":    r.Minute,
			"home_name": homeName,
			"away_name": awayName,
			"timestamp": time.Now().Unix(),
		}
		go GlobalLiveHub.BroadcastJSON(payload)
	}
}
