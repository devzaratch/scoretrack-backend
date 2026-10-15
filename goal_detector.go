package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type MatchScoreState struct {
	HomeScore  int
	AwayScore  int
	HomeName   string
	AwayName   string
	Status     string
	Started    bool
	Finished   bool
	LastGoal   time.Time
	LastUpdate time.Time
}

type GoalDetector struct {
	states map[int]*MatchScoreState
	mu     sync.RWMutex
}

var GlobalGoalDetector = NewGoalDetector()

func NewGoalDetector() *GoalDetector {
	return &GoalDetector{
		states: make(map[int]*MatchScoreState),
	}
}

type GoalEventPayload struct {
	Type      string `json:"type"`
	MatchID   int    `json:"match_id"`
	HomeName  string `json:"home_name"`
	AwayName  string `json:"away_name"`
	Scorer    string `json:"scorer"`
	HomeScore int    `json:"home_score"`
	AwayScore int    `json:"away_score"`
	ScoreStr  string `json:"score"`
	Status    string `json:"status"`
	Timestamp int64  `json:"timestamp"`
}

// matchEvent คือ event ที่ computeEvents คืนมา (kind: kickoff | goal | ft)
type matchEvent struct {
	Kind   string
	Title  string
	Body   string
	Scorer string // ชื่อทีมที่ยิง (ใช้เฉพาะ kind=goal)
}

// isFinishedStatus คืน true เมื่อ status บอกว่าแมตช์จบแล้ว
// (รองรับ: FT, FT PEN, FINISHED, AET, PEN, ABANDONED, AWARDED — ไม่รวม POSTP/CANC ที่ไม่ได้เตะ)
func isFinishedStatus(s string) bool {
	up := strings.ToUpper(strings.TrimSpace(s))
	for _, p := range []string{"FT", "FINISHED", "AET", "PEN", "ABAND", "AWARDED"} {
		if strings.HasPrefix(up, p) {
			return true
		}
	}
	return false
}

// isEarlyKickoffStatus คืน true เมื่อ status บอกว่าเพิ่งเริ่มเตะ (นาที 1-5 หรือ LIVE)
// ใช้กัน push "แมตช์เริ่มแล้ว" ผิดตอน server restart กลางแมตช์
// (เห็นแมตช์ครั้งแรกนาที 60+ = เตะมานานแล้ว ไม่ต้องแจ้ง)
func isEarlyKickoffStatus(s string) bool {
	t := strings.TrimSpace(s)
	if t == "LIVE" {
		return true
	}
	if strings.HasSuffix(t, "'") {
		digits := t[:strings.Index(t, "'")]
		if i := strings.Index(digits, "+"); i >= 0 {
			digits = digits[:i] // "45+2'" -> "45"
		}
		if m, err := strconv.Atoi(digits); err == nil && m >= 1 && m <= 5 {
			return true
		}
	}
	return false
}

// computeEvents อัปเดตสถานะแมตช์และคืนรายการ event ที่เพิ่งเกิด
// - kickoff: เห็นครั้งแรกตอนเพิ่งเตะ (นาที 1-5) หรือ status ข้ามจากไม่เริ่มเป็น live
// - goal: สกอร์เพิ่มขึ้น (ของเดิม)
// - ft: status เปลี่ยนเป็นจบเกม — push ครั้งเดียวเท่านั้น
// First-sight ตอนเตะกลางเกม (server restart) ไม่ push kickoff กัน spam
func (gd *GoalDetector) computeEvents(matchID int, homeScore, awayScore int, status, homeName, awayName string) []matchEvent {
	gd.mu.Lock()
	defer gd.mu.Unlock()

	now := time.Now()
	events := []matchEvent{}

	kickoffEvent := func() matchEvent {
		return matchEvent{
			Kind:  "kickoff",
			Title: "⏳ แมตช์เริ่มแข่งขันแล้ว!",
			Body:  fmt.Sprintf("%s vs %s", homeName, awayName),
		}
	}
	ftEvent := func() matchEvent {
		return matchEvent{
			Kind:  "ft",
			Title: "🏁 จบเกม!",
			Body:  fmt.Sprintf("%s %d - %d %s", homeName, homeScore, awayScore, awayName),
		}
	}
	goalEvent := func(scoringTeam string) matchEvent {
		return matchEvent{
			Kind:   "goal",
			Title:  fmt.Sprintf("⚽ GOAL! %s ยิงประตู!", scoringTeam),
			Body:   fmt.Sprintf("%s %d - %d %s (%s)", homeName, homeScore, awayScore, awayName, status),
			Scorer: scoringTeam,
		}
	}

	state, exists := gd.states[matchID]
	if !exists {
		// เห็นครั้งแรก -> seed state
		state = &MatchScoreState{
			HomeScore:  homeScore,
			AwayScore:  awayScore,
			HomeName:   homeName,
			AwayName:   awayName,
			Status:     status,
			LastUpdate: now,
		}
		gd.states[matchID] = state

		if isFinishedStatus(status) {
			// restart กลาง/หลังแมตช์ -> ห้าม push อะไรทั้งสิ้น (กัน spam ทุกคู่ตอนบูต)
			state.Finished = true
			state.Started = true
		} else if isLiveStatus(status) {
			state.Started = true
			if isEarlyKickoffStatus(status) {
				events = append(events, kickoffEvent())
			}
		}
		return events
	}

	if state.Finished {
		return events // จบไปแล้ว -> นิ่ง
	}

	prevHome, prevAway := state.HomeScore, state.AwayScore
	wasStarted := state.Started

	state.HomeScore = homeScore
	state.AwayScore = awayScore
	state.HomeName = homeName
	state.AwayName = awayName
	state.Status = status
	state.LastUpdate = now

	// 1. จบเกม (เช็คก่อน goal เพื่อให้ push "จบเกม" มาก่อนถ้ามาพร้อมกัน)
	if isFinishedStatus(status) {
		state.Finished = true
		state.Started = true
		events = append(events, ftEvent())
	}

	// 2. ประตูใหม่
	if homeScore > prevHome {
		state.LastGoal = now
		events = append(events, goalEvent(homeName))
	} else if awayScore > prevAway {
		state.LastGoal = now
		events = append(events, goalEvent(awayName))
	}

	// 3. kickoff แบบ transition (เห็น NS ก่อนแล้วค่อย live)
	if !wasStarted && !state.Finished && isLiveStatus(status) {
		state.Started = true
		events = append(events, kickoffEvent())
	}

	return events
}

// pruneOlderThan ลบสถานะแมตช์ที่ไม่อัปเดตนานกว่า maxAge (กัน memory leak)
func (gd *GoalDetector) pruneOlderThan(maxAge time.Duration) int {
	gd.mu.Lock()
	defer gd.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	removed := 0
	for id, st := range gd.states {
		if st.LastUpdate.Before(cutoff) {
			delete(gd.states, id)
			removed++
		}
	}
	return removed
}

func (gd *GoalDetector) ProcessMatchUpdate(matchID int, homeScore, awayScore int, status, homeName, awayName string) {
	if homeName == "" {
		homeName = "เจ้าบ้าน"
	}
	if awayName == "" {
		awayName = "ทีมเยือน"
	}

	events := gd.computeEvents(matchID, homeScore, awayScore, status, homeName, awayName)
	if len(events) == 0 {
		return
	}

	pushEnabled := envBool("PUSH_AUTO_NOTIFY", true)

	for _, ev := range events {
		switch ev.Kind {
		case "goal":
			log.Printf("⚽ GOAL DETECTED! Match #%d: %s %d - %d %s (%s)", matchID, homeName, homeScore, awayScore, awayName, status)
			payload := GoalEventPayload{
				Type:      "GOAL",
				MatchID:   matchID,
				HomeName:  homeName,
				AwayName:  awayName,
				Scorer:    ev.Scorer,
				HomeScore: homeScore,
				AwayScore: awayScore,
				ScoreStr:  fmt.Sprintf("%d - %d", homeScore, awayScore),
				Status:    status,
				Timestamp: time.Now().Unix(),
			}
			// Broadcast ให้ WebSocket clients เสมอ (หน้า live ต้องอัปเดต)
			go GlobalLiveHub.BroadcastJSON(payload)
		case "kickoff":
			log.Printf("⏳ MATCH STARTED! #%d: %s vs %s (%s)", matchID, homeName, awayName, status)
		case "ft":
			log.Printf("🏁 MATCH FINISHED! #%d: %s %d - %d %s", matchID, homeName, homeScore, awayScore, awayName)
		}

		if pushEnabled {
			go DispatchFCMPush(matchID, ev.Title, ev.Body)
		}
	}
}

// DispatchFCMPush sends Web & Mobile Push notification to subscribed FCM tokens (Supports FCM HTTP v1 API & Legacy Key)
func DispatchFCMPush(matchID int, title, body string) {
	if GlobalDB == nil {
		return
	}

	tokens := GlobalDB.GetMatchSubscribers(matchID)
	if len(tokens) == 0 {
		return
	}

	log.Printf("📱 Dispatching FCM Push Notification for Match #%d to %d subscribers", matchID, len(tokens))

	projectID := getEnv("FCM_PROJECT_ID", "")
	accessToken := getFCMAccessToken()

	// 1. FCM HTTP v1 API (Recommended by Google)
	if projectID != "" && accessToken != "" {
		v1URL := fmt.Sprintf("https://fcm.googleapis.com/v1/projects/%s/messages:send", projectID)
		client := &http.Client{Timeout: 5 * time.Second}

		successCount := 0
		for _, token := range tokens {
			v1Payload := map[string]interface{}{
				"message": map[string]interface{}{
					"token": token,
					"notification": map[string]interface{}{
						"title": title,
						"body":  body,
					},
					"data": map[string]interface{}{
						"match_id":     fmt.Sprintf("%d", matchID),
						"click_action": fmt.Sprintf("/match/%d", matchID),
					},
				},
			}

			jsonData, err := json.Marshal(v1Payload)
			if err != nil {
				continue
			}

			req, err := http.NewRequest("POST", v1URL, bytes.NewBuffer(jsonData))
			if err != nil {
				continue
			}
			req.Header.Set("Authorization", "Bearer "+accessToken)
			req.Header.Set("Content-Type", "application/json")

			resp, err := client.Do(req)
			if err == nil {
				if resp.StatusCode == http.StatusOK {
					successCount++
				}
				resp.Body.Close()
			}
		}
		log.Printf("✅ FCM HTTP v1 Push delivered successfully (%d/%d devices)", successCount, len(tokens))
		return
	}

	// 2. FCM Legacy API Fallback
	serverKey := getEnv("FCM_SERVER_KEY", "")
	if serverKey == "" {
		log.Printf("ℹ️ FCM Push Ready: Title='%s', Body='%s' (%d devices)", title, body, len(tokens))
		return
	}

	// Construct FCM Multicast payload
	payload := map[string]interface{}{
		"registration_ids": tokens,
		"notification": map[string]interface{}{
			"title": title,
			"body":  body,
			"icon":  "/favicon.ico",
			"sound": "default",
		},
		"data": map[string]interface{}{
			"match_id":     matchID,
			"click_action": fmt.Sprintf("/match/%d", matchID),
		},
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return
	}

	req, err := http.NewRequest("POST", "https://fcm.googleapis.com/fcm/send", bytes.NewBuffer(jsonData))
	if err != nil {
		return
	}

	req.Header.Set("Authorization", "key="+serverKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("FCM Push delivery notice: %v", err)
		return
	}
	defer resp.Body.Close()
	log.Printf("✅ FCM Push legacy delivered (HTTP %d)", resp.StatusCode)
}
