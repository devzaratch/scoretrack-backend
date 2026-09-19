package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

type MatchScoreState struct {
	HomeScore int
	AwayScore int
	HomeName  string
	AwayName  string
	Status    string
	LastGoal  time.Time
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

func (gd *GoalDetector) ProcessMatchUpdate(matchID int, homeScore, awayScore int, status, homeName, awayName string) {
	gd.mu.Lock()
	defer gd.mu.Unlock()

	if homeName == "" {
		homeName = "เจ้าบ้าน"
	}
	if awayName == "" {
		awayName = "ทีมเยือน"
	}

	state, exists := gd.states[matchID]
	if !exists {
		gd.states[matchID] = &MatchScoreState{
			HomeScore: homeScore,
			AwayScore: awayScore,
			HomeName:  homeName,
			AwayName:  awayName,
			Status:    status,
		}
		return
	}

	// Detect if home or away score has increased
	hasGoal := false
	var scoringTeam string
	if homeScore > state.HomeScore {
		hasGoal = true
		scoringTeam = homeName
	} else if awayScore > state.AwayScore {
		hasGoal = true
		scoringTeam = awayName
	}

	// Update state
	state.HomeScore = homeScore
	state.AwayScore = awayScore
	state.HomeName = homeName
	state.AwayName = awayName
	state.Status = status

	if hasGoal {
		log.Printf("⚽ GOAL DETECTED! Match #%d: %s ยิงประตู! (%s %d - %d %s | %s)", matchID, scoringTeam, homeName, homeScore, awayScore, awayName, status)

		payload := GoalEventPayload{
			Type:      "GOAL",
			MatchID:   matchID,
			HomeName:  homeName,
			AwayName:  awayName,
			Scorer:    scoringTeam,
			HomeScore: homeScore,
			AwayScore: awayScore,
			ScoreStr:  fmt.Sprintf("%d - %d", homeScore, awayScore),
			Status:    status,
			Timestamp: time.Now().Unix(),
		}

		// 1. Broadcast Goal Event to all connected live WebSocket clients
		go GlobalLiveHub.BroadcastJSON(payload)

		// 2. Dispatch FCM Push Notifications to all subscribers of this match
		go DispatchFCMPush(matchID, fmt.Sprintf("⚽ GOAL! %s ยิงประตู!", scoringTeam), fmt.Sprintf("%s %d - %d %s (%s)", homeName, homeScore, awayScore, awayName, status))
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
	accessToken := getEnv("FCM_ACCESS_TOKEN", "")

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
