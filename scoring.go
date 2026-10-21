package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// ============================================================
// Prediction Scoring — ให้คะแนนคำทำนายผลบอล
// กฎ: ถูกสกอร์เป๊ะ +3 / ถูกผล (ชนะ-เสมอ-แพ้) +1 / ผิด 0
// แมตช์ยังไม่จบ = ยังไม่คิดคะแนน (null)
// ============================================================

// MatchResult ผลแมตช์จบแล้ว (immutable -> cache ยาว)
type MatchResult struct {
	Home     int  `json:"home"`
	Away     int  `json:"away"`
	Finished bool `json:"finished"`
}

// scorePrediction ให้คะแนน 1 prediction ต่อผลแมตช์
// คืน (*int, finished) — nil = ยังไม่จบหรือดึงผลไม่ได้
func scorePrediction(pred map[string]interface{}, res *MatchResult) (int, bool) {
	if res == nil || !res.Finished {
		return 0, false
	}
	predHome, okH := toInt(pred["home"])
	predAway, okA := toInt(pred["away"])
	if !okH || !okA {
		return 0, true
	}
	if predHome == res.Home && predAway == res.Away {
		return 3, true
	}
	// เทียบผลชนะ-เสมอ-แพ้ด้วยเครื่องหมาย
	if sign(predHome-predAway) == sign(res.Home-res.Away) {
		return 1, true
	}
	return 0, true
}

func toInt(v interface{}) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	case json.Number:
		i, err := n.Int64()
		return int(i), err == nil
	}
	return 0, false
}

func sign(n int) int {
	if n > 0 {
		return 1
	}
	if n < 0 {
		return -1
	}
	return 0
}

// getMatchResult ดึงผลแมตช์ — cache ยาวเพราะแมตช์จบแล้วผลไม่เปลี่ยน
func getMatchResult(matchID string) *MatchResult {
	cacheKey := "matchresult:" + matchID
	if cached, _, found := cacheGet(cacheKey); found {
		var res MatchResult
		if err := json.Unmarshal(cached, &res); err == nil && res.Finished {
			return &res
		}
	}

	data, _, ok := fetchMatchDetailsFromProviders(matchID)
	if !ok || len(data) == 0 {
		return nil
	}
	if !matchDetailsFinished(data) {
		return nil // ยังไม่จบ -> ไม่ cache (รอคิดตอนจบ)
	}

	var probe struct {
		Header struct {
			Teams []struct {
				Score interface{} `json:"score"`
			} `json:"teams"`
		} `json:"header"`
	}
	if err := json.Unmarshal(data, &probe); err != nil || len(probe.Header.Teams) < 2 {
		return nil
	}
	home, okH := toInt(probe.Header.Teams[0].Score)
	away, okA := toInt(probe.Header.Teams[1].Score)
	if !okH || !okA {
		return nil
	}

	res := &MatchResult{Home: home, Away: away, Finished: true}
	raw, _ := json.Marshal(res)
	cacheSet(cacheKey, raw, 6*time.Hour) // ผลจบแล้วคงที่ -> cache 6 ชม.
	return res
}

// LeaderboardEntry แถว leaderboard คำนวณแล้ว
type LeaderboardEntry struct {
	UserID      string `json:"user_id"`
	Points      int    `json:"points"`
	Predictions int    `json:"predictions"`
	Finished    int    `json:"finished"`     // แมตช์ที่จบแล้ว (คิดคะแนนแล้ว)
	ExactHits   int    `json:"exact_hits"`   // ถูกเป๊ะ
	CorrectHits int    `json:"correct_hits"` // ถูกผล
}

// computeLeaderboard คำนวณคะแนนทุก user จากผลแมตช์จริง
func computeLeaderboard() []LeaderboardEntry {
	if GlobalDB == nil {
		return []LeaderboardEntry{}
	}
	allPreds := GlobalDB.GetAllPredictions()

	// เก็บผลแมตช์ที่ต้องใช้ (unique) — ดึงครั้งเดียว
	resultCache := map[string]*MatchResult{}
	for _, preds := range allPreds {
		for matchID := range preds {
			if _, seen := resultCache[matchID]; !seen {
				resultCache[matchID] = getMatchResult(matchID)
			}
		}
	}

	entries := make([]LeaderboardEntry, 0, len(allPreds))
	for userID, preds := range allPreds {
		entry := LeaderboardEntry{UserID: userID, Predictions: len(preds)}
		for matchID, predRaw := range preds {
			pred, ok := predRaw.(map[string]interface{})
			if !ok {
				continue
			}
			p, finished := scorePrediction(pred, resultCache[matchID])
			if !finished {
				continue
			}
			entry.Finished++
			entry.Points += p
			if p == 3 {
				entry.ExactHits++
			} else if p == 1 {
				entry.CorrectHits++
			}
		}
		entries = append(entries, entry)
	}

	// เรียง: คะแนนมากสุด -> ถูกเป๊ะมากสุด -> ทำนายเยอะสุด
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Points != entries[j].Points {
			return entries[i].Points > entries[j].Points
		}
		if entries[i].ExactHits != entries[j].ExactHits {
			return entries[i].ExactHits > entries[j].ExactHits
		}
		return entries[i].Predictions > entries[j].Predictions
	})
	return entries
}

// userPoints คำนวณคะแนนของ user เดียว (ใช้หน้า my predictions)
func userPoints(userID string) map[string]interface{} {
	if GlobalDB == nil {
		return map[string]interface{}{"points": 0}
	}
	preds := GlobalDB.GetPredictionsByUser(userID)
	points, exact, correct, finished := 0, 0, 0, 0
	for matchID, predRaw := range preds {
		pred, ok := predRaw.(map[string]interface{})
		if !ok {
			continue
		}
		p, ok := scorePrediction(pred, getMatchResult(matchID))
		if !ok {
			continue
		}
		finished++
		points += p
		if p == 3 {
			exact++
		} else if p == 1 {
			correct++
		}
	}
	return map[string]interface{}{
		"points":       points,
		"exact_hits":   exact,
		"correct_hits": correct,
		"finished":     finished,
		"predictions":  len(preds),
		"updated_at":   fmt.Sprintf("%d", time.Now().Unix()),
	}
}
