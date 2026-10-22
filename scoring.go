package main

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
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
// แผนข้อ 5: เพิ่ม streak (ทายถูกติดกันกี่ครั้ง) + จำนวนที่ผิด + ความแม่นยำ
func userPoints(userID string) map[string]interface{} {
	if GlobalDB == nil {
		return map[string]interface{}{"points": 0, "streak": 0, "exact_hits": 0, "correct_hits": 0, "wrong": 0, "finished": 0, "predictions": 0, "accuracy": 0, "updated_at": fmt.Sprintf("%d", time.Now().Unix())}
	}
	preds := GlobalDB.GetPredictionsByUser(userID)
	points, exact, correct, finished := 0, 0, 0, 0

	// เก็บผลของแต่ละแมตช์ไว้ (พร้อมเวลาที่ทาย) เพื่อหา streak เรียงตามลำดับเวลา
	type scored struct {
		points   int
		created  int64
		finished bool
	}
	results := make([]scored, 0, len(preds))
	for matchID, predRaw := range preds {
		pred, ok := predRaw.(map[string]interface{})
		if !ok {
			continue
		}
		p, done := scorePrediction(pred, getMatchResult(matchID))
		if !done {
			continue
		}
		finished++
		points += p
		if p == 3 {
			exact++
		} else if p == 1 {
			correct++
		}
		results = append(results, scored{points: p, created: predictionCreatedAt(pred), finished: true})
	}

	// streak = ทายถูกติดกันจากล่าสุด (เรียงตามเวลาที่ทาย — ใกล้เคียงลำดับเวลาแมตช์)
	streak := 0
	sort.Slice(results, func(i, j int) bool {
		return results[i].created > results[j].created
	})
	for _, r := range results {
		if r.points > 0 {
			streak++
			continue
		}
		break
	}

	accuracy := 0.0
	if finished > 0 {
		accuracy = float64(exact+correct) / float64(finished) * 100
	}

	return map[string]interface{}{
		"points":       points,
		"exact_hits":   exact,
		"correct_hits": correct,
		"wrong":        finished - exact - correct,
		"finished":     finished,
		"predictions":  len(preds),
		"streak":       streak,
		"accuracy":     math.Round(accuracy*10) / 10,
		"updated_at":   fmt.Sprintf("%d", time.Now().Unix()),
	}
}

// predictionCreatedAt — เวลาที่ผู้ใช้ทาย (ใช้เรียงลำดับหา streak)
func predictionCreatedAt(pred map[string]interface{}) int64 {
	if v, ok := pred["createdAt"].(string); ok {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t.Unix()
		}
	}
	return 0
}

// matchHasStarted — แมตช์เริ่มไปหรือยัง (ใช้ปิดรอบทายผล)
// ดึงจาก cache ที่มีอยู่แล้ว (ไม่ยิง provider เพิ่ม) — ถ้าไม่มีข้อมูล = ยังไม่เริ่ม
// (กันบล็อกการทายผิดวันที่ยังไม่มีข้อมูลใน cache)
func matchHasStarted(matchID string) (bool, string) {
	data, _, found := cacheGet("match:" + matchID)
	if !found || len(data) == 0 {
		return false, ""
	}
	var probe struct {
		Header struct {
			Status struct {
				Started   bool   `json:"started"`
				Finished  bool   `json:"finished"`
				StatusStr string `json:"statusStr"`
			} `json:"status"`
		} `json:"header"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return false, ""
	}
	if probe.Header.Status.Finished {
		return true, "finished"
	}
	if probe.Header.Status.Started {
		return true, "started"
	}
	st := strings.ToUpper(probe.Status)
	if st != "" && st != "NS" && st != "NOT_STARTED" && st != "SCHEDULED" && st != "POSTPONED" {
		return true, st
	}
	return false, st
}
