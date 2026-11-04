package main

// leaderboard_period.go — จัดอันดับทำนายตามช่วงเวลา (สัปดาห์นี้ / เดือนนี้) (ข้อ 4)
//
// หลักการ: prediction มี createdAt (RFC3339) อยู่แล้ว (ใช้ทำ streak) — นับเฉพาะที่ทาย
// 'ในช่วงเวลา' ไม่ใช่แค่แมตช์เล่นในช่วง กติกาคะแนนเหมือน all-time ทุกอย่าง
// (+3 เป๊ะ / +1 ผล / 0 ผิด) — เรียง: คะแนน > ถูกเป๊ะ > จำนวนทาย

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
)

// periodStartUnix — ต้นช่วงเวลาของ kind (week = วันจันทร์เที่ยงคืน / month = ต้นเดือน)
// ใช้ timezone ของเซิร์ฟเวอร์ (UTC บน Render) อธิบายไว้ตรงนี้กัน retrieve ผิดเข้าใจ
func periodStartUnix(kind string, now time.Time) int64 {
	switch kind {
	case "month":
		return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location()).Unix()
	default: // "week"
		wd := int(now.Weekday())
		if wd == 0 {
			wd = 7
		}
		dayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
		return dayStart.AddDate(0, 0, -(wd - 1)).Unix()
	}
}

// computePeriodLeaderboard — จัดอันดับเฉพาะ prediction ที่ทายในช่วง (เริ่ม start)
func computePeriodLeaderboard(kind string) []LeaderboardEntry {
	if GlobalDB == nil {
		return []LeaderboardEntry{}
	}
	start := periodStartUnix(kind, time.Now())
	allPreds := GlobalDB.GetAllPredictions()

	resultCache := map[string]*MatchResult{}
	entries := make([]LeaderboardEntry, 0, len(allPreds))

	for userID, preds := range allPreds {
		inPeriod := map[string]map[string]interface{}{}
		for matchID, predRaw := range preds {
			pred, ok := predRaw.(map[string]interface{})
			if !ok {
				continue
			}
			// ไม่มี createdAt = ข้อมูลเก่าก่อนระบบ timestamp — ข้าม (คู่กรณี all-time นับครบ)
			if ts := predictionCreatedAt(pred); ts == 0 || ts < start {
				continue
			}
			inPeriod[matchID] = pred
			if _, seen := resultCache[matchID]; !seen {
				resultCache[matchID] = getMatchResult(matchID)
			}
		}
		if len(inPeriod) == 0 {
			continue
		}
		entry := LeaderboardEntry{UserID: userID, Predictions: len(inPeriod)}
		for matchID, pred := range inPeriod {
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

// handlePeriodLeaderboard — route handler ต่อชนิดช่วง (cache เดียวกับ all-time: TTLLeaderboard)
func handlePeriodLeaderboard(kind string) gin.HandlerFunc {
	cacheKey := "predictions:leaderboard:" + kind
	return func(c *gin.Context) {
		if cached, source, found := cacheGet(cacheKey); found {
			c.Header("X-Cache", "HIT")
			c.Header("X-Data-Source", source)
			c.Data(http.StatusOK, "application/json; charset=utf-8", cached)
			return
		}
		if GlobalDB == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "DB not ready"})
			return
		}

		payload := gin.H{
			"leaderboard": computePeriodLeaderboard(kind),
			"period":      kind,
			"scoring":     "ถูกสกอร์เป๊ะ +3 · ถูกผล +1 · ผิด 0 (นับเฉพาะทายในช่วงเวลา)",
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "marshal failed"})
			return
		}
		cacheSet(cacheKey, raw, TTLLeaderboard)

		c.Header("X-Cache", "MISS")
		c.Data(http.StatusOK, "application/json; charset=utf-8", raw)
	}
}
