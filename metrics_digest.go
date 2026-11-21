package main

// metrics_digest.go — แผน-v2 ข้อ 4: digest ของ metrics แบบรายวัน
//
// metrics.go เก็บตัวเลขรวมสะสม (pageviews/actions/vitals) — ไม่มีมุมมอง
// "วันนี้เทียบเมื่อวาน" ทำให้ admin เห็นแต่ยอดรวมที่คลึงขึ้นเรื่อย ๆ
// digest นี้ snapshot ค่าสะสมทุกวัน (ครั้งแรกของวัน) แล้วเก็บแยกต่อวันที่:
//
//	Redis: metrics:digest:<YYYY-MM-DD> (JSON, TTL 40 วัน)
//	ไม่มี Redis: หน่วยความจำ (memDigests — หายตอน restart เหมือน metrics หลัก)
//
// GET /api/admin/metrics/digest — คืนวันนี้ + ประวัติย้อนหลัง

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// vitalStat ย้ายมาจาก handleAdminMetrics เดิม (ใช้ร่วมกัน)
type vitalStat struct {
	Count int64   `json:"count"`
	Avg   float64 `json:"avg"`
}

// metricsAgg — ค่าสะสมทั้งหมดที่อ่านจาก Redis/หน่วยความจำ
type metricsAgg struct {
	PVTotal int64
	Paths   map[string]int64
	Actions map[string]int64
	Vitals  map[string]vitalStat
}

// readMetricsAgg — แยกจาก handleAdminMetrics เดิม เพื่อให้ digest ใช้ตัวอ่านเดียวกัน
func readMetricsAgg() metricsAgg {
	agg := metricsAgg{
		Paths:   map[string]int64{},
		Actions: map[string]int64{},
		Vitals:  map[string]vitalStat{},
	}
	if rdb != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if v, err := rdb.Get(ctx, "metrics:pageviews:total").Int64(); err == nil {
			agg.PVTotal = v
		}
		if acts, err := rdb.HGetAll(ctx, "metrics:actions:count").Result(); err == nil {
			for a, v := range acts {
				if n, e := strconv.ParseInt(v, 10, 64); e == nil {
					agg.Actions[a] = n
				}
			}
		}
		if paths, err := rdb.HGetAll(ctx, "metrics:pageviews:paths").Result(); err == nil {
			for p, v := range paths {
				if n, e := strconv.ParseInt(v, 10, 64); e == nil {
					agg.Paths[p] = n
				}
			}
		}
		counts, _ := rdb.HGetAll(ctx, "metrics:vitals:count").Result()
		sums, _ := rdb.HGetAll(ctx, "metrics:vitals:sum").Result()
		for name, cv := range counts {
			n, e := strconv.ParseInt(cv, 10, 64)
			if e != nil || n == 0 {
				continue
			}
			s, _ := strconv.ParseFloat(sums[name], 64)
			agg.Vitals[name] = vitalStat{Count: n, Avg: math.Round(s/float64(n)*100) / 100}
		}
		return agg
	}

	metricsMu.Lock()
	defer metricsMu.Unlock()
	agg.PVTotal = memPageviewsTotl
	for p, v := range memPageviews {
		agg.Paths[p] = v
	}
	for name, c := range memActions {
		if c != nil && *c > 0 {
			agg.Actions[name] = *c
		}
	}
	for name, a := range memVitals {
		if a.Count == 0 {
			continue
		}
		agg.Vitals[name] = vitalStat{Count: a.Count, Avg: math.Round(a.Sum/float64(a.Count)*100) / 100}
	}
	return agg
}

// DailyDigest — snapshot หนึ่งวัน (หน่วยตัวเลขเดียวกับ handleAdminMetrics)
type DailyDigest struct {
	Date      string               `json:"date"`      // YYYY-MM-DD (UTC)
	Pageviews int64                `json:"pageviews"` // ยอดสะสม ณ เวลา snapshot
	TopPaths  []pathCount          `json:"top_paths"` // 5 อันดับ
	Actions   map[string]int64     `json:"actions"`   // การใช้ฟีเจอร์ (ต่อชื่อ)
	Vitals    map[string]vitalStat `json:"vitals"`    // avg หน่วย: CLS=จุด ที่เหลือ=มิลลิวินาที
	TakenAt   string               `json:"taken_at"`  // RFC3339
}

type pathCount struct {
	Path  string `json:"path"`
	Count int64  `json:"count"`
}

// buildDailyDigest — pure: แปลง aggregate เป็น digest ของวัน (ทดสอบได้ไม่ต้องแตะ Redis)
func buildDailyDigest(date string, now time.Time, agg metricsAgg) DailyDigest {
	paths := make([]pathCount, 0, len(agg.Paths))
	for p, v := range agg.Paths {
		paths = append(paths, pathCount{Path: p, Count: v})
	}
	sort.Slice(paths, func(i, j int) bool {
		if paths[i].Count != paths[j].Count {
			return paths[i].Count > paths[j].Count
		}
		return paths[i].Path < paths[j].Path
	})
	if len(paths) > 5 {
		paths = paths[:5]
	}
	return DailyDigest{
		Date:      date,
		Pageviews: agg.PVTotal,
		TopPaths:  paths,
		Actions:   agg.Actions,
		Vitals:    agg.Vitals,
		TakenAt:   now.UTC().Format(time.RFC3339),
	}
}

// memDigests — สำรองเมื่อไม่มี Redis (key = YYYY-MM-DD)
var memDigests = map[string]DailyDigest{}

// snapshotDailyDigest — บันทึก digest ของวันนี้ (เรียกซ้ำในวันเดียวกัน = แทนที่)
// คืน digest ที่บันทึก + error ฝั่ง Redis (ถ้ามี)
func snapshotDailyDigest(now time.Time) (DailyDigest, error) {
	date := now.UTC().Format("2006-01-02")
	d := buildDailyDigest(date, now, readMetricsAgg())
	if rdb != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		raw, err := json.Marshal(d)
		if err != nil {
			return d, err
		}
		if err := rdb.Set(ctx, "metrics:digest:"+date, raw, 40*24*time.Hour).Err(); err != nil {
			return d, err
		}
		return d, nil
	}
	memDigests[date] = d
	return d, nil
}

// loadDigests — อ่าน digest ย้อนหลัง (เรียงวันที่ล่าสุดก่อน) เท่าที่มี
func loadDigests(limit int) ([]DailyDigest, error) {
	if limit <= 0 {
		limit = 30
	}
	if rdb != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		out := []DailyDigest{}
		for i := 0; i < limit; i++ {
			date := time.Now().UTC().AddDate(0, 0, -i).Format("2006-01-02")
			raw, err := rdb.Get(ctx, "metrics:digest:"+date).Result()
			if err != nil {
				continue
			}
			var d DailyDigest
			if json.Unmarshal([]byte(raw), &d) == nil {
				out = append(out, d)
			}
		}
		return out, nil
	}
	dates := make([]string, 0, len(memDigests))
	for k := range memDigests {
		dates = append(dates, k)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(dates)))
	out := []DailyDigest{}
	for i, k := range dates {
		if i >= limit {
			break
		}
		out = append(out, memDigests[k])
	}
	return out, nil
}

// startMetricsDigest — goroutine จาก main: snapshot ทันทีตอนเริ่ม (ถ้าวันนี้ยังไม่มี)
// แล้วตรวจทุกชั่วโมง วันเปลี่ยนจะ snapshot วันใหม่ พร้อม log บรรทัดเดียวให้ admin อ่าน
func startMetricsDigest(ctx context.Context) {
	snap := func() {
		d, err := snapshotDailyDigest(time.Now())
		if err != nil {
			logWarnf("metrics digest snapshot ผิดพลาด: %v", err)
			return
		}
		logInfof("📊 metrics digest %s: %d pageviews, %d top-path=%v", d.Date, d.Pageviews, len(d.Actions), d.TopPaths)
	}
	snap()
	go func() {
		t := time.NewTicker(time.Hour)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				snap()
			}
		}
	}()
}

// handleAdminMetricsDigest — GET /api/admin/metrics/digest
func handleAdminMetricsDigest(c *gin.Context) {
	history, err := loadDigests(30)
	if err != nil {
		c.JSON(500, gin.H{"error": fmt.Sprintf("อ่าน digest ไม่สำเร็จ: %v", err)})
		return
	}
	var today *DailyDigest
	if len(history) > 0 && history[0].Date == time.Now().UTC().Format("2006-01-02") {
		today = &history[0]
	}
	c.JSON(200, gin.H{"today": today, "history": history})
}
