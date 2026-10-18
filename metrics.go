package main

import (
	"context"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// ============================================================
// Analytics self-hosted — web vitals + pageview (ตัวเลขรวมล้วน)
//
// ไม่ใช้ GA / Vercel Analytics: ไม่มี cookie = ไม่ต้องขอ consent, ไม่พึ่ง
// โดเมนที่ ISP บางเจ้าบล็อก, ไม่มีบัญชี third-party — client ส่งผ่าน
// sendBeacon มาที่ /api/metrics (same-origin -> Next rewrite -> ที่นี่)
//
// เก็บลง Redis (rdb) ถ้ามี ถ้าไม่มี = in-memory (เหมือน cache backend):
//
//	metrics:pageviews:total  (string counter)
//	metrics:pageviews:paths  (hash: path -> count)
//	metrics:vitals:count     (hash: name -> count)
//	metrics:vitals:sum       (hash: name -> sum ของค่า)
//
// GET /api/admin/metrics — ดูยอดรวม (X-Admin-Token)
// ============================================================

// validVitalNames — whitelist ที่ client ส่งได้ (กัน key ชื่อมั่วในฐานข้อมูล)
var validVitalNames = map[string]bool{
	"LCP": true, "CLS": true, "FCP": true, "TTFB": true, "INP": true,
}

type metricEvent struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
	Path  string  `json:"path"` // ใช้เฉพาะ name=pageview
}

type vitalAgg struct {
	Count int64
	Sum   float64
}

// in-memory fallback (ใช้เมื่อ rdb == nil)
var (
	metricsMu        sync.Mutex
	memVitals        = map[string]*vitalAgg{}
	memPageviews     = map[string]int64{}
	memPageviewsTotl int64
)

// sanitizePath — ตัด query/fragment + จำกัดความยาว กัน key ระเบิดจาก
// path แปลกๆ (?utm=... หรือ path ยาวบ้าคลั่งจาก bot)
func sanitizePath(p string) string {
	p = strings.TrimSpace(p)
	if i := strings.IndexAny(p, "?#"); i >= 0 {
		p = p[:i]
	}
	if p == "" || len(p) > 100 {
		return "unknown"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

// handleMetrics — POST /api/metrics (beacon จาก client, ตอบ 204 ให้เร็ว)
func handleMetrics(c *gin.Context) {
	var ev metricEvent
	if err := c.ShouldBindJSON(&ev); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "payload ไม่ถูกต้อง"})
		return
	}
	name := strings.TrimSpace(ev.Name)
	if name != "pageview" && !validVitalNames[name] {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "ชื่อ metric ไม่รู้จัก"})
		return
	}
	value := ev.Value
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1e9 {
		value = 0 // กันค่า NaN/บ้า จาก client ปลอม
	}
	recordMetric(name, value, ev.Path)
	c.Status(http.StatusNoContent)
}

func recordMetric(name string, value float64, path string) {
	if rdb != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if name == "pageview" {
			rdb.HIncrBy(ctx, "metrics:pageviews:paths", sanitizePath(path), 1)
			rdb.Incr(ctx, "metrics:pageviews:total")
			return
		}
		rdb.HIncrBy(ctx, "metrics:vitals:count", name, 1)
		rdb.HIncrByFloat(ctx, "metrics:vitals:sum", name, value)
		return
	}

	metricsMu.Lock()
	defer metricsMu.Unlock()
	if name == "pageview" {
		memPageviews[sanitizePath(path)]++
		memPageviewsTotl++
		return
	}
	agg := memVitals[name]
	if agg == nil {
		agg = &vitalAgg{}
		memVitals[name] = agg
	}
	agg.Count++
	agg.Sum += value
}

// handleAdminMetrics — GET /api/admin/metrics: ยอดรวม pageviews + vitals (avg)
func handleAdminMetrics(c *gin.Context) {
	type vitalStat struct {
		Count int64   `json:"count"`
		Avg   float64 `json:"avg"`
	}
	pageviews := map[string]int64{}
	var pvTotal int64
	vitals := map[string]vitalStat{}

	if rdb != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if v, err := rdb.Get(ctx, "metrics:pageviews:total").Int64(); err == nil {
			pvTotal = v
		}
		if paths, err := rdb.HGetAll(ctx, "metrics:pageviews:paths").Result(); err == nil {
			for p, v := range paths {
				if n, e := strconv.ParseInt(v, 10, 64); e == nil {
					pageviews[p] = n
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
			vitals[name] = vitalStat{Count: n, Avg: math.Round(s/float64(n)*100) / 100}
		}
	} else {
		metricsMu.Lock()
		pvTotal = memPageviewsTotl
		for p, v := range memPageviews {
			pageviews[p] = v
		}
		for name, agg := range memVitals {
			if agg.Count == 0 {
				continue
			}
			vitals[name] = vitalStat{Count: agg.Count, Avg: math.Round(agg.Sum/float64(agg.Count)*100) / 100}
		}
		metricsMu.Unlock()
	}

	c.JSON(http.StatusOK, gin.H{
		"pageviews": gin.H{"total": pvTotal, "paths": pageviews},
		"vitals":    vitals, // avg หน่วย: CLS=จุด ที่เหลือ=มิลลิวินาที
	})
}
