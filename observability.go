package main

// observability — structured request log + server metrics (แผนข้อ 9)
//
// แทน gin.Default() เดิม (gin Logger = ข้อความ [GIN] free text + gin Recovery):
//   - requestLogMiddleware: ทุก request = 1 บรรทัด JSON (method/path/route/status/
//     ms/bytes/ip) — 5xx=error, 4xx=warn, ปกติ=debug (prod default info = เตือน/
//     ข้อผิดพลาดล้วน — log drain ไม่บวม; ตั้ง LOG_LEVEL=debug เพื่อ full access log)
//     /healthz กับ / ไม่ log (uptime monitor ยิงถี่ = noise) แต่ยังนับใน metrics
//   - recoveryMiddleware: panic -> error log มี stack + 500 (แทน gin.Recovery free text)
//
// metrics ที่เก็บ (atomic + map protected):
//   requests_total / by_status_class (2xx-5xx) / by_route (count, avg+max ms,
//   errors — route = gin template กัน cardinality จาก path แปลก ๆ ของ bot) /
//   rate_limited_total / panics_total / cache hit+miss
//
// ดูผ่าน GET /api/admin/metrics (X-Admin-Token) — section "server"
// ห้าม expose สาธารณะ: ตัวเลขอายุกระบวนการ + traffic pattern ไม่ใช่ของที่
// ควรโชว์ให้บุคคลทั่วไป

import (
	"fmt"
	"net/http"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
)

var serverStartedAt = time.Now()

type routeStat struct {
	Count   int64 `json:"count"`
	TotalMs int64 `json:"total_ms"`
	MaxMs   int64 `json:"max_ms"`
	Errors  int64 `json:"errors"` // 5xx
}

var (
	obsMu            sync.Mutex
	routeStatsByName = map[string]*routeStat{}
	reqsTotal        atomic.Int64
	rateLimitedTotal atomic.Int64
	panicsTotal      atomic.Int64
	cacheHitsTotal   atomic.Int64
	cacheMissesTotal atomic.Int64
)

// reqsByStatusClass[0]=2xx ... [3]=5xx
var reqsByStatusClass [4]atomic.Int64

func statusClassIndex(status int) int {
	i := status/100 - 2
	if i < 0 {
		i = 0
	}
	if i > 3 {
		i = 3
	}
	return i
}

// recordRequest — บันทึกเมตริก request เดียว (route = template เช่น /api/match/:id)
func recordRequest(route string, status int, ms int64) {
	reqsTotal.Add(1)
	reqsByStatusClass[statusClassIndex(status)].Add(1)

	obsMu.Lock()
	rs := routeStatsByName[route]
	if rs == nil {
		rs = &routeStat{}
		routeStatsByName[route] = rs
	}
	rs.Count++
	rs.TotalMs += ms
	if ms > rs.MaxMs {
		rs.MaxMs = ms
	}
	if status >= 500 {
		rs.Errors++
	}
	obsMu.Unlock()
}

// snapshotServerMetrics — ค่า metrics ปัจจุบันสำหรับ /api/admin/metrics
func snapshotServerMetrics() gin.H {
	obsMu.Lock()
	routes := make(gin.H, len(routeStatsByName)) // gin.H (map[string]any) — JSON เหมือนกันแต่ assert ง่าย
	for name, rs := range routeStatsByName {
		avg := int64(0)
		if rs.Count > 0 {
			avg = rs.TotalMs / rs.Count
		}
		routes[name] = gin.H{
			"count":  rs.Count,
			"avg_ms": avg,
			"max_ms": rs.MaxMs,
			"errors": rs.Errors,
		}
	}
	obsMu.Unlock()

	hits := cacheHitsTotal.Load()
	misses := cacheMissesTotal.Load()
	hitRate := 0.0
	if hits+misses > 0 {
		hitRate = float64(hits) / float64(hits+misses)
	}

	return gin.H{
		"uptime_sec":     int64(time.Since(serverStartedAt).Seconds()),
		"requests_total": reqsTotal.Load(),
		"by_status_class": gin.H{
			"2xx": reqsByStatusClass[0].Load(),
			"3xx": reqsByStatusClass[1].Load(),
			"4xx": reqsByStatusClass[2].Load(),
			"5xx": reqsByStatusClass[3].Load(),
		},
		"rate_limited_total": rateLimitedTotal.Load(),
		"panics_total":       panicsTotal.Load(),
		"cache": gin.H{
			"hits":     hits,
			"misses":   misses,
			"hit_rate": hitRate,
		},
		"ws_live_clients":       GlobalLiveHub.ClientCount(),
		"ws_dropped_broadcasts": GlobalLiveHub.DroppedBroadcasts(),
		"routes":                routes,
	}
}

// requestLogMiddleware — structured access log + metrics ทุก request
func requestLogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		lat := time.Since(start)
		ms := lat.Milliseconds()
		status := c.Writer.Status()

		// route template (เช่น /api/match/:id) — กัน cardinality ระเบิดจาก path แปลก ๆ
		route := c.FullPath()
		display := route
		if display == "" {
			display = sanitizePath(c.Request.URL.Path)
		}
		recordRequest(display, status, ms)

		// /healthz กับ / = health checks ยิงถี่ทุก 30-60 วิ -> ไม่ log (ยังนับ metrics)
		p := c.Request.URL.Path
		if p == "/healthz" || p == "/" {
			return
		}

		fields := []any{
			"method", c.Request.Method,
			"path", sanitizePath(p),
			"route", display,
			"status", status,
			"ms", ms,
			"bytes", c.Writer.Size(),
			"ip", c.ClientIP(),
		}
		switch {
		case status >= 500:
			logError("http request", fields...)
		case status >= 400:
			logWarn("http request", fields...)
		default:
			logDebug("http request", fields...)
		}
	}
}

// recoveryMiddleware — panic -> structured error log (มี stack) + 500
// (แทน gin.Recovery ที่ log free text "panic recovered" แล้วทิ้ง context)
func recoveryMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				panicsTotal.Add(1)
				logError("panic recovered",
					"path", sanitizePath(c.Request.URL.Path),
					"method", c.Request.Method,
					"route", c.FullPath(),
					"panic", fmt.Sprint(r),
					"stack", string(debug.Stack()),
				)
				// panic ทำให้ post-code ของ requestLogMiddleware ไม่ถูกเรียก
				// (panic วิ่งผ่านมันก่อน) -> นับ 5xx ที่นี่แทน (ไม่ double count)
				route := c.FullPath()
				if route == "" {
					route = sanitizePath(c.Request.URL.Path)
				}
				recordRequest(route, http.StatusInternalServerError, 0)
				c.AbortWithStatus(http.StatusInternalServerError)
			}
		}()
		c.Next()
	}
}
