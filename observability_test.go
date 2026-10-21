package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// ---------------------------------------------------------------------------
// แผนข้อ 9 — metrics + structured log: test พฤติกรรม logger กลาง (JSON envelope,
// level filter) + middleware (access log, recovery, healthz skip) + counters
// ---------------------------------------------------------------------------

// captureLogs — สลับ logWriter ชั่วคราวเก็บ output ระหว่าง fn
func captureLogs(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	old := logWriter
	logWriter = &buf
	defer func() { logWriter = old }()
	fn()
	return buf.String()
}

func parseLastJSONLine(t *testing.T, out string) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) == 0 || lines[0] == "" {
		t.Fatalf("ไม่มี output จาก logx")
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &m); err != nil {
		t.Fatalf("output ไม่ใช่ JSON ทั้งบรรทัด: %v (raw: %q)", err, lines[len(lines)-1])
	}
	return m
}

func TestLogx_JSONEnvelope(t *testing.T) {
	out := captureLogs(t, func() {
		logInfo("proxy ok", "path", "/api/matches", "status", 200)
	})
	m := parseLastJSONLine(t, out)
	if m["level"] != "info" {
		t.Errorf("level ผิด: %v", m["level"])
	}
	if m["msg"] != "proxy ok" {
		t.Errorf("msg ผิด: %v", m["msg"])
	}
	if m["path"] != "/api/matches" {
		t.Errorf("field path ผิด: %v", m["path"])
	}
	if m["status"] != float64(200) {
		t.Errorf("field status ผิด: %v", m["status"])
	}
	if ts, _ := m["ts"].(string); ts == "" {
		t.Error("ts ต้องไม่ว่าง")
	}
}

func TestLogx_LevelFilter(t *testing.T) {
	setLogLevel("error")
	defer setLogLevel("info")

	out := captureLogs(t, func() {
		logInfo("hidden-info")
		logWarn("hidden-warn")
		logError("shown-error")
	})
	if strings.Contains(out, "hidden-info") || strings.Contains(out, "hidden-warn") {
		t.Errorf("LOG_LEVEL=error ต้องปิด info/warn (got: %q)", out)
	}
	m := parseLastJSONLine(t, out)
	if m["level"] != "error" || m["msg"] != "shown-error" {
		t.Errorf("บรรทัดสุดท้ายต้องเป็น error/shown-error (got: %v)", m)
	}
}

func TestLogx_Infof_PutsFormattedMsgInEnvelope(t *testing.T) {
	out := captureLogs(t, func() {
		logInfof("fetch %s in %dms", "ok", 12)
	})
	m := parseLastJSONLine(t, out)
	if m["msg"] != "fetch ok in 12ms" {
		t.Errorf("msg ต้องเป็นข้อความ format เหมือน log.Printf เดิม (got: %v)", m["msg"])
	}
}

func TestLogx_OddKVFallsToExtra(t *testing.T) {
	out := captureLogs(t, func() {
		logInfo("odd", "k1", 1, "dangling")
	})
	m := parseLastJSONLine(t, out)
	if m["_extra"] != "dangling" {
		t.Errorf("kv คี่ท้ายสุดต้องเก็บเป็น _extra (got: %v)", m)
	}
}

func TestRecordRequest_RouteStatsAndClasses(t *testing.T) {
	route := "/__test/metrics/stats"
	recordRequest(route, 200, 10)
	recordRequest(route, 200, 30)
	recordRequest(route, 500, 50)

	snap := snapshotServerMetrics()
	routes, _ := snap["routes"].(gin.H)
	rs, ok := routes[route].(gin.H)
	if !ok {
		t.Fatalf("ไม่เจอ route %s ใน snapshot", route)
	}
	if rs["count"] != int64(3) {
		t.Errorf("count ผิด: %v", rs["count"])
	}
	if rs["avg_ms"] != int64(30) {
		t.Errorf("avg_ms ผิด: %v", rs["avg_ms"])
	}
	if rs["max_ms"] != int64(50) {
		t.Errorf("max_ms ผิด: %v", rs["max_ms"])
	}
	if rs["errors"] != int64(1) {
		t.Errorf("errors ผิด: %v", rs["errors"])
	}

	classes, _ := snap["by_status_class"].(gin.H)
	if classes["2xx"].(int64) < 2 || classes["5xx"].(int64) < 1 {
		t.Errorf("by_status_class ผิด: %v", classes)
	}
	if snap["requests_total"].(int64) < 3 {
		t.Errorf("requests_total ผิด: %v", snap["requests_total"])
	}
}

func TestSnapshotServerMetrics_CacheHitRate(t *testing.T) {
	if rdb != nil {
		t.Skip("ต้องใช้ in-memory mode (ไม่มี redis) — cache counter คนละแบบ")
	}
	key := "test:obs:hitrate"
	cacheSet(key, []byte("v"), time.Minute)
	cacheGet(key) // hit
	cacheGet(key + ":missing")

	snap := snapshotServerMetrics()
	cache, _ := snap["cache"].(gin.H)
	hits, misses := cache["hits"].(int64), cache["misses"].(int64)
	if hits < 1 {
		t.Errorf("cache hits ต้อง >= 1 (got %d)", hits)
	}
	if misses < 1 {
		t.Errorf("cache misses ต้อง >= 1 (got %d)", misses)
	}
	if rate, ok := cache["hit_rate"].(float64); !ok || rate <= 0 || rate > 1 {
		t.Errorf("hit_rate ผิด: %v", cache["hit_rate"])
	}
}

func TestRequestLogMiddleware_AccessLogLine(t *testing.T) {
	setLogLevel("debug") // request ปกติ log ที่ debug
	defer setLogLevel("info")

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(requestLogMiddleware())
	r.GET("/api/ping", func(c *gin.Context) { c.Status(http.StatusOK) })

	out := captureLogs(t, func() {
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/ping", nil))
	})

	m := parseLastJSONLine(t, out)
	if m["msg"] != "http request" {
		t.Errorf("msg ต้องเป็น 'http request' (got: %v)", m["msg"])
	}
	if m["status"] != float64(200) {
		t.Errorf("status ผิด: %v", m["status"])
	}
	if m["route"] != "/api/ping" {
		t.Errorf("route ผิด: %v", m["route"])
	}
}

func TestRequestLogMiddleware_SkipsHealthz(t *testing.T) {
	setLogLevel("debug")
	defer setLogLevel("info")

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(requestLogMiddleware())
	r.GET("/healthz", func(c *gin.Context) { c.Status(http.StatusOK) })
	r.GET("/api/ping", func(c *gin.Context) { c.Status(http.StatusOK) })

	out := captureLogs(t, func() {
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/healthz", nil))
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/ping", nil))
	})

	// /healthz ห้าม log (uptime monitor ยิงถี่) แต่ /api/ping ต้อง log
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "http request") && strings.Contains(line, "/healthz") {
			t.Errorf("/healthz ไม่ต้อง log access line: %s", line)
		}
	}
	if !strings.Contains(out, "/api/ping") {
		t.Errorf("/api/ping ต้องถูก log (got: %q)", out)
	}
}

func TestRecoveryMiddleware_PanicTo500AndMetrics(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(recoveryMiddleware(), requestLogMiddleware())
	r.GET("/__test/boom", func(c *gin.Context) { panic("boom-test") })

	beforePanic := panicsTotal.Load()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/__test/boom", nil))

	if w.Code != http.StatusInternalServerError {
		t.Errorf("panic ต้องคืน 500 (got %d)", w.Code)
	}
	if panicsTotal.Load() != beforePanic+1 {
		t.Errorf("panics_total ต้องเพิ่ม 1 (before=%d after=%d)", beforePanic, panicsTotal.Load())
	}
	// route ต้องเข้า metrics (recordRequest ใน recovery — requestLog post-code ไม่ถูกเรียก)
	routes := snapshotServerMetrics()["routes"].(gin.H)
	rs, ok := routes["/__test/boom"].(gin.H)
	if !ok || rs["errors"].(int64) < 1 {
		t.Errorf("panic request ต้องนับใน route errors: %v", rs)
	}
}
