package main

// cachewarm — เติม cache อัตโนมัติเป็นระยะ (แผนข้อ 2: provider health + warm cache)
//
// ทำไมต้องมี:
//  1) Render free tier ปิด instance เมื่อไม่มี request -> คนแรกของช่วงเวลาต้องรอ
//     cold start 30-50 วิ (เจอจริง) -> ยิงเป็นระยะทำให้ instance ไม่หลับ
//  2) คนแรกที่เข้ามาได้ cache HIT ทันที -> TTFB สั้น ไม่ต้องรอ provider
//  3) provider quota ไม่ถูกใช้พลาดตอนที่คนเยอะแน่น (stampede)
//
// ป้องกันไม่ให้กินโควตาเกินจำเป็น:
//   - ข้ามถ้า cache ยังไม่เก่า (ไม่ต้องดึงใหม่)
//   - provider budget ไม่อนุญาต (Allow() = false) -> ข้าม
//   - ตารางคะแนนลีกที่ warm ได้จาก env (WARMUP_LEAGUES) ไม่ใช่ทุกลีก
//
// env: WARMUP_ENABLED=1, WARMUP_INTERVAL_SEC=900, WARMUP_MATCHES=1,
//      WARMUP_LEAGUES="47,87,169", WARMUP_DAYS_AHEAD=1

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// warmHTTPClient - client สำหรับยิง /healthz ของตัวเอง
var warmHTTPClient = &http.Client{Timeout: 10 * time.Second}

// ตัวนับสำหรับ metrics
var (
	warmupRuns    atomic.Int64
	warmupOK      atomic.Int64
	warmupFailed  atomic.Int64
	warmupLastAt  atomic.Int64 // unix วินาที
	warmupLastOK  atomic.Bool
	warmupLastErr atomic.Value // string
)

// warmupLastError — ข้อความ error ล่าสุด (อ่านอย่างเดียว)
func warmupLastError() string {
	if v := warmupLastErr.Load(); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// warmupEnabled — เปิด/ปิดได้ด้วย env (ปิดไว้ตอน debug ที่ไม่อยากยิง provider)
func warmupEnabled() bool {
	return envBool("WARMUP_ENABLED", true)
}

// warmupLeagues — รายการ league id ที่จะ warm ตารางคะแนน
func warmupLeagues() []string {
	raw := strings.TrimSpace(os.Getenv("WARMUP_LEAGUES"))
	if raw == "" {
		raw = "47,87,169" // Premier League / Champions League / La Liga
	}
	out := []string{}
	for _, part := range strings.Split(raw, ",") {
		id := strings.TrimSpace(part)
		if id != "" {
			out = append(out, id)
		}
	}
	return out
}

// bangkokDate — วันที่ตามเวลาไทย (เกมในไทยใช้วันที่ของไทย)
func bangkokDate(offsetDays int) string {
	now := time.Now().In(time.FixedZone("ICT", 7*3600))
	return now.AddDate(0, 0, offsetDays).Format("20060102")
}

// warmMatches — ดึงแมตช์ของวันที่ + ใส่ cache (ข้ามถ้า cache ยังสด หรือ provider ไม่อนุญาต)
func warmMatches(date string) (string, bool) {
	key := fmt.Sprintf("matches:%s", date)
	if _, _, found := cacheGet(key); found {
		return "cache-fresh", true
	}
	data, source, ok := fetchMatchesFromProviders(date)
	if !ok || len(data) == 0 {
		return "no-provider-data", false
	}
	cacheSet(key, data, 60*time.Second)
	noteProvenance(key, source)
	return source, true
}

// warmLeagueTable — ดึงตารางคะแนนลีก + ใส่ cache
func warmLeagueTable(leagueID string) (string, bool) {
	key := fmt.Sprintf("league:table:%s", leagueID)
	if _, _, found := cacheGet(key); found {
		return "cache-fresh", true
	}
	data, source, ok := fetchLeagueFromProviders("table", leagueID)
	if !ok || len(data) == 0 {
		return "no-provider-data", false
	}
	cacheSet(key, data, 10*time.Minute)
	noteProvenance(key, source)
	return source, true
}

// warmOnce — ทำ 1 รอบของการเติม cache
func warmOnce() {
	warmupRuns.Add(1)
	warmupLastAt.Store(time.Now().Unix())
	var problems []string

	// 1) ping ตัวเอง = กัน Render ปิด instance (ถ้ามี request เข้ามาระหว่างนี้
	//    instance ก็ยังไม่หลับ และ healthz คืน 503 ถ้า DB/Redis พัง = เห็นปัญหาทันที)
	if _, err := doWarmPing(); err != nil {
		problems = append(problems, "healthz: "+err.Error())
	}

	// 2) แมตช์วันนี้ (+ พรุ่งนี้ถ้าเปิด)
	if envBool("WARMUP_MATCHES", true) {
		days := envInt("WARMUP_DAYS_AHEAD", 1)
		for d := 0; d <= days; d++ {
			date := bangkokDate(d)
			source, ok := warmMatches(date)
			if !ok {
				problems = append(problems, "matches "+date+": "+source)
			} else if source != "cache-fresh" {
				logInfof("🔥 warm cache: แมตช์ %s <- %s", date, source)
			}
		}
	}

	// 3) ตารางคะแนนลีกที่ตั้งไว้
	for _, leagueID := range warmupLeagues() {
		source, ok := warmLeagueTable(leagueID)
		if !ok {
			problems = append(problems, "league table "+leagueID+": "+source)
		} else if source != "cache-fresh" {
			logInfof("🔥 warm cache: ตารางลีก %s <- %s", leagueID, source)
		}
	}

	if len(problems) > 0 {
		warmupFailed.Add(1)
		warmupLastOK.Store(false)
		warmupLastErr.Store(strings.Join(problems, "; "))
		logWarnf("⚠️ warm cache มีบางส่วนไม่สำเร็จ: %s", strings.Join(problems, "; "))
		return
	}
	warmupOK.Add(1)
	warmupLastOK.Store(true)
	warmupLastErr.Store("")
	logInfof("🔥 warm cache: รอบสำเร็จ")
}

// doWarmPing — ยิง /healthz ของตัวเอง (keep-alive)
func doWarmPing() (string, error) {
	port := getEnv("PORT", "8080")
	url := "http://127.0.0.1:" + port + "/healthz"
	client := warmHTTPClient
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("healthz status %d", resp.StatusCode)
	}
	return "ok", nil
}

// startCacheWarmer — เริ่มลูปเติม cache
func startCacheWarmer() {
	if !warmupEnabled() {
		logInfof("🔥 cache warmer: ปิดอยู่ (WARMUP_ENABLED=0)")
		return
	}
	interval := time.Duration(envInt("WARMUP_INTERVAL_SEC", 900)) * time.Second
	if interval < 60*time.Second {
		interval = 60 * time.Second
	}
	// รอบแรกหลังขึ้น 45 วิ (ให้เซิร์ฟเวอร์ + provider พร้อมก่อน)
	time.Sleep(45 * time.Second)
	warmOnce()
	logInfof("🔥 cache warmer เริ่มทำงาน (ทุก %v)", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		warmOnce()
	}
}

// snapshotWarmup — สถานะของ cache warmer (ไว้โชว์ในหน้า admin)
func snapshotWarmup() map[string]interface{} {
	lastAt := ""
	if ts := warmupLastAt.Load(); ts > 0 {
		lastAt = time.Unix(ts, 0).UTC().Format(time.RFC3339)
	}
	return map[string]interface{}{
		"enabled":      warmupEnabled(),
		"runs":         warmupRuns.Load(),
		"ok":           warmupOK.Load(),
		"failed":       warmupFailed.Load(),
		"last_run_at":  lastAt,
		"last_ok":      warmupLastOK.Load(),
		"last_error":   warmupLastError(),
		"interval_sec": envInt("WARMUP_INTERVAL_SEC", 900),
		"leagues":      warmupLeagues(),
	}
}
