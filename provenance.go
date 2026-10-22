package main

// provenance — บอกว่าข้อมูลที่เสิร์ฟ "มาจากไหน" และ "เก่าแค่ไหน" (แผนข้อ 3)
//
// ทำไม: เคยเจอผู้ใช้ถามว่าสกอร์อัปเดตตอนไหน และข้อมูลมาจาก provider ไหน
// ตอนนี้ server แค่ตอบ JSON เปล่า ๆ = ผู้ใช้/ผู้ดูแลระบบเดาไม่ได้ว่าข้อมูลสดไหม
// และเราเองก็ debug ยาก (เจอข้อมูลสำรองที่ใช้แทนของจริงโดยไม่รู้ตัว)
//
// ส่งเป็น HTTP header (ไม่แตะ shape ของ JSON — ไม่พัง client เดิม/contract เดิม):
//   X-Data-Source      = fotmob | goalapi | apifootball | snapshot | cache-recheck ...
//   X-Data-Fetched-At  = unix วินาทีที่ดึงข้อมูลมา
//   X-Data-Age-Seconds = อายุข้อมูลตอนตอบ
//   X-Data-Stale        = true/false (เกินเกณฑ์ freshness)
//
// ฝั่ง web อ่าน header เหล่านี้ผ่าน SWR cache แล้วแสดงเป็นป้ายกำกับข้อมูล

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// provItem — ข้อมูลที่มาของ cache key 1 ตัว
type provItem struct {
	Source    string
	FetchedAt time.Time
}

var provenance = struct {
	mu    sync.Mutex
	items map[string]provItem
}{items: map[string]provItem{}}

// noteProvenance — จำว่า cache key นี้มาจากแหล่งไหน เมื่อไหร่
func noteProvenance(key string, source string) {
	if key == "" {
		return
	}
	source = strings.TrimSpace(source)
	if source == "" {
		source = "unknown"
	}
	provenance.mu.Lock()
	defer provenance.mu.Unlock()
	provenance.items[key] = provItem{Source: source, FetchedAt: time.Now()}

	// กัน map บวม: ลบของที่เก่ากว่า 2 ชั่วโมง (cache TTL สูงสุด 2 ชม. = league table)
	if len(provenance.items) > 5000 {
		cutoff := time.Now().Add(-2 * time.Hour)
		for k, v := range provenance.items {
			if v.FetchedAt.Before(cutoff) {
				delete(provenance.items, k)
			}
		}
	}
}

// provenanceOf — ดึงข้อมูลที่มาของ cache key (ถ้าไม่รู้ = ไม่มีข้อมูล)
func provenanceOf(key string) (provItem, bool) {
	provenance.mu.Lock()
	defer provenance.mu.Unlock()
	item, ok := provenance.items[key]
	return item, ok
}

// staleAfterSeconds — เกินกี่วินาทีถือว่า "เก่า" (ใช้ค่าเดียวกับเกณฑ์ SLO freshness)
func staleAfterSeconds() int64 {
	return int64(sloIntEnv("SLO_FRESHNESS_WARN_SEC", 900))
}

// setProvenanceHeaders — ตั้ง header บอกที่มาของข้อมูล
// source = แหล่งจริง, fetchedAt = เวลาที่ดึง (ถ้าไม่รู้ = ตอนนี้)
func setProvenanceHeaders(c *gin.Context, source string, fetchedAt time.Time) {
	if source == "" {
		source = "unknown"
	}
	if fetchedAt.IsZero() {
		fetchedAt = time.Now()
	}
	age := int64(time.Since(fetchedAt).Seconds())
	if age < 0 {
		age = 0
	}
	c.Header("X-Data-Source", source)
	c.Header("X-Data-Fetched-At", strconv.FormatInt(fetchedAt.Unix(), 10))
	c.Header("X-Data-Age-Seconds", strconv.FormatInt(age, 10))
	if age >= staleAfterSeconds() {
		c.Header("X-Data-Stale", "true")
	} else {
		c.Header("X-Data-Stale", "false")
	}
}

// setProvenanceHeadersForKey — ตั้ง header จาก provenance ที่บันทึกไว้
// (ใช้ตอนเสิร์ฟจาก cache: ข้อมูลยังเป็นของเดิมที่ดึงมาเมื่อกี้นาทีที่แล้ว)
func setProvenanceHeadersForKey(c *gin.Context, key string, fallbackSource string) {
	if item, ok := provenanceOf(key); ok {
		setProvenanceHeaders(c, item.Source, item.FetchedAt)
		return
	}
	setProvenanceHeaders(c, fallbackSource, time.Time{})
}
