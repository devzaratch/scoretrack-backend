package main

import (
	"context"
	"time"
)

// ============================================================
// Unified Cache Layer
// - ใช้ Redis ถ้ามี (รันหลาย instance / persistent)
// - fallback เป็น in-memory (single instance / dev)
// - Janitor ล้าง expired items กัน memory leak
// ============================================================

const (
	// TTL มาตรฐานต่อชนิดข้อมูล
	TTLMatchLive   = 30 * time.Second // แมตช์กำลังแข่ง (realtime)
	TTLMatchRecent = 90 * time.Second // แมตช์เพิ่งจบ (รอ confirm)
	TTLMatchDone   = 6 * time.Hour    // แมตช์จบแล้ว ( immutable)
	TTLMatchesList = 60 * time.Second // รายการแมตช์รายวัน
	TTLNews        = 10 * time.Minute // ข่าว RSS (เปลี่ยนช้า)
	TTLSearch      = 1 * time.Hour    // search suggest
	TTLLeaderboard = 2 * time.Minute  // leaderboard (คำนวณจาก DB)
	TTLTeamLeague  = 30 * time.Minute // team / league detail (static-ish)
	TTLPlayer      = 30 * time.Minute // player profile
)

// cacheBackendName คืนชื่อ backend ของ cache ปัจจุบัน (ใช้แสดงใน Admin Panel)
func cacheBackendName() string {
	if rdb != nil {
		return "redis"
	}
	return "in-memory"
}

// cacheGet คืนข้อมูลจาก cache ถ้ามี (Redis ก่อน ค่อย memory)
// คืน ([]byte, source, found) + นับ hit/miss ให้ metrics (แผนข้อ 9)
func cacheGet(key string) ([]byte, string, bool) {
	if rdb != nil {
		cached, err := rdb.Get(context.Background(), key).Result()
		if err == nil && cached != "" {
			cacheHitsTotal.Add(1)
			return []byte(cached), "Redis-Cache", true
		}
		cacheMissesTotal.Add(1)
		return nil, "", false
	}
	if data, found := memoryCache.Get(key); found {
		cacheHitsTotal.Add(1)
		return data, "In-Memory-Cache", true
	}
	cacheMissesTotal.Add(1)
	return nil, "", false
}

// cacheSet เก็บข้อมูลลง cache (Redis ถ้ามี ไม่งั้น memory)
func cacheSet(key string, data []byte, ttl time.Duration) {
	if rdb != nil {
		rdb.Set(context.Background(), key, string(data), ttl)
		return
	}
	memoryCache.Set(key, data, ttl)
}

// cacheDelete ลบ key เดียว (ใช้ตอน invalidate)
func cacheDelete(key string) {
	if rdb != nil {
		rdb.Del(context.Background(), key)
		return
	}
	memoryCache.Delete(key)
}

// cacheDeletePrefix ลบ key ที่ขึ้นต้นด้วย prefix ทั้งหมด
// Redis: SCAN + DEL / Memory: iterate map
func cacheDeletePrefix(prefix string) {
	if rdb != nil {
		ctx := context.Background()
		iter := rdb.Scan(ctx, 0, prefix+"*", 100).Iterator()
		var keys []string
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		if len(keys) > 0 {
			rdb.Del(ctx, keys...)
		}
		return
	}
	memoryCache.DeletePrefix(prefix)
}

// Delete ลบ key เดียว (memory cache method)
func (c *MemoryCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

// DeletePrefix ลบ key ที่ขึ้นต้นด้วย prefix (memory cache method)
func (c *MemoryCache) DeletePrefix(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.items {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			delete(c.items, k)
		}
	}
}

// startJanitor ล้าง expired items ทุก 5 นาที (memory cache เท่านั้น)
// Redis มี TTL ในตัว ไม่ต้อง janitor
func startJanitor() {
	if rdb != nil {
		return // Redis จัดการ TTL เอง
	}
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			memoryCache.Cleanup()
		}
	}()
}

// Cleanup ลบ items ที่หมดอายุแล้ว (memory cache method)
func (c *MemoryCache) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for k, item := range c.items {
		if now.After(item.ExpiresAt) {
			delete(c.items, k)
		}
	}
}
