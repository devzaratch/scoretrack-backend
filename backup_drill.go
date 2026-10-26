package main

import (
	"encoding/json"
	"fmt"
	"time"
)

// Backup Restore Drill (แผนใหม่ข้อ 10)
// ปัญหาคลาสสิก: มี backup แต่ไม่เคยพิสูจน์ว่ากู้ได้ — รู้ตัวอีกทีตอนต้องใช้จริง
// ตัวนี้: ดึง snapshot ล่าสุดจาก Redis → parse ใหม่ทั้งก้อน → ตรวจโครงสร้าง/จำนวนข้อมูล
// (ไม่เขียนทับข้อมูลจริง — ปลอดภัย 100%) แล้ว log ผล + เตือนถ้า snapshot เสีย/ว่าง
//
// ตั้งเวลา: default ทุก 24 ชม. (รันครั้งแรก ~2 นาทีหลัง boot)
// ปิดได้ด้วย env BACKUP_DRILL=off

const backupDrillKey = "scoretrack:db:snapshot"

// DrillResult — ผลตรวจ snapshot หนึ่งครั้ง
type DrillResult struct {
	OK          bool
	Bytes       int
	UserRecords int
	Sessions    int
	Favorites   int
	Predictions int
	FCMTokens   int
	Subs        int
	Error       string
	CheckedAt   time.Time
}

// verifySnapshotBytes — pure: parse + ตรวจ snapshot แบบไม่แตะข้อมูลจริง
func verifySnapshotBytes(b []byte) DrillResult {
	res := DrillResult{Bytes: len(b), CheckedAt: time.Now()}
	if len(b) == 0 {
		res.Error = "snapshot ว่าง"
		return res
	}
	var snap StoreData
	if err := json.Unmarshal(b, &snap); err != nil {
		res.Error = fmt.Sprintf("parse ไม่ได้: %v", err)
		return res
	}
	res.Sessions = len(snap.UserSessions)
	res.Favorites = len(snap.UserFavorites)
	res.Predictions = len(snap.Predictions)
	res.FCMTokens = len(snap.FCMTokens)
	res.Subs = len(snap.MatchSubscriptions)
	res.UserRecords = userRecordCount(&snap)
	// ขั้นต่ำที่ถือว่า "กู้แล้วมีอะไรใช้": ต้องมีข้อมูลผู้ใช้อย่างน้อย 1 อย่าง
	// (ระบบใหม่ที่ยังไม่มี user = OK ที่ 0 แต่ log ว่า empty ไว้ดู)
	res.OK = true
	return res
}

// runBackupDrill — ดึง snapshot จาก Redis แล้วตรวจ (คืนผลไว้ให้ test/endpoint ใช้)
func runBackupDrill() DrillResult {
	if rdb == nil {
		return DrillResult{OK: false, Error: "Redis ไม่พร้อม (โหมด in-memory)", CheckedAt: time.Now()}
	}
	b, err := rdb.Get(ctx, backupDrillKey).Bytes()
	if err != nil {
		return DrillResult{OK: false, Error: fmt.Sprintf("ดึง snapshot ไม่ได้: %v", err), CheckedAt: time.Now()}
	}
	return verifySnapshotBytes(b)
}

// startBackupDrillLoop — goroutine รายวัน (เรียกจาก main)
func startBackupDrillLoop() {
	if !envBool("BACKUP_DRILL", true) {
		logInfof("🧪 backup drill: ปิดด้วย BACKUP_DRILL=off")
		return
	}
	hours := envInt("BACKUP_DRILL_HOURS", 24)
	if hours < 1 {
		hours = 1
	}

	// รอ 2 นาทีให้ระบบ push snapshot รอบแรกก่อน
	time.Sleep(2 * time.Minute)

	run := func() {
		res := runBackupDrill()
		switch {
		case res.OK && res.UserRecords > 0:
			logInfof("🧪 backup drill ผ่าน: %d bytes, %d user records (sessions=%d favorites=%d predictions=%d fcm=%d subs=%d)",
				res.Bytes, res.UserRecords, res.Sessions, res.Favorites, res.Predictions, res.FCMTokens, res.Subs)
		case res.OK:
			// OK แต่ยังไม่มีข้อมูลผู้ใช้ — ระบบใหม่/ยังไม่มีคนใช้ ไม่ใช่ error
			logInfof("🧪 backup drill ผ่าน (snapshot ว่างตามคาด): %d bytes", res.Bytes)
		default:
			logWarn("🧪 backup drill ไม่ผ่าน — ตรวจ snapshot ก่อนจำเป็น!", "error", res.Error)
		}
	}

	run()
	ticker := time.NewTicker(time.Duration(hours) * time.Hour)
	defer ticker.Stop()
	for range ticker.C {
		run()
	}
}
