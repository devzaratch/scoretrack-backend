package main

import (
	"encoding/json"
	"time"
)

// ------------------------------------------------------------
// Backup/Restore store.json ขึ้น Redis
//
// ปัญหา: Render free tier มีดิสก์ ephemeral — ข้อมูลผู้ใช้ (บัญชี, ดาว,
// คำทำนาย, แต้ม) ใน data/store.json หายได้ทุกครั้งที่ restart/redeploy
// ทางแก้: sync snapshot ทั้งไฟล์ขึ้น Redis (Upstash) ทุกครั้งที่ flush
// สำเร็จ (throttle 60 วิ/ครั้ง กันชนโควตาฟรี 10k commands/วัน) แล้วกู้
// กลับอัตโนมัติตอน boot เมื่อพบว่า local ว่าง (ดิสก์ถูกล้าง)
// ------------------------------------------------------------

const dbSnapshotKey = "scoretrack:db:snapshot"
const snapshotEvery = 60 * time.Second

// userRecordCount นับจำนวน "ข้อมูลผู้ใช้ที่มีค่า" (ไม่นับแชท) ใช้ตัดสินใจ restore
func userRecordCount(d *StoreData) int {
	if d == nil {
		return 0
	}
	return len(d.UserSessions) + len(d.UserFavorites) + len(d.Predictions) +
		len(d.Achievements) + len(d.FCMTokens) + len(d.MatchSubscriptions)
}

// shouldRestoreFromSnapshot คืน true เมื่อ local ไม่มีข้อมูลผู้ใช้เลย
// (ดิสก์ถูกล้างตอน restart) แต่ snapshot ใน Redis ยังมีอยู่
func shouldRestoreFromSnapshot(local, snap *StoreData) bool {
	return userRecordCount(local) == 0 && userRecordCount(snap) > 0
}

// restoreFromSnapshot ดึง snapshot จาก Redis ถ้า local ว่าง — เรียกจาก
// InitDB หลังโหลดไฟล์ ก่อน start syncWorker คืน true ถ้ากู้สำเร็จ
func (db *PersistentDB) restoreFromSnapshot() bool {
	if rdb == nil {
		return false
	}
	snapBytes, err := rdb.Get(ctx, dbSnapshotKey).Bytes()
	if err != nil || len(snapBytes) == 0 {
		return false
	}
	var snap StoreData
	if err := json.Unmarshal(snapBytes, &snap); err != nil {
		logInfof("⚠️ Redis snapshot parse ไม่ได้ (ข้าม restore): %v", err)
		return false
	}
	if !shouldRestoreFromSnapshot(&db.data, &snap) {
		return false
	}
	// local ว่างอยู่แล้ว -> ใช้ของ snapshot ทั้งชุด (เฉพาะ field ที่ไม่ nil)
	if snap.ChatMessages != nil {
		db.data.ChatMessages = snap.ChatMessages
	}
	if snap.UserFavorites != nil {
		db.data.UserFavorites = snap.UserFavorites
	}
	if snap.UserSessions != nil {
		db.data.UserSessions = snap.UserSessions
	}
	if snap.MatchSubscriptions != nil {
		db.data.MatchSubscriptions = snap.MatchSubscriptions
	}
	if snap.FCMTokens != nil {
		db.data.FCMTokens = snap.FCMTokens
	}
	if snap.Predictions != nil {
		db.data.Predictions = snap.Predictions
	}
	if snap.UserNotificationPrefs != nil {
		db.data.UserNotificationPrefs = snap.UserNotificationPrefs
	}
	if snap.Achievements != nil {
		db.data.Achievements = snap.Achievements
	}
	db.dirty = true // syncWorker เขียนไฟล์ local ให้ mirror ภายใน 3 วิ
	logInfof("♻️ กู้ข้อมูลผู้ใช้จาก Redis snapshot สำเร็จ (%d sessions, %d favorites, %d predictions, %d fcm)",
		len(snap.UserSessions), len(snap.UserFavorites), len(snap.Predictions), len(snap.FCMTokens))
	return true
}

// maybePushSnapshot สำรอง snapshot ขึ้น Redis — เรียกจาก syncWorker
// หลังเขียนไฟล์สำเร็จ (ต้องถือ db.mu ตอนแก้ lastSnapshot;
// ยิง network ใน goroutine แยก กันบล็อก mutex ตอน Upstash ช้า)
func (db *PersistentDB) maybePushSnapshot(jsonData []byte) {
	if rdb == nil {
		return
	}
	if time.Since(db.lastSnapshot) < snapshotEvery {
		return
	}
	db.lastSnapshot = time.Now()
	payload := string(jsonData)
	go func() {
		if err := rdb.Set(ctx, dbSnapshotKey, payload, 0).Err(); err != nil {
			logInfof("⚠️ สำรอง snapshot ขึ้น Redis ไม่สำเร็จ: %v", err)
		}
	}()
}
