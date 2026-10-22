package main

// account — สิทธิของผู้ใช้ต่อข้อมูลส่วนบุคคล (แผนข้อ 10: PDPA)
//
// พ.ร.บ.คุ้มคลองข้อมูลส่วนบุคคล (PDPA) ให้สิทธิ 3 ข้อที่ต้องทำให้ได้จริง:
//   1) เข้าถึงข้อมูลของตัวเอง (portability) -> GET /api/user/data
//   2) ลบข้อมูล (erasure)                -> DELETE /api/user/account
//   3) ระบุว่าเก็บข้อมูลอะไร/นานเท่าไร    -> หน้า /privacy (ฝั่ง web)
//
// หลักการ:
//   - ลบเฉพาะ "ของผู้ใช้คนนี้" (user key เดียว) ไม่ไปแตะข้อมูลคนอื่น
//   - ผู้ที่ยังไม่ล็อกอิน (device key) ก็ลบของตัวเองได้ (ไม่ผูกกับ Google)
//   - session ของตัวเองถูกลบด้วย (logout จริง ไม่ใช่แค่ล้างบนเครื่อง)
//   - FCM token เป็นข้อมูลระดับอุปกรณ์ ไม่ผูกกับ user key -> ไม่ลบในนี้
//     (แต่รายงานจำนวนไว้ให้ผู้ใช้เห็นว่ามีอยู่)

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// userDataSnapshot — ข้อมูลของผู้ใช้ 1 คน (สำหรับ export)
func userDataSnapshot(userKey string) gin.H {
	out := gin.H{
		"user_key":    userKey,
		"exported_at": time.Now().UTC().Format(time.RFC3339),
	}
	if GlobalDB == nil {
		return out
	}
	out["favorites"] = GlobalDB.GetUserFavorites(userKey)
	out["predictions"] = GlobalDB.GetPredictionsByUser(userKey)
	out["notification_prefs"] = GlobalDB.GetNotificationPrefs(userKey)
	out["achievements"] = GlobalDB.GetUserAchievements(userKey)
	out["fcm_token_count"] = GlobalDB.CountFCMTokens()
	return out
}

// handleUserDataExport — GET /api/user/data -> ดาวน์โหลดข้อมูลของฉัน (PDPA: เข้าถึงข้อมูล)
func handleUserDataExport(c *gin.Context) {
	userKey := resolveUserKey(c)
	c.JSON(http.StatusOK, userDataSnapshot(userKey))
}

// handleDeleteAccount — DELETE /api/user/account -> ลบข้อมูลของฉันทั้งหมด (PDPA: ลบข้อมูล)
func handleDeleteAccount(c *gin.Context) {
	userKey := resolveUserKey(c)

	// ลบ session ของผู้ใช้คนนี้ (ถ้า login อยู่) -> token เดิมใช้ต่อไม่ได้
	if GlobalDB != nil {
		if sub := GlobalDB.GetSessionSubByUser(userKey); sub != "" {
			GlobalDB.DeleteUserSessionsBySub(sub)
		}
	}

	removed := gin.H{}
	if GlobalDB != nil {
		removed = GlobalDB.DeleteUserData(userKey)
	}

	// ล้าง cookie ฝั่ง client ด้วย (ถ้าเป็น session cookie)
	clearSessionCookie(c)

	logInfof("🗑️ ลบข้อมูลผู้ใช้ [%s]: %v", userKey, removed)
	c.JSON(http.StatusOK, gin.H{
		"status":   "deleted",
		"user_key": userKey,
		"removed":  removed,
	})
}
