package main

// tailOf คืนอักขระ n ตัวสุดท้ายของ s
// ใช้สำหรับ log ค่าที่เป็นความลับ เช่น FCM token โดยไม่เผยค่าเต็ม
func tailOf(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
