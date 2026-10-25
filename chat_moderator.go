package main

import (
	"regexp"
	"strings"
	"sync"
	"time"
)

// --------------------------------------------------
// Chat Moderation — กรองคำหยาบ + sanitize ข้อความแชท
// ใช้โดย ChatClient.readPump ก่อน broadcast/บันทึก DB
// --------------------------------------------------

const (
	maxChatTextLen = 300 // ตัดข้อความยาวเกิน (นับเป็น rune)
	maxChatUserLen = 20  // ตัดชื่อผู้ใช้ยาวเกิน
	maxChatTimeLen = 10
)

// คำหยาบภาษาไทย — จับแบบ substring (ภาษาไทยไม่มี word boundary)
// ระวังคำที่มีความหมายปกติด้วย เช่น ไม่ใส่ "กู" (กูรู/กูเกิล) หรือ "เวร" (เวรประจำวัน)
var thaiBadWords = []string{
	"เหี้ย", "หี", "ควย", "ไอ้ควย", "โคตรควย",
	"เย็ด", "เยส", "แตด",
	"สัส", "ไอสัส", "ไอ้สัส",
	"แม่ง", "มึง",
	"ระยำ", "ตอแหล", "ดอกทอง", "กระหรี่",
	"อีดอก", "อีเหี้ย", "ไอ้เหี้ย", "โคตรเหี้ย",
	"ชิบหาย", "ฉิบหาย",
	"อีเวร", "ไอ้เวร",
	"แรด", "กาก",
}

// คำหยาบภาษาอังกฤษ — จับแบบ word boundary กัน mask คำปกติ เช่น "classic" หรือ "assess"
var englishBadWords = []string{
	"fuck", "fucker", "fucking", "shit", "shitty", "bitch", "asshole",
	"bastard", "dick", "dickhead", "pussy", "cunt", "nigger", "faggot", "whore",
}

var englishBadRe *regexp.Regexp

func init() {
	escaped := make([]string, len(englishBadWords))
	for i, w := range englishBadWords {
		escaped[i] = regexp.QuoteMeta(w)
	}
	englishBadRe = regexp.MustCompile(`(?i)\b(` + strings.Join(escaped, "|") + `)\b`)
}

// maskWord คืน string ของ * ยาวเท่าจำนวน rune ของคำ (เก็บความยาวเดิม กันคนเดาได้)
func maskWord(s string) string {
	return strings.Repeat("*", len([]rune(s)))
}

// MaskBadWords แทนคำหยาบด้วย * คืน (ข้อความใหม่, จำนวนคำที่เจอ)
func MaskBadWords(text string) (string, int) {
	found := 0

	// 1. อังกฤษ (word boundary)
	out := englishBadRe.ReplaceAllStringFunc(text, func(m string) string {
		found++
		return maskWord(m)
	})

	// 2. ไทย (substring วนจนหมด — word ที่ถูก mask แล้วจะไม่ match ซ้ำ จึงจบแน่นอน)
	for _, w := range thaiBadWords {
		wLen := len([]rune(w))
		for {
			idx := strings.Index(out, w)
			if idx < 0 {
				break
			}
			runes := []rune(out[idx:])
			if len(runes) < wLen {
				break
			}
			out = out[:idx] + maskWord(w) + string(runes[wLen:])
			found++
		}
	}

	return out, found
}

// truncateChatRunes ตัดข้อความให้ไม่เกิน max rune (ปลอดภัยกับภาษาไทย
// — ต่างจาก truncateRunes ใน provider_goalapi.go ที่ตัดเป็น byte แล้วตัดกลางอักขระได้)
func truncateChatRunes(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max])
}

// CleanChatMessage sanitize ข้อความแชทจาก client:
//   - เก็บเฉพาะ user/text/time/type (กันยัด key ขยะลง DB)
//   - mask คำหยาบใน text และ user
//   - ตัดความยาว + ลบ control chars + collapse ช่องว่าง
//
// คืน (msg ใหม่, จำนวนคำที่ถูก mask, ok) — ok=false เมื่อ text ว่างหลัง trim
func CleanChatMessage(raw map[string]interface{}) (map[string]interface{}, int, bool) {
	text, _ := raw["text"].(string)
	user, _ := raw["user"].(string)
	tm, _ := raw["time"].(string)

	// ลบ control chars (ยกเว้น \n ถอดออกหมดเลย — แชทบรรทัดเดียว)
	text = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return -1
		}
		return r
	}, text)
	user = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return -1
		}
		return r
	}, user)

	// collapse ช่องว่างหลายตัว + trim
	text = strings.Join(strings.Fields(text), " ")
	user = strings.Join(strings.Fields(user), " ")

	if text == "" {
		return nil, 0, false
	}

	if user == "" {
		user = "แฟนบอล"
	}

	maskedText, n1 := MaskBadWords(text)
	maskedUser, n2 := MaskBadWords(user)

	return map[string]interface{}{
		"user": truncateChatRunes(maskedUser, maxChatUserLen),
		"text": truncateChatRunes(maskedText, maxChatTextLen),
		"time": truncateChatRunes(strings.TrimSpace(tm), maxChatTimeLen),
		"type": "text",
	}, n1 + n2, true
}

// chatRateLimited คืน true เมื่อ client ส่งข้อความถี่เกิน (12 ข้อความ / 30 วินาที)
// ใช้ GlobalRateLimiter ตัวเดียวกับ HTTP (key แยกเป็น chat|<ip>)
func chatRateLimited(ip string) bool {
	return !GlobalRateLimiter.AllowKey("chat|"+ip, 12, 30*time.Second)
}

// --------------------------------------------------
// Repeat Offender — สะสม strike เมื่อพูดคำหยาบ
// 3 strike ใน 10 นาที → เปิด mute ส่งข้อความไม่ได้ 5 นาที
// (AI แท้ต้อง dataset — กฎชัดเจนแบบนี้ตรวจสอบได้ โปร่งใส และไม่ทำผิดคน)
// --------------------------------------------------

const (
	strikeThreshold = 3
	strikeWindow    = 10 * time.Minute
	muteDuration    = 5 * time.Minute
)

type offenderRecord struct {
	strikes   int
	firstSeen time.Time
	until     time.Time // mute จนถึงเวลานี้ (zero = ไม่เคยถูก mute)
}

var chatOffenders sync.Map // ip -> *offenderRecord
var offendersMu sync.Mutex

// registerChatStrike เพิ่ม strike ของ ip คืน true เมื่อครบ threshold (เปิด mute แล้ว)
func registerChatStrike(ip string) bool {
	now := time.Now()
	offendersMu.Lock()
	defer offendersMu.Unlock()
	v, _ := chatOffenders.LoadOrStore(ip, &offenderRecord{})
	rec := v.(*offenderRecord)
	// นอกหน้าต่าง 10 นาที → เริ่มนับใหม่
	if rec.firstSeen.IsZero() || now.Sub(rec.firstSeen) > strikeWindow {
		rec.firstSeen = now
		rec.strikes = 0
	}
	rec.strikes++
	if rec.strikes >= strikeThreshold {
		rec.until = now.Add(muteDuration)
		rec.strikes = 0
		return true
	}
	return false
}

// chatMuted คืน (ถูก mute อยู่, เหลือเวลาเท่าไหร่)
func chatMuted(ip string) (bool, time.Duration) {
	v, ok := chatOffenders.Load(ip)
	if !ok {
		return false, 0
	}
	rec := v.(*offenderRecord)
	remain := time.Until(rec.until)
	if remain > 0 {
		return true, remain
	}
	return false, 0
}

// --------------------------------------------------
// Anti-duplicate — กันส่งข้อความเดิมซ้ำจาก ip เดียวกัน (spam ทั่วไป)
// --------------------------------------------------

var chatLastMsg sync.Map // ip -> ข้อความล่าสุดที่ส่งผ่านแล้ว

// chatDuplicateMessage คืน true ถ้าข้อความนี้เหมือนล่าสุดที่ส่งมาจาก ip เดียวกัน
func chatDuplicateMessage(ip, cleanedText string) bool {
	prev, loaded := chatLastMsg.LoadOrStore(ip, cleanedText)
	if loaded && prev.(string) == cleanedText {
		return true
	}
	chatLastMsg.Store(ip, cleanedText)
	return false
}
