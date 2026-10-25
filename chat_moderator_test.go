package main

import (
	"testing"
	"time"
)

// กฎ moderation: mask คำหยาบด้วย * / sanitize ข้อความ / ตัดความยาว / กัน control chars

func TestMaskBadWordsThai(t *testing.T) {
	out, n := MaskBadWords("แมทช์นี้ห่วยแตก กากมาก")
	if n == 0 {
		t.Fatal("expected at least 1 masked word")
	}
	if out == "แมทช์นี้ห่วยแตก กากมาก" {
		t.Fatalf("expected masking, got unchanged: %q", out)
	}
}

func TestMaskBadWordsEnglishBoundary(t *testing.T) {
	// word boundary: fuck ใน "fucking"? fuck ต้อง match (fucking มี separate entry)
	out, n := MaskBadWords("what the fuck")
	if n == 0 {
		t.Fatalf("expected fuck masked, got %q", out)
	}
	if out != "what the ****" {
		t.Fatalf("expected 'what the ****', got %q", out)
	}
}

func TestMaskBadWordsEnglishNoFalsePositive(t *testing.T) {
	// "classic" มี ass อยู่ข้างใน -> ห้าม match (word boundary)
	out, n := MaskBadWords("what a classic goal")
	if n != 0 {
		t.Fatalf("expected 0 masked (classic has no standalone ass), got %d: %q", n, out)
	}
	if out != "what a classic goal" {
		t.Fatalf("expected unchanged, got %q", out)
	}
}

func TestMaskBadWordsKeepsLength(t *testing.T) {
	// mask ต้องเก็บความยาว rune เดิม
	out, _ := MaskBadWords("fuck")
	if len([]rune(out)) != 4 {
		t.Fatalf("expected 4 runes, got %d (%q)", len([]rune(out)), out)
	}
}

func TestMaskBadWordsCleanText(t *testing.T) {
	out, n := MaskBadWords("บอลวันนี้สนุกมาก")
	if n != 0 || out != "บอลวันนี้สนุกมาก" {
		t.Fatalf("expected unchanged clean text, got %q n=%d", out, n)
	}
}

func TestCleanChatMessageMasksAndSanitizes(t *testing.T) {
	msg, masked, ok := CleanChatMessage(map[string]interface{}{
		"user": "แฟนบอล",
		"text": "hello fuck world",
		"time": "18:30",
	})
	if !ok {
		t.Fatal("expected ok")
	}
	if masked == 0 {
		t.Fatal("expected masked > 0")
	}
	if msg["text"] == "hello fuck world" {
		t.Fatalf("text not masked: %v", msg["text"])
	}
	if msg["type"] != "text" {
		t.Fatalf("expected type=text, got %v", msg["type"])
	}
}

func TestCleanChatMessageEmptyText(t *testing.T) {
	_, _, ok := CleanChatMessage(map[string]interface{}{
		"user": "x",
		"text": "   ",
	})
	if ok {
		t.Fatal("expected ok=false for whitespace-only text")
	}
}

func TestCleanChatMessageDefaultUser(t *testing.T) {
	msg, _, ok := CleanChatMessage(map[string]interface{}{"text": "สวัสดี"})
	if !ok {
		t.Fatal("expected ok")
	}
	if msg["user"] != "แฟนบอล" {
		t.Fatalf("expected default user แฟนบอล, got %v", msg["user"])
	}
}

func TestCleanChatMessageControlChars(t *testing.T) {
	msg, _, ok := CleanChatMessage(map[string]interface{}{
		"user": "u",
		"text": "สวัสด\x00ี\x07ครับ\n",
	})
	if !ok {
		t.Fatal("expected ok")
	}
	if msg["text"] != "สวัสดีครับ" {
		t.Fatalf("expected control chars stripped, got %q", msg["text"])
	}
}

func TestCleanChatMessageTruncatesLongText(t *testing.T) {
	long := make([]rune, maxChatTextLen+50)
	for i := range long {
		long[i] = 'ก'
	}
	msg, _, ok := CleanChatMessage(map[string]interface{}{"text": string(long)})
	if !ok {
		t.Fatal("expected ok")
	}
	if n := len([]rune(msg["text"].(string))); n != maxChatTextLen {
		t.Fatalf("expected %d runes, got %d", maxChatTextLen, n)
	}
}

func TestCleanChatMessageDropsExtraKeys(t *testing.T) {
	msg, _, ok := CleanChatMessage(map[string]interface{}{
		"text": "hi",
		"evil": "payload",
	})
	if !ok {
		t.Fatal("expected ok")
	}
	if _, exists := msg["evil"]; exists {
		t.Fatal("expected extra keys dropped")
	}
}

func TestRepeatOffenderMuteAfterThreshold(t *testing.T) {
	ip := "test-ip-mute"
	// strike 2 ครั้ง — ยังไม่ mute
	if registerChatStrike(ip) {
		t.Fatal("did not expect mute before threshold")
	}
	registerChatStrike(ip)

	// ยังไม่ถูก mute
	if muted, _ := chatMuted(ip); muted {
		t.Fatal("did not expect muted yet")
	}

	// strike ครบ 3 → mute
	if !registerChatStrike(ip) {
		t.Fatal("expected mute at threshold")
	}
	if muted, remain := chatMuted(ip); !muted || remain <= 0 {
		t.Fatal("expected muted with remaining time")
	}
}

func TestRepeatOffenderWindowReset(t *testing.T) {
	ip := "test-ip-window"
	// จำลองหน้าต่างเก่ากว่า strikeWindow
	v, _ := chatOffenders.LoadOrStore(ip, &offenderRecord{
		strikes:   strikeThreshold - 1,
		firstSeen: time.Now().Add(-strikeWindow - time.Minute),
	})
	_ = v
	if registerChatStrike(ip) {
		t.Fatal("expected window reset, strike count restarted")
	}
}

func TestChatDuplicateMessage(t *testing.T) {
	ip := "test-ip-dup"
	if chatDuplicateMessage(ip, "hello") {
		t.Fatal("first message must not be duplicate")
	}
	if !chatDuplicateMessage(ip, "hello") {
		t.Fatal("same message again = duplicate")
	}
	if chatDuplicateMessage(ip, "world") {
		t.Fatal("different message must not be duplicate")
	}
}
