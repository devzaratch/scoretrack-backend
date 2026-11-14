package main

// fcm_payload_test.go — แผน-v2 ข้อ 5: ยืนยัน payload ของ notification chain
// ต่อ device (HTTP v1) และ multicast (legacy) ตรงตามที่ client (FCMManager) คาดหวัง:
// data.match_id + click_action=/match/:id เพื่อให้แตะแจ้งเตือนแล้วเปิดหน้า match ถูกตัว

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestFCMV1PayloadShape(t *testing.T) {
	raw := fcmV1Payload("tok-1", "ประตู!", "Alpha 1-0 Beta", 900001)
	if raw == nil {
		t.Fatal("payload nil")
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	msg, ok := m["message"].(map[string]interface{})
	if !ok {
		t.Fatal("ต้องมี message")
	}
	if msg["token"] != "tok-1" {
		t.Fatalf("token = %v", msg["token"])
	}
	data, ok := msg["data"].(map[string]interface{})
	if !ok {
		t.Fatal("ต้องมี data")
	}
	if data["match_id"] != "900001" {
		t.Fatalf("match_id = %v (ต้องเป็น string ตาม v1 spec)", data["match_id"])
	}
	if data["click_action"] != "/match/900001" {
		t.Fatalf("click_action = %v", data["click_action"])
	}
}

func TestFCMLegacyPayloadShape(t *testing.T) {
	raw := fcmLegacyPayload([]string{"a", "b"}, "จบเกม", "1-0", 900002)
	if raw == nil {
		t.Fatal("payload nil")
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	ids, ok := m["registration_ids"].([]interface{})
	if !ok || len(ids) != 2 || ids[0] != "a" {
		t.Fatalf("registration_ids = %v", m["registration_ids"])
	}
	if !strings.Contains(string(raw), "/match/900002") {
		t.Fatal("click_action ต้องชี้หน้า match")
	}
}
