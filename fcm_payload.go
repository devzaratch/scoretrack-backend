package main

// fcm_payload.go — payload builders ของ FCM (แผน-v2 ข้อ 5: notification chain)
// แยกเป็น pure function เพื่อ unit test ได้โดยไม่ยิง network — เดิมฝังอยู่ใน
// DispatchFCMPush (goal_detector.go) แบบ inline จึงไม่เคยถูกทดสอบเลย

import (
	"encoding/json"
	"fmt"
)

// fcmV1Payload — payload ของ FCM HTTP v1 (ต่อ 1 token) nil เมื่อ marshal ล้ม (ไม่ควรเกิด)
func fcmV1Payload(token, title, body string, matchID int) []byte {
	payload := map[string]interface{}{
		"message": map[string]interface{}{
			"token": token,
			"notification": map[string]interface{}{
				"title": title,
				"body":  body,
			},
			"data": map[string]interface{}{
				"match_id":     fmt.Sprintf("%d", matchID),
				"click_action": fmt.Sprintf("/match/%d", matchID),
			},
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	return raw
}

// fcmLegacyPayload — payload ของ FCM legacy multicast (หลาย token ในครั้งเดียว)
func fcmLegacyPayload(tokens []string, title, body string, matchID int) []byte {
	payload := map[string]interface{}{
		"registration_ids": tokens,
		"notification": map[string]interface{}{
			"title": title,
			"body":  body,
			"icon":  "/favicon.ico",
			"sound": "default",
		},
		"data": map[string]interface{}{
			"match_id":     matchID,
			"click_action": fmt.Sprintf("/match/%d", matchID),
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	return raw
}
