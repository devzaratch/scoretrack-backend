package main

// line_notify_test.go — ทดสอบ LINE Messaging API wrapper โดยไม่ยิง api.line.me จริง
// (httptest override lineAPIBase + env-gate ตรวจทั้งค่าและ header)

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestComposeLINEText(t *testing.T) {
	got := composeLINEText("T", "B")
	if got != "T\nB" {
		t.Fatalf("composeLINEText ต้องรวม title+body แยกบรรทัด: got %q", got)
	}
}

func TestBuildLINEPayload(t *testing.T) {
	raw, jsonStr := buildLINEPayload("Title", "Body", "")
	if jsonStr == "" {
		t.Fatal("buildLINEPayload ต้องคืน JSON string ไม่เป็นค่าว่าง")
	}
	var p map[string]interface{}
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("payload ไม่ใช่ JSON ถูกต้อง: %v", err)
	}
	msgs, ok := p["messages"].([]interface{})
	if !ok || len(msgs) != 1 {
		t.Fatalf("payload ต้องมี messages 1 ข้อความ: %v", p)
	}
	m0 := msgs[0].(map[string]interface{})
	if m0["type"] != "text" || m0["text"] != "Title\nBody" {
		t.Fatalf("text message ต้อง type=text, text='Title\\nBody': %v", m0)
	}
	if _, hasTo := p["to"]; hasTo {
		t.Fatalf("broadcast ต้องไม่มี field 'to': %v", p)
	}
}

func TestDispatchLINENotifyUnconfigured(t *testing.T) {
	t.Setenv("LINE_CHANNEL_ACCESS_TOKEN", "")
	// ต้อง return เงียบ ๆ โดยไม่ panic และไม่ยิง network (base ชี้ไปเซิร์ฟเวอร์ทดสอบที่จะ fail ถ้าถูกยิง)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("ต้องไม่ยิง HTTP เมื่อ token ไม่ได้ตั้ง")
	}))
	defer srv.Close()
	orig := lineAPIBase
	lineAPIBase = srv.URL
	defer func() { lineAPIBase = orig }()

	DispatchLINENotify(1, "T", "B")
}

func TestDispatchLINENotifyBroadcastSends(t *testing.T) {
	t.Setenv("LINE_CHANNEL_ACCESS_TOKEN", "test-token-123")
	t.Setenv("LINE_TARGET_ID", "")

	var gotAuth, gotPath string
	var gotBody map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		_ = json.NewDecoder(bytes.NewReader(readAllOnce(r))).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	orig := lineAPIBase
	lineAPIBase = srv.URL
	defer func() { lineAPIBase = orig }()

	DispatchLINENotify(7, "⚽ GOAL", "A 1-0 B")

	if gotAuth != "Bearer test-token-123" {
		t.Fatalf("Authorization ผิด: %q", gotAuth)
	}
	if gotPath != "/v2/bot/message/broadcast" {
		t.Fatalf("ไม่ใส่ LINE_TARGET_ID ต้องยิง /broadcast: got %q", gotPath)
	}
	msgs := gotBody["messages"].([]interface{})[0].(map[string]interface{})
	if msgs["text"] != "⚽ GOAL\nA 1-0 B" {
		t.Fatalf("text ผิด: %v", msgs["text"])
	}
}

func TestDispatchLINENotifyTargetPushes(t *testing.T) {
	t.Setenv("LINE_CHANNEL_ACCESS_TOKEN", "tok")
	t.Setenv("LINE_TARGET_ID", "Udeadbeef")

	var gotPath string
	var gotTo string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		var p map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&p)
		gotTo, _ = p["to"].(string)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	orig := lineAPIBase
	lineAPIBase = srv.URL
	defer func() { lineAPIBase = orig }()

	DispatchLINENotify(8, "T", "B")

	if gotPath != "/v2/bot/message/push" {
		t.Fatalf("ใส่ LINE_TARGET_ID ต้องยิง /push: got %q", gotPath)
	}
	if gotTo != "Udeadbeef" {
		t.Fatalf("to field ต้องเป็น LINE_TARGET_ID: %q", gotTo)
	}
}

// readAllOnce — อ่าน body ของ request ให้ decode ได้จาก bytes.NewReader ทีเดียว
func readAllOnce(r *http.Request) []byte {
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(r.Body)
	_ = r.Body.Close()
	return buf.Bytes()
}
