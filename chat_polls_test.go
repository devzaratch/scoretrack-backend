package main

import (
	"encoding/json"
	"testing"
	"time"
)

func resetPolls() {
	pollsMu.Lock()
	defer pollsMu.Unlock()
	matchPolls = make(map[string]*PollState)
	pollsLastSweep = time.Time{}
}

func TestVotePollCountsAndOnePerVoter(t *testing.T) {
	resetPolls()
	state, ok := votePoll("100", "alice", "home")
	if !ok {
		t.Fatal("vote should succeed")
	}
	if state.Total != 1 || state.Options["home"] != 1 {
		t.Fatalf("unexpected state: %+v", state)
	}

	// alice โหวตซ้ำตัวเดิม — ไม่นับเพิ่ม
	state, _ = votePoll("100", "alice", "home")
	if state.Total != 1 || state.Options["home"] != 1 {
		t.Fatalf("duplicate vote must not count: %+v", state)
	}

	// bob โหวตอีกเสียง
	state, _ = votePoll("100", "bob", "away")
	if state.Total != 2 || state.Options["away"] != 1 {
		t.Fatalf("unexpected after bob: %+v", state)
	}
}

func TestVotePollCanChangeMind(t *testing.T) {
	resetPolls()
	votePoll("200", "alice", "home")
	state, _ := votePoll("200", "alice", "draw")
	if state.Total != 1 {
		t.Fatalf("total must stay 1 after changing vote, got %d", state.Total)
	}
	if state.Options["home"] != 0 || state.Options["draw"] != 1 {
		t.Fatalf("vote not moved: %+v", state.Options)
	}
}

func TestVotePollRejectsInvalidOption(t *testing.T) {
	resetPolls()
	if _, ok := votePoll("300", "alice", "winner"); ok {
		t.Fatal("invalid option must be rejected")
	}
	if _, ok := votePoll("", "alice", "home"); ok {
		t.Fatal("empty match id must be rejected")
	}
}

func TestPollJSONHidesVoters(t *testing.T) {
	resetPolls()
	votePoll("400", "secret-voter", "home")
	b := currentPollJSON("400")
	if len(b) == 0 {
		t.Fatal("expected JSON")
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if decoded["type"] != "poll" {
		t.Fatalf("type=%v", decoded["type"])
	}
	poll, _ := decoded["poll"].(map[string]interface{})
	if poll == nil {
		t.Fatal("missing poll object")
	}
	if _, leaked := poll["voters"]; leaked {
		t.Fatal("voters must not be exposed")
	}
}

func TestHandleLiveClientMessagePollGet(t *testing.T) {
	resetPolls()
	resp := handleLiveClientMessage([]byte(`{"type":"poll_get","match_id":"500"}`), "10.0.0.1")
	if len(resp) == 0 {
		t.Fatal("poll_get must return state")
	}
	if resp := handleLiveClientMessage([]byte(`not json`), "10.0.0.1"); resp != nil {
		t.Fatal("garbage input must be ignored")
	}
	if resp := handleLiveClientMessage([]byte(`{"type":"chat"}`), "10.0.0.1"); resp != nil {
		t.Fatal("unknown type must be ignored")
	}
}
