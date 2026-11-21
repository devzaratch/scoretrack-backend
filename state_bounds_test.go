package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMemoryRateLimiterBoundsEntries(t *testing.T) {
	rl := &MemoryRateLimiter{records: make(map[string]*ClientRateRecord)}
	for i := 0; i < rateLimiterMaxKeys+500; i++ {
		rl.AllowKey(fmt.Sprintf("ip-%d", i), 10, time.Minute)
	}
	rl.mu.RLock()
	n := len(rl.records)
	rl.mu.RUnlock()
	if n > rateLimiterMaxKeys {
		t.Fatalf("rate limiter records grew to %d, want <= %d", n, rateLimiterMaxKeys)
	}
	if n == 0 {
		t.Fatal("expected some records to remain")
	}
}

func TestPruneChatModerationState(t *testing.T) {
	oldOffenders := chatOffenders
	oldLast := chatLastMsg
	t.Cleanup(func() {
		chatOffenders = oldOffenders
		chatLastMsg = oldLast
	})

	chatOffenders = sync.Map{}
	chatLastMsg = sync.Map{}

	chatOffenders.Store("old", &offenderRecord{firstSeen: time.Now().Add(-2 * time.Hour), strikes: 1})
	chatOffenders.Store("muted-active", &offenderRecord{until: time.Now().Add(2 * time.Minute)})
	for i := 0; i < chatModerationMaxEntries+50; i++ {
		chatLastMsg.Store(fmt.Sprintf("ip-%d", i), "msg")
	}

	pruneChatModerationState()

	if _, ok := chatOffenders.Load("old"); ok {
		t.Fatal("expected stale offender pruned")
	}
	if _, ok := chatOffenders.Load("muted-active"); !ok {
		t.Fatal("expected active mute offender kept")
	}

	n := 0
	chatLastMsg.Range(func(_, _ interface{}) bool { n++; return true })
	if n > chatModerationMaxEntries {
		t.Fatalf("chatLastMsg entries %d, want <= %d", n, chatModerationMaxEntries)
	}
}
