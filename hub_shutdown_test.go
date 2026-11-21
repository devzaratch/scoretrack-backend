package main

import (
	"context"
	"testing"
	"time"
)

func TestLiveHubShutdownClosesClients(t *testing.T) {
	h := NewLiveHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.RunContext(ctx)

	c := newTestClient(h)
	h.register <- c
	waitFor(t, func() bool { return h.ClientCount() == 1 })

	h.Shutdown()
	waitFor(t, func() bool { return h.ClientCount() == 0 })

	// send channel should be closed
	select {
	case _, ok := <-c.send:
		if ok {
			for range c.send {
			}
		}
	case <-time.After(time.Second):
		t.Fatal("client send not closed after shutdown")
	}
}

func TestChatHubShutdownClosesClients(t *testing.T) {
	h := NewChatHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.RunContext(ctx)

	c := &ChatClient{hub: h, send: make(chan []byte, sendChannelCap), matchID: "m1"}
	h.register <- c
	waitFor(t, func() bool { return h.ClientCount() == 1 })

	h.Shutdown()
	waitFor(t, func() bool { return h.ClientCount() == 0 })
}
