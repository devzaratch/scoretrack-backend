package main

import (
	"sync"
	"testing"
	"time"
)

// newTestClient สร้าง LiveClient ปลอม (ไม่ต้องมี conn จริง — ทดสอบเฉพาะชั้น map/channel)
func newTestClient(h *LiveHub) *LiveClient {
	return &LiveClient{hub: h, send: make(chan []byte, sendChannelCap)}
}

// TestLiveHubConcurrentMapAccess — regression ของ data race เดิม:
// บรรทัด broadcast เคย delete() ออกจาก h.clients ขณะถือ RLock เท่านั้น
// ชนกับ ClientCount() ที่ถือ RLock พร้อมกัน = concurrent map read/write
// พิสูจน์ด้วย: go test -race ./... (ก่อนแก้ fail หลังแก้ผ่าน)
func TestLiveHubConcurrentMapAccess(t *testing.T) {
	h := NewLiveHub()
	go h.Run()

	// ลงทะเบียนลูกค้าหลายตัว
	const n = 50
	clients := make([]*LiveClient, n)
	for i := range clients {
		clients[i] = newTestClient(h)
		h.register <- clients[i]
	}
	// รอให้ Run ประมวลผล register ครบ
	waitFor(t, func() bool { return h.ClientCount() == n })

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// goroutine 1: broadcast รัว ๆ (บางตัวจะโดนตัดเมื่อ send เต็ม = $path ที่เคย race)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = h.BroadcastJSON(map[string]int{"tick": 1})
			}
		}
	}()

	// goroutine 2-5: ClientCount คู่ขนาน (คือ reader ที่ชนกับ delete)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = h.ClientCount()
				}
			}
		}()
	}

	// goroutine 6: register/unregister สลับกัน (close+delete คู่ขนาน)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			c := newTestClient(h)
			h.register <- c
			h.unregister <- c
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	if h.ClientCount() < 0 {
		t.Fatalf("unexpected count %d", h.ClientCount())
	}
}

// TestBroadcastJSONNonBlocking — regression ของ broadcast blocking เดิม:
// h.broadcast <- data ตรง ๆ ทำให้ caller (live ticker) ค้างถาวรเมื่อ chan เต็ม
// → sweep/prune หยุดทั้งระบบ ตอนนี้ต้องทิ้ง message แล้วนับสถิติแทน
func TestBroadcastJSONNonBlocking(t *testing.T) {
	h := NewLiveHub()
	// ไม่ start Run() → ไม่มีใคร consume broadcast → เต็ม 1024 ช่องจริง
	done := make(chan struct{})
	go func() {
		defer close(done)
		// ยิงมากกว่า capacity เพื่อเข้าสู่ condition default
		for i := 0; i < 2000; i++ {
			_ = h.BroadcastJSON(map[string]int{"i": i})
		}
	}()

	select {
	case <-done:
		// ผ่าน: ไม่ block แม้คิวเต็ม
	case <-time.After(3 * time.Second):
		t.Fatal("BroadcastJSON blocked when broadcast channel is full")
	}

	if dropped := h.DroppedBroadcasts(); dropped == 0 {
		t.Fatalf("expected dropped counter > 0, got %d", dropped)
	}
}

// TestSlowClientDropped — ลูกค้าที่ไม่อ่าน send (ช้า/ค้าง) ต้องถูกตัดทิ้ง
// โดยไม่ทำให้ hub ค้าง และ ClientCount ลดลงจริง
func TestSlowClientDropped(t *testing.T) {
	h := NewLiveHub()
	go h.Run()

	slow := newTestClient(h)
	h.register <- slow
	waitFor(t, func() bool { return h.ClientCount() == 1 })

	// broadcast จน send buffer (256) เต็ม → รอบถัดไปต้องโดนตัด
	for i := 0; i < sendChannelCap+10; i++ {
		_ = h.BroadcastJSON(map[string]int{"i": i})
	}
	waitFor(t, func() bool { return h.ClientCount() == 0 })

	// channel ของลูกค้าที่ถูกตัดต้องปิดแล้ว (writePump เดิมจะเห็น !ok)
	select {
	case _, ok := <-slow.send:
		if ok {
			// ยังมีค่าค้างอยู่ — อ่านจนกว่าจะปิด
			for range slow.send {
			}
		}
	default:
		// ว่างแต่ยังไม่ปิด — รอสักครู่
		select {
		case _, ok := <-slow.send:
			if ok {
				for range slow.send {
				}
			}
		case <-time.After(time.Second):
			t.Fatal("slow client channel was not closed after drop")
		}
	}
}

// waitFor — รอ condition ให้เป็นจริง (กัน flaky จาก channel processing แบบ async)
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within 2s")
}
