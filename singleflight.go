package main

import "sync"

// ---------------------------------------------------------------------------
// singleflight เล็ก ๆ — กัน cache stampede
//
// ปัญหาเดิม: ตอน cache หมดอายุพร้อมกัน (หรือ cold start ตอนเช้า)
// คำขอหลายสิบหน้าที่ไม่มีใครเริ่มก่อนจะยิง upstream พร้อมกัน
// = N calls เปล่า ๆ โดน minute-limit ของ provider (60/min) ทันที
// -> 429 -> circuit breaker เปิด -> ระบบล่มเป็นทอด ๆ
//
// วิธี: คีย์เดียวกัน -> คำขอแรกยิงจริง ที่เหลือรอแล้วใช้ผลเดียวกัน (0 calls)
// ---------------------------------------------------------------------------

// flightCall ผลลัพธ์ของงานที่กำลังวิ่ง — ตัวรออ่านผ่าน channel done
type flightCall struct {
	done   chan struct{}
	data   []byte
	source string
	err    error
}

type flightGroup struct {
	mu    sync.Mutex
	calls map[string]*flightCall
}

// Do รัน fn ต่อคีย์ — ถ้ามีงานเดิมวิ่งอยู่ (คีย์เดียวกัน) จะรอแล้วใช้ผลเดียวกัน
// fn panic -> defer ยังล้าง call ออก + ปลุกตัวรอ (ได้ค่าว่าง ไม่แขวนถาวร)
func (g *flightGroup) Do(key string, fn func() ([]byte, string, error)) ([]byte, string, error) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*flightCall)
	}
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		<-c.done // รอคำขอแรกเสร็จ แล้วใช้ผลเดียวกัน (ไม่ยิง upstream เพิ่ม)
		return c.data, c.source, c.err
	}
	c := &flightCall{done: make(chan struct{})}
	g.calls[key] = c
	g.mu.Unlock()

	defer func() {
		g.mu.Lock()
		delete(g.calls, key)
		g.mu.Unlock()
		close(c.done)
	}()

	c.data, c.source, c.err = fn()
	return c.data, c.source, c.err
}

// proxyFlights — group กลางของ handleProxy (คีย์ = cacheKey + query string)
var proxyFlights flightGroup
