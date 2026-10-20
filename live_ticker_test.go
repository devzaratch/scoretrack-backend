package main

import (
	"testing"
	"time"
)

// TestLiveSweepInterval_Active — มีคนดู/มี sub -> ใช้รอบปกติ (ไม่ช้าลง)
func TestLiveSweepInterval_Active(t *testing.T) {
	base := 180 * time.Second
	got := liveSweepInterval(base, true)
	if got != base {
		t.Fatalf("active ต้องได้รอบปกติ %v แต่ได้ %v", base, got)
	}
}

// TestLiveSweepInterval_Idle — ไม่มีใครดู -> ช้าลงเป็น IDLE_SWEEP_SEC (default 900)
// regression: เดิม idle -> continue ข้าม sweep เลย -> liveState ว่าง -> goal WS
// subscribe 0 คู่ -> ระบบ live ไม่ตื่นเองจนกว่าจะมีคนเปิดหน้าแรก
func TestLiveSweepInterval_Idle(t *testing.T) {
	base := 180 * time.Second
	got := liveSweepInterval(base, false)
	if got != 900*time.Second {
		t.Fatalf("idle ต้องได้ 900s แต่ได้ %v", got)
	}
	if got <= base {
		t.Fatalf("idle ต้องช้ากว่า base (%v <= %v)", got, base)
	}
}

// TestLiveSweepInterval_IdleConfigured — IDLE_SWEEP_SEC ตั้งค่าเองได้
func TestLiveSweepInterval_IdleConfigured(t *testing.T) {
	t.Setenv("IDLE_SWEEP_SEC", "600")
	got := liveSweepInterval(180*time.Second, false)
	if got != 600*time.Second {
		t.Fatalf("IDLE_SWEEP_SEC=600 ต้องได้ 600s แต่ได้ %v", got)
	}
}

// TestLiveSweepInterval_IdleFloor — ห้ามตั้ง idle ให้เร็วกว่ารอบปกติ
// (กัน env พังทำให้ idle กินโควตาเร็วกว่าตอนมีคนดู)
func TestLiveSweepInterval_IdleFloor(t *testing.T) {
	t.Setenv("IDLE_SWEEP_SEC", "10")
	got := liveSweepInterval(180*time.Second, false)
	if got != 180*time.Second {
		t.Fatalf("idle ต่ำกว่า base ต้องโดน floor ที่ base แต่ได้ %v", got)
	}
}
