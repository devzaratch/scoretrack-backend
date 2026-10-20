package main

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// parseScoresStrGoals — regression สูตรปลอม Wins*2+Draws
// เดิม GF = Wins*2+Draws -> Liverpool 25 wins 9 draws คืน 59 ทั้งที่ยิงจริง 86
// ---------------------------------------------------------------------------

func TestParseScoresStrGoals_Valid(t *testing.T) {
	gf, ga := parseScoresStrGoals("86:41")
	if gf != 86 || ga != 41 {
		t.Fatalf("86:41 -> ได้ %d:%d ต้องเป็น 86:41", gf, ga)
	}
}

func TestParseScoresStrGoals_WithSpaces(t *testing.T) {
	gf, ga := parseScoresStrGoals(" 52 : 45 ")
	if gf != 52 || ga != 45 {
		t.Fatalf("' 52 : 45 ' -> ได้ %d:%d ต้องเป็น 52:45", gf, ga)
	}
}

func TestParseScoresStrGoals_Invalid(t *testing.T) {
	for _, in := range []string{"", "86", "abc", "86:xx", "86:41:20"} {
		gf, ga := parseScoresStrGoals(in)
		// "86:41:20" -> SplitN(2) ตัดเหลือ "86" กับ "41:20" -> Atoi("41:20") fail -> 0,0
		if gf != 0 || ga != 0 {
			t.Fatalf("%q -> ได้ %d:%d ต้องเป็น 0:0", in, gf, ga)
		}
	}
}

// TestFetchRapidAPIStandings_NoFakeGoals — ตรวจสอบ struct ที่ map เข้า CalculatedStanding
// ต้องใช้ GF/GA จาก scoresStr ไม่ใช่สูตร Wins*2 (ตัวแทน regression ของฟังก์ชันหลัก)
func TestParseScoresStrGoals_MatchesRealisticData(t *testing.T) {
	// ตัวเลขจริงจากฤดูกาลที่ผ่านมา: ทีมชนะ 25 เสมอ 9 แพ้ 4 ยิง 86 เสีย 41
	gf, ga := parseScoresStrGoals("86:41")
	fakeGF := 25*2 + 9 // สูตรเก่า = 59
	if gf == fakeGF {
		t.Fatal("ได้ค่าเท่าสูตรปลอม — ต้องมาจาก scoresStr")
	}
	if gf != 86 {
		t.Fatalf("ต้องได้ 86 จาก scoresStr แต่ได้ %d", gf)
	}
	_ = ga
}

// ---------------------------------------------------------------------------
// flightGroup (singleflight) — กัน cache stampede
// ---------------------------------------------------------------------------

// TestFlightGroup_Dedup — คำขอ 20 ตัวคีย์เดียวกัน -> fn วิ่งครั้งเดียว ทุกตัวได้ผลเดียวกัน
func TestFlightGroup_Dedup(t *testing.T) {
	var g flightGroup
	var calls int64

	const n = 20
	results := make([][]byte, n)
	sources := make([]string, n)
	errs := make([]error, n)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], sources[i], errs[i] = g.Do("key1", func() ([]byte, string, error) {
				atomic.AddInt64(&calls, 1)
				time.Sleep(50 * time.Millisecond) // จำลอง upstream ที่ช้า
				return []byte("data"), "goalapi", nil
			})
		}(i)
	}
	wg.Wait()

	if calls != 1 {
		t.Fatalf("fn ต้องวิ่งครั้งเดียวแต่วิ่ง %d ครั้ง (stampede)", calls)
	}
	for i := 0; i < n; i++ {
		if string(results[i]) != "data" || sources[i] != "goalapi" || errs[i] != nil {
			t.Fatalf("ตัวที่ %d ได้ผลคนละอย่าง: %q/%q/%v", i, results[i], sources[i], errs[i])
		}
	}
}

// TestFlightGroup_SeparateKeys — คีย์ต่างกันต้องวิ่งแยก ไม่รอ elkaar
func TestFlightGroup_SeparateKeys(t *testing.T) {
	var g flightGroup
	var calls int64

	var wg sync.WaitGroup
	for _, key := range []string{"a", "b", "c"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			_, _, _ = g.Do(key, func() ([]byte, string, error) {
				atomic.AddInt64(&calls, 1)
				return []byte(key), "x", nil
			})
		}(key)
	}
	wg.Wait()

	if calls != 3 {
		t.Fatalf("คีย์ต่างกัน 3 ตัวต้องวิ่ง 3 ครั้ง แต่วิ่ง %d", calls)
	}
}

// TestFlightGroup_ErrorPropagation — คำแรกล้มเหลว -> ตัวรอได้ error เดียวกัน (ไม่แขวน)
func TestFlightGroup_ErrorPropagation(t *testing.T) {
	var g flightGroup
	wantErr := fmt.Errorf("upstream 502")

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := g.Do("errkey", func() ([]byte, string, error) {
				time.Sleep(20 * time.Millisecond)
				return nil, "", wantErr
			})
			if err == nil || !strings.Contains(err.Error(), "502") {
				t.Errorf("ตัวรอต้องได้ error ของคำแรก แต่ได้ %v", err)
			}
		}()
	}
	wg.Wait()
}

// TestFlightGroup_ReusableAfterDone — งานเสร็จแล้ว คีย์เดิมต้องวิ่งใหม่ได้ (ไม่ deadlock)
func TestFlightGroup_ReusableAfterDone(t *testing.T) {
	var g flightGroup

	for round := 0; round < 3; round++ {
		data, _, err := g.Do("reuse", func() ([]byte, string, error) {
			return []byte("round"), "s", nil
		})
		if err != nil || string(data) != "round" {
			t.Fatalf("รอบ %d พัง: %q/%v", round, data, err)
		}
	}
}

// ---------------------------------------------------------------------------
// backfillOneTeam — เติม registry นักเตะจาก squad
// ---------------------------------------------------------------------------

// TestBackfillDone_PersistsAndSkips — done-set บันทึกลงไฟล์และโหลดกลับได้
func TestBackfillDone_PersistsAndSkips(t *testing.T) {
	// ใช้ path ชั่วคราวใน temp dir
	oldPath := backfillStatePath
	tmpDir := t.TempDir()
	backfillStatePath = tmpDir + "/pb.json"
	defer func() { backfillStatePath = oldPath }()

	// รีเซ็ต in-memory
	backfillMu.Lock()
	backfillDone = make(map[string]bool)
	backfillMu.Unlock()

	backfillMu.Lock()
	backfillDone["ULID_A"] = true
	backfillMu.Unlock()
	saveBackfillState()

	// รีเซ็ตแล้วโหลดใหม่
	backfillMu.Lock()
	backfillDone = make(map[string]bool)
	backfillMu.Unlock()
	loadBackfillState()

	backfillMu.Lock()
	got := backfillDone["ULID_A"]
	backfillMu.Unlock()
	if !got {
		t.Fatal("done-set ไม่ถูกบันทึก/โหลดกลับ")
	}
}

// ---------------------------------------------------------------------------
// liveSweepInterval ตรวจโควตา idle (มี test ใน live_ticker_test.go แล้ว)
// ที่นี่เพิ่ม test ว่า backfill ไม่ยิงเมื่อ Remaining ต่ำกว่าเกณฑ์
// ---------------------------------------------------------------------------

// TestBackfillSkip_LowBudget — โควตาเหลือต่ำ -> backfill ต้องไม่ยิง (หลีกทางหน้าเว็บ)
func TestBackfillSkip_LowBudget(t *testing.T) {
	// สร้าง provider เปล่า ๆ ไม่ต่อ network
	g := &GoalProvider{}
	// Budget() ต้องไม่ nil — ตั้ง budget ด้วย limit ต่ำ
	g.budget = NewSourceBudget("test-goal", 10, 100)
	// ใช้โควตาหมด
	for i := 0; i < 10; i++ {
		g.budget.Success(1, nil)
	}
	if g.Budget().Remaining() != 0 {
		t.Fatalf("เตรียมข้อมูลผิด remaining=%d", g.Budget().Remaining())
	}

	// เรียก backfillOneTeam ด้วย minRemain=300 — remaining=0 ต้อง return ก่อนยิงอะไรทั้งนั้น
	// (ถ้าไม่ guard จะ panic ตอนยิง network จริงใน test env)
	backfillOneTeam(g, 300) // ต้องไม่ panic และไม่ยิง
}
