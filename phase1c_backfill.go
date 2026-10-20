package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Phase 1c backfill — เติม registry นักเตะจาก squad ของทีมที่รู้จักอยู่แล้ว
//
// ปัญหา: registry นักเตะค้างที่ 144 ตัว เพราะมี remember แค่ตอนคนค้นหา/กด top scorer
// ทำให้ /player/{apiId} resolve ULID ไม่ได้ = หน้านักเตะพังเงียบ
//
// ทีม 7,000 ULID อยู่ใน registry แล้ว (จำมาจากแมตช์) — /teams/{ulid}/players
// คืนนักเตะ ~25 คน/ทีม พร้อม APIID+ULID ฟรี 1 call ต่อทีม
//
// หลักการทำงาน:
//   - ยิงทีมละ 1 call ต่อรอบ (default 5 นาที) — ไม่เร่ง ไม่กินโควตาผู้ใช้
//   - ยิงก็ต่อเมื่อโควตาเหลือ > BACKFILL_MIN_REMAIN (default 300) เสมอ
//     (งานไม่เร่งด่วน ห้ามแย่งโควตาหน้าเว็บ)
//   - ทำทีมซ้ำได้ไม่เป็นไร (Remember ค่าเดิม = no-op) แต่มี done-set กันเสียเปล่า
//   - kill switch: PLAYER_BACKFILL=off
// ---------------------------------------------------------------------------

type playerBackfillState struct {
	Done map[string]bool `json:"done"` // team ULID ที่ดึง squad แล้ว
}

var (
	backfillMu   sync.Mutex
	backfillDone = make(map[string]bool)
)

// backfillStatePath เปลี่ยนใน test ได้ (สลับไป temp dir)
var backfillStatePath = "data/player_backfill.json"

func loadBackfillState() {
	backfillMu.Lock()
	defer backfillMu.Unlock()

	raw, err := os.ReadFile(backfillStatePath)
	if err != nil {
		return // ยังไม่เคยมี — เริ่มจากว่าง
	}
	var st playerBackfillState
	if err := json.Unmarshal(raw, &st); err != nil || st.Done == nil {
		log.Printf("ℹ️ player backfill: อ่าน state พัง (%v) -> เริ่มใหม่", err)
		return
	}
	backfillDone = st.Done
	log.Printf("🧩 player backfill: โหลด state แล้ว (ทีมที่ทำไปแล้ว %d)", len(backfillDone))
}

func saveBackfillState() {
	backfillMu.Lock()
	st := playerBackfillState{Done: backfillDone}
	backfillMu.Unlock()

	raw, err := json.Marshal(st)
	if err != nil {
		return
	}
	_ = os.MkdirAll(filepath.Dir(backfillStatePath), 0755)
	tmp := backfillStatePath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0644); err != nil {
		return
	}
	_ = os.Rename(tmp, backfillStatePath)
}

// startPlayerBackfill เปิด goroutine backfill นักเตะ (เคารพ kill switch + โควตา)
func startPlayerBackfill() {
	loadBackfillState()

	if !envBool("PLAYER_BACKFILL", true) {
		log.Println("🧩 player backfill: ปิดใช้งาน (PLAYER_BACKFILL=off)")
		return
	}

	interval := time.Duration(envInt("PLAYER_BACKFILL_INTERVAL_MIN", 5)) * time.Minute
	minRemain := envInt("PLAYER_BACKFILL_MIN_REMAIN", 300)

	go func() {
		// หน่วงรอบแรก 60 วิ — ให้ warm league index + traffic จริงมาก่อน
		// (งานนี้ไม่สำคัญเท่าหน้าแรก ต้องหลีกทาง)
		time.Sleep(60 * time.Second)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			if g := goalProvider(); g != nil {
				backfillOneTeam(g, minRemain)
			}
		}
	}()
	log.Printf("🧩 player backfill: เปิดแล้ว (ทุก %v ทีมละครั้ง, ยิงเมื่อโควตาเหลือ >%d)", interval, minRemain)
}

// backfillOneTeam ดึง squad ของทีมถัดไปที่ยังไม่เคยทำ แล้ว remember นักเตะทั้งทีม
func backfillOneTeam(g *GoalProvider, minRemain int) {
	// งานฟุ่มเฟือย — โควตาใกล้หมดต้องหลีกทางให้หน้าเว็บเสมอ
	if g.Budget().Remaining() < minRemain {
		return
	}

	// เลือกทีมถัดไป: ULID ที่ยังไม่อยู่ใน done-set เรียงตามตัวอักษร (ผล deterministic)
	teams := registry.All("team")
	ulids := make([]string, 0, len(teams))
	backfillMu.Lock()
	for _, ref := range teams {
		if ref.Source != g.Name() || ref.ULID == "" {
			continue
		}
		if backfillDone[ref.ULID] {
			continue
		}
		ulids = append(ulids, ref.ULID)
	}
	backfillMu.Unlock()

	if len(ulids) == 0 {
		return // ทุกทีมที่รู้จักถูกดึงแล้ว (หรือไม่มีทีม goal ใน registry)
	}
	sort.Strings(ulids)
	ulid := ulids[0]

	body, err := g.do("/teams/" + ulid + "/players")
	if err != nil {
		// budget/ล้มเหลว — ไม่นับเป็น done ทีมนี้ จะลองใหม่รอบหน้า
		log.Printf("🧩 player backfill: ดึง squad ทีม %s ไม่สำเร็จ: %v", ulid, err)
		return
	}

	var resp goalTeamPlayersResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		log.Printf("🧩 player backfill: parse squad ทีม %s พัง: %v", ulid, err)
		return
	}

	before := registry.Counts()["players"]
	remembered := 0
	for _, pl := range resp.Data {
		if pl.APIID == "" || pl.ID == "" {
			continue
		}
		registry.Remember("player", pl.APIID, g.Name(), pl.ID)
		remembered++
	}

	backfillMu.Lock()
	backfillDone[ulid] = true
	backfillMu.Unlock()
	saveBackfillState()

	after := registry.Counts()["players"]
	log.Printf("🧩 player backfill: ทีม %s -> นักเตะ %d คน (registry นักเตะ %d -> %d)",
		ulid, remembered, before, after)
}
