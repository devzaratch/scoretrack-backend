package main

import (
	"strconv"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// standings_compute_test — ตารางคะแนนที่คำนวณจากผลแมตช์จริง (แผนข้อ 1)
// regression: ตอน standings ต้นทางล่ม เคยโชว์ตาราง hardcode ฤดูกาลเก่า
// ตอนนี้ต้องคำนวณจาก FINISHED fixtures ของฤดูกาลนี้แทน
// ---------------------------------------------------------------------------

func strPtr(s string) *string { return &s }

// finishedFixture สร้างแมตช์จบแล้วแบบสั้น ๆ สำหรับ test
func finishedFixture(kickoff, homeID, homeName string, hg int, awayID, awayName string, ag int) goalFixture {
	return goalFixture{
		KickoffUTC:    kickoff,
		MatchStatus:   "FINISHED",
		HomeTeamID:    homeID,
		HomeTeamName:  homeName,
		HomeTeamFT:    strPtr(strconv.Itoa(hg)),
		AwayTeamID:    awayID,
		AwayTeamName:  awayName,
		AwayTeamFT:    strPtr(strconv.Itoa(ag)),
		TeamHomeBadge: "https://media.example/" + homeID + ".jpg",
		TeamAwayBadge: "https://media.example/" + awayID + ".jpg",
	}
}

func TestAggregateStandings_BasicTable(t *testing.T) {
	fixtures := []goalFixture{
		finishedFixture("2026-08-10T18:00:00Z", "1", "Alpha", 2, "2", "Beta", 1),
		finishedFixture("2026-08-10T20:00:00Z", "3", "Gamma", 0, "4", "Delta", 0),
		finishedFixture("2026-08-17T18:00:00Z", "1", "Alpha", 1, "3", "Gamma", 1),
		finishedFixture("2026-08-17T20:00:00Z", "2", "Beta", 3, "4", "Delta", 2),
		finishedFixture("2026-08-24T18:00:00Z", "1", "Alpha", 0, "4", "Delta", 2),
	}
	rows := aggregateStandingsFromFixtures(fixtures)
	if len(rows) != 4 {
		t.Fatalf("ต้องได้ 4 ทีม ได้ %d", len(rows))
	}

	// คำนวนเอง:
	//   Delta P3 W1 D1 L1 GF4 GA3 GD+1 = 4pts (ฟอร์ม DLW)
	//   Alpha P3 W1 D1 L1 GF3 GA4 GD-1 = 4pts (ฟอร์ม WDL)
	//   Beta  P2 W1 L1   GF4 GA4 GD 0 = 3pts (ฟอร์ม LW)
	//   Gamma P2 D2      GF1 GA1 GD 0 = 2pts (ฟอร์ม DD)
	// อันดับ: Delta(4,+1) > Alpha(4,-1) > Beta(3) > Gamma(2) — แต้มเท่าตัดด้วย GD
	wantOrder := []string{"Delta", "Alpha", "Beta", "Gamma"}
	wantPts := []int{4, 4, 3, 2}
	wantForm := []string{"DLW", "WDL", "LW", "DD"}
	for i, r := range rows {
		if r.Name != wantOrder[i] {
			t.Errorf("อันดับ %d: ต้องได้ %s ได้ %s", i+1, wantOrder[i], r.Name)
		}
		if r.Pts != wantPts[i] {
			t.Errorf("%s: ต้องได้ %d คะแนน ได้ %d", r.Name, wantPts[i], r.Pts)
		}
		if r.Form != wantForm[i] {
			t.Errorf("%s: form ต้องเป็น %s ได้ %s", r.Name, wantForm[i], r.Form)
		}
		if r.Rank != i+1 {
			t.Errorf("%s: rank ต้องเป็น %d ได้ %d", r.Name, i+1, r.Rank)
		}
	}

	alpha := rows[1]
	if alpha.Played != 3 || alpha.Wins != 1 || alpha.Draws != 1 || alpha.Losses != 1 {
		t.Errorf("Alpha สถิติผิด: P%d W%d D%d L%d", alpha.Played, alpha.Wins, alpha.Draws, alpha.Losses)
	}
	if alpha.UID != "1" {
		t.Errorf("UID ต้องเก็บ id ต้นทางของ provider ไว้สร้างลิงก์ทีม ได้ %q", alpha.UID)
	}
	if alpha.GF != 3 || alpha.GA != 4 || alpha.GD != -1 {
		t.Errorf("Alpha ประตูผิด: GF%d GA%d GD%d", alpha.GF, alpha.GA, alpha.GD)
	}
	if alpha.ScoresStr != "3:4" {
		t.Errorf("Alpha scoresStr ต้องเป็น 3:4 ได้ %s", alpha.ScoresStr)
	}
}

func TestAggregateStandings_SkipUnfinishedAndMalformed(t *testing.T) {
	fixtures := []goalFixture{
		finishedFixture("2026-08-10T18:00:00Z", "1", "Alpha", 2, "2", "Beta", 1),
		// ยังไม่จบ -> ต้องข้าม
		{KickoffUTC: "2026-08-17T18:00:00Z", MatchStatus: "NS", HomeTeamID: "1", HomeTeamName: "Alpha", AwayTeamID: "3", AwayTeamName: "Gamma"},
		// จบแต่ไม่มีสกอร์ทั้งคู่ (ข้อมูลผิดรูป) -> ต้องข้าม อย่าเดา 0-0
		{KickoffUTC: "2026-08-17T20:00:00Z", MatchStatus: "FINISHED", HomeTeamID: "2", HomeTeamName: "Beta", AwayTeamID: "3", AwayTeamName: "Gamma"},
		// จบแต่ชื่อทีมหาย -> ต้องข้าม
		{KickoffUTC: "2026-08-17T21:00:00Z", MatchStatus: "FINISHED", HomeTeamID: "4", HomeTeamName: "", HomeTeamFT: strPtr("1"), AwayTeamID: "3", AwayTeamName: "Gamma", AwayTeamFT: strPtr("0")},
	}
	rows := aggregateStandingsFromFixtures(fixtures)
	if len(rows) != 2 {
		t.Fatalf("ต้องได้แค่ 2 ทีม (Alpha/Beta) ได้ %d", len(rows))
	}
	if rows[0].Name != "Alpha" || rows[1].Name != "Beta" {
		t.Errorf("ทีมผิด: %+v", rows)
	}
	if rows[0].Played != 1 || rows[1].Played != 1 {
		t.Errorf("played ต้องเป็น 1 ทั้งคู่: Alpha=%d Beta=%d", rows[0].Played, rows[1].Played)
	}
}

func TestAggregateStandings_TieBreakers(t *testing.T) {
	fixtures := []goalFixture{
		finishedFixture("2026-08-10T18:00:00Z", "1", "Zebra", 3, "2", "Apple", 0), // Zebra +3, Apple -3
		finishedFixture("2026-08-10T20:00:00Z", "3", "Mango", 2, "4", "Kiwi", 1),  // Mango +1, Kiwi -1
		finishedFixture("2026-08-17T18:00:00Z", "1", "Zebra", 1, "3", "Mango", 0), // Zebra 6pts, Mango 3pts
		finishedFixture("2026-08-17T20:00:00Z", "2", "Apple", 2, "4", "Kiwi", 0),  // Apple 3pts, Kiwi 0pts
	}
	rows := aggregateStandingsFromFixtures(fixtures)
	// Zebra 6pts +4 | Mango 3pts +1 | Apple 3pts -1 | Kiwi 0pts -3
	// Mango กับ Apple แต้มเท่า (3) -> ตัดด้วย GD: Mango +1 ก่อน Apple -1
	wantOrder := []string{"Zebra", "Mango", "Apple", "Kiwi"}
	for i, r := range rows {
		if r.Name != wantOrder[i] {
			t.Errorf("อันดับ %d: ต้องได้ %s ได้ %s (tie ต้องตัดด้วย GD)", i+1, wantOrder[i], r.Name)
		}
	}
	if rows[0].Pts != 6 || rows[1].Pts != 3 || rows[3].Pts != 0 {
		t.Errorf("แต้มผิด: Zebra=%d Mango=%d Kiwi=%d", rows[0].Pts, rows[1].Pts, rows[3].Pts)
	}
}

func TestAggregateStandings_FormChronologicalAndCapped(t *testing.T) {
	// Alpha: 7 นัด (L W D W L W W) -> form ต้องเป็น 5 นัดล่าสุด เรียงเก่า->ใหม่ = DWLWW
	fixtures := []goalFixture{
		finishedFixture("2026-08-03T18:00:00Z", "1", "Alpha", 0, "2", "Beta", 1), // L
		finishedFixture("2026-08-10T18:00:00Z", "1", "Alpha", 1, "2", "Beta", 0), // W
		finishedFixture("2026-08-17T18:00:00Z", "1", "Alpha", 1, "2", "Beta", 1), // D
		finishedFixture("2026-08-24T18:00:00Z", "1", "Alpha", 2, "2", "Beta", 0), // W
		finishedFixture("2026-08-31T18:00:00Z", "1", "Alpha", 0, "2", "Beta", 1), // L
		finishedFixture("2026-09-07T18:00:00Z", "1", "Alpha", 3, "2", "Beta", 0), // W
		finishedFixture("2026-09-14T18:00:00Z", "1", "Alpha", 2, "2", "Beta", 0), // W
	}
	rows := aggregateStandingsFromFixtures(fixtures)
	alpha := rows[0]
	if alpha.Form != "DWLWW" {
		t.Errorf("form ต้องเป็น DWLWW (ตัด 2 นัดแรกทิ้ง) ได้ %s", alpha.Form)
	}
	if alpha.Played != 7 {
		t.Errorf("played ต้องเป็น 7 ได้ %d", alpha.Played)
	}
}

func TestAggregateStandings_UnsortedInput(t *testing.T) {
	// provider ส่งล่าสุดก่อน (desc) -> ต้องยังเรียงอันดับ/ฟอร์มถูก ( sorter ภายใน )
	fixtures := []goalFixture{
		finishedFixture("2026-09-14T18:00:00Z", "1", "Alpha", 2, "2", "Beta", 0),
		finishedFixture("2026-08-10T18:00:00Z", "1", "Alpha", 0, "2", "Beta", 1),
	}
	rows := aggregateStandingsFromFixtures(fixtures)
	if rows[0].Name != "Alpha" || rows[0].Pts != 3 || rows[0].Form != "LW" {
		t.Errorf("ต้องเรียงตามเวลาภายในได้ถูก: %+v", rows[0])
	}
}

func TestCurrentSeasonStart(t *testing.T) {
	cases := []struct {
		now  time.Time
		want string
	}{
		{time.Date(2026, time.October, 23, 0, 0, 0, 0, time.UTC), "2026-07-01"}, // กลางฤดูกาล 2026/27
		{time.Date(2027, time.March, 5, 0, 0, 0, 0, time.UTC), "2026-07-01"},    // ครึ่งหลังฤดูกาล 2026/27
		{time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC), "2026-07-01"},     // วันแรกพอดี
		{time.Date(2026, time.June, 30, 0, 0, 0, 0, time.UTC), "2025-07-01"},    // วันสุดท้ายฤดูกาลก่อน
		{time.Date(2027, time.December, 1, 0, 0, 0, 0, time.UTC), "2027-07-01"}, // ฤดูกาลถัดไป
	}
	for _, c := range cases {
		got := currentSeasonStart(c.now).Format("2006-01-02")
		if got != c.want {
			t.Errorf("currentSeasonStart(%s) = %s ต้องการ %s", c.now.Format("2006-01-02"), got, c.want)
		}
	}
}

func TestLastNForm(t *testing.T) {
	// input เรียงเก่า->ใหม่ -> เก็บ 5 ตัวท้าย คงลำดับไว้
	if got := lastNForm([]rune("WWDLWLL"), 5); got != "DLWLL" {
		t.Errorf("lastNForm 7 ตัว -> ต้องเก็บ 5 ตัวท้าย DLWLL ได้ %s", got)
	}
	if got := lastNForm([]rune("WD"), 5); got != "WD" {
		t.Errorf("lastNForm สั้นกว่า n ต้องคืนตามเดิม ได้ %s", got)
	}
	if got := lastNForm(nil, 5); got != "" {
		t.Errorf("lastNForm ว่างต้องคืน \"\" ได้ %s", got)
	}
}

// TestAggregateStandings_NoGuessZeroZero — แมตช์จบแต่สกอร์หายทั้งคู่
// ห้ามเดา 0-0 (ข้อมูลผิดรูป) ต้องไม่เข้าตารางเลย
func TestAggregateStandings_NoGuessZeroZero(t *testing.T) {
	fixtures := []goalFixture{
		{KickoffUTC: "2026-08-10T18:00:00Z", MatchStatus: "FINISHED", HomeTeamID: "1", HomeTeamName: "Alpha", AwayTeamID: "2", AwayTeamName: "Beta"},
	}
	rows := aggregateStandingsFromFixtures(fixtures)
	if len(rows) != 0 {
		t.Errorf("ห้ามเดา 0-0 -> ต้องได้ 0 ทีม ได้ %d", len(rows))
	}
}
