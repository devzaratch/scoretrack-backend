package main

// standings_compute.go — ตารางคะแนน "คำนวณจากผลแมตช์จริง" ของฤดูกาลปัจจุบัน
//
// ทำไมต้องมี: ตอนตารางคะแนนต้นทางล่ม (GOAL /leagues/{id}/standings ตอบ 502)
// สาย fallback เดิมพุ่งผ่าน RapidAPI (ไม่มี key) -> TheSportsDB (ฤดูกาล 2024-25 เก่า)
// -> hardcode ฤดูกาลเก่า = ผู้ใช้เห็นอันดับปลอมของปีก่อนทั้งตาราง
//
// จุดนี้คั่นกลาง: ดึงผลแมตช์ "จบแล้ว" (FINISHED) ของฤดูกาลปัจจุบันจาก
// GET /leagues/{ulid}/fixtures?status=FINISHED&from=&to= (endpoint คนละตัวกับ
// standings — standings ล่มแต่ fixtures มักยังรอด) แล้วคำนวณ W/D/L/GF/GA/Pts เอง
// -> ตารางจริงจากผลจริง ถึงจะยังไม่ครบ 38 นัดแต่ไม่มีทางผิดแบบ hardcode ปีก่อน
//
// ที่มาของ tableSource:
//   - "computed-live"    = คำนวณจากผลแมตช์จบแล้วของฤดูกาลนี้ (ข้อมูลจริง cache ได้เต็มที่)
//   - "hardcoded-stale"  = hardcode ฤดูกาลเก่า (ตัวสุดท้าย handleProxy cache สั้น 3 นาที)

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// completedStandingTeam เก็บสถิติสะสมของ 1 ทีมระหว่างกวาดผลแมตช์
type completedStandingTeam struct {
	teamID    string
	name      string
	logo      string
	played    int
	wins      int
	draws     int
	losses    int
	gf        int
	ga        int
	formChars []rune // ผลทุกนัด (W/D/L) เรียงเก่า->ใหม่ (sort ตาม kickoff ก่อนกวาดแล้ว)
}

// currentSeasonStart คืนวันที่ 1 กรกฎาคมของฤดูกาลปัจจุบัน (ปฏิทินฟุตบอลยุโรป)
// ต.ค. 2026 -> 2026-07-01 | มี.ค. 2027 -> 2026-07-01 (ฤดูกาล 2026/27)
func currentSeasonStart(now time.Time) time.Time {
	year := now.Year()
	if now.Month() < time.July {
		year--
	}
	return time.Date(year, time.July, 1, 0, 0, 0, 0, time.UTC)
}

// aggregateStandingsFromFixtures รวมผลแมตช์ที่ "จบแล้ว" เป็นตารางคะแนน
// (pure function -> ทดสอบได้โดยไม่ต้องยิง API)
//
// เรียงตามกติกาสากล: คะแนนมากสุด -> ผลต่างประตูได้-เสีย -> ได้ประตูมากสุด -> ชื่อทีม
// ฟอร์ม 5 นัดหลังแนบเป็น string "WWDLW" (เก่า->ใหม่ ตามที่ client ตัด slice(-5))
func aggregateStandingsFromFixtures(fixtures []goalFixture) []CalculatedStanding {
	teams := map[string]*completedStandingTeam{}

	// เรียงตาม kickoff ก่อนเพื่อให้ฟอร์มเรียงลำดับเวลาถูกต้อง
	// ไม่ว่า provider จะส่งมากี่แบบ (GOAL ส่งล่าสุดก่อน)
	sorted := make([]goalFixture, len(fixtures))
	copy(sorted, fixtures)
	sortFixturesByKickoff(sorted)

	for _, f := range sorted {
		status := strings.ToUpper(strings.TrimSpace(f.MatchStatus))
		isFinished := status == "FINISHED" || status == "FT" || status == "ENDED"
		if !isFinished {
			continue
		}
		// แมตช์จบแล้วต้องมีสกอร์อย่างน้อยฝั่งใดฝั่งหนึ่ง
		// (ทั้งคู่ nil = ข้อมูลผิดรูป -> ข้าม อย่าเดา 0-0)
		if f.HomeTeamFT == nil && f.HomeTeamScore == nil && f.AwayTeamFT == nil && f.AwayTeamScore == nil {
			continue
		}
		homeName := strings.TrimSpace(f.HomeTeamName)
		awayName := strings.TrimSpace(f.AwayTeamName)
		if homeName == "" || awayName == "" || f.HomeTeamID == "" || f.AwayTeamID == "" {
			continue
		}
		homeGoals := goalIntScore(firstNonNil(f.HomeTeamFT, f.HomeTeamScore))
		awayGoals := goalIntScore(firstNonNil(f.AwayTeamFT, f.AwayTeamScore))

		home := getOrCreateStandingTeam(teams, f.HomeTeamID, homeName, f.TeamHomeBadge)
		away := getOrCreateStandingTeam(teams, f.AwayTeamID, awayName, f.TeamAwayBadge)
		home.played++
		away.played++
		home.gf += homeGoals
		home.ga += awayGoals
		away.gf += awayGoals
		away.ga += homeGoals
		switch {
		case homeGoals > awayGoals:
			home.wins++
			away.losses++
			home.formChars = append(home.formChars, 'W')
			away.formChars = append(away.formChars, 'L')
		case homeGoals < awayGoals:
			home.losses++
			away.wins++
			home.formChars = append(home.formChars, 'L')
			away.formChars = append(away.formChars, 'W')
		default:
			home.draws++
			away.draws++
			home.formChars = append(home.formChars, 'D')
			away.formChars = append(away.formChars, 'D')
		}
	}

	rows := make([]CalculatedStanding, 0, len(teams))
	for _, t := range teams {
		rows = append(rows, CalculatedStanding{
			TeamID:    atoiOr(t.teamID, 0),
			UID:       t.teamID, // ULID ของ GOAL -> client ใช้ลิงก์ /team/{id} (TeamID int ใช้ไม่ได้กับ ULID)
			Name:      t.name,
			Played:    t.played,
			Wins:      t.wins,
			Draws:     t.draws,
			Losses:    t.losses,
			GF:        t.gf,
			GA:        t.ga,
			GD:        t.gf - t.ga,
			ScoresStr: fmt.Sprintf("%d:%d", t.gf, t.ga),
			Pts:       t.wins*3 + t.draws,
			Logo:      t.logo,
			Form:      lastNForm(t.formChars, 5),
		})
	}

	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Pts != rows[j].Pts {
			return rows[i].Pts > rows[j].Pts
		}
		if rows[i].GD != rows[j].GD {
			return rows[i].GD > rows[j].GD
		}
		if rows[i].GF != rows[j].GF {
			return rows[i].GF > rows[j].GF
		}
		return rows[i].Name < rows[j].Name
	})
	for i := range rows {
		rows[i].Rank = i + 1
	}
	return rows
}

// lastNForm คืนฟอร์ม n นัดล่าสุดเป็น string เรียงเก่า->ใหม่ ("WWDLW")
// formChars มาเรียงเก่า->ใหม่อยู่แล้ว (ถูก sortFixturesByKickoff ก่อนกวาด)
// -> เก็บ n ตัวท้าย (นัดล่าสุด) คงลำดับเวลาไว้ ไม่ต้องกลับด้าน
func lastNForm(chars []rune, n int) string {
	if len(chars) == 0 {
		return ""
	}
	take := chars
	if len(take) > n {
		take = take[len(take)-n:]
	}
	return string(take)
}

// sortFixturesByKickoff เรียงแมตช์เก่า->ใหม่ตามเวลาเตะ (ข้ามคู่ที่ไม่มีเวลา)
func sortFixturesByKickoff(list []goalFixture) {
	sort.SliceStable(list, func(i, j int) bool {
		ki, _ := time.Parse(time.RFC3339, list[i].KickoffUTC)
		kj, _ := time.Parse(time.RFC3339, list[j].KickoffUTC)
		if !ki.IsZero() && !kj.IsZero() {
			return ki.Before(kj)
		}
		return list[i].KickoffUTC < list[j].KickoffUTC
	})
}

// getOrCreateStandingTeam หยิบ/สร้างสถิติทีม (key = teamId ของ provider)
func getOrCreateStandingTeam(teams map[string]*completedStandingTeam, id, name, logo string) *completedStandingTeam {
	if t, ok := teams[id]; ok {
		// เติมชื่อ/โลโก้ถ้ายังไม่มี (แมตช์แรกบางคู่ badge ว่าง)
		if t.name == "" {
			t.name = name
		}
		if t.logo == "" {
			t.logo = logo
		}
		return t
	}
	t := &completedStandingTeam{teamID: id, name: name, logo: logo}
	teams[id] = t
	return t
}

// fetchComputedLeagueStandings ดึงผลแมตช์ FINISHED ของฤดูกาลปัจจุบันจาก GOAL
// แล้วคำนวณตารางคะแนนเอง -> (ตาราง, รายการแมตช์จริงล่าสุด, ok)
//
// ตั้งใจใช้จากสาย fallback ตอนตารางคะแนนต้นทางล่มเท่านั้น — ถ้า provider
// ปกติอยู่แล้ว สายหลัก (fetchLeagueFromProviders) ตอบไปก่อนแล้ว
func fetchComputedLeagueStandings(leagueID string) ([]CalculatedStanding, []map[string]interface{}, bool) {
	gp := goalProvider()
	if gp == nil || !gp.Enabled() {
		return nil, nil, false
	}
	ulid, err := gp.resolveLeagueULID(leagueID)
	if err != nil {
		logInfof("ℹ️ standings %s: คำนวณตารางเองไม่ได้ (resolve ULID): %v", leagueID, err)
		return nil, nil, false
	}
	now := time.Now().UTC()
	from := currentSeasonStart(now).Format("2006-01-02")
	to := now.Format("2006-01-02")

	// ⚠️ GOAL จำกัด limit <= 100 (เกิน = 400 Bad Request — ทดสอบจริง 2026-10-23)
	// ฤดูกาลเต็มของลีก 20 ทีม = 380 นัด -> ต้องแบ่งหน้าด้วย offset เก็บจนครบ
	fixtures := make([]goalFixture, 0, 256)
	for offset := 0; offset <= 600; offset += 100 {
		path := fmt.Sprintf("/leagues/%s/fixtures?status=FINISHED&from=%s&to=%s&limit=100&offset=%d", ulid, from, to, offset)
		body, err := gp.do(path)
		if err != nil || len(body) == 0 {
			if offset == 0 {
				logInfof("ℹ️ standings %s: ดึงผลแมตช์จบแล้วไม่ได้ -> ใช้ fallback ถัดไป (%v)", leagueID, err)
				return nil, nil, false
			}
			break // หน้าถัดไปล้ม -> ใช้ที่เก็บได้ก่อนหน้า (ข้อมูลจริงบางส่วนดีกว่าของปลอม)
		}
		var resp goalFixturesResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			if offset == 0 {
				logInfof("ℹ️ standings %s: ผลแมตช์จบแล้วพัง (%v)", leagueID, err)
				return nil, nil, false
			}
			break
		}
		fixtures = append(fixtures, resp.Data...)
		// หน้าแรกว่าง = ไม่มีข้อมูลเลย | pagination.total = จำนวนทั้งหมดที่มีจริง
		if len(resp.Data) == 0 {
			break
		}
		if resp.Pagination != nil && len(fixtures) >= resp.Pagination.Total {
			break
		}
		if len(resp.Data) < 100 {
			break // หน้าสุดท้ายมาน้อยกว่า limit = หมดแล้ว
		}
	}
	if len(fixtures) == 0 {
		logInfof("ℹ️ standings %s: ผลแมตช์จบแล้วว่าง", leagueID)
		return nil, nil, false
	}

	rows := aggregateStandingsFromFixtures(fixtures)
	if len(rows) < 2 {
		// ฤดูกาลเพิ่งเริ่มหรือข้อมูลยังไม่เข้าที่ -> ให้ fallback ถัดไปตัดสิน
		logInfof("ℹ️ standings %s: คำนวณตารางได้ทีมน้อยกว่า 2 -> ใช้ fallback ถัดไป", leagueID)
		return nil, nil, false
	}

	// รายการแมตช์จริง (ล่าสุดก่อน ตามที่ provider ส่งมา) ให้หน้าลีกใช้แทน
	// sampleMatches hardcode เดิม (แมตช์ปลอม Liverpoool 2-1 Man City ถูกแทนที่แล้ว)
	items := make([]map[string]interface{}, 0, 24)
	seen := map[string]bool{}
	// provider ส่งล่าสุดก่อนอยู่แล้ว -> เติมจากต้น list จนครบ 24
	for _, f := range fixtures {
		gp.rememberFixture(f) // ทุกแมตช์ในฤดูกาล -> /match/{id} resolve ULID ได้หมด
		if len(items) >= 24 {
			continue
		}
		key := f.APIID
		if key == "" {
			key = f.ID
		}
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		items = append(items, goalLeagueFixtureItem(f))
	}
	return rows, items, true
}
