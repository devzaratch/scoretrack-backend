package main

// ---------------------------------------------------------------------------
// Phase 1b P2 — หน้าทีม (/team/:id, /squad, /fixtures) จาก GOAL API
//
//  สัญญาที่พิสูจน์จาก API จริง (2026-10-01):
//   - GET /teams/{ulid}              -> name/country/founded/badge/venue*/leagues[]/coaches
//   - GET /teams/{ulid}/players      -> type ("Midfielders")/number/country/goals/assists
//   - GET /teams/{ulid}/fixtures?from=&to=&limit= -> kickoffUtc/matchStatus/score/badges
//     (คืน descend, มี pagination.hasMore)
//
//  JSON ที่คืนถูกจูนให้ team/client.tsx อ่านออกตรงๆ:
//   - kind "":        {details:{id,name,country,primaryLeagueName}, venue:{widget,statPairs},
//                      overview:{form:[W/D/L], nextMatch}, table:[{data:{table:{all,home,away}}}],
//                      name, country, primaryLeagueName, data_source}
//   - kind "squad":   {teamName, squad:[{title:"coach|keepers|defenders|midfielders|attackers|others",
//                      members:[{id,name,role,shirtNumber,cname}]}]}
//   - kind "fixtures": {fixtures:[{id,timeUTC,status:{finished,scoreStr,reason:{short}},
//                      home:{id,name,logo}, away:{id,name,logo}}], tournament}
// ---------------------------------------------------------------------------

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ---------- รูปคำตอบจาก GOAL ----------

type goalTeamLeagueRef struct {
	Season    string `json:"season"`
	CreatedAt string `json:"createdAt"`
	UpdatedAt string `json:"updatedAt"`
	League    struct {
		ID     string `json:"id"`
		Name   string `json:"name"`
		Season string `json:"season"`
		Logo   string `json:"logo"`
	} `json:"league"`
}

type goalTeamDetailEnvelope struct {
	Success bool `json:"success"`
	Data    struct {
		ID          string `json:"id"`
		APIID       string `json:"apiId"`
		Name        string `json:"name"`
		Country     string `json:"country"`
		Founded     string `json:"founded"`
		Badge       string `json:"badge"`
		VenueName   string `json:"venueName"`
		VenueCity   string `json:"venueCity"`
		VenueCap    string `json:"venueCapacity"`
		VenueStreet string `json:"venueAddress"`
		VenueGrass  string `json:"venueSurface"`
		Leagues     []goalTeamLeagueRef
		Coaches     json.RawMessage `json:"coaches"`
	} `json:"data"`
	Error string `json:"error"`
}

type goalTeamPlayer struct {
	ID      string  `json:"id"`
	APIID   string  `json:"apiId"`
	Name    string  `json:"name"`
	Image   *string `json:"image"`
	Number  *string `json:"number"`
	Country *string `json:"country"`
	Type    *string `json:"type"`
	Age     *string `json:"age"`
	Captain bool    `json:"isCaptain"`
	Goals   *string `json:"goals"`
	Assists *string `json:"assists"`
	Rating  *string `json:"rating"`
	Team    *struct {
		ID    string `json:"id"`
		Name  string `json:"name"`
		Badge string `json:"badge"`
	} `json:"team"`
}

type goalTeamPlayersResponse struct {
	Success bool             `json:"success"`
	Data    []goalTeamPlayer `json:"data"`
	Error   string           `json:"error"`
}

type goalTeamCoach struct {
	ID      string  `json:"id"`
	Name    string  `json:"name"`
	Country *string `json:"country"`
}

// ---------- จุดเข้าหลัก ----------

// TeamData อ่านข้อมูลหน้าทีมจาก GOAL (kind: "" | "squad" | "fixtures")
func (p *GoalProvider) TeamData(kind, teamID string) ([]byte, error) {
	if !p.Enabled() {
		return nil, fmt.Errorf("goalapi disabled")
	}
	ulid, err := p.resolveTeamULID(teamID)
	if err != nil {
		return nil, err
	}
	switch kind {
	case "", "details":
		return p.teamOverviewJSON(ulid, teamID)
	case "squad":
		return p.teamSquadJSON(ulid, teamID)
	case "fixtures":
		return p.teamFixturesJSON(ulid, teamID)
	}
	return nil, fmt.Errorf("team kind %q ไม่รองรับ", kind)
}

// resolveTeamULID เปลี่ยน id ที่หน้าเว็บส่งมา (ULID ตรงๆ หรือเลข apiId เก่า)
// เป็น ULID ของ GOAL โดยไม่กินโควตา (อ่านจาก registry ที่ warm ไว้แล้ว)
func (p *GoalProvider) resolveTeamULID(teamID string) (string, error) {
	id := strings.TrimSpace(teamID)
	if id == "" {
		return "", fmt.Errorf("team id ว่าง")
	}
	if isGoalULID(id) {
		return id, nil
	}
	if ref, ok := registry.Lookup("team", id); ok && ref.Source == p.Name() && ref.ULID != "" {
		return ref.ULID, nil
	}
	return "", fmt.Errorf("team %s ไม่อยู่ใน goal registry", id)
}

// ---------- kind "" : ภาพรวมทีม ----------

func (p *GoalProvider) teamOverviewJSON(ulid, teamID string) ([]byte, error) {
	detailBody, err := p.do("/teams/" + ulid)
	if err != nil {
		return nil, err
	}
	var det goalTeamDetailEnvelope
	if err := json.Unmarshal(detailBody, &det); err != nil {
		return nil, err
	}
	if det.Data.ID == "" || det.Data.Name == "" {
		return nil, fmt.Errorf("goalapi team detail ว่าง (%s)", teamID)
	}
	d := det.Data

	// เก็บเลข apiId -> ULID ให้ลิงก์ /team/{เลขเก่า} เปิดได้ในครั้งต่อไป
	if d.APIID != "" {
		registry.Remember("team", d.APIID, p.Name(), d.ID)
	}

	// โปรแกรม (ใช้คำนวณฟอร์ม + นัดถัดไป) — หน้าต่างกว้างครอบทั้งฤดูกาล
	fixtures, ferr := p.teamFixtureWindow(ulid, -180, 210)
	if ferr != nil && len(fixtures) == 0 {
		return nil, ferr
	}
	now := time.Now().UTC()
	sort.SliceStable(fixtures, func(i, j int) bool {
		return fixtures[i].KickoffUTC < fixtures[j].KickoffUTC
	})

	// ฟอร์ม 5 นัดหลัง (เรียงเก่า->ใหม่ ตามที่ client แสดงซ้ายไปขวา)
	form := []string{}
	for _, f := range fixtures {
		if len(form) >= 5 {
			break
		}
		ki, kerr := time.Parse(time.RFC3339, f.KickoffUTC)
		if kerr != nil || ki.After(now) || !goalFixtureFinished(f) {
			continue
		}
		if r := goalFixtureResult(f, ulid); r != "" {
			form = append(form, r)
		}
	}

	// นัดถัดไป
	var nextMatch map[string]interface{}
	for _, f := range fixtures {
		ki, kerr := time.Parse(time.RFC3339, f.KickoffUTC)
		if kerr != nil || ki.Before(now) || goalFixtureFinished(f) {
			continue
		}
		p.rememberFixture(f)
		nextMatch = goalTeamFixtureItem(f)
		break
	}

	// ลีกปัจจุมัน (ใช้เป็นตารางคะแนน + ชื่อ badge บนหัวหน้า)
	leagueName := pickTeamPrimaryLeague(d.Leagues, now)

	out := map[string]interface{}{
		"details": map[string]interface{}{
			"id":                firstNonEmptyStr(d.APIID, teamID),
			"name":              d.Name,
			"shortName":         d.Name,
			"country":           d.Country,
			"primaryLeagueName": leagueName,
			"logo":              d.Badge,
			"founded":           d.Founded,
		},
		"name":                d.Name,
		"country":             d.Country,
		"primaryLeagueName":   leagueName,
		"allAvailableSeasons": []string{},
		"data_source":         "goalapi",
	}

	if nextMatch != nil {
		out["overview"] = map[string]interface{}{
			"form":      form,
			"nextMatch": nextMatch,
		}
		out["nextMatch"] = nextMatch
	} else {
		out["overview"] = map[string]interface{}{"form": form}
	}
	if len(form) > 0 {
		out["teamForm"] = form
	}

	if d.VenueName != "" {
		statPairs := []interface{}{}
		if d.VenueCap != "" {
			statPairs = append(statPairs, []interface{}{"Capacity", d.VenueCap})
		}
		if d.VenueGrass != "" {
			statPairs = append(statPairs, []interface{}{"Surface", d.VenueGrass})
		}
		if d.Founded != "" {
			statPairs = append(statPairs, []interface{}{"Opened", d.Founded})
		}
		out["venue"] = map[string]interface{}{
			"widget": map[string]interface{}{
				"name":     d.VenueName,
				"city":     d.VenueCity,
				"location": []string{d.VenueStreet, d.VenueCity},
			},
			"statPairs": statPairs,
		}
	}

	// ตารางคะแนนลีกปัจจุบัน (1 call — ถ้าลีกไม่มี standings ข้าม ไม่ให้หน้าพัง)
	if lgULID := pickTeamPrimaryLeagueULID(d.Leagues, now); lgULID != "" {
		if tableJSON, terr := p.leagueTableJSON(lgULID, lgULID); terr == nil && len(tableJSON) > 0 {
			var tbl map[string]interface{}
			if json.Unmarshal(tableJSON, &tbl) == nil {
				lgName := leagueName
				if s, ok := tbl["leagueName"].(string); ok && s != "" {
					lgName = s
				}
				out["table"] = []interface{}{
					map[string]interface{}{
						"leagueName": lgName,
						"data": map[string]interface{}{
							"leagueName": lgName,
							"table": map[string]interface{}{
								"all":  tbl["all"],
								"home": tbl["home"],
								"away": tbl["away"],
							},
						},
					},
				}
			}
		}
	}

	return json.Marshal(out)
}

// pickTeamPrimaryLeagueULID เลือกลีกหลักของทีม (ฤดูกาลปัจจุบัน อัปเดตล่าสุด ไม่ใช่ปรีซีซัน)
func pickTeamPrimaryLeague(leagues []goalTeamLeagueRef, now time.Time) string {
	lg := pickTeamPrimaryLeagueRef(leagues, now)
	if lg == nil {
		return ""
	}
	return lg.League.Name
}

func pickTeamPrimaryLeagueULID(leagues []goalTeamLeagueRef, now time.Time) string {
	lg := pickTeamPrimaryLeagueRef(leagues, now)
	if lg == nil {
		return ""
	}
	return lg.League.ID
}

func pickTeamPrimaryLeagueRef(leagues []goalTeamLeagueRef, now time.Time) *goalTeamLeagueRef {
	year := strconv.Itoa(now.Year())
	best := -1
	var bestRef *goalTeamLeagueRef
	for i := range leagues {
		lg := &leagues[i]
		if lg.League.ID == "" {
			continue
		}
		score := 0
		if strings.Contains(lg.League.Season, year) || strings.Contains(lg.Season, year) {
			score += 3
		}
		if t, err := time.Parse(time.RFC3339, lg.UpdatedAt); err == nil && now.Sub(t) < 45*24*time.Hour {
			score += 2
		}
		name := strings.ToLower(lg.League.Name)
		if strings.Contains(name, "friendly") || strings.Contains(name, "summer series") ||
			strings.Contains(name, "pre-season") || strings.Contains(name, "club") {
			score -= 5
		}
		if score > best {
			best = score
			bestRef = lg
		}
	}
	return bestRef
}

// ---------- kind "squad" : รายชื่อนักเตะ ----------

func (p *GoalProvider) teamSquadJSON(ulid, teamID string) ([]byte, error) {
	playersBody, err := p.do("/teams/" + ulid + "/players")
	if err != nil {
		return nil, err
	}
	var resp goalTeamPlayersResponse
	if err := json.Unmarshal(playersBody, &resp); err != nil {
		return nil, err
	}
	if len(resp.Data) == 0 {
		return nil, fmt.Errorf("goalapi team players ว่าง (%s)", teamID)
	}

	teamName := ""
	if resp.Data[0].Team != nil {
		teamName = resp.Data[0].Team.Name
	}

	// coaches มาจาก /teams/{ulid} (คนเดียวหรือ array ก็รองรับทั้งคู่)
	coaches := []goalTeamCoach{}
	if detBody, derr := p.do("/teams/" + ulid); derr == nil {
		var det goalTeamDetailEnvelope
		if json.Unmarshal(detBody, &det) == nil && det.Data.Name != "" {
			teamName = det.Data.Name
			coaches = parseGoalCoaches(det.Data.Coaches)
		}
	}

	order := []string{"coach", "keepers", "defenders", "midfielders", "attackers", "others"}
	buckets := map[string][]map[string]interface{}{}

	if len(coaches) > 0 {
		members := make([]map[string]interface{}, 0, len(coaches))
		for _, c := range coaches {
			m := map[string]interface{}{
				"id":   c.ID,
				"name": c.Name,
				"role": "Coach",
			}
			if c.Country != nil && *c.Country != "" {
				m["cname"] = *c.Country
			}
			members = append(members, m)
		}
		buckets["coach"] = members
	}

	for _, pl := range resp.Data {
		key := squadGroupTitle(pl.Type)
		m := map[string]interface{}{
			"id":   pl.ID,
			"name": pl.Name,
		}
		if pl.Type != nil && *pl.Type != "" {
			role := *pl.Type
			if pl.Captain {
				role += " • C"
			}
			m["role"] = role
		} else if pl.Captain {
			m["role"] = "Captain"
		}
		if pl.Number != nil && *pl.Number != "" {
			m["shirtNumber"] = *pl.Number
		}
		if pl.Country != nil && *pl.Country != "" {
			m["cname"] = *pl.Country
		}
		buckets[key] = append(buckets[key], m)
	}

	// เรียงเบอร์เสื้อ (ว่างอยู่ท้าย) ตามด้วยชื่อ
	for _, members := range buckets {
		sort.SliceStable(members, func(i, j int) bool {
			ni, _ := members[i]["shirtNumber"].(string)
			nj, _ := members[j]["shirtNumber"].(string)
			if ni != nj {
				if ni == "" {
					return false
				}
				if nj == "" {
					return true
				}
				bi, ei := strconv.Atoi(ni)
				bj, ej := strconv.Atoi(nj)
				if ei == nil && ej == nil {
					return bi < bj
				}
				return ni < nj
			}
			si, _ := members[i]["name"].(string)
			sj, _ := members[j]["name"].(string)
			return si < sj
		})
	}

	groups := make([]map[string]interface{}, 0, len(order))
	for _, title := range order {
		if members, ok := buckets[title]; ok && len(members) > 0 {
			groups = append(groups, map[string]interface{}{
				"title":   title,
				"members": members,
			})
		}
	}
	if len(groups) == 0 {
		return nil, fmt.Errorf("goalapi team squad ว่าง (%s)", teamID)
	}

	out := map[string]interface{}{
		"teamName":    teamName,
		"squad":       groups,
		"data_source": "goalapi",
	}
	return json.Marshal(out)
}

// squadGroupTitle แปลง type ของ GOAL เป็น title ที่ client รู้จัก
func squadGroupTitle(t *string) string {
	if t == nil {
		return "others"
	}
	switch strings.ToLower(strings.TrimSpace(*t)) {
	case "goalkeeper", "goalkeepers", "keeper", "keepers":
		return "keepers"
	case "defender", "defenders":
		return "defenders"
	case "midfielder", "midfielders":
		return "midfielders"
	case "forward", "forwards", "attacker", "attackers":
		return "attackers"
	case "coach", "coaching":
		return "coach"
	}
	return "others"
}

func parseGoalCoaches(raw json.RawMessage) []goalTeamCoach {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var many []goalTeamCoach
	if err := json.Unmarshal(raw, &many); err == nil {
		return many
	}
	var one goalTeamCoach
	if err := json.Unmarshal(raw, &one); err == nil && one.Name != "" {
		return []goalTeamCoach{one}
	}
	return nil
}

// ---------- kind "fixtures" : โปรแกรม + ผลการแข่งขัน ----------

func (p *GoalProvider) teamFixturesJSON(ulid, teamID string) ([]byte, error) {
	fixtures, err := p.teamFixtureWindow(ulid, -180, 210)
	if err != nil {
		return nil, err
	}
	if len(fixtures) == 0 {
		return nil, fmt.Errorf("goalapi team fixtures ว่าง (%s)", teamID)
	}

	// เรียงเก่า -> ใหม่ ตาม kickoff (GOAL คืน descend)
	sort.SliceStable(fixtures, func(i, j int) bool {
		ki, _ := time.Parse(time.RFC3339, fixtures[i].KickoffUTC)
		kj, _ := time.Parse(time.RFC3339, fixtures[j].KickoffUTC)
		if !ki.IsZero() && !kj.IsZero() {
			return ki.Before(kj)
		}
		return fixtures[i].KickoffUTC < fixtures[j].KickoffUTC
	})

	items := make([]map[string]interface{}, 0, len(fixtures))
	for _, f := range fixtures {
		p.rememberFixture(f) // ผูก match_id -> ULID ให้หน้า /match/[id] เปิดได้
		items = append(items, goalTeamFixtureItem(f))
	}

	out := map[string]interface{}{
		"fixtures":    items,
		"allFixtures": items,
		"data_source": "goalapi",
	}
	return json.Marshal(out)
}

// teamFixtureWindow ดึงโปรแกรมทีมในช่วง [today+from, today+to] (วนสูงสุด 3 หน้า)
func (p *GoalProvider) teamFixtureWindow(ulid string, fromDays, toDays int) ([]goalFixture, error) {
	now := time.Now().UTC()
	from := now.AddDate(0, 0, fromDays).Format("2006-01-02")
	to := now.AddDate(0, 0, toDays).Format("2006-01-02")

	seen := map[string]bool{}
	all := make([]goalFixture, 0, 64)
	var lastErr error
	for offset, page := 0, 0; page < 3; page++ {
		path := fmt.Sprintf("/teams/%s/fixtures?from=%s&to=%s&limit=100&offset=%d", ulid, from, to, offset)
		body, err := p.do(path)
		if err != nil {
			lastErr = err
			break
		}
		var resp goalFixturesResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			lastErr = err
			break
		}
		if len(resp.Data) == 0 {
			break
		}
		for _, f := range resp.Data {
			key := f.APIID
			if key == "" {
				key = f.ID
			}
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			all = append(all, f)
		}
		if resp.Pagination == nil || !resp.Pagination.HasMore {
			break
		}
		offset += len(resp.Data)
	}
	if len(all) == 0 && lastErr != nil {
		return nil, lastErr
	}
	return all, nil
}

// goalFixtureFinished ตรวจว่าแข่งจบแล้ว (ใช้ทั้ง form และ nextMatch)
func goalFixtureFinished(f goalFixture) bool {
	s := goalStatusString(f)
	return s == "FT" || s == "AET" || s == "PEN" || f.MatchStatus == "FINISHED"
}

// goalFixtureResult คำนวณ W/D/L จากมุมมองทีม ulid
func goalFixtureResult(f goalFixture, teamULID string) string {
	hs := goalIntScore(firstNonNil(f.HomeTeamScore, f.HomeTeamFT))
	as := goalIntScore(firstNonNil(f.AwayTeamScore, f.AwayTeamFT))
	home := f.HomeTeamID == teamULID
	if home {
		if hs > as {
			return "W"
		}
		if hs < as {
			return "L"
		}
		return "D"
	}
	if as > hs {
		return "W"
	}
	if as < hs {
		return "L"
	}
	return "D"
}

// goalTeamFixtureItem แปลง fixture เป็นรูป FixtureMatch ที่ team/client.tsx อ่าน
func goalTeamFixtureItem(f goalFixture) map[string]interface{} {
	statusShort := goalStatusString(f)
	finished := goalFixtureFinished(f)

	homeScore := goalIntScore(firstNonNil(f.HomeTeamScore, f.HomeTeamFT))
	awayScore := goalIntScore(firstNonNil(f.AwayTeamScore, f.AwayTeamFT))
	scoreStr := ""
	if f.HomeTeamScore != nil || f.HomeTeamFT != nil || f.AwayTeamScore != nil || f.AwayTeamFT != nil {
		scoreStr = fmt.Sprintf("%d - %d", homeScore, awayScore)
	}

	_, _, _, homeName, homeBadge, awayName, awayBadge := goalNameBundle(f)

	item := map[string]interface{}{
		"id":      f.APIID,
		"timeUTC": f.KickoffUTC,
		"status": map[string]interface{}{
			"finished": finished,
			"scoreStr": scoreStr,
			"reason": map[string]interface{}{
				"short": statusShort,
			},
		},
		"home": map[string]interface{}{
			"id":   f.HomeTeamID,
			"name": homeName,
			"logo": homeBadge,
		},
		"away": map[string]interface{}{
			"id":   f.AwayTeamID,
			"name": awayName,
			"logo": awayBadge,
		},
	}
	if f.LeagueName != "" {
		item["tournament"] = map[string]interface{}{"name": f.LeagueName}
	}
	return item
}
