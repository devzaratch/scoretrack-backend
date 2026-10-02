package main

// ---------------------------------------------------------------------------
// Phase 1b — หน้าลีก (/league/:id, /table, /fixtures) จาก GOAL API
//
//  สัญญาที่พิสูจน์จาก API จริงแล้ว (2026-10-01):
//   - GET /leagues/{ulid}                 -> name/countryName/season/logo/_count
//   - GET /leagues/{ulid}/standings       -> 20 แถว overall/home/away (string "5")
//   - GET /leagues/{ulid}/fixtures?status=FINISHED&from=&to=&limit=100
//     GET /leagues/{ulid}/fixtures?from=&to=&limit=100       (คืนลำดับ descend)
//   - ทุก call ผ่าน Authorization: Bearer + budget governor ของ GoalProvider
//
//  JSON ที่คืนถูกจูนให้ league/client.tsx อ่านออกตรงๆ:
//   - kind "":        {details:{id,name,country,selectedSeason,logo,totalTeams}, ...}
//   - kind "table":   {leagueName, season, all, home, away} (+ table:[กลุ่ม] ถ้าหลาย stage)
//   - kind "fixtures": {allFixtures:[{id,timeUTC,status:{finished,scoreStr,reason:{short}},
//                      home:{id,name,logo}, away:{id,name,logo}}]}
//   - kind "stats":    {topScorers:[{id?,rank,name,teamName,stat:{value}}],
//                      players:[{header,topThree:[...]}], teams:[{header,items:[...]}]}
// ---------------------------------------------------------------------------

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

// ---------- รูปคำตอบจาก GOAL ----------

type goalLeagueDetailEnvelope struct {
	Success bool `json:"success"`
	Data    struct {
		ID          string `json:"id"`
		APIID       string `json:"apiId"`
		Name        string `json:"name"`
		CountryName string `json:"countryName"`
		Season      string `json:"season"`
		Logo        string `json:"logo"`
		Count       struct {
			Teams     int `json:"teams"`
			Fixtures  int `json:"fixtures"`
			Standings int `json:"standings"`
		} `json:"_count"`
	} `json:"data"`
	Error string `json:"error"`
}

type goalStandingRow struct {
	ID       string `json:"id"`
	LeagueID string `json:"leagueId"`
	TeamID   string `json:"teamId"`
	TeamName string `json:"teamName"`

	FKStageKey string `json:"fkStageKey"`
	StageName  string `json:"stageName"`

	OverallPromotion string `json:"overallPromotion"`
	OverallPosition  string `json:"overallLeaguePosition"`
	OverallPlayed    string `json:"overallLeaguePlayed"`
	OverallW         string `json:"overallLeagueW"`
	OverallD         string `json:"overallLeagueD"`
	OverallL         string `json:"overallLeagueL"`
	OverallGF        string `json:"overallLeagueGF"`
	OverallGA        string `json:"overallLeagueGA"`
	OverallPTS       string `json:"overallLeaguePTS"`

	HomePromotion string `json:"homePromotion"`
	HomePosition  string `json:"homeLeaguePosition"`
	HomePlayed    string `json:"homeLeaguePlayed"`
	HomeW         string `json:"homeLeagueW"`
	HomeD         string `json:"homeLeagueD"`
	HomeL         string `json:"homeLeagueL"`
	HomeGF        string `json:"homeLeagueGF"`
	HomeGA        string `json:"homeLeagueGA"`
	HomePTS       string `json:"homeLeaguePTS"`

	AwayPromotion string `json:"awayPromotion"`
	AwayPosition  string `json:"awayLeaguePosition"`
	AwayPlayed    string `json:"awayLeaguePlayed"`
	AwayW         string `json:"awayLeagueW"`
	AwayD         string `json:"awayLeagueD"`
	AwayL         string `json:"awayLeagueL"`
	AwayGF        string `json:"awayLeagueGF"`
	AwayGA        string `json:"awayLeagueGA"`
	AwayPTS       string `json:"awayLeaguePTS"`

	Team *struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		Badge   string `json:"badge"`
		Country string `json:"country"`
	} `json:"team"`
	League *struct {
		ID     string `json:"id"`
		Name   string `json:"name"`
		Season string `json:"season"`
	} `json:"league"`
}

type goalStandingsEnvelope struct {
	Success  bool              `json:"success"`
	LeagueID string            `json:"leagueId"`
	Data     []goalStandingRow `json:"data"`
	Error    string            `json:"error"`
}

// ---------- จุดเข้าหลัก ----------

// LeagueData อ่านข้อมูลหน้าลีกจาก GOAL (kind: "" | "table" | "fixtures" | "stats")
func (p *GoalProvider) LeagueData(kind, leagueID string) ([]byte, error) {
	if !p.Enabled() {
		return nil, fmt.Errorf("goalapi disabled")
	}
	ulid, err := p.resolveLeagueULID(leagueID)
	if err != nil {
		return nil, err
	}
	switch kind {
	case "", "details":
		return p.leagueOverviewJSON(ulid, leagueID)
	case "table":
		return p.leagueTableJSON(ulid, leagueID)
	case "fixtures":
		return p.leagueFixturesJSON(ulid, leagueID)
	case "stats":
		return p.leagueStatsJSON(ulid, leagueID)
	}
	return nil, fmt.Errorf("league kind %q ไม่รองรับ", kind)
}

// resolveLeagueULID เปลี่ยน id ที่หน้าเว็บส่งมา (apiId "152" หรือ ULID ตรงๆ)
// เป็น ULID ของ GOAL โดยไม่กินโควตา (อ่านจาก registry ที่ warm ไว้แล้ว)
func (p *GoalProvider) resolveLeagueULID(leagueID string) (string, error) {
	id := strings.TrimSpace(leagueID)
	if id == "" {
		return "", fmt.Errorf("league id ว่าง")
	}
	if isGoalULID(id) {
		return id, nil
	}
	if ref, ok := registry.Lookup("league", id); ok && ref.Source == p.Name() && ref.ULID != "" {
		return ref.ULID, nil
	}
	//  registry ยังไม่ครบ -> ลอง build index (อ่านไฟล์/หน่วยความจำก่อน ไม่กินโควตาถ้าข้อมูลพอ)
	if err := p.ensureLeagueIndex(); err == nil {
		if ref, ok := registry.Lookup("league", id); ok && ref.Source == p.Name() && ref.ULID != "" {
			return ref.ULID, nil
		}
	}
	return "", fmt.Errorf("league %s ไม่อยู่ใน goal registry", id)
}

// isGoalULID ตรวจว่าเป็น ULID ของ GOAL (ยาว 25-26 ตัว — ของ GOAL ส่วนใหญ่ 25)
// เช่น cmr7fp1wp2n8yrx061vxmb1a5 และต้องมีตัวอักษรเพื่อไม่ให้เลข id เก่าผ่าน
func isGoalULID(s string) bool {
	if len(s) < 25 || len(s) > 26 {
		return false
	}
	hasLetter := false
	for _, r := range s {
		if r >= '0' && r <= '9' {
			continue
		}
		if r >= 'a' && r <= 'z' {
			hasLetter = true
			continue
		}
		if r >= 'A' && r <= 'Z' {
			hasLetter = true
			continue
		}
		return false
	}
	return hasLetter
}

// ---------- kind "" : รายละเอียดลีก ----------

func (p *GoalProvider) leagueOverviewJSON(ulid, leagueID string) ([]byte, error) {
	body, err := p.do("/leagues/" + ulid)
	if err != nil {
		return nil, err
	}
	var env goalLeagueDetailEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, err
	}
	if env.Data.ID == "" {
		return nil, fmt.Errorf("goalapi league detail ว่าง (%s)", leagueID)
	}
	d := env.Data

	details := map[string]interface{}{
		"id":             firstNonEmptyStr(d.APIID, leagueID),
		"name":           d.Name,
		"country":        firstNonEmptyStr(d.CountryName, "INT"),
		"selectedSeason": d.Season,
	}
	if d.Logo != "" {
		details["logo"] = d.Logo
	}
	if d.Count.Standings > 0 {
		details["totalTeams"] = d.Count.Standings
	} else if d.Count.Teams > 0 {
		details["totalTeams"] = d.Count.Teams
	}
	if d.Count.Fixtures > 0 {
		details["totalMatches"] = d.Count.Fixtures
	}

	out := map[string]interface{}{
		"details":             details,
		"name":                d.Name,
		"country":             firstNonEmptyStr(d.CountryName, "INT"),
		"selectedSeason":      d.Season,
		"allAvailableSeasons": []string{firstNonEmptyStr(d.Season, "2026/2027")},
		"data_source":         "goalapi",
	}
	return json.Marshal(out)
}

// ---------- kind "table" : ตารางคะแนน ----------

func (p *GoalProvider) leagueTableJSON(ulid, leagueID string) ([]byte, error) {
	body, err := p.do("/leagues/" + ulid + "/standings")
	if err != nil {
		return nil, err
	}
	var env goalStandingsEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, err
	}
	if len(env.Data) == 0 {
		return nil, fmt.Errorf("goalapi standings ว่าง (%s)", leagueID)
	}

	// จัดกลุ่มตาม stage (ลีกส่วนใหญ่มี stage เดียว -> คืนรูปตรง)
	groups := map[string][]goalStandingRow{}
	var groupOrder []string
	leagueName, season := "", ""
	for _, r := range env.Data {
		key := r.FKStageKey + "|" + r.StageName
		if _, seen := groups[key]; !seen {
			groupOrder = append(groupOrder, key)
		}
		groups[key] = append(groups[key], r)
		if leagueName == "" && r.League != nil {
			leagueName = r.League.Name
			season = r.League.Season
		}
	}
	sort.Strings(groupOrder)

	buildGroup := func(rows []goalStandingRow) (all, home, away []map[string]interface{}) {
		for _, r := range rows {
			all = append(all, goalStandingToRow(r, "overall"))
			home = append(home, goalStandingToRow(r, "home"))
			away = append(away, goalStandingToRow(r, "away"))
		}
		sortRowsByPos(all)
		sortRowsByPos(home)
		sortRowsByPos(away)
		return
	}

	out := map[string]interface{}{
		"leagueName":  leagueName,
		"season":      season,
		"data_source": "goalapi",
	}

	if len(groupOrder) == 1 {
		all, home, away := buildGroup(groups[groupOrder[0]])
		out["all"] = all
		out["home"] = home
		out["away"] = away
	} else {
		// หลาย stage -> รูป array-of-groups ที่ client อ่านผ่าน tableGroups
		grpItems := make([]map[string]interface{}, 0, len(groupOrder))
		for _, key := range groupOrder {
			all, home, away := buildGroup(groups[key])
			stageLabel := key
			if i := strings.Index(key, "|"); i >= 0 && key[i+1:] != "" {
				stageLabel = key[i+1:]
			}
			grpItems = append(grpItems, map[string]interface{}{
				"name": stageLabel,
				"all":  all,
				"home": home,
				"away": away,
			})
		}
		out["table"] = grpItems
	}
	return json.Marshal(out)
}

// goalStandingToRow แปลงแถว standings ของ GOAL เป็นรูปที่ league/client.tsx อ่าน
// (ค่าจาก GOAL เป็น string ทั้งหมด -> แปลงเป็น number ตรงที่ client ใช้)
func goalStandingToRow(r goalStandingRow, scope string) map[string]interface{} {
	var pos, played, w, d, l, gf, ga, pts int
	promotion := r.OverallPromotion
	switch scope {
	case "home":
		pos = atoiOr(r.HomePosition, 0)
		played = atoiOr(r.HomePlayed, 0)
		w = atoiOr(r.HomeW, 0)
		d = atoiOr(r.HomeD, 0)
		l = atoiOr(r.HomeL, 0)
		gf = atoiOr(r.HomeGF, 0)
		ga = atoiOr(r.HomeGA, 0)
		pts = atoiOr(r.HomePTS, 0)
		promotion = r.HomePromotion
	case "away":
		pos = atoiOr(r.AwayPosition, 0)
		played = atoiOr(r.AwayPlayed, 0)
		w = atoiOr(r.AwayW, 0)
		d = atoiOr(r.AwayD, 0)
		l = atoiOr(r.AwayL, 0)
		gf = atoiOr(r.AwayGF, 0)
		ga = atoiOr(r.AwayGA, 0)
		pts = atoiOr(r.AwayPTS, 0)
		promotion = r.AwayPromotion
	default:
		pos = atoiOr(r.OverallPosition, 0)
		played = atoiOr(r.OverallPlayed, 0)
		w = atoiOr(r.OverallW, 0)
		d = atoiOr(r.OverallD, 0)
		l = atoiOr(r.OverallL, 0)
		gf = atoiOr(r.OverallGF, 0)
		ga = atoiOr(r.OverallGA, 0)
		pts = atoiOr(r.OverallPTS, 0)
	}

	row := map[string]interface{}{
		"idx":         pos,
		"id":          r.TeamID,
		"name":        r.TeamName,
		"played":      played,
		"wins":        w,
		"draws":       d,
		"losses":      l,
		"scoresStr":   fmt.Sprintf("%d:%d", gf, ga),
		"goalConDiff": gf - ga,
		"pts":         pts,
		"form":        []string{},
		"qualColor":   goalZoneColor(promotion),
	}
	if r.Team != nil && r.Team.Badge != "" {
		row["logo"] = r.Team.Badge
	}
	return row
}

func sortRowsByPos(rows []map[string]interface{}) {
	sort.SliceStable(rows, func(i, j int) bool {
		pi, _ := rows[i]["idx"].(int)
		pj, _ := rows[j]["idx"].(int)
		return pi < pj
	})
}

// goalZoneColor แปลงข้อความ promotion เป็นสีโซน (ตาม legend ใน client)
func goalZoneColor(promotion string) string {
	p := strings.ToLower(promotion)
	switch {
	case strings.Contains(p, "champions league"):
		return "#22c55e"
	case strings.Contains(p, "europa"):
		return "#f59e0b"
	case strings.Contains(p, "conference"):
		return "#3b82f6"
	case strings.Contains(p, "relegation"):
		return "#ef4444"
	}
	return ""
}

// ---------- kind "fixtures" : โปรแกรม + ผลการแข่งขัน ----------

func (p *GoalProvider) leagueFixturesJSON(ulid, leagueID string) ([]byte, error) {
	now := time.Now().UTC()
	today := now.Format("2006-01-02")

	// 1) ผลแข่งย้อนหลัง 60 วัน (สถานะ FINISHED เท่านั้น)
	finishedPath := fmt.Sprintf("/leagues/%s/fixtures?status=FINISHED&from=%s&to=%s&limit=100",
		ulid, now.AddDate(0, 0, -60).Format("2006-01-02"), today)
	// 2) ทุกคู่ในหน้าต่าง -1..+75 วัน (รวมวันนี้, คู่กำลังแข่ง, โปรแกรมล่วงหน้า)
	upcomingPath := fmt.Sprintf("/leagues/%s/fixtures?from=%s&to=%s&limit=100",
		ulid, now.AddDate(0, 0, -1).Format("2006-01-02"), now.AddDate(0, 0, 75).Format("2006-01-02"))

	bodyA, errA := p.do(finishedPath)
	bodyB, errB := p.do(upcomingPath)
	if errA != nil && errB != nil {
		return nil, fmt.Errorf("goalapi league fixtures: %v / %v", errA, errB)
	}

	seen := map[string]bool{}
	merged := make([]goalFixture, 0, 128)
	mergeBody := func(body []byte) {
		if len(body) == 0 {
			return
		}
		var resp goalFixturesResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return
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
			merged = append(merged, f)
		}
	}
	mergeBody(bodyA)
	mergeBody(bodyB)
	if len(merged) == 0 {
		return nil, fmt.Errorf("goalapi league fixtures ว่าง (%s)", leagueID)
	}

	// เรียงเก่า -> ใหม่ ตาม kickoff (GOAL คืน come descend)
	sort.SliceStable(merged, func(i, j int) bool {
		ki, _ := time.Parse(time.RFC3339, merged[i].KickoffUTC)
		kj, _ := time.Parse(time.RFC3339, merged[j].KickoffUTC)
		if !ki.IsZero() && !kj.IsZero() {
			return ki.Before(kj)
		}
		return merged[i].KickoffUTC < merged[j].KickoffUTC
	})

	items := make([]map[string]interface{}, 0, len(merged))
	for _, f := range merged {
		p.rememberFixture(f) // ผูก match_id -> ULID ให้หน้า /match/[id] เปิดได้
		items = append(items, goalLeagueFixtureItem(f))
	}

	out := map[string]interface{}{
		"allFixtures": items,
		"data_source": "goalapi",
	}
	return json.Marshal(out)
}

// goalLeagueFixtureItem แปลง fixture เป็นรูป MatchItem ที่ client อ่าน
func goalLeagueFixtureItem(f goalFixture) map[string]interface{} {
	statusShort := goalStatusString(f)
	finished := statusShort == "FT" || statusShort == "AET" || statusShort == "PEN"

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
	return item
}

// ---------- kind "stats" : ดาวซัลโว / แอสซิสต์ / สถิติทีม ----------

// goalTopScorerRow แถวจาก GET /leagues/{ulid}/top-scorers (พิสูจน์จาก API จริง 2026-10-02)
// playerKey = apiId ของนักเตะ (ตรงกับ field apiId ของ /players?search=)
type goalTopScorerRow struct {
	PlayerPlace  string `json:"playerPlace"`
	PlayerName   string `json:"playerName"`
	PlayerKey    string `json:"playerKey"`
	TeamName     string `json:"teamName"`
	TeamKey      string `json:"teamKey"`
	Goals        string `json:"goals"`
	Assists      string `json:"assists"`
	PenaltyGoals string `json:"penaltyGoals"`
}

type goalTopScorersResponse struct {
	Success  bool               `json:"success"`
	LeagueID string             `json:"leagueId"`
	Data     []goalTopScorerRow `json:"data"`
	Error    string             `json:"error"`
}

// leagueStatsJSON สรุปสถิติหน้าลีกให้ league/client.tsx อ่านออกตรง ๆ
// คืนรูป: {topScorers:[...], players:[{header,topThree}], teams:[{header,items}], data_source}
func (p *GoalProvider) leagueStatsJSON(ulid, leagueID string) ([]byte, error) {
	body, err := p.do("/leagues/" + ulid + "/top-scorers")
	if err != nil {
		return nil, err
	}
	var resp goalTopScorersResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if len(resp.Data) == 0 {
		return nil, fmt.Errorf("goalapi top-scorers ว่าง (%s)", leagueID)
	}

	// เรียงเอง — GOAL ไม่ได้รับประกันลำดับ (playerPlace บางลีกโดด)
	rows := make([]goalTopScorerRow, len(resp.Data))
	copy(rows, resp.Data)
	sort.SliceStable(rows, func(i, j int) bool {
		gi, gj := atoiOr(rows[i].Goals, 0), atoiOr(rows[j].Goals, 0)
		if gi != gj {
			return gi > gj
		}
		return atoiOr(rows[i].Assists, 0) > atoiOr(rows[j].Assists, 0)
	})
	byAssists := make([]goalTopScorerRow, len(rows))
	copy(byAssists, rows)
	sort.SliceStable(byAssists, func(i, j int) bool {
		ai, aj := atoiOr(byAssists[i].Assists, 0), atoiOr(byAssists[j].Assists, 0)
		if ai != aj {
			return ai > aj
		}
		return atoiOr(byAssists[i].Goals, 0) > atoiOr(byAssists[j].Goals, 0)
	})

	// หน้าเว็บแสดงเฉพาะ 3 อันดับแรก -> resolve ULID (กิน 1 call ครั้งแรกแล้วจำใน registry)
	topPlayer := func(r goalTopScorerRow, rank int, val string, wantLink bool) map[string]interface{} {
		m := map[string]interface{}{
			"rank":     rank,
			"name":     r.PlayerName,
			"teamName": r.TeamName,
			"stat":     map[string]interface{}{"value": val},
		}
		if ulid := p.resolveScorerULID(r, wantLink); ulid != "" {
			m["id"] = ulid
		} else if r.PlayerKey != "" {
			m["id"] = r.PlayerKey
		}
		return m
	}

	topScorers := make([]map[string]interface{}, 0, len(rows))
	for i, r := range rows {
		topScorers = append(topScorers, topPlayer(r, i+1, r.Goals, i < 3))
	}

	scorerTop3 := make([]interface{}, 0, 3)
	for i := 0; i < 3 && i < len(rows); i++ {
		scorerTop3 = append(scorerTop3, topPlayer(rows[i], i+1, rows[i].Goals, true))
	}
	assistTop3 := make([]interface{}, 0, 3)
	for i := 0; i < 3 && i < len(byAssists); i++ {
		assistTop3 = append(assistTop3, topPlayer(byAssists[i], i+1, byAssists[i].Assists, true))
	}

	players := []interface{}{
		map[string]interface{}{"header": "ผู้นำทำประตู (Top Scorers)", "topThree": scorerTop3},
		map[string]interface{}{"header": "ผู้นำทำแอสซิสต์ (Top Assists)", "topThree": assistTop3},
	}

	out := map[string]interface{}{
		"topScorers":  topScorers,
		"players":     players,
		"data_source": "goalapi",
	}
	// สถิติทีมมาจาก standings (ถ้าดึงไม่ได้ ไม่ให้หน้า stats ทั้งหมดล้ม)
	if teams := p.leagueTeamStats(ulid); len(teams) > 0 {
		out["teams"] = teams
	}
	return json.Marshal(out)
}

// resolveScorerULID หา ULID ของนักเตะจาก top-scorers
// ลำดับ: registry (ไม่กินโควตา) -> /players?search=แบบชื่อตรงเป๊ะ (1 call แล้วจำถาวร)
// โควตาไม่พอ/หาไม่เจอ -> คืน "" (caller ใช้ playerKey ไปก่อน หน้าเว็บไม่พัง)
func (p *GoalProvider) resolveScorerULID(r goalTopScorerRow, wantLink bool) string {
	if r.PlayerKey != "" {
		if ref, ok := registry.Lookup("player", r.PlayerKey); ok && ref.Source == p.Name() && ref.ULID != "" {
			return ref.ULID
		}
	}
	if !wantLink {
		return ""
	}
	name := strings.TrimSpace(r.PlayerName)
	if name == "" {
		return ""
	}
	if ok, _ := p.budget.Allow(); !ok {
		return ""
	}
	body, err := p.do("/players?search=" + url.QueryEscape(name))
	if err != nil {
		return ""
	}
	var resp goalTeamPlayersResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return ""
	}
	for _, pl := range resp.Data {
		if !strings.EqualFold(strings.TrimSpace(pl.Name), name) {
			continue
		}
		if pl.ID == "" {
			continue
		}
		if pl.APIID != "" {
			registry.Remember("player", pl.APIID, p.Name(), pl.ID)
		}
		if r.PlayerKey != "" {
			registry.Remember("player", r.PlayerKey, p.Name(), pl.ID)
		}
		return pl.ID
	}
	return ""
}

// leagueTeamStats สถิติทีมจากตารางคะแนน (1 call) — แทนข้อมูลตัวอย่าง hardcode ใน client
func (p *GoalProvider) leagueTeamStats(ulid string) []interface{} {
	body, err := p.do("/leagues/" + ulid + "/standings")
	if err != nil {
		return nil
	}
	var env goalStandingsEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil
	}

	type teamRow struct {
		id, name, logo string
		gf, ga, w      int
	}
	seen := map[string]bool{}
	rows := make([]teamRow, 0, len(env.Data))
	for _, r := range env.Data {
		if r.TeamID == "" || seen[r.TeamID] || atoiOr(r.OverallPlayed, 0) == 0 {
			continue
		}
		seen[r.TeamID] = true
		tr := teamRow{
			id:   r.TeamID,
			name: r.TeamName,
			gf:   atoiOr(r.OverallGF, 0),
			ga:   atoiOr(r.OverallGA, 0),
			w:    atoiOr(r.OverallW, 0),
		}
		if r.Team != nil {
			tr.logo = r.Team.Badge
		}
		rows = append(rows, tr)
	}
	if len(rows) < 3 {
		return nil
	}

	item := func(rank int, t teamRow, val string) map[string]interface{} {
		m := map[string]interface{}{"rank": rank, "id": t.id, "name": t.name, "val": val}
		if t.logo != "" {
			m["logo"] = t.logo
		}
		return m
	}
	top3 := func(less func(a, b teamRow) bool) []interface{} {
		sorted := make([]teamRow, len(rows))
		copy(sorted, rows)
		sort.SliceStable(sorted, func(i, j int) bool { return less(sorted[i], sorted[j]) })
		out := make([]interface{}, 0, 3)
		for i := 0; i < 3 && i < len(sorted); i++ {
			out = append(out, sorted[i])
		}
		return out
	}

	mostGoals := top3(func(a, b teamRow) bool { return a.gf > b.gf })
	fewestConceded := top3(func(a, b teamRow) bool { return a.ga < b.ga })
	mostWins := top3(func(a, b teamRow) bool { return a.w > b.w })

	items := func(list []interface{}, val func(teamRow) string) []interface{} {
		out := make([]interface{}, 0, len(list))
		for i, v := range list {
			t := v.(teamRow)
			out = append(out, item(i+1, t, val(t)))
		}
		return out
	}

	return []interface{}{
		map[string]interface{}{"header": "ทีมที่ทำประตูมากที่สุด (Most Goals)",
			"items": items(mostGoals, func(t teamRow) string { return fmt.Sprintf("%d ประตู", t.gf) })},
		map[string]interface{}{"header": "ทีมที่เสียประตูน้อยที่สุด (Fewest Conceded)",
			"items": items(fewestConceded, func(t teamRow) string { return fmt.Sprintf("%d ประตู", t.ga) })},
		map[string]interface{}{"header": "ทีมที่ชนะมากที่สุด (Most Wins)",
			"items": items(mostWins, func(t teamRow) string { return fmt.Sprintf("%d นัด", t.w) })},
	}
}

func firstNonEmptyStr(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
