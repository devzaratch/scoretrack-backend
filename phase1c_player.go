package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

// ---------------------------------------------------------------------------
// Phase 1c - หน้าโปรไฟล์นักเตะ (/player/:id) ให้ดึงข้อมูลจาก GOAL API
//
// ก่อนหน้านี้ route นี้เป็น proxy ล้วนไป FotMob (main.go playerHandler)
// ซึ่งติดโควตา 15 req/วัน และรูปนักเตะของ FotMob ใช้ id คนละระบบกับ GOAL
// ทำให้หน้า /player/... พัง/ว่างเมื่อ FotMob โดน 429
//
// การทำงานของไฟล์นี้:
//  1. resolve id ที่หน้าเว็บส่งมา (ULID มาจากกล่องค้นหา หรือเลข apiId ที่ registry เคยจำไว้)
//  2. GET /players/{ulid}
//  3. map ให้ตรง shape เดิมที่ PlayerClient.tsx อ่านอยู่ (หน้าตา FotMob playerData)
//  ถ้า resolve ไม่ได้ หรือ provider ล้มทั้งหมด -> caller ถอยไป FotMob ของเดิมเหมือนเดิม
// ---------------------------------------------------------------------------

// PlayerProvider - provider ที่รองรับโปรไฟล์นักเตะ (Phase 1c)
type PlayerProvider interface {
	PlayerData(playerID string) ([]byte, error)
}

// fetchPlayerFromProviders - ไล่ provider ตามลำดับใน providerChain
// (คุมด้วย Enabled + budget ตัวเดียวกับ fetchTeamFromProviders)
// คืน false เมื่อ resolve id ไม่ได้หรือไม่มีตัวไหนตอบ -> caller ใช้ FotMob ของเดิม
func fetchPlayerFromProviders(playerID string) ([]byte, string, bool) {
	for _, p := range providerChain {
		if !p.Enabled() {
			continue
		}
		pp, ok := p.(PlayerProvider)
		if !ok {
			continue
		}
		if okBudget, _ := p.Budget().Allow(); !okBudget {
			continue
		}
		data, err := pp.PlayerData(playerID)
		if err != nil {
			log.Printf("ข้าม %s PlayerData(%q): %v", p.Name(), playerID, err)
			continue
		}
		if len(data) > 0 {
			return data, p.Name(), true
		}
	}
	return nil, "", false
}

// resolvePlayerULID - แปลง id ที่ได้รับเป็น ULID ของ GOAL
// รูปแบบที่เข้ามา: ULID (26 ตัวอักษร จาก /api/search) หรือ เลข apiId (จาก registry)
func (p *GoalProvider) resolvePlayerULID(playerID string) (string, error) {
	id := strings.TrimSpace(playerID)
	if id == "" {
		return "", fmt.Errorf("player id ว่าง")
	}
	if isGoalULID(id) {
		return id, nil
	}
	if ref, ok := registry.Lookup("player", id); ok && ref.Source == p.Name() && ref.ULID != "" {
		return ref.ULID, nil
	}
	return "", fmt.Errorf("player %s ไม่มีใน goal registry", id)
}

// goalPlayerTeam - object ทีมที่มาพร้อมข้อมูลนักเตะ
type goalPlayerTeam struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Badge   string `json:"badge"`
	Country string `json:"country"`
}

// goalPlayerEnvelope - โครงคำตอบของ GET /players/{ulid}
type goalPlayerEnvelope struct {
	Success bool `json:"success"`
	Data    struct {
		ID          string `json:"id"`
		APIID       string `json:"apiId"`
		Name        string `json:"name"`
		Image       string `json:"image"`
		Number      string `json:"number"`
		Country     string `json:"country"`
		Type        string `json:"type"`
		Age         string `json:"age"`
		Birthdate   string `json:"birthdate"`
		TeamID      string `json:"teamId"`
		IsActive    bool   `json:"isActive"`
		Injured     string `json:"injured"`
		IsCaptain   bool   `json:"isCaptain"`
		MatchPlayed string `json:"matchPlayed"`
		Goals       string `json:"goals"`
		Assists     string `json:"assists"`
		YellowCards string `json:"yellowCards"`
		RedCards    string `json:"redCards"`
		Team        *goalPlayerTeam `json:"team"`
	} `json:"data"`
}

// PlayerData - interface method ของ GoalProvider (Phase 1c)
func (p *GoalProvider) PlayerData(playerID string) ([]byte, error) {
	ulid, err := p.resolvePlayerULID(playerID)
	if err != nil {
		return nil, err
	}
	return p.playerDetailJSON(ulid, playerID)
}

// playerDetailJSON - ยิง /players/{ulid} แล้วแปลงเป็น shape ที่หน้า player อ่าน
func (p *GoalProvider) playerDetailJSON(ulid, playerID string) ([]byte, error) {
	body, err := p.do("/players/" + ulid)
	if err != nil {
		return nil, err
	}

	var env goalPlayerEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, err
	}
	if !env.Success || env.Data.ID == "" || env.Data.Name == "" {
		return nil, fmt.Errorf("goalapi player detail ไม่มีข้อมูล (%s)", playerID)
	}
	d := env.Data

	// จำ apiId -> ULID กลับเข้า registry (กันต้องยิงใหม่รอบหน้า)
	if d.APIID != "" {
		registry.Remember("player", d.APIID, p.Name(), d.ID)
	}

	teamName, teamBadge := "", ""
	if d.Team != nil {
		teamName = d.Team.Name
		teamBadge = d.Team.Badge
	}

	// ตำแหน่ง: GOAL คืนเป็นข้อความเดียว เช่น "Defenders"
	position := strings.TrimSpace(d.Type)
	positionBlock := map[string]interface{}{}
	if position != "" {
		positionBlock["primaryPosition"] = map[string]interface{}{"label": position}
		positionBlock["positions"] = []interface{}{
			map[string]interface{}{
				"isMainPosition": true,
				"strPos":         map[string]interface{}{"label": position},
			},
		}
	}

	// ข้อมูลพื้นฐานที่หน้า Bio อ่านผ่าน playerInformation[] (title ต้องตรงกับที่หน้าค้น)
	info := make([]interface{}, 0, 5)
	addInfo := func(title, value string) {
		if strings.TrimSpace(value) == "" {
			return
		}
		info = append(info, map[string]interface{}{"title": title, "value": value})
	}
	addInfo("Age", d.Age)
	addInfo("Country", firstNonEmptyStr(d.Country, countryOf(d.Team)))
	addInfo("Shirt", d.Number)
	addInfo("Team", teamName)

	// สถิติฤดูกาล (แท็บ Stats)
	statItems := make([]interface{}, 0, 6)
	addStat := func(title, value string) {
		if strings.TrimSpace(value) == "" {
			return
		}
		statItems = append(statItems, map[string]interface{}{"title": title, "value": value})
	}
	addStat("ลงสนาม", d.MatchPlayed)
	addStat("ประตู", d.Goals)
	addStat("แอสซิสต์", d.Assists)
	addStat("ใบเหลือง", d.YellowCards)
	addStat("ใบแดง", d.RedCards)

	out := map[string]interface{}{
		"id":             firstNonEmptyStr(d.APIID, d.ID),
		"name":           d.Name,
		"nameFormatted":  d.Name,
		"isCaptain":      d.IsCaptain,
		"primaryTeam":    map[string]interface{}{"teamId": d.TeamID, "teamName": teamName, "teamLogo": teamBadge},
		"playerInformation": info,
	}

	if d.Image != "" {
		out["image"] = d.Image
	}
	if d.Birthdate != "" {
		out["birthDate"] = map[string]interface{}{"utcTime": d.Birthdate}
	}
	if len(positionBlock) > 0 {
		out["positionDescription"] = positionBlock
	}
	if len(statItems) > 0 {
		out["firstSeasonStats"] = map[string]interface{}{
			"statsSection": map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{"title": "สถิติฤดูกาลนี้", "items": statItems},
				},
			},
		}
	}
	if strings.EqualFold(strings.TrimSpace(d.Injured), "Yes") {
		out["injuryInformation"] = map[string]interface{}{"text": "มีรายงานอาการบาดเจ็บ"}
	}

	encoded, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

// countryOf - ดึงประเทศจาก object ทีม (ใช้เติมช่อง Country เมื่อตัวนักเตะไม่ระบุ)
func countryOf(team *goalPlayerTeam) string {
	if team == nil {
		return ""
	}
	return team.Country
}
