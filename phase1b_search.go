package main

// ---------------------------------------------------------------------------
// Phase 1b P3 — กล่องค้นหา (/api/search) จาก GOAL API
//
//  ปัญหาเดิม: searchHandler ใช้ handleProxy("search:%s") ซึ่ง ResolveDataType
//  ตก default = "matches" -> คืนรายการแมตช์วันนี้ กล่องค้นหาจึงไม่มีอะไรให้โชว์
//  FotMob suggest API ตายแล้ว (404) -> ต้องค้น GOAL 3 endpoint แยก
//  (GOAL ไม่มี /search — พิสูจน์แล้ว ROUTE_NOT_FOUND)
//
//  คำตอบเป็น array-of-groups รูปเดียวกับ FotMob suggest ที่
//  GlobalSearchBar.tsx อ่านออก (branch Array.isArray(data) + group.suggestions)
//  พร้อม cache ต่อคำ 1 ชม. (ไม่กินโควตาเมื่อพิมพ์ซ้ำ)
// ---------------------------------------------------------------------------

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// searchSuggestHandler ค้น GOAL แล้วคืน array-of-groups ให้ GlobalSearchBar
func searchSuggestHandler(c *gin.Context) {
	term := strings.TrimSpace(c.Query("q"))
	if term == "" {
		term = strings.TrimSpace(c.Query("term"))
	}
	if term == "" {
		c.Header("X-Cache", "MISS")
		c.JSON(http.StatusOK, []interface{}{})
		return
	}
	if len([]rune(term)) < 2 {
		c.Header("X-Cache", "MISS")
		c.JSON(http.StatusOK, []interface{}{})
		return
	}

	cacheKey := "search:suggest:" + strings.ToLower(term)

	// 1. Cache (Redis ก่อน ค่อย in-memory)
	if rdb != nil {
		if cached, err := rdb.Get(ctx, cacheKey).Result(); err == nil && cached != "" {
			c.Header("X-Cache", "HIT")
			c.Header("X-Data-Source", "Redis-Cache")
			c.Data(http.StatusOK, "application/json; charset=utf-8", []byte(cached))
			return
		}
	} else if cached, found := memoryCache.Get(cacheKey); found {
		c.Header("X-Cache", "HIT")
		c.Header("X-Data-Source", "In-Memory-Cache")
		c.Data(http.StatusOK, "application/json; charset=utf-8", cached)
		return
	}

	// 2. ค้นจาก GOAL (ผ่าน provider chain -> budget governor)
	groups, source, ok := searchTermFromProviders(term)
	if !ok {
		// ไม่มีแหล่งตอบได้ -> คืนกลุ่มว่าง (หน้าเว็บแสดง "ไม่พบผลลัพธ์")
		groups = []interface{}{}
		source = "empty"
	}

	raw, err := json.Marshal(groups)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 3. เก็บ cache 1 ชม.
	if rdb != nil {
		rdb.Set(ctx, cacheKey, string(raw), time.Hour)
	} else {
		memoryCache.Set(cacheKey, raw, time.Hour)
	}

	c.Header("X-Cache", "MISS")
	c.Header("X-Data-Source", source)
	c.Data(http.StatusOK, "application/json; charset=utf-8", raw)
}

// searchTermFromProviders ค้นคำเดียวจาก provider แรกที่รองรับ
func searchTermFromProviders(term string) ([]interface{}, string, bool) {
	for _, p := range providerChain {
		if !p.Enabled() {
			continue
		}
		sp, ok := p.(SearchProvider)
		if !ok {
			continue
		}
		if okBudget, _ := p.Budget().Allow(); !okBudget {
			continue
		}
		groups, err := sp.SearchSuggestions(term)
		if err != nil {
			log.Printf("⚠️ %s SearchSuggestions(%q): %v", p.Name(), term, err)
			continue
		}
		if groups != nil {
			return groups, p.Name(), true
		}
	}
	return nil, "", false
}

// SearchProvider — แหล่งข้อมูลที่ค้นคำทั่วไปได้ (Phase 1b P3)
type SearchProvider interface {
	SearchSuggestions(term string) ([]interface{}, error)
}

// SearchSuggestions ค้น 3 กลุ่ม (teams/leagues/players) แล้วรวมเป็น array-of-groups
func (p *GoalProvider) SearchSuggestions(term string) ([]interface{}, error) {
	if !p.Enabled() {
		return nil, fmt.Errorf("goalapi disabled")
	}
	q := strings.TrimSpace(term)

	teamGroups, errT := p.searchGroup("team", "/teams?search="+url.QueryEscape(q)+"&limit=6")
	leagueGroups, errL := p.searchGroup("league", "/leagues?search="+url.QueryEscape(q)+"&limit=5")
	playerGroups, errP := p.searchGroup("player", "/players?search="+url.QueryEscape(q)+"&limit=6")
	if errT != nil {
		log.Printf("ℹ️ goalapi search teams(%q): %v", q, errT)
	}
	if errL != nil {
		log.Printf("ℹ️ goalapi search leagues(%q): %v", q, errL)
	}
	if errP != nil {
		log.Printf("ℹ️ goalapi search players(%q): %v", q, errP)
	}

	// ถ้าทุกกลุ่มล้มเหลวจริง -> ส่ง err ให้ caller fallback
	if errT != nil && errL != nil && errP != nil {
		return nil, fmt.Errorf("goalapi search: %v / %v / %v", errT, errL, errP)
	}

	groups := []interface{}{}
	for _, g := range []struct {
		items []interface{}
	}{{teamGroups}, {playerGroups}, {leagueGroups}} {
		if len(g.items) > 0 {
			groups = append(groups, map[string]interface{}{"suggestions": g.items})
		}
	}
	return groups, nil
}

// searchGroup ยิง endpoint ค้นเดียวแล้วแปลงเป็น suggestions ของกลุ่มนั้น
func (p *GoalProvider) searchGroup(kind, path string) ([]interface{}, error) {
	body, err := p.do(path)
	if err != nil {
		return nil, err
	}

	items := make([]interface{}, 0, 8)
	switch kind {
	case "league":
		// /leagues?search= -> country เป็น object {id,name,logo} (ไม่ใช่ string!)
		var resp struct {
			Data []struct {
				ID          string `json:"id"`
				APIID       string `json:"apiId"`
				Name        string `json:"name"`
				CountryName string `json:"countryName"`
				Logo        string `json:"logo"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, err
		}
		for _, it := range resp.Data {
			if it.ID == "" || it.Name == "" {
				continue
			}
			item := map[string]interface{}{
				"id":      it.ID,
				"name":    it.Name,
				"type":    "league",
				"subtext": firstNonEmptyStr(it.CountryName, "การแข่งขัน"),
			}
			if it.Logo != "" {
				item["image"] = it.Logo
			}
			if it.APIID != "" {
				registry.Remember("league", it.APIID, p.Name(), it.ID)
			}
			items = append(items, item)
		}
	case "team":
		var resp struct {
			Data []struct {
				ID      string `json:"id"`
				APIID   string `json:"apiId"`
				Name    string `json:"name"`
				Country string `json:"country"`
				Badge   string `json:"badge"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, err
		}
		for _, it := range resp.Data {
			if it.ID == "" || it.Name == "" {
				continue
			}
			item := map[string]interface{}{
				"id":      it.ID,
				"name":    it.Name,
				"type":    "team",
				"subtext": firstNonEmptyStr(it.Country, "สโมสร"),
			}
			if it.Badge != "" {
				item["image"] = it.Badge
			}
			if it.APIID != "" {
				registry.Remember("team", it.APIID, p.Name(), it.ID)
			}
			items = append(items, item)
		}
	case "player":
		var resp struct {
			Data []struct {
				ID     string `json:"id"`
				APIID  string `json:"apiId"`
				Name   string `json:"name"`
				Image  string `json:"image"`
				Type   string `json:"type"`
				Number string `json:"number"`
				Team   *struct {
					Name string `json:"name"`
				} `json:"team"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, err
		}
		for _, it := range resp.Data {
			if it.ID == "" || it.Name == "" {
				continue
			}
			sub := "นักฟุตบอล"
			if it.Team != nil && it.Team.Name != "" {
				sub = it.Team.Name
			}
			item := map[string]interface{}{
				"id":      it.ID,
				"name":    it.Name,
				"type":    "player",
				"subtext": sub,
			}
			if it.Image != "" {
				item["image"] = it.Image
			}
			if it.Type != "" {
				item["position"] = it.Type
			}
			if it.APIID != "" {
				registry.Remember("player", it.APIID, p.Name(), it.ID)
			}
			items = append(items, item)
		}
	default:
		return nil, fmt.Errorf("search kind %q ไม่รองรับ", kind)
	}
	return items, nil
}
