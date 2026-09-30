package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// ---------------------------------------------------------------------------
// GOAL API WebSocket — ดึงคะแนนแบบ push realtime โดยไม่กินโควตา REST
// พิสูจน์จากแผน FREE ด้วยการทดสอบจริงแล้ว:
//   - POST /ws/token ฟรี (ไม่มี header X-RateLimit / ไม่นับจาก /fixtures) + token อายุ 60 วิ
//   - wss://api.goal-api.com/ws?wsToken=... แล้วส่ง {"type":"auth","token":"..."}
//   - auth_success คืน plan=FREE, maxSubscriptions=25, maxConnections=1
//   - subscribe {"type":"subscribe","resource":"match","matchId":"<ULID>"} -> subscribe_response
//   - ได้ match_update กลับมาทันที (match_id = apiId + คะแนน + clock)
//   - การเชื่อมต่ออยู่รอดนานเกิน 100 วิ (ไม่ถูกตัดตอน token หมดอายุ)
// ---------------------------------------------------------------------------

const (
	goalWSURL         = "wss://api.goal-api.com/ws"
	goalWSMaxSubs     = 25 // ข้อจำกัดของแผน FREE
	goalWSReadTimeout = 5 * time.Minute
)

// errWSReconnect ใช้สั่งให้ session ปิดแล้วเปิดใหม่ (ตอนชุด subscribe เต็ม)
var errWSReconnect = fmt.Errorf("ต้องรีเซ็ตชุด subscribe")

type goalWSClock struct {
	Minute  string `json:"minute"`
	Elapsed *int   `json:"elapsed"`
	Extra   *string `json:"extra"`
	Period  string `json:"period"`
}

type goalWSUpdate struct {
	MatchID     string      `json:"match_id"`
	CountryName string      `json:"country_name"`
	LeagueName  string      `json:"league_name"`
	MatchDate   string      `json:"match_date"`
	MatchTime   string      `json:"match_time"`
	MatchStatus string      `json:"match_status"`
	Clock       goalWSClock `json:"clock"`
	HomeName    string      `json:"match_hometeam_name"`
	HomeScore   string      `json:"match_hometeam_score"`
	AwayName    string      `json:"match_awayteam_name"`
	AwayScore   string      `json:"match_awayteam_score"`
}

type goalWSEnvelope struct {
	Type    string          `json:"type"`
	Success bool            `json:"success"`
	Error   string          `json:"error"`
	Data    json.RawMessage `json:"data"`
}

// StartLiveStream เปิดการเชื่อมต่อ WebSocket แบบ background (มีชีวิตตลอดอายุ process)
func (p *GoalProvider) StartLiveStream() {
	if !p.Enabled() {
		return
	}
	go p.wsLoop()
}

func (p *GoalProvider) wsLoop() {
	backoff := 3 * time.Second
	for p.Enabled() {
		start := time.Now()
		err := p.wsSession()

		// ชุด subscribe เต็ม/ต้องจัดชุดใหม่ -> เปิด session ใหม่ทันที (ไม่ต้อง backoff)
		if err == errWSReconnect {
			backoff = 3 * time.Second
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if err != nil {
			log.Printf("ℹ️ goal WS: session จบ -> %v", err)
		}
		if time.Since(start) > 2*time.Minute {
			backoff = 3 * time.Second
		}
		time.Sleep(backoff)
		if backoff < 60*time.Second {
			backoff *= 2
		}
	}
}

// requestResubscribe ให้ WS รับสมัครคู่ที่เพิ่งมีในการ sweep ล่าสุด
// เรียกแบบ fire-and-forget เพราะอาจรอ p.mu อยู่ (ตัวเรียกอาจถือ lock อยู่)
func (p *GoalProvider) requestResubscribe() {
	go func() {
		p.mu.Lock()
		conn := p.wsConn
		p.mu.Unlock()
		if conn == nil {
			return // WS ยังไม่ต่อ -> รอบถัดไป/ตอน auth จะจัดการเอง
		}
		if err := p.wsSyncSubs(conn); err != nil {
			if err == errWSReconnect {
				// ชุดเต็ม -> ปิด connection นี้เพื่อให้ wsLoop เปิดใหม่พร้อมชุดที่จัดใหม่
				p.mu.Lock()
				p.wsNeedResub = true
				p.mu.Unlock()
				conn.Close()
				return
			}
			log.Printf("ℹ️ goal WS: subscribe เพิ่มไม่สำเร็จ -> %v", err)
		}
	}()
}

func (p *GoalProvider) wsSession() error {
	token, err := p.fetchWSToken()
	if err != nil {
		return err
	}

	target := goalWSURL + "?wsToken=" + url.QueryEscape(token)
	conn, _, err := websocket.DefaultDialer.Dial(target, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	p.mu.Lock()
	p.wsConn = conn
	p.wsUp = true
	p.wsSubs = make(map[int]bool)
	p.wsNeedResub = false
	p.mu.Unlock()

	defer func() {
		conn.Close()
		p.mu.Lock()
		p.wsUp = false
		p.wsConn = nil
		p.wsSubs = make(map[int]bool)
		p.mu.Unlock()
	}()

	if err := p.wsWriteJSON(conn, map[string]interface{}{"type": "auth", "token": token}); err != nil {
		return fmt.Errorf("auth: %w", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(goalWSReadTimeout)); err != nil {
		return err
	}

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			// connection ถูกปิดเพราะชุด subscribe เต็ม -> สั่งเปิดใหม่ทันที
			p.mu.Lock()
			needResub := p.wsNeedResub
			p.wsNeedResub = false
			p.mu.Unlock()
			if needResub {
				return errWSReconnect
			}
			return err
		}
		p.mu.Lock()
		p.lastWS = time.Now()
		p.mu.Unlock()

		if err := conn.SetReadDeadline(time.Now().Add(goalWSReadTimeout)); err != nil {
			return err
		}
		if err := p.wsHandleMessage(conn, raw); err != nil {
			return err
		}
	}
}

// fetchWSToken — POST /ws/token (ฟรี ไม่นับโควตา แต่ยังผ่าน budget governor ตามระเบียบ)
func (p *GoalProvider) fetchWSToken() (string, error) {
	body, err := p.doMethod("POST", "/ws/token")
	if err != nil {
		return "", err
	}
	var resp struct {
		Success bool `json:"success"`
		Data    struct {
			Token     string `json:"token"`
			ExpiresIn int    `json:"expiresIn"`
		} `json:"data"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", err
	}
	if resp.Data.Token == "" {
		return "", fmt.Errorf("ws token ว่าง (%s)", resp.Error)
	}
	return resp.Data.Token, nil
}

func (p *GoalProvider) wsWriteJSON(conn *websocket.Conn, payload interface{}) error {
	p.wsWriteMu.Lock()
	defer p.wsWriteMu.Unlock()
	if conn == nil {
		return fmt.Errorf("ws ยังไม่เชื่อมต่อ")
	}
	return conn.WriteJSON(payload)
}

func (p *GoalProvider) wsHandleMessage(conn *websocket.Conn, raw []byte) error {
	var env goalWSEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil // ข้อความที่ไม่ใช่ JSON -> ข้าม
	}

	switch env.Type {
	case "auth_success":
		var info struct {
			Plan             string `json:"plan"`
			MaxSubscriptions int    `json:"maxSubscriptions"`
		}
		_ = json.Unmarshal(env.Data, &info)
		log.Printf("🔁 goal WS: เชื่อมต่อสำเร็จ (plan=%s, subscribe ได้ %d คู่)", info.Plan, info.MaxSubscriptions)
		return p.wsSyncSubs(conn)

	case "match_update":
		var u goalWSUpdate
		if err := json.Unmarshal(env.Data, &u); err != nil {
			return nil
		}
		p.ApplyWSUpdate(u)
		return nil

	case "subscribe_response":
		return nil

	case "auth_error", "error", "subscribe_error":
		return fmt.Errorf("goal WS %s: %s", env.Type, env.Error)
	}
	return nil
}

// wsSyncSubs ปรับชุด subscribe ให้ตรงกับคู่ที่กำลังแข่ง (สูงสุด 25 คู่)
// ถ้าชุดเต็มแล้วมีคู่ใหม่ต้องเข้า -> คืน errWSReconnect ให้เปิด session ใหม่
func (p *GoalProvider) wsSyncSubs(conn *websocket.Conn) error {
	candidates := p.liveCandidatesLocked()

	p.mu.Lock()
	subscribed := make(map[int]bool, len(p.wsSubs))
	for k, v := range p.wsSubs {
		subscribed[k] = v
	}
	atCap := len(subscribed) >= goalWSMaxSubs
	p.mu.Unlock()

	newOnes := 0
	for _, id := range candidates {
		if !subscribed[id] {
			newOnes++
		}
	}
	if atCap && newOnes > 0 {
		// ชุดเต็ม 25 คู่แล้วแต่มีคู่ใหม่ต้องเข้า
		// -> สั่งให้ session ปิดตัวเองเพื่อเปิดใหม่ด้วยชุดที่จัดใหม่
		go conn.Close()
		return errWSReconnect
	}

	for _, id := range candidates {
		if subscribed[id] {
			continue
		}
		ref, ok := registry.Lookup("match", strconv.Itoa(id))
		if !ok || ref.ULID == "" {
			continue
		}
		if err := p.wsWriteJSON(conn, map[string]interface{}{
			"type":     "subscribe",
			"resource": "match",
			"matchId":  ref.ULID,
		}); err != nil {
			return err
		}
		p.mu.Lock()
		p.wsSubs[id] = true
		p.mu.Unlock()
	}
	return nil
}

// liveCandidatesLocked คัดคู่ที่ควร subscribe: ลีกใหญ่ก่อน แล้วค่อย match_id (ลำดับคงที่)
func (p *GoalProvider) liveCandidatesLocked() []int {
	type candidate struct {
		id    int
		major bool
	}
	p.mu.Lock()
	list := make([]candidate, 0, len(p.liveState))
	for _, m := range p.liveState {
		if !isLiveStatus(m.Status) {
			continue
		}
		if _, ok := registry.Lookup("match", strconv.Itoa(m.MatchID)); !ok {
			continue // ยังไม่รู้ ULID -> REST sweep จะไปจำให้เอง
		}
		list = append(list, candidate{
			id:    m.MatchID,
			major: isMajorLeagueName(m.League),
		})
	}
	p.mu.Unlock()

	sort.Slice(list, func(i, j int) bool {
		if list[i].major != list[j].major {
			return list[i].major
		}
		return list[i].id < list[j].id
	})

	out := make([]int, 0, goalWSMaxSubs)
	for _, c := range list {
		if len(out) >= goalWSMaxSubs {
			break
		}
		out = append(out, c.id)
	}
	return out
}

// isMajorLeagueName ใช้จัดลำดับสิทธิ์ 25 คู่ (ลีกใหญ่ได้สิทธิ์ก่อน)
func isMajorLeagueName(name string) bool {
	return matchMajorLeagueRegex.MatchString(name)
}

// ApplyWSUpdate อัปเดต liveState จากข้อความ push (เรียกโดย read loop ของ WS)
func (p *GoalProvider) ApplyWSUpdate(u goalWSUpdate) {
	id := atoiOr(u.MatchID, 0)
	if id == 0 {
		return
	}
	homeScore := atoiOr(u.HomeScore, -1)
	awayScore := atoiOr(u.AwayScore, -1)
	status := goalWSStatus(u)

	p.mu.Lock()
	defer p.mu.Unlock()

	m, ok := p.liveState[id]
	if !ok {
		// คู่เพิ่งเริ่มแข่งและยังไม่เคย sweep เห็น -> สร้าง entry ขั้นต่ำไว้ก่อน
		if strings.TrimSpace(u.HomeName) == "" || strings.TrimSpace(u.AwayName) == "" {
			return
		}
		m = RealMatch{
			MatchID:   id,
			League:    u.LeagueName,
			Country:   u.CountryName,
			Status:    "NS",
			MatchTime: u.MatchDate + " " + u.MatchTime,
			HomeTeam:  RealTeam{Name: u.HomeName},
			AwayTeam:  RealTeam{Name: u.AwayName},
		}
	}
	if homeScore >= 0 {
		m.HomeTeam.Score = homeScore
	}
	if awayScore >= 0 {
		m.AwayTeam.Score = awayScore
	}
	m.Status = status

	p.liveState[id] = m
	p.lastSeen[id] = m
	if isLiveStatus(status) {
		delete(p.doneAt, id)
	} else {
		p.doneAt[id] = time.Now()
	}
	p.lastWS = time.Now()
}

// goalWSStatus แปลงสถานะจาก WS เป็นรูปเดียวกับที่หน้าแรกใช้
func goalWSStatus(u goalWSUpdate) string {
	raw := strings.TrimSpace(u.MatchStatus)
	upper := strings.ToUpper(raw)

	switch upper {
	case "FINISHED", "FT", "ENDED":
		return "FT"
	case "HALF TIME", "HALF_TIME", "HT", "PAUSED":
		return "HT"
	case "POSTPONED":
		return "PST"
	case "CANCELLED", "CANCELED":
		return "CANC"
	case "SUSPENDED":
		return "SUSP"
	case "ABANDONED":
		return "ABD"
	}
	if strings.Contains(upper, "FINISH") || strings.Contains(upper, "FULL TIME") {
		return "FT"
	}

	period := strings.ToUpper(strings.TrimSpace(u.Clock.Period))
	if strings.Contains(period, "HALF_TIME") || period == "HT" {
		return "HT"
	}
	if u.Clock.Elapsed != nil && *u.Clock.Elapsed > 0 {
		return fmt.Sprintf("%d'", *u.Clock.Elapsed)
	}
	if n, err := strconv.Atoi(strings.TrimSuffix(raw, "'")); err == nil && n > 0 {
		return fmt.Sprintf("%d'", n)
	}
	if strings.TrimSpace(u.Clock.Minute) == "Half Time" {
		return "HT"
	}
	if raw == "" || period != "" {
		return "LIVE"
	}
	return "LIVE"
}

// WSStatus ใช้แสดงที่ /api/_sources
func (p *GoalProvider) WSStatus() map[string]interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := map[string]interface{}{
		"connected":         p.wsUp,
		"subscriptions":     len(p.wsSubs),
		"max_subscriptions": goalWSMaxSubs,
		"rest_swept":        p.liveSwept,
		"live_matches":      len(p.liveState),
	}
	if !p.lastWS.IsZero() {
		out["last_message_at"] = p.lastWS.Format(time.RFC3339)
	}
	if !p.liveAt.IsZero() {
		out["last_rest_sweep_at"] = p.liveAt.Format(time.RFC3339)
	}
	return out
}
