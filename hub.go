package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512 * 1024
	sendChannelCap = 256
)

// ==========================================
// 1. Live Scores WebSocket Hub
// ==========================================

type LiveClient struct {
	hub  *LiveHub
	conn *websocket.Conn
	send chan []byte
}

func (c *LiveClient) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func (c *LiveClient) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Drain queued messages
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

type LiveHub struct {
	clients    map[*LiveClient]bool
	broadcast  chan []byte
	register   chan *LiveClient
	unregister chan *LiveClient
	mu         sync.RWMutex
}

func NewLiveHub() *LiveHub {
	return &LiveHub{
		broadcast:  make(chan []byte, 1024),
		register:   make(chan *LiveClient),
		unregister: make(chan *LiveClient),
		clients:    make(map[*LiveClient]bool),
	}
}

func (h *LiveHub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			log.Printf("Live Client connected (Total active: %d)", h.ClientCount())
		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
			log.Printf("Live Client disconnected (Total active: %d)", h.ClientCount())
		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}
			h.mu.RUnlock()
		}
	}
}

func (h *LiveHub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func (h *LiveHub) BroadcastJSON(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	h.broadcast <- data
	return nil
}

// Global Hub Instances
var (
	GlobalLiveHub = NewLiveHub()
	GlobalChatHub = NewChatHub()
)

// ==========================================
// 2. Real-time Match Chat Hub (Room-based)
// ==========================================

type ChatClient struct {
	hub     *ChatHub
	conn    *websocket.Conn
	send    chan []byte
	matchID string
}

func (c *ChatClient) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	for {
		var msg map[string]interface{}
		err := c.conn.ReadJSON(&msg)
		if err != nil {
			break
		}
		c.hub.BroadcastToRoom(c.matchID, msg)
	}
}

func (c *ChatClient) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			c.conn.WriteMessage(websocket.TextMessage, message)
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

type ChatHub struct {
	rooms      map[string]map[*ChatClient]bool
	history    map[string][]map[string]interface{}
	register   chan *ChatClient
	unregister chan *ChatClient
	mu         sync.RWMutex
}

func NewChatHub() *ChatHub {
	return &ChatHub{
		rooms:      make(map[string]map[*ChatClient]bool),
		history:    make(map[string][]map[string]interface{}),
		register:   make(chan *ChatClient),
		unregister: make(chan *ChatClient),
	}
}

func (h *ChatHub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			if h.rooms[client.matchID] == nil {
				h.rooms[client.matchID] = make(map[*ChatClient]bool)
			}
			h.rooms[client.matchID][client] = true
			h.mu.Unlock()
		case client := <-h.unregister:
			h.mu.Lock()
			if clients, ok := h.rooms[client.matchID]; ok {
				if _, exists := clients[client]; exists {
					delete(clients, client)
					close(client.send)
				}
				if len(clients) == 0 {
					delete(h.rooms, client.matchID)
				}
			}
			h.mu.Unlock()
		}
	}
}

func (h *ChatHub) GetRoomHistory(matchID string) []map[string]interface{} {
	if GlobalDB != nil {
		return GlobalDB.GetChatHistory(matchID, 50)
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	hist, ok := h.history[matchID]
	if !ok || len(hist) == 0 {
		return []map[string]interface{}{
			{
				"id":   1,
				"user": "System",
				"text": "ยินดีต้อนรับสู่ห้องแชทสด DoBallLaos!",
				"type": "text",
			},
		}
	}
	return hist
}

func (h *ChatHub) BroadcastToRoom(matchID string, msg map[string]interface{}) {
	if GlobalDB != nil {
		GlobalDB.SaveChatMessage(matchID, msg)
	}
	h.mu.Lock()
	h.history[matchID] = append(h.history[matchID], msg)
	if len(h.history[matchID]) > 50 {
		h.history[matchID] = h.history[matchID][len(h.history[matchID])-50:]
	}

	clients := h.rooms[matchID]
	data, err := json.Marshal(msg)
	h.mu.Unlock()

	if err != nil || len(clients) == 0 {
		return
	}

	h.mu.RLock()
	for client := range clients {
		select {
		case client.send <- data:
		default:
		}
	}
	h.mu.RUnlock()
}

// ==========================================
// 3. Rate Limiter with Auto Garbage Collection
// ==========================================

type ClientRateRecord struct {
	LastSeen time.Time
	Count    int
}

type MemoryRateLimiter struct {
	records map[string]*ClientRateRecord
	mu      sync.RWMutex
}

var GlobalRateLimiter = NewMemoryRateLimiter()

func NewMemoryRateLimiter() *MemoryRateLimiter {
	limiter := &MemoryRateLimiter{
		records: make(map[string]*ClientRateRecord),
	}
	go limiter.startCleaner(5*time.Minute, 3*time.Minute)
	return limiter
}

func (r *MemoryRateLimiter) Allow(ip string, maxReqs int, window time.Duration) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	rec, exists := r.records[ip]
	if !exists || now.Sub(rec.LastSeen) > window {
		r.records[ip] = &ClientRateRecord{LastSeen: now, Count: 1}
		return true
	}

	rec.Count++
	rec.LastSeen = now

	return rec.Count <= maxReqs
}

func (r *MemoryRateLimiter) startCleaner(interval time.Duration, ttl time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		r.mu.Lock()
		now := time.Now()
		for ip, rec := range r.records {
			if now.Sub(rec.LastSeen) > ttl {
				delete(r.records, ip)
			}
		}
		r.mu.Unlock()
	}
}
