package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/zcop/log-agent/internal/parser"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		return true // auth is handled at middleware level
	},
}

// Tail upgrades to WebSocket and streams new log entries in real time.
// GET /api/tail?level=ERROR&rid=abc&uid=42&uri=/api/problems
//
// Client can send JSON filter messages:
//
//	{"level":"ERROR","rid":"abc123","uid":"42","msg":"timeout"}
func (h *Handlers) Tail(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[ws] upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	// Initial filter from query params
	filter := tailFilter{
		Level: strings.ToUpper(r.URL.Query().Get("level")),
		RID:   r.URL.Query().Get("rid"),
		UID:   r.URL.Query().Get("uid"),
		URI:   r.URL.Query().Get("uri"),
		Msg:   r.URL.Query().Get("msg"),
	}

	// Subscribe to live entries
	subID, ch := h.store.Subscribe(256)
	defer h.store.Unsubscribe(subID)

	// Read filter updates from client in background
	filterCh := make(chan tailFilter, 4)
	go h.readFilterUpdates(conn, filterCh)

	pingTicker := time.NewTicker(15 * time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case entry, ok := <-ch:
			if !ok {
				return
			}
			if !filter.matches(entry) {
				continue
			}
			msg := map[string]any{
				"type":      "log",
				"timestamp": entry.Timestamp.Format(time.RFC3339Nano),
				"level":     entry.Level,
				"thread":    entry.Thread,
				"rid":       entry.RID,
				"uid":       entry.UID,
				"uri":       entry.URI,
				"logger":    entry.Logger,
				"msg":       entry.Message,
				"file":      entry.File,
			}
			if err := conn.WriteJSON(msg); err != nil {
				return
			}

		case newFilter := <-filterCh:
			filter = newFilter

		case <-pingTicker.C:
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (h *Handlers) readFilterUpdates(conn *websocket.Conn, ch chan<- tailFilter) {
	defer close(ch)
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var f tailFilter
		if err := json.Unmarshal(msg, &f); err != nil {
			continue
		}
		f.Level = strings.ToUpper(f.Level)
		select {
		case ch <- f:
		default:
		}
	}
}

type tailFilter struct {
	Level string `json:"level"`
	RID   string `json:"rid"`
	UID   string `json:"uid"`
	URI   string `json:"uri"`
	Msg   string `json:"msg"`
}

func (f *tailFilter) matches(e *parser.LogEntry) bool {
	if f.Level != "" && e.Level != f.Level {
		return false
	}
	if f.RID != "" && e.RID != f.RID {
		return false
	}
	if f.UID != "" && e.UID != f.UID {
		return false
	}
	if f.URI != "" && !strings.Contains(strings.ToLower(e.URI), strings.ToLower(f.URI)) {
		return false
	}
	if f.Msg != "" && !strings.Contains(strings.ToLower(e.Message), strings.ToLower(f.Msg)) {
		return false
	}
	return true
}
