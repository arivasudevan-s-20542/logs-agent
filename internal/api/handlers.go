package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/zcop/log-agent/internal/query"
	"github.com/zcop/log-agent/internal/store"
)

const maxQuerySize = 4096

// Handlers contains all REST API handlers.
type Handlers struct {
	store *store.Store
}

// Query executes a SQL query against the log store.
// POST /api/query  { "sql": "SELECT * FROM logs WHERE level = 'ERROR' LIMIT 100" }
func (h *Handlers) Query(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxQuerySize))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	var req struct {
		SQL string `json:"sql"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	sql := strings.TrimSpace(req.SQL)
	if sql == "" {
		writeErr(w, http.StatusBadRequest, "sql field is required")
		return
	}

	entries := h.store.All()
	result, err := query.ParseAndExecute(sql, entries)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "query error: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// Trace returns all log entries for a given request ID.
// GET /api/trace/{rid}
func (h *Handlers) Trace(w http.ResponseWriter, r *http.Request) {
	rid := r.PathValue("rid")
	if rid == "" {
		writeErr(w, http.StatusBadRequest, "rid path parameter is required")
		return
	}

	entries := h.store.ByRID(rid)

	rows := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		rows = append(rows, map[string]any{
			"timestamp": e.Timestamp,
			"level":     e.Level,
			"thread":    e.Thread,
			"rid":       e.RID,
			"uid":       e.UID,
			"uri":       e.URI,
			"logger":    e.Logger,
			"msg":       e.Message,
			"file":      e.File,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"rid":   rid,
		"count": len(rows),
		"logs":  rows,
	})
}

// Stats returns aggregate statistics.
// GET /api/stats
func (h *Handlers) Stats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.store.Stats())
}

// Files returns list of monitored log files with entry counts.
// GET /api/files
func (h *Handlers) Files(w http.ResponseWriter, r *http.Request) {
	stats := h.store.Stats()
	writeJSON(w, http.StatusOK, stats["fileCounts"])
}

// Levels returns entry counts by log level.
// GET /api/levels
func (h *Handlers) Levels(w http.ResponseWriter, r *http.Request) {
	stats := h.store.Stats()
	writeJSON(w, http.StatusOK, stats["levelCounts"])
}

// Health returns a simple health check.
// GET /api/health
func (h *Handlers) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- Helpers ---

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
