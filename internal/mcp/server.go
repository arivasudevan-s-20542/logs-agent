package mcp

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/zcop/log-agent/internal/parser"
	"github.com/zcop/log-agent/internal/query"
	"github.com/zcop/log-agent/internal/store"
	"github.com/zcop/log-agent/internal/tailer"
)

// Server wraps an MCP server exposing log agent tools.
type Server struct {
	store     *store.Store
	index     *store.FileIndex
	tailer    *tailer.Tailer
	mcpServer *server.MCPServer

	// Auth
	token      string
	allowedIPs []string
}

// NewServer creates an MCP server with all log agent tools registered.
func NewServer(s *store.Store, idx *store.FileIndex, t *tailer.Tailer, token string, allowedIPs []string) *Server {
	srv := &Server{
		store:      s,
		index:      idx,
		tailer:     t,
		token:      token,
		allowedIPs: allowedIPs,
	}

	srv.mcpServer = server.NewMCPServer(
		"zcop-log-agent",
		"1.0.0",
		server.WithToolCapabilities(true),
	)

	srv.registerTools()
	return srv
}

// ServeStdio starts the MCP server on stdin/stdout (blocking).
func (s *Server) ServeStdio(ctx context.Context) error {
	stdio := server.NewStdioServer(s.mcpServer)
	return stdio.Listen(ctx, nil, nil)
}

// ServeSSE starts the MCP server over HTTP/SSE with auth + IP restrictions.
func (s *Server) ServeSSE(ctx context.Context, addr string) error {
	sseServer := server.NewSSEServer(s.mcpServer,
		server.WithBaseURL(fmt.Sprintf("http://%s", addr)),
	)

	mux := http.NewServeMux()
	mux.Handle("/", sseServer)

	var handler http.Handler = mux
	if len(s.allowedIPs) > 0 {
		handler = s.ipAllowMiddleware(handler)
	}
	if s.token != "" {
		handler = s.tokenAuthMiddleware(handler)
	}
	handler = s.logMiddleware(handler)

	httpServer := &http.Server{Addr: addr, Handler: handler}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		httpServer.Shutdown(shutdownCtx)
	}()

	log.Printf("[mcp] SSE server listening on %s", addr)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// --- Middleware ---

func (s *Server) tokenAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == "" {
			// Also check query param for SSE connections (browsers can't set headers on EventSource)
			auth = "Bearer " + r.URL.Query().Get("token")
		}

		if !strings.HasPrefix(auth, "Bearer ") {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		provided := strings.TrimPrefix(auth, "Bearer ")
		if subtle.ConstantTimeCompare([]byte(provided), []byte(s.token)) != 1 {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (s *Server) ipAllowMiddleware(next http.Handler) http.Handler {
	// Parse CIDR ranges and plain IPs at setup time
	var nets []*net.IPNet
	var ips []net.IP
	for _, entry := range s.allowedIPs {
		if strings.Contains(entry, "/") {
			_, network, err := net.ParseCIDR(entry)
			if err == nil {
				nets = append(nets, network)
				continue
			}
		}
		if ip := net.ParseIP(entry); ip != nil {
			ips = append(ips, ip)
		}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientIP := extractIP(r)
		if clientIP == nil {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		for _, ip := range ips {
			if ip.Equal(clientIP) {
				next.ServeHTTP(w, r)
				return
			}
		}
		for _, network := range nets {
			if network.Contains(clientIP) {
				next.ServeHTTP(w, r)
				return
			}
		}

		log.Printf("[mcp] blocked request from %s", clientIP)
		http.Error(w, "Forbidden", http.StatusForbidden)
	})
}

func extractIP(r *http.Request) net.IP {
	// Check X-Forwarded-For first (trusted reverse proxy)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.SplitN(xff, ",", 2)
		if ip := net.ParseIP(strings.TrimSpace(parts[0])); ip != nil {
			return ip
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return net.ParseIP(r.RemoteAddr)
	}
	return net.ParseIP(host)
}

func (s *Server) logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[mcp] %s %s from %s", r.Method, r.URL.Path, r.RemoteAddr)
		next.ServeHTTP(w, r)
	})
}

// --- Tool Registration ---

func (s *Server) registerTools() {
	s.mcpServer.AddTool(
		mcp.NewTool("query_logs",
			mcp.WithDescription("Execute a SQL-like query against the log store. "+
				"Supported: SELECT columns FROM logs WHERE conditions ORDER BY col [ASC|DESC] LIMIT n. "+
				"Columns: timestamp, level, thread, rid, uid, uri, logger, msg, file. "+
				"Operators: =, !=, >, <, >=, <=, LIKE, IN. Aggregates: COUNT, AVG, MIN, MAX. GROUP BY supported."),
			mcp.WithString("sql", mcp.Required(), mcp.Description("SQL query string")),
		),
		s.handleQueryLogs,
	)

	s.mcpServer.AddTool(
		mcp.NewTool("trace_request",
			mcp.WithDescription("Get all log entries for a given request ID (RID), ordered chronologically. Useful for tracing a single HTTP request through the system."),
			mcp.WithString("rid", mcp.Required(), mcp.Description("Request ID to trace")),
		),
		s.handleTraceRequest,
	)

	s.mcpServer.AddTool(
		mcp.NewTool("get_stats",
			mcp.WithDescription("Get aggregate statistics: total entries in memory, level counts, file counts, time range, unique RIDs/UIDs. Also shows file index summary (hot vs cold files, available time ranges)."),
		),
		s.handleGetStats,
	)

	s.mcpServer.AddTool(
		mcp.NewTool("search_logs",
			mcp.WithDescription("Search logs with optional filters. Returns matching entries sorted by timestamp descending. Only searches hot (in-memory) data. Use get_file_index to see cold data availability."),
			mcp.WithString("level", mcp.Description("Filter by log level: DEBUG, INFO, WARNING, ERROR")),
			mcp.WithString("uid", mcp.Description("Filter by user ID")),
			mcp.WithString("rid", mcp.Description("Filter by request ID")),
			mcp.WithString("file", mcp.Description("Filter by log file name")),
			mcp.WithString("keyword", mcp.Description("Search keyword in message (case-insensitive)")),
			mcp.WithString("since", mcp.Description("Only logs after this time (RFC3339, e.g. 2024-01-15T10:00:00Z)")),
			mcp.WithString("until", mcp.Description("Only logs before this time (RFC3339)")),
			mcp.WithNumber("limit", mcp.Description("Max results to return (default 100, max 1000)")),
		),
		s.handleSearchLogs,
	)

	s.mcpServer.AddTool(
		mcp.NewTool("get_recent_logs",
			mcp.WithDescription("Get the most recent log entries from hot store."),
			mcp.WithNumber("count", mcp.Description("Number of recent entries to return (default 50, max 500)")),
		),
		s.handleGetRecentLogs,
	)

	s.mcpServer.AddTool(
		mcp.NewTool("get_errors",
			mcp.WithDescription("Get recent ERROR and WARNING level log entries from hot store."),
			mcp.WithNumber("count", mcp.Description("Max number of entries (default 50, max 500)")),
			mcp.WithString("level", mcp.Description("Specific level: ERROR or WARNING (default: both)")),
		),
		s.handleGetErrors,
	)

	s.mcpServer.AddTool(
		mcp.NewTool("get_file_index",
			mcp.WithDescription("List all known log files with their time ranges and load status. Shows which files are 'hot' (in memory) vs 'cold' (on disk, available for on-demand loading)."),
		),
		s.handleGetFileIndex,
	)

	s.mcpServer.AddTool(
		mcp.NewTool("load_time_range",
			mcp.WithDescription("Load cold (on-disk) log files into memory for a specific time range. Use get_file_index first to see what cold data is available."),
			mcp.WithString("since", mcp.Required(), mcp.Description("Start time (RFC3339, e.g. 2024-01-15T00:00:00Z)")),
			mcp.WithString("until", mcp.Required(), mcp.Description("End time (RFC3339)")),
		),
		s.handleLoadTimeRange,
	)
}

// --- Tool Handlers ---

func (s *Server) handleQueryLogs(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()
	sql, _ := args["sql"].(string)
	if sql == "" {
		return mcp.NewToolResultError("sql parameter is required"), nil
	}

	entries := s.store.All()
	result, err := query.ParseAndExecute(sql, entries)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("query error: %v", err)), nil
	}

	return toTextResult(result)
}

func (s *Server) handleTraceRequest(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()
	rid, _ := args["rid"].(string)
	if rid == "" {
		return mcp.NewToolResultError("rid parameter is required"), nil
	}

	entries := s.store.ByRID(rid)
	rows := entriesToMaps(entries)

	return toTextResult(map[string]any{
		"rid":   rid,
		"count": len(rows),
		"logs":  rows,
	})
}

func (s *Server) handleGetStats(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	storeStats := s.store.Stats()
	indexSummary := s.index.Summary()

	storeStats["fileIndex"] = indexSummary
	return toTextResult(storeStats)
}

func (s *Server) handleSearchLogs(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()

	level, _ := args["level"].(string)
	uid, _ := args["uid"].(string)
	rid, _ := args["rid"].(string)
	file, _ := args["file"].(string)
	keyword, _ := args["keyword"].(string)
	sinceStr, _ := args["since"].(string)
	untilStr, _ := args["until"].(string)
	limitF, _ := args["limit"].(float64)

	limit := 100
	if limitF > 0 {
		limit = int(limitF)
	}
	if limit > 1000 {
		limit = 1000
	}

	entries := s.getBaseEntries(sinceStr, untilStr)

	keyword = strings.ToLower(keyword)
	level = strings.ToUpper(level)

	var filtered []map[string]any
	for i := len(entries) - 1; i >= 0 && len(filtered) < limit; i-- {
		e := entries[i]
		if level != "" && e.Level != level {
			continue
		}
		if uid != "" && e.UID != uid {
			continue
		}
		if rid != "" && e.RID != rid {
			continue
		}
		if file != "" && e.File != file {
			continue
		}
		if keyword != "" && !strings.Contains(strings.ToLower(e.Message), keyword) {
			continue
		}
		filtered = append(filtered, entryToMap(e))
	}

	return toTextResult(map[string]any{
		"count":   len(filtered),
		"filters": compactFilters(args),
		"logs":    filtered,
	})
}

func (s *Server) handleGetRecentLogs(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()
	countF, _ := args["count"].(float64)
	count := 50
	if countF > 0 {
		count = int(countF)
	}
	if count > 500 {
		count = 500
	}

	all := s.store.All()
	start := len(all) - count
	if start < 0 {
		start = 0
	}

	recent := all[start:]
	rows := make([]map[string]any, 0, len(recent))
	for i := len(recent) - 1; i >= 0; i-- {
		rows = append(rows, entryToMap(recent[i]))
	}

	return toTextResult(map[string]any{
		"count": len(rows),
		"logs":  rows,
	})
}

func (s *Server) handleGetErrors(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()
	countF, _ := args["count"].(float64)
	count := 50
	if countF > 0 {
		count = int(countF)
	}
	if count > 500 {
		count = 500
	}

	levelFilter, _ := args["level"].(string)
	levelFilter = strings.ToUpper(levelFilter)

	all := s.store.All()
	rows := make([]map[string]any, 0, count)

	for i := len(all) - 1; i >= 0 && len(rows) < count; i-- {
		e := all[i]
		if levelFilter != "" {
			if e.Level != levelFilter {
				continue
			}
		} else if e.Level != "ERROR" && e.Level != "WARNING" {
			continue
		}
		rows = append(rows, entryToMap(e))
	}

	return toTextResult(map[string]any{
		"count": len(rows),
		"logs":  rows,
	})
}

func (s *Server) handleGetFileIndex(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	files := s.index.All()

	result := make([]map[string]any, 0, len(files))
	for _, f := range files {
		entry := map[string]any{
			"name":   f.Name,
			"path":   f.Path,
			"size":   f.Size,
			"loaded": f.Loaded,
			"gzip":   f.IsGzip,
		}
		if !f.FirstTS.IsZero() {
			entry["firstTimestamp"] = f.FirstTS.Format(time.RFC3339)
			entry["lastTimestamp"] = f.LastTS.Format(time.RFC3339)
		}
		if f.Loaded {
			entry["entries"] = f.Entries
		}
		result = append(result, entry)
	}

	summary := s.index.Summary()

	return toTextResult(map[string]any{
		"summary": summary,
		"files":   result,
	})
}

func (s *Server) handleLoadTimeRange(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()
	sinceStr, _ := args["since"].(string)
	untilStr, _ := args["until"].(string)

	from, err := time.Parse(time.RFC3339, sinceStr)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("invalid 'since' time: %v", err)), nil
	}
	to, err := time.Parse(time.RFC3339, untilStr)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("invalid 'until' time: %v", err)), nil
	}

	loaded, files := s.tailer.LoadTimeRange(from, to)

	return toTextResult(map[string]any{
		"message":       fmt.Sprintf("Loaded %d entries from %d cold files", loaded, files),
		"entriesLoaded": loaded,
		"filesLoaded":   files,
		"range": map[string]string{
			"from": from.Format(time.RFC3339),
			"to":   to.Format(time.RFC3339),
		},
	})
}

// --- Helpers ---

func (s *Server) getBaseEntries(sinceStr, untilStr string) []*parser.LogEntry {
	if sinceStr == "" && untilStr == "" {
		return s.store.All()
	}

	from := time.Time{}
	to := time.Now().Add(time.Hour)

	if sinceStr != "" {
		if t, err := time.Parse(time.RFC3339, sinceStr); err == nil {
			from = t
		}
	}
	if untilStr != "" {
		if t, err := time.Parse(time.RFC3339, untilStr); err == nil {
			to = t
		}
	}

	return s.store.ByTimeRange(from, to)
}

func entriesToMaps(entries []*parser.LogEntry) []map[string]any {
	rows := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		rows = append(rows, entryToMap(e))
	}
	return rows
}

func entryToMap(e *parser.LogEntry) map[string]any {
	return map[string]any{
		"timestamp": e.Timestamp.Format(time.RFC3339Nano),
		"level":     e.Level,
		"thread":    e.Thread,
		"rid":       e.RID,
		"uid":       e.UID,
		"uri":       e.URI,
		"logger":    e.Logger,
		"msg":       e.Message,
		"file":      e.File,
	}
}

func compactFilters(args map[string]any) map[string]any {
	filters := make(map[string]any)
	for _, key := range []string{"level", "uid", "rid", "file", "keyword", "since", "until", "limit"} {
		if v, ok := args[key]; ok && v != nil && v != "" && v != 0.0 {
			filters[key] = v
		}
	}
	return filters
}

func toTextResult(data any) (*mcp.CallToolResult, error) {
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("marshal error: %v", err)), nil
	}
	return mcp.NewToolResultText(string(b)), nil
}
