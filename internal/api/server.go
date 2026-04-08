package api

import (
	"log"
	"net/http"

	"github.com/zcop/log-agent/internal/auth"
	"github.com/zcop/log-agent/internal/config"
	"github.com/zcop/log-agent/internal/store"
)

// Server is the HTTP server for the log agent.
type Server struct {
	cfg   *config.Config
	store *store.Store
	index *store.FileIndex
	mux   *http.ServeMux
}

func NewServer(cfg *config.Config, s *store.Store, idx *store.FileIndex) *Server {
	srv := &Server{cfg: cfg, store: s, index: idx, mux: http.NewServeMux()}
	srv.routes()
	return srv
}

func (s *Server) routes() {
	h := &Handlers{store: s.store, index: s.index}

	s.mux.HandleFunc("POST /api/query", h.Query)
	s.mux.HandleFunc("GET /api/trace/{rid}", h.Trace)
	s.mux.HandleFunc("GET /api/stats", h.Stats)
	s.mux.HandleFunc("GET /api/files", h.Files)
	s.mux.HandleFunc("GET /api/files/index", h.FileIndex)
	s.mux.HandleFunc("GET /api/levels", h.Levels)
	s.mux.HandleFunc("GET /api/tail", h.Tail)
	s.mux.HandleFunc("GET /api/health", h.Health)

	s.mux.Handle("GET /", noCacheWrap(http.FileServer(http.FS(staticFS()))))
}

func (s *Server) Handler() http.Handler {
	var handler http.Handler = s.mux

	handler = corsMiddleware(s.cfg.CORSOrigins, handler)

	if s.cfg.AuthEnabled {
		handler = skipAuthPaths(auth.Middleware(s.cfg.Username, s.cfg.Password, handler), handler, "/api/health")
	}

	handler = logMiddleware(handler)

	return handler
}

// skipAuthPaths bypasses the auth middleware for specific paths (e.g. health checks).
func skipAuthPaths(authed, raw http.Handler, paths ...string) http.Handler {
	skip := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		skip[p] = struct{}{}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := skip[r.URL.Path]; ok {
			raw.ServeHTTP(w, r)
			return
		}
		authed.ServeHTTP(w, r)
	})
}

func (s *Server) ListenAndServe() error {
	addr := s.cfg.Addr()
	log.Printf("[server] listening on http://%s", addr)
	return http.ListenAndServe(addr, s.Handler())
}

func noCacheWrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		next.ServeHTTP(w, r)
	})
}

func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[http] %s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

func corsMiddleware(origins []string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		allowed := false
		for _, o := range origins {
			if o == "*" || o == origin {
				allowed = true
				break
			}
		}
		if allowed {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		}
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
