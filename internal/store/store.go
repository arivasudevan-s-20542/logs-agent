package store

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zcop/log-agent/internal/parser"
)

// Store is a thread-safe, bounded in-memory log store with secondary indexes.
type Store struct {
	mu      sync.RWMutex
	entries []*parser.LogEntry
	max     int

	// Secondary indexes for fast lookup
	byLevel map[string][]*parser.LogEntry
	byRID   map[string][]*parser.LogEntry
	byUID   map[string][]*parser.LogEntry
	byFile  map[string][]*parser.LogEntry

	// Subscribers for live streaming
	subs   map[int64]chan *parser.LogEntry
	subSeq int64
	subMu  sync.Mutex
}

func New(maxEntries int) *Store {
	return &Store{
		max:     maxEntries,
		entries: make([]*parser.LogEntry, 0, min(maxEntries, 100000)),
		byLevel: make(map[string][]*parser.LogEntry),
		byRID:   make(map[string][]*parser.LogEntry),
		byUID:   make(map[string][]*parser.LogEntry),
		byFile:  make(map[string][]*parser.LogEntry),
		subs:    make(map[int64]chan *parser.LogEntry),
	}
}

// Add inserts a log entry and notifies live subscribers.
func (s *Store) Add(e *parser.LogEntry) {
	s.mu.Lock()

	// Evict oldest if at capacity
	if len(s.entries) >= s.max {
		s.evict(len(s.entries) / 10) // evict 10%
	}

	s.entries = append(s.entries, e)
	s.indexAdd(e)

	s.mu.Unlock()

	// Notify subscribers (non-blocking)
	s.subMu.Lock()
	for _, ch := range s.subs {
		select {
		case ch <- e:
		default:
			// Slow subscriber, drop
		}
	}
	s.subMu.Unlock()
}

// AddBatch inserts multiple entries (used for initial file load).
func (s *Store) AddBatch(entries []*parser.LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	needed := len(entries)
	if len(s.entries)+needed > s.max {
		evictCount := (len(s.entries) + needed) - s.max
		if evictCount > 0 {
			s.evict(evictCount)
		}
	}

	for _, e := range entries {
		s.entries = append(s.entries, e)
		s.indexAdd(e)
	}
}

// All returns all entries (snapshot). Use Query for filtered results.
func (s *Store) All() []*parser.LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make([]*parser.LogEntry, len(s.entries))
	copy(cp, s.entries)
	return cp
}

// ByRID returns all entries for a request ID (for request tracing).
func (s *Store) ByRID(rid string) []*parser.LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries := s.byRID[rid]
	cp := make([]*parser.LogEntry, len(entries))
	copy(cp, entries)
	return cp
}

// ByLevel returns entries by log level.
func (s *Store) ByLevel(level string) []*parser.LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries := s.byLevel[strings.ToUpper(level)]
	cp := make([]*parser.LogEntry, len(entries))
	copy(cp, entries)
	return cp
}

// ByTimeRange returns entries within a time range using binary search.
func (s *Store) ByTimeRange(from, to time.Time) []*parser.LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	start := sort.Search(len(s.entries), func(i int) bool {
		return !s.entries[i].Timestamp.Before(from)
	})
	end := sort.Search(len(s.entries), func(i int) bool {
		return s.entries[i].Timestamp.After(to)
	})

	if start >= end {
		return nil
	}

	cp := make([]*parser.LogEntry, end-start)
	copy(cp, s.entries[start:end])
	return cp
}

// Stats returns summary statistics.
func (s *Store) Stats() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	levelCounts := make(map[string]int)
	for k, v := range s.byLevel {
		levelCounts[k] = len(v)
	}

	fileCounts := make(map[string]int)
	for k, v := range s.byFile {
		fileCounts[k] = len(v)
	}

	var oldest, newest time.Time
	if len(s.entries) > 0 {
		oldest = s.entries[0].Timestamp
		newest = s.entries[len(s.entries)-1].Timestamp
	}

	return map[string]any{
		"totalEntries":    len(s.entries),
		"maxEntries":      s.max,
		"uniqueRIDs":      len(s.byRID),
		"uniqueUIDs":      len(s.byUID),
		"levelCounts":     levelCounts,
		"fileCounts":      fileCounts,
		"oldestEntry":     oldest.Format(time.RFC3339),
		"newestEntry":     newest.Format(time.RFC3339),
		"subscriberCount": len(s.subs),
	}
}

// Subscribe returns a channel that receives new log entries in real time.
func (s *Store) Subscribe(bufSize int) (int64, <-chan *parser.LogEntry) {
	ch := make(chan *parser.LogEntry, bufSize)
	s.subMu.Lock()
	s.subSeq++
	id := s.subSeq
	s.subs[id] = ch
	s.subMu.Unlock()
	return id, ch
}

// Unsubscribe removes a live subscriber.
func (s *Store) Unsubscribe(id int64) {
	s.subMu.Lock()
	if ch, ok := s.subs[id]; ok {
		close(ch)
		delete(s.subs, id)
	}
	s.subMu.Unlock()
}

// --- Internal ---

func (s *Store) indexAdd(e *parser.LogEntry) {
	s.byLevel[e.Level] = append(s.byLevel[e.Level], e)
	if e.RID != "" {
		s.byRID[e.RID] = append(s.byRID[e.RID], e)
	}
	if e.UID != "" {
		s.byUID[e.UID] = append(s.byUID[e.UID], e)
	}
	if e.File != "" {
		s.byFile[e.File] = append(s.byFile[e.File], e)
	}
}

func (s *Store) evict(count int) {
	if count > len(s.entries) {
		count = len(s.entries)
	}

	// Rebuild indexes after eviction (simpler than tracking removals)
	s.entries = s.entries[count:]
	s.rebuildIndexes()
}

func (s *Store) rebuildIndexes() {
	s.byLevel = make(map[string][]*parser.LogEntry)
	s.byRID = make(map[string][]*parser.LogEntry)
	s.byUID = make(map[string][]*parser.LogEntry)
	s.byFile = make(map[string][]*parser.LogEntry)
	for _, e := range s.entries {
		s.indexAdd(e)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
