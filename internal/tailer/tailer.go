package tailer

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/zcop/log-agent/internal/parser"
	"github.com/zcop/log-agent/internal/store"
)

// Tailer watches log directories and streams new lines into the store.
type Tailer struct {
	dirs      []string
	logGlob   string
	gzipGlob  string
	parser    *parser.Parser
	store     *store.Store
	index     *store.FileIndex
	poll      time.Duration
	readGzip  bool
	hotWindow time.Duration

	positions map[string]int64
	posMu     sync.Mutex

	stopCh chan struct{}
}

func New(dirs []string, logGlob, gzipGlob string, pollInterval time.Duration, readGzip bool, hotWindow time.Duration, p *parser.Parser, s *store.Store, idx *store.FileIndex) *Tailer {
	return &Tailer{
		dirs:      dirs,
		logGlob:   logGlob,
		gzipGlob:  gzipGlob,
		parser:    p,
		store:     s,
		index:     idx,
		poll:      pollInterval,
		readGzip:  readGzip,
		hotWindow: hotWindow,
		positions: make(map[string]int64),
		stopCh:    make(chan struct{}),
	}
}

// LoadExisting indexes all log files and loads only those within the hot window.
func (t *Tailer) LoadExisting() {
	cutoff := time.Time{} // zero = load all
	if t.hotWindow > 0 {
		cutoff = time.Now().Add(-t.hotWindow)
	}

	for _, dir := range t.dirs {
		t.indexAndLoadGlob(dir, t.logGlob, false, cutoff)
		if t.readGzip {
			t.indexAndLoadGlob(dir, t.gzipGlob, true, cutoff)
		}
	}

	summary := t.index.Summary()
	log.Printf("[tailer] indexed %v files (%v loaded, %v cold)",
		summary["totalFiles"], summary["loadedFiles"], summary["coldFiles"])
	if hotFrom, ok := summary["hotFrom"]; ok {
		log.Printf("[tailer] hot range: %v → %v", hotFrom, summary["hotTo"])
	}
	if avFrom, ok := summary["availableFrom"]; ok {
		log.Printf("[tailer] available range: %v → %v", avFrom, summary["availableTo"])
	}
}

// LoadColdFile loads a specific cold file into the hot store on demand.
func (t *Tailer) LoadColdFile(path string) (int, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("stat %s: %w", path, err)
	}

	isGzip := strings.HasSuffix(path, ".gz")
	var entries []*parser.LogEntry

	if isGzip {
		entries, err = t.readGzipEntries(path)
	} else {
		entries, err = t.readFileEntries(path)
	}
	if err != nil {
		return 0, err
	}

	if len(entries) > 0 {
		t.store.AddBatch(entries)
	}

	// Update index
	fi := &store.FileInfo{
		Path:    path,
		Name:    filepath.Base(path),
		Size:    info.Size(),
		Entries: len(entries),
		Loaded:  true,
		IsGzip:  isGzip,
	}
	if len(entries) > 0 {
		fi.FirstTS = entries[0].Timestamp
		fi.LastTS = entries[len(entries)-1].Timestamp
	}
	t.index.Register(fi)

	log.Printf("[tailer] cold-loaded %d entries from %s", len(entries), filepath.Base(path))
	return len(entries), nil
}

// LoadTimeRange loads all cold files overlapping [from, to] into the hot store.
func (t *Tailer) LoadTimeRange(from, to time.Time) (loaded int, files int) {
	coldFiles := t.index.FilesInRange(from, to)
	for _, fi := range coldFiles {
		if fi.Loaded {
			continue
		}
		n, err := t.LoadColdFile(fi.Path)
		if err != nil {
			log.Printf("[tailer] error loading cold file %s: %v", fi.Path, err)
			continue
		}
		loaded += n
		files++
	}
	return
}

// Start begins polling for new log lines.
func (t *Tailer) Start() {
	go t.pollLoop()
	log.Printf("[tailer] watching %v every %v", t.dirs, t.poll)
}

// Stop terminates the tailer.
func (t *Tailer) Stop() {
	close(t.stopCh)
}

func (t *Tailer) pollLoop() {
	ticker := time.NewTicker(t.poll)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopCh:
			return
		case <-ticker.C:
			t.pollOnce()
		}
	}
}

func (t *Tailer) pollOnce() {
	for _, dir := range t.dirs {
		matches, err := filepath.Glob(filepath.Join(dir, t.logGlob))
		if err != nil {
			continue
		}
		for _, path := range matches {
			t.tailFile(path)
		}
	}
}

func (t *Tailer) tailFile(path string) {
	info, err := os.Stat(path)
	if err != nil {
		return
	}

	t.posMu.Lock()
	lastPos := t.positions[path]
	t.posMu.Unlock()

	size := info.Size()

	if size < lastPos {
		lastPos = 0
	}

	if size == lastPos {
		return
	}

	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	if _, err := f.Seek(lastPos, io.SeekStart); err != nil {
		return
	}

	baseName := filepath.Base(path)
	lineNum := countLines(lastPos)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024)

	var pending *parser.LogEntry

	for scanner.Scan() {
		line := scanner.Text()
		lineNum++

		entry := t.parser.Parse(line, baseName, lineNum)
		if entry != nil {
			if pending != nil {
				t.store.Add(pending)
			}
			pending = entry
		} else if pending != nil {
			pending.Message += "\n" + line
			pending.Raw += "\n" + line
		}
	}

	if pending != nil {
		t.store.Add(pending)
	}

	newPos, _ := f.Seek(0, io.SeekCurrent)
	t.posMu.Lock()
	t.positions[path] = newPos
	t.posMu.Unlock()
}

func (t *Tailer) indexAndLoadGlob(dir, glob string, isGzip bool, cutoff time.Time) {
	matches, err := filepath.Glob(filepath.Join(dir, glob))
	if err != nil {
		return
	}
	for _, path := range matches {
		info, err := os.Stat(path)
		if err != nil {
			continue
		}

		// Quick timestamp probe: read first and last parseable lines
		firstTS, lastTS := t.probeTimestamps(path, isGzip)

		fi := &store.FileInfo{
			Path:    path,
			Name:    filepath.Base(path),
			Size:    info.Size(),
			FirstTS: firstTS,
			LastTS:  lastTS,
			IsGzip:  isGzip,
		}

		// Decide whether to load (hot) or just index (cold)
		shouldLoad := cutoff.IsZero() || lastTS.IsZero() || !lastTS.Before(cutoff)

		if shouldLoad {
			if isGzip {
				t.loadGzipFile(path)
			} else {
				t.loadFile(path)
			}
			fi.Loaded = true
			// Re-count from store (loadFile already added)
			fi.Entries = t.countFileEntries(fi.Name)
		}

		t.index.Register(fi)
	}
}

// probeTimestamps reads just the first and last parseable lines to get time range.
func (t *Tailer) probeTimestamps(path string, isGzip bool) (first, last time.Time) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	var r io.Reader = f
	if isGzip {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return
		}
		defer gz.Close()
		r = gz
	}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 512*1024)

	baseName := filepath.Base(path)
	var lineNum int64
	const maxProbeLines = 5000 // scan at most this many lines for first TS

	for scanner.Scan() {
		lineNum++
		entry := t.parser.Parse(scanner.Text(), baseName, lineNum)
		if entry != nil {
			if first.IsZero() {
				first = entry.Timestamp
			}
			last = entry.Timestamp
		}
		if !first.IsZero() && lineNum > maxProbeLines {
			// Got first TS; for large files, modtime approximates the end
			break
		}
	}

	// If we broke early on a large non-gzip file, use modtime as last estimate
	if !first.IsZero() && lineNum > maxProbeLines && !isGzip {
		if info, err := os.Stat(path); err == nil {
			last = info.ModTime()
		}
	}

	return
}

func (t *Tailer) countFileEntries(fileName string) int {
	all := t.store.All()
	count := 0
	for _, e := range all {
		if e.File == fileName {
			count++
		}
	}
	return count
}

func (t *Tailer) readFileEntries(path string) ([]*parser.LogEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return t.scanReader(f, filepath.Base(path)), nil
}

func (t *Tailer) readGzipEntries(path string) ([]*parser.LogEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	return t.scanReader(gz, filepath.Base(path)), nil
}

func (t *Tailer) loadGlob(dir, glob string, isGzip bool) {
	matches, err := filepath.Glob(filepath.Join(dir, glob))
	if err != nil {
		return
	}
	for _, path := range matches {
		if isGzip {
			t.loadGzipFile(path)
		} else {
			t.loadFile(path)
		}
	}
}

func (t *Tailer) loadFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		log.Printf("[tailer] skip %s: %v", path, err)
		return
	}
	defer f.Close()

	entries := t.scanReader(f, filepath.Base(path))
	if len(entries) > 0 {
		t.store.AddBatch(entries)
		log.Printf("[tailer] loaded %d entries from %s", len(entries), filepath.Base(path))
	}

	pos, _ := f.Seek(0, io.SeekCurrent)
	t.posMu.Lock()
	t.positions[path] = pos
	t.posMu.Unlock()
}

func (t *Tailer) loadGzipFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		log.Printf("[tailer] skip gzip %s: %v", path, err)
		return
	}
	defer gz.Close()

	entries := t.scanReader(gz, filepath.Base(path))
	if len(entries) > 0 {
		t.store.AddBatch(entries)
		log.Printf("[tailer] loaded %d entries from %s (gzip)", len(entries), filepath.Base(path))
	}
}

func (t *Tailer) scanReader(r io.Reader, fileName string) []*parser.LogEntry {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024)

	var entries []*parser.LogEntry
	var pending *parser.LogEntry
	var lineNum int64

	for scanner.Scan() {
		line := scanner.Text()
		lineNum++

		if strings.TrimSpace(line) == "" {
			continue
		}

		entry := t.parser.Parse(line, fileName, lineNum)
		if entry != nil {
			if pending != nil {
				entries = append(entries, pending)
			}
			pending = entry
		} else if pending != nil {
			pending.Message += "\n" + line
			pending.Raw += "\n" + line
		}
	}

	if pending != nil {
		entries = append(entries, pending)
	}

	return entries
}

func countLines(offset int64) int64 {
	return offset / 120
}
