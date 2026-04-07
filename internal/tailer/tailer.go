package tailer

import (
	"bufio"
	"compress/gzip"
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
	dirs     []string
	logGlob  string
	gzipGlob string
	parser   *parser.Parser
	store    *store.Store
	poll     time.Duration
	readGzip bool

	positions map[string]int64
	posMu     sync.Mutex

	stopCh chan struct{}
}

func New(dirs []string, logGlob, gzipGlob string, pollInterval time.Duration, readGzip bool, p *parser.Parser, s *store.Store) *Tailer {
	return &Tailer{
		dirs:      dirs,
		logGlob:   logGlob,
		gzipGlob:  gzipGlob,
		parser:    p,
		store:     s,
		poll:      pollInterval,
		readGzip:  readGzip,
		positions: make(map[string]int64),
		stopCh:    make(chan struct{}),
	}
}

// LoadExisting reads all existing log files into the store on startup.
func (t *Tailer) LoadExisting() {
	for _, dir := range t.dirs {
		t.loadGlob(dir, t.logGlob, false)
		if t.readGzip {
			t.loadGlob(dir, t.gzipGlob, true)
		}
	}
	log.Printf("[tailer] loaded existing logs: %d dirs", len(t.dirs))
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
