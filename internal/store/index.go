package store

import (
	"sync"
	"time"
)

// FileInfo holds metadata about a log file without loading its contents.
type FileInfo struct {
	Path    string    `json:"path"`
	Name    string    `json:"name"`
	Size    int64     `json:"size"`
	FirstTS time.Time `json:"firstTimestamp"`
	LastTS  time.Time `json:"lastTimestamp"`
	Entries int       `json:"entries"`
	Loaded  bool      `json:"loaded"`
	IsGzip  bool      `json:"isGzip"`
}

// FileIndex tracks all known log files and their time ranges.
type FileIndex struct {
	mu    sync.RWMutex
	files map[string]*FileInfo
}

func NewFileIndex() *FileIndex {
	return &FileIndex{files: make(map[string]*FileInfo)}
}

// Register adds or updates a file in the index.
func (fi *FileIndex) Register(info *FileInfo) {
	fi.mu.Lock()
	fi.files[info.Path] = info
	fi.mu.Unlock()
}

// MarkLoaded marks a file as loaded into the hot store.
func (fi *FileIndex) MarkLoaded(path string) {
	fi.mu.Lock()
	if f, ok := fi.files[path]; ok {
		f.Loaded = true
	}
	fi.mu.Unlock()
}

// All returns a snapshot of all file info, sorted by first timestamp.
func (fi *FileIndex) All() []*FileInfo {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	result := make([]*FileInfo, 0, len(fi.files))
	for _, f := range fi.files {
		cp := *f
		result = append(result, &cp)
	}
	return result
}

// ColdFiles returns files that are indexed but not loaded.
func (fi *FileIndex) ColdFiles() []*FileInfo {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	var result []*FileInfo
	for _, f := range fi.files {
		if !f.Loaded {
			cp := *f
			result = append(result, &cp)
		}
	}
	return result
}

// FilesInRange returns files whose time range overlaps [from, to].
func (fi *FileIndex) FilesInRange(from, to time.Time) []*FileInfo {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	var result []*FileInfo
	for _, f := range fi.files {
		if f.FirstTS.IsZero() && f.LastTS.IsZero() {
			continue
		}
		// Overlaps if file doesn't end before the range and doesn't start after
		if !f.LastTS.Before(from) && !f.FirstTS.After(to) {
			cp := *f
			result = append(result, &cp)
		}
	}
	return result
}

// TimeRange returns the overall available time range (all indexed files).
func (fi *FileIndex) TimeRange() (oldest, newest time.Time) {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	for _, f := range fi.files {
		if f.FirstTS.IsZero() {
			continue
		}
		if oldest.IsZero() || f.FirstTS.Before(oldest) {
			oldest = f.FirstTS
		}
		if f.LastTS.After(newest) {
			newest = f.LastTS
		}
	}
	return
}

// Summary returns index statistics.
func (fi *FileIndex) Summary() map[string]any {
	fi.mu.RLock()
	defer fi.mu.RUnlock()

	totalFiles := len(fi.files)
	var loadedCount, coldCount int
	var totalEntries int
	var totalSize int64

	for _, f := range fi.files {
		totalSize += f.Size
		totalEntries += f.Entries
		if f.Loaded {
			loadedCount++
		} else {
			coldCount++
		}
	}

	oldest, newest := time.Time{}, time.Time{}
	hotOldest, hotNewest := time.Time{}, time.Time{}
	for _, f := range fi.files {
		if f.FirstTS.IsZero() {
			continue
		}
		if oldest.IsZero() || f.FirstTS.Before(oldest) {
			oldest = f.FirstTS
		}
		if f.LastTS.After(newest) {
			newest = f.LastTS
		}
		if f.Loaded {
			if hotOldest.IsZero() || f.FirstTS.Before(hotOldest) {
				hotOldest = f.FirstTS
			}
			if f.LastTS.After(hotNewest) {
				hotNewest = f.LastTS
			}
		}
	}

	result := map[string]any{
		"totalFiles":     totalFiles,
		"loadedFiles":    loadedCount,
		"coldFiles":      coldCount,
		"totalEntries":   totalEntries,
		"totalSizeBytes": totalSize,
	}

	if !oldest.IsZero() {
		result["availableFrom"] = oldest.Format(time.RFC3339)
		result["availableTo"] = newest.Format(time.RFC3339)
	}
	if !hotOldest.IsZero() {
		result["hotFrom"] = hotOldest.Format(time.RFC3339)
		result["hotTo"] = hotNewest.Format(time.RFC3339)
	}

	return result
}
