package parser

import (
	"regexp"
	"strings"
	"time"
)

// LogEntry is a single parsed log line.
type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Level     string    `json:"level"`
	Thread    string    `json:"thread"`
	RID       string    `json:"rid"`
	UID       string    `json:"uid"`
	URI       string    `json:"uri"`
	Logger    string    `json:"logger"`
	Message   string    `json:"msg"`
	File      string    `json:"file"`
	Raw       string    `json:"raw"`
	Line      int64     `json:"line"`
}

// Parser parses ZCOP log lines into structured LogEntry values.
type Parser struct {
	newFmt *regexp.Regexp
	oldFmt *regexp.Regexp
}

func New() *Parser {
	newPat := `^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[^\s]*)\s+(\w+)\s+\[([^\]]*)\]\s+\[([^\]]*)\]\s+\[([^\]]*)\]\s+\[([^\]]*)\]\s+(\S+)\s+:\s+(.*)$`
	oldPat := `^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[^\s]*)\s+(\w+)\s+---\s+\[([^\]]*)\]\s+(\S+)\s+:\s+(.*)$`

	return &Parser{
		newFmt: regexp.MustCompile(newPat),
		oldFmt: regexp.MustCompile(oldPat),
	}
}

func (p *Parser) Parse(line, file string, lineNum int64) *LogEntry {
	if m := p.newFmt.FindStringSubmatch(line); m != nil {
		ts := parseTime(m[1])
		return &LogEntry{
			Timestamp: ts,
			Level:     normalizeLevel(m[2]),
			Thread:    strings.TrimSpace(m[3]),
			RID:       strings.TrimSpace(m[4]),
			UID:       strings.TrimSpace(m[5]),
			URI:       strings.TrimSpace(m[6]),
			Logger:    strings.TrimSpace(m[7]),
			Message:   m[8],
			File:      file,
			Raw:       line,
			Line:      lineNum,
		}
	}

	if m := p.oldFmt.FindStringSubmatch(line); m != nil {
		ts := parseTime(m[1])
		return &LogEntry{
			Timestamp: ts,
			Level:     normalizeLevel(m[2]),
			Thread:    strings.TrimSpace(m[3]),
			Logger:    strings.TrimSpace(m[4]),
			Message:   m[5],
			File:      file,
			Raw:       line,
			Line:      lineNum,
		}
	}

	return nil
}

func normalizeLevel(s string) string {
	s = strings.ToUpper(strings.TrimSpace(s))
	switch s {
	case "WARN":
		return "WARNING"
	case "TRACE":
		return "TRACE"
	case "DEBUG", "FINE", "FINER":
		return "DEBUG"
	case "SEVERE", "FATAL":
		return "ERROR"
	default:
		return s
	}
}

var timeLayouts = []string{
	"2006-01-02T15:04:05.000-07:00",
	"2006-01-02T15:04:05.000Z07:00",
	"2006-01-02T15:04:05.000Z",
	"2006-01-02T15:04:05-07:00",
	"2006-01-02 15:04:05.000",
	"2006-01-02 15:04:05",
}

func parseTime(s string) time.Time {
	for _, layout := range timeLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}
