package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	// Server
	Port int    `json:"port"`
	Host string `json:"host"`

	// Auth
	AuthEnabled bool   `json:"authEnabled"`
	Username    string `json:"username"`
	Password    string `json:"password"`

	// Log sources
	LogDirs  []string `json:"logDirs"`
	LogGlob  string   `json:"logGlob"`
	GzipGlob string   `json:"gzipGlob"`

	// Store
	MaxEntries    int           `json:"maxEntries"`
	FlushInterval time.Duration `json:"flushInterval"`

	// Tailer
	PollInterval time.Duration `json:"pollInterval"`
	ReadArchived bool          `json:"readArchived"`

	// CORS
	CORSOrigins []string `json:"corsOrigins"`
}

func Default() *Config {
	return &Config{
		Port:          4080,
		Host:          "0.0.0.0",
		AuthEnabled:   true,
		Username:      "admin",
		Password:      "changeme",
		LogDirs:       []string{"../zcop/logs"},
		LogGlob:       "*.log",
		GzipGlob:      "*.log.gz",
		MaxEntries:    1_000_000,
		FlushInterval: 5 * time.Second,
		PollInterval:  500 * time.Millisecond,
		ReadArchived:  false,
		CORSOrigins:   []string{"*"},
	}
}

func (c *Config) LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	return json.Unmarshal(data, c)
}

func (c *Config) LoadFromEnv() {
	if v := os.Getenv("LOG_AGENT_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.Port = n
		}
	}
	if v := os.Getenv("LOG_AGENT_HOST"); v != "" {
		c.Host = v
	}
	if v := os.Getenv("LOG_AGENT_AUTH"); v == "false" {
		c.AuthEnabled = false
	}
	if v := os.Getenv("LOG_AGENT_USER"); v != "" {
		c.Username = v
	}
	if v := os.Getenv("LOG_AGENT_PASS"); v != "" {
		c.Password = v
	}
	if v := os.Getenv("LOG_AGENT_DIRS"); v != "" {
		c.LogDirs = splitCSV(v)
	}
	if v := os.Getenv("LOG_AGENT_MAX_ENTRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.MaxEntries = n
		}
	}
	if v := os.Getenv("LOG_AGENT_READ_ARCHIVED"); v == "true" {
		c.ReadArchived = true
	}
}

func (c *Config) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

func splitCSV(s string) []string {
	var result []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			part := s[start:i]
			trimmed := trimSpace(part)
			if trimmed != "" {
				result = append(result, trimmed)
			}
			start = i + 1
		}
	}
	return result
}

func trimSpace(s string) string {
	i, j := 0, len(s)
	for i < j && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	for j > i && (s[j-1] == ' ' || s[j-1] == '\t') {
		j--
	}
	return s[i:j]
}
