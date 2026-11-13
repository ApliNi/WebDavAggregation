package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func findTestConfigFile() string {
	for _, p := range []string{"../../config.yml", "../../config.example.yml"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return "../../config.yml"
}

func TestLoadConfig(t *testing.T) {
	testFile := findTestConfigFile()
	cfg, err := Load(testFile)
	if err != nil {
		t.Fatalf("failed to load %s: %v", testFile, err)
	}

	if cfg.Listen != "127.0.0.1:5245" {
		t.Errorf("expected listen 127.0.0.1:5245, got %s", cfg.Listen)
	}
	if cfg.Retry != 3 {
		t.Errorf("expected retry 3, got %d", cfg.Retry)
	}
	if len(cfg.Upstream) != 2 {
		t.Fatalf("expected 2 upstreams, got %d", len(cfg.Upstream))
	}

	u1 := cfg.Upstream[0]
	if u1.URL != "http://127.0.0.1:5243" || u1.Capacity != "2T" {
		t.Errorf("unexpected upstream 1: %+v", u1)
	}
	capBytes, err := u1.CapacityBytes()
	if err != nil || capBytes != 2*1024*1024*1024*1024 {
		t.Errorf("unexpected capacity bytes: %d, err: %v", capBytes, err)
	}

	u2 := cfg.Upstream[1]
	if u2.URL != "http://127.0.0.1:5244" || u2.Capacity != "2T" {
		t.Errorf("unexpected upstream 2: %+v", u2)
	}

	if cfg.Auth.User != "admin" || cfg.Auth.Pass != "123456" {
		t.Errorf("unexpected auth config: %+v", cfg.Auth)
	}

	if cfg.CacheExpire != "24h" {
		t.Errorf("expected CacheExpire 24h, got %q", cfg.CacheExpire)
	}
	if cfg.CacheExpireDuration() != 24*time.Hour {
		t.Errorf("expected CacheExpireDuration 24h, got %v", cfg.CacheExpireDuration())
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(c *Config)
		wantErr bool
	}{
		{
			name: "valid config",
			mutate: func(c *Config) {
				c.Listen = "127.0.0.1:8080"
			},
			wantErr: false,
		},
		{
			name: "empty listen",
			mutate: func(c *Config) {
				c.Listen = ""
			},
			wantErr: true,
		},
		{
			name: "invalid listen format",
			mutate: func(c *Config) {
				c.Listen = "not-a-valid-address"
			},
			wantErr: true,
		},
		{
			name: "invalid listen port",
			mutate: func(c *Config) {
				c.Listen = "127.0.0.1:99999"
			},
			wantErr: true,
		},
		{
			name: "negative retry",
			mutate: func(c *Config) {
				c.Retry = -1
			},
			wantErr: true,
		},
		{
			name: "invalid proxy url",
			mutate: func(c *Config) {
				c.Proxy = "ftp://127.0.0.1:1080"
			},
			wantErr: true,
		},
		{
			name: "valid socks5 proxy",
			mutate: func(c *Config) {
				c.Proxy = "socks5://127.0.0.1:1080"
			},
			wantErr: false,
		},
		{
			name: "upstream non-positive id",
			mutate: func(c *Config) {
				c.Upstream = append(c.Upstream, UpstreamConfig{
					ID:  0,
					URL: "http://127.0.0.1:5000",
				})
			},
			wantErr: true,
		},
		{
			name: "duplicate upstream id",
			mutate: func(c *Config) {
				c.Upstream = []UpstreamConfig{
					{ID: 1, URL: "http://127.0.0.1:5001"},
					{ID: 1, URL: "http://127.0.0.1:5002"},
				}
			},
			wantErr: true,
		},
		{
			name: "upstream empty url",
			mutate: func(c *Config) {
				c.Upstream = append(c.Upstream, UpstreamConfig{
					ID:  10,
					URL: "",
				})
			},
			wantErr: true,
		},
		{
			name: "upstream invalid url scheme",
			mutate: func(c *Config) {
				c.Upstream = append(c.Upstream, UpstreamConfig{
					ID:  10,
					URL: "ftp://127.0.0.1:5000",
				})
			},
			wantErr: true,
		},
		{
			name: "upstream invalid capacity",
			mutate: func(c *Config) {
				c.Upstream = append(c.Upstream, UpstreamConfig{
					ID:       10,
					URL:      "http://127.0.0.1:5000",
					Capacity: "invalid-cap",
				})
			},
			wantErr: true,
		},
		{
			name: "upstream missing capacity",
			mutate: func(c *Config) {
				c.Upstream = append(c.Upstream, UpstreamConfig{
					ID:       10,
					URL:      "http://127.0.0.1:5000",
					Capacity: "",
				})
			},
			wantErr: true,
		},
		{
			name: "upstream valid ratio capacity",
			mutate: func(c *Config) {
				c.Upstream = append(c.Upstream, UpstreamConfig{
					ID:       10,
					URL:      "http://127.0.0.1:5000",
					Capacity: "2.00",
				})
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := New()
			tt.mutate(cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestIDOperations(t *testing.T) {
	cfg := New()
	if id := cfg.NextAvailableID(); id != 1 {
		t.Errorf("expected next ID 1 for empty upstream, got %d", id)
	}

	u1, err := cfg.AddUpstream(UpstreamConfig{
		URL:  "http://127.0.0.1:5243",
		User: "admin",
		Pass: "12345",
	})
	if err != nil {
		t.Fatalf("AddUpstream failed: %v", err)
	}
	if u1.ID != 1 {
		t.Errorf("expected auto ID 1, got %d", u1.ID)
	}

	if id := cfg.NextAvailableID(); id != 2 {
		t.Errorf("expected next ID 2, got %d", id)
	}

	u2, err := cfg.AddUpstream(UpstreamConfig{
		ID:   100,
		URL:  "http://127.0.0.1:5244",
		User: "admin",
		Pass: "12345",
	})
	if err != nil {
		t.Fatalf("AddUpstream failed: %v", err)
	}
	if u2.ID != 100 {
		t.Errorf("expected ID 100, got %d", u2.ID)
	}

	if id := cfg.NextAvailableID(); id != 101 {
		t.Errorf("expected next ID 101, got %d", id)
	}

	// Try adding duplicate ID 100
	_, err = cfg.AddUpstream(UpstreamConfig{
		ID:  100,
		URL: "http://127.0.0.1:5245",
	})
	if err == nil {
		t.Errorf("expected error when adding duplicate ID 100, got nil")
	}

	// GetUpstream
	retrieved, ok := cfg.GetUpstream(100)
	if !ok || retrieved.URL != "http://127.0.0.1:5244" {
		t.Errorf("failed to retrieve upstream 100")
	}
	_, ok = cfg.GetUpstream(999)
	if ok {
		t.Errorf("expected non-existent ID 999 to return false")
	}

	// MarkUpstreamType
	if err := cfg.MarkUpstreamType(100, "del"); err != nil {
		t.Fatalf("MarkUpstreamType failed: %v", err)
	}
	if retrieved.Type != "del" {
		t.Errorf("expected Type 'del', got %s", retrieved.Type)
	}
	if err := cfg.MarkUpstreamType(999, "del"); err == nil {
		t.Errorf("expected error marking non-existent upstream, got nil")
	}

	// RemoveUpstream
	if !cfg.RemoveUpstream(1) {
		t.Errorf("expected RemoveUpstream(1) to return true")
	}
	if cfg.RemoveUpstream(1) {
		t.Errorf("expected subsequent RemoveUpstream(1) to return false")
	}
	if len(cfg.Upstream) != 1 {
		t.Errorf("expected 1 remaining upstream, got %d", len(cfg.Upstream))
	}
}

func TestSaveAndPreserveStructure(t *testing.T) {
	tempDir := t.TempDir()
	testConfigFile := filepath.Join(tempDir, "config.yml")

	// Read original config content
	sourceFile := findTestConfigFile()
	origContent, err := os.ReadFile(sourceFile)
	if err != nil {
		t.Fatalf("failed to read original config file %s: %v", sourceFile, err)
	}
	if err := os.WriteFile(testConfigFile, origContent, 0644); err != nil {
		t.Fatalf("failed to write test config file: %v", err)
	}

	// Load from testConfigFile
	cfg, err := Load(testConfigFile)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	firstID := cfg.Upstream[0].ID
	secondID := cfg.Upstream[1].ID

	// Mark first node with type "del"
	if err := cfg.MarkUpstreamType(firstID, "del"); err != nil {
		t.Fatalf("MarkUpstreamType failed: %v", err)
	}

	// Add node 3
	_, err = cfg.AddUpstream(UpstreamConfig{
		ID:       3,
		URL:      "http://127.0.0.1:5246",
		User:     "admin",
		Pass:     "12345",
		Capacity: "4T",
	})
	if err != nil {
		t.Fatalf("AddUpstream failed: %v", err)
	}

	// Save
	if err := cfg.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Verify saved content contains comments and type: del
	savedBytes, err := os.ReadFile(testConfigFile)
	if err != nil {
		t.Fatalf("failed to read saved config file: %v", err)
	}
	savedStr := string(savedBytes)

	if !strings.Contains(savedStr, "# 上游请求失败重试") {
		t.Errorf("expected comments to be preserved, missing '# 上游请求失败重试'")
	}
	if !strings.Contains(savedStr, "# 上游服务列表") {
		t.Errorf("expected comments to be preserved, missing '# 上游服务列表'")
	}
	if !strings.Contains(savedStr, "type: del") {
		t.Errorf("expected 'type: del' to be present in YAML")
	}
	if !strings.Contains(savedStr, "id: 3") {
		t.Errorf("expected 'id: 3' to be present in YAML")
	}

	// Reload to verify validity
	reloaded, err := Load(testConfigFile)
	if err != nil {
		t.Fatalf("failed to reload saved config: %v", err)
	}
	if len(reloaded.Upstream) != 3 {
		t.Fatalf("expected 3 upstreams in reloaded config, got %d", len(reloaded.Upstream))
	}
	u1, ok := reloaded.GetUpstream(firstID)
	if !ok || u1.Type != "del" {
		t.Errorf("expected upstream %d to have Type 'del', got %+v", firstID, u1)
	}
	u3, ok := reloaded.GetUpstream(3)
	if !ok || u3.Capacity != "4T" {
		t.Errorf("expected upstream 3 to have capacity '4T', got %+v", u3)
	}

	// Remove second upstream and save again
	if !reloaded.RemoveUpstream(secondID) {
		t.Fatalf("failed to remove upstream %d", secondID)
	}
	if err := reloaded.Save(); err != nil {
		t.Fatalf("failed to save after removing upstream %d: %v", secondID, err)
	}

	reloaded2, err := Load(testConfigFile)
	if err != nil {
		t.Fatalf("failed to reload config: %v", err)
	}
	if len(reloaded2.Upstream) != 2 {
		t.Fatalf("expected 2 upstreams, got %d", len(reloaded2.Upstream))
	}
	if _, ok := reloaded2.GetUpstream(secondID); ok {
		t.Errorf("upstream %d was not removed from saved file", secondID)
	}
}

func TestSaveFromScratch(t *testing.T) {
	tempDir := t.TempDir()
	testConfigFile := filepath.Join(tempDir, "scratch_config.yml")

	cfg := New()
	cfg.Listen = "0.0.0.0:9000"
	cfg.Retry = 5
	cfg.Proxy = "http://127.0.0.1:7890"

	_, err := cfg.AddUpstream(UpstreamConfig{
		ID:       10,
		Type:     "del",
		URL:      "http://127.0.0.1:5243",
		User:     "user",
		Pass:     "pass",
		Capacity: "1T",
	})
	if err != nil {
		t.Fatalf("AddUpstream failed: %v", err)
	}

	if err := cfg.SaveToFile(testConfigFile); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	loaded, err := Load(testConfigFile)
	if err != nil {
		t.Fatalf("Load scratch config failed: %v", err)
	}
	if loaded.Listen != "0.0.0.0:9000" || loaded.Retry != 5 || loaded.Proxy != "http://127.0.0.1:7890" {
		t.Errorf("scratch config fields mismatch: %+v", loaded)
	}
	if len(loaded.Upstream) != 1 || loaded.Upstream[0].Type != "del" {
		t.Errorf("scratch upstream mismatch: %+v", loaded.Upstream)
	}
}

func TestGetDataFile(t *testing.T) {
	if DefaultDataFile != "data.db" {
		t.Errorf("expected DefaultDataFile to be data.db, got %s", DefaultDataFile)
	}

	cfg := New()
	if cfg.DataFile != "data.db" {
		t.Errorf("expected new cfg.DataFile to be data.db, got %s", cfg.DataFile)
	}
	if cfg.GetDataFile() != "data.db" {
		t.Errorf("expected GetDataFile to return data.db, got %s", cfg.GetDataFile())
	}

	cfg.DataFile = "data.yml"
	if cfg.GetDataFile() != "data.yml" {
		t.Errorf("expected GetDataFile to return data.yml, got %s", cfg.GetDataFile())
	}

	cfg.DataFile = ""
	if cfg.GetDataFile() != "data.db" {
		t.Errorf("expected empty DataFile to resolve to data.db, got %s", cfg.GetDataFile())
	}

	cfg.DataFile = "custom.db"
	cfg.SetFilePath(filepath.Join("var", "test", "config.yml"))
	expected := filepath.Join("var", "test", "custom.db")
	if cfg.GetDataFile() != expected {
		t.Errorf("expected %s, got %s", expected, cfg.GetDataFile())
	}
}
func TestCacheExpireDuration(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *Config
		expected time.Duration
	}{
		{
			name:     "nil config",
			cfg:      nil,
			expected: 24 * time.Hour,
		},
		{
			name:     "new default config",
			cfg:      New(),
			expected: 24 * time.Hour,
		},
		{
			name:     "empty string",
			cfg:      &Config{CacheExpire: ""},
			expected: 24 * time.Hour,
		},
		{
			name:     "whitespace string",
			cfg:      &Config{CacheExpire: "   \t\n  "},
			expected: 24 * time.Hour,
		},
		{
			name:     "zero string 0",
			cfg:      &Config{CacheExpire: "0"},
			expected: 0,
		},
		{
			name:     "zero string 0s",
			cfg:      &Config{CacheExpire: "0s"},
			expected: 0,
		},
		{
			name:     "zero string 0m",
			cfg:      &Config{CacheExpire: "0m"},
			expected: 0,
		},
		{
			name:     "zero string 0h",
			cfg:      &Config{CacheExpire: "0h"},
			expected: 0,
		},
		{
			name:     "negative integer -1",
			cfg:      &Config{CacheExpire: "-1"},
			expected: -1 * time.Second,
		},
		{
			name:     "negative duration -1s",
			cfg:      &Config{CacheExpire: "-1s"},
			expected: -1 * time.Second,
		},
		{
			name:     "negative duration -24h",
			cfg:      &Config{CacheExpire: "-24h"},
			expected: -24 * time.Hour,
		},
		{
			name:     "negative integer -60",
			cfg:      &Config{CacheExpire: "-60"},
			expected: -60 * time.Second,
		},
		{
			name:     "negative duration -5m",
			cfg:      &Config{CacheExpire: "-5m"},
			expected: -5 * time.Minute,
		},
		{
			name:     "standard duration 12h",
			cfg:      &Config{CacheExpire: "12h"},
			expected: 12 * time.Hour,
		},
		{
			name:     "standard duration 30m",
			cfg:      &Config{CacheExpire: "30m"},
			expected: 30 * time.Minute,
		},
		{
			name:     "standard duration 100s",
			cfg:      &Config{CacheExpire: "100s"},
			expected: 100 * time.Second,
		},
		{
			name:     "pure numeric seconds 86400",
			cfg:      &Config{CacheExpire: "86400"},
			expected: 86400 * time.Second,
		},
		{
			name:     "pure numeric seconds 3600",
			cfg:      &Config{CacheExpire: "3600"},
			expected: 1 * time.Hour,
		},
		{
			name:     "invalid string fallback to 24h",
			cfg:      &Config{CacheExpire: "invalid"},
			expected: 24 * time.Hour,
		},
		{
			name:     "invalid negative string fallback to 24h",
			cfg:      &Config{CacheExpire: "-invalid"},
			expected: 24 * time.Hour,
		},
		{
			name:     "malformed duration fallback to 24h",
			cfg:      &Config{CacheExpire: "123foobar"},
			expected: 24 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.CacheExpireDuration()
			if got != tt.expected {
				t.Errorf("CacheExpireDuration() = %v (%d ns), want %v (%d ns)", got, got, tt.expected, tt.expected)
			}
		})
	}
}
