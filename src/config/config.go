package config

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"webdav-agg/src/logger"
)

const (
	DefaultListen         = "127.0.0.1:5245"
	DefaultRetry          = 3
	DefaultConfigFileName = "config.yml"
	DefaultDataFile       = "data.db"
	DefaultCacheExpire    = "24h"
)

// UpstreamConfig represents the configuration for a single upstream WebDAV server.
type UpstreamConfig struct {
	ID       int    `yaml:"id"`
	Type     string `yaml:"type,omitempty"`
	URL      string `yaml:"url"`
	User     string `yaml:"user"`
	Pass     string `yaml:"pass"`
	Capacity string `yaml:"capacity"`
}

// ParseCapacity returns the parsed representation of the upstream capacity.
func (u UpstreamConfig) ParseCapacity() (ParsedCapacity, error) {
	return ParseCapacity(u.Capacity)
}

// CapacityBytes returns the parsed byte count for the upstream capacity in limit mode.
// If capacity is in ratio mode or empty, it returns 0 and nil.
func (u UpstreamConfig) CapacityBytes() (uint64, error) {
	if strings.TrimSpace(u.Capacity) == "" {
		return 0, nil
	}
	pc, err := ParseCapacity(u.Capacity)
	if err != nil {
		return 0, err
	}
	if pc.Mode == CapacityModeLimit {
		return pc.Bytes, nil
	}
	return 0, nil
}

// AuthConfig represents downstream HTTP basic authentication credentials.
type AuthConfig struct {
	User string `yaml:"user"`
	Pass string `yaml:"pass"`
}

// Enabled returns true if authentication is configured.
func (a AuthConfig) Enabled() bool {
	return a.User != "" || a.Pass != ""
}

// Config represents the top-level configuration for the webdav-agg service.
type Config struct {
	Listen   string           `yaml:"listen"`
	Retry    int              `yaml:"retry"`
	Proxy    string           `yaml:"proxy"`
	Auth        AuthConfig       `yaml:"auth,omitempty"`
	CacheExpire string           `yaml:"cache_expire,omitempty"`
	DataFile    string           `yaml:"data_file,omitempty"`
	Upstream []UpstreamConfig `yaml:"upstream"`
	filePath string           `yaml:"-"`
	docNode  *yaml.Node       `yaml:"-"`
}

// GetDataFile returns the configured data file path or the default value,
// resolved relative to the configuration file directory if relative.
// Supports SQLite database files (default: data.db).
// If the configured path is data.yml or empty, it is properly resolved.
func (c *Config) GetDataFile() string {
	target := DefaultDataFile
	if strings.TrimSpace(c.DataFile) != "" {
		target = strings.TrimSpace(c.DataFile)
	}
	if filepath.IsAbs(target) || c.filePath == "" {
		return target
	}
	return filepath.Join(filepath.Dir(c.filePath), target)
}

// New creates a new Config initialized with default settings.
func New() *Config {
	return &Config{
		Listen:      DefaultListen,
		Retry:       DefaultRetry,
		Proxy:       "",
		CacheExpire: DefaultCacheExpire,
		DataFile:    DefaultDataFile,
		Upstream:    make([]UpstreamConfig, 0),
	}
}

// CacheExpireDuration returns the parsed duration for upstream probe caching.
// - Returns 24h if empty or whitespace.
// - Returns 0 if "0" or "0s" (always query upstream).
// - Returns negative duration if starts with "-" or parses to negative duration (never expire).
// - Returns parsed duration if valid via time.ParseDuration.
// - Returns parsed seconds as duration if a pure numeric string.
// - Falls back to default 24h with a warning log if parsing fails.
func (c *Config) CacheExpireDuration() time.Duration {
	if c == nil {
		return 24 * time.Hour
	}
	raw := strings.TrimSpace(c.CacheExpire)
	if raw == "" {
		return 24 * time.Hour
	}
	if raw == "0" || raw == "0s" {
		return 0
	}

	// Try time.ParseDuration first (supports "-1s", "-24h", "12h", "30m", etc.)
	if d, err := time.ParseDuration(raw); err == nil {
		if d < 0 {
			return d
		}
		if d == 0 {
			return 0
		}
		return d
	}

	// Try pure numeric string in seconds (supports "-1", "86400", etc.)
	if sec, err := strconv.ParseInt(raw, 10, 64); err == nil {
		d := time.Duration(sec) * time.Second
		if d < 0 {
			return d
		}
		if d == 0 {
			return 0
		}
		return d
	}

	logger.Warn("Config", "Invalid cache_expire duration %q, falling back to 24h", c.CacheExpire)
	return 24 * time.Hour
}

// FilePath returns the file path from which the config was loaded or last saved.
func (c *Config) FilePath() string {
	return c.filePath
}

// SetFilePath updates the target file path for saving.
func (c *Config) SetFilePath(path string) {
	c.filePath = path
}

// Load loads and parses the YAML configuration file from the specified path.
func Load(filePath string) (*Config, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %q: %w", filePath, err)
	}

	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("failed to parse yaml in %q: %w", filePath, err)
	}

	var cfg Config
	if err := doc.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to decode config from %q: %w", filePath, err)
	}

	cfg.filePath = filePath
	cfg.docNode = &doc

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config in %q: %w", filePath, err)
	}

	return &cfg, nil
}

// LoadConfig is an alias for Load.
func LoadConfig(filePath string) (*Config, error) {
	return Load(filePath)
}

// Parse parses YAML configuration data from bytes and validates it.
func Parse(data []byte) (*Config, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("failed to parse yaml: %w", err)
	}

	var cfg Config
	if err := doc.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to decode config: %w", err)
	}

	cfg.docNode = &doc

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

// Validate validates the configuration values.
func (c *Config) Validate() error {
	listen := strings.TrimSpace(c.Listen)
	if listen == "" {
		return errors.New("config error: 'listen' address cannot be empty")
	}

	_, port, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("config error: invalid 'listen' address %q: %w", c.Listen, err)
	}
	if p, err := strconv.Atoi(port); err != nil || p <= 0 || p > 65535 {
		return fmt.Errorf("config error: 'listen' port %q must be between 1 and 65535", port)
	}

	if c.Retry < 0 {
		return fmt.Errorf("config error: 'retry' must be non-negative (got %d)", c.Retry)
	}

	if strings.TrimSpace(c.Proxy) != "" {
		proxyURL, err := url.Parse(c.Proxy)
		if err != nil || proxyURL.Scheme == "" || proxyURL.Host == "" {
			return fmt.Errorf("config error: invalid 'proxy' URL %q", c.Proxy)
		}
		scheme := strings.ToLower(proxyURL.Scheme)
		if scheme != "http" && scheme != "https" && scheme != "socks5" && scheme != "socks5h" {
			return fmt.Errorf("config error: unsupported proxy scheme %q in %q", scheme, c.Proxy)
		}
	}

	if err := c.CheckDuplicateIDs(); err != nil {
		return fmt.Errorf("config error: %w", err)
	}

	for i, u := range c.Upstream {
		if u.ID <= 0 {
			return fmt.Errorf("config error: upstream[%d] id must be positive (got %d)", i, u.ID)
		}
		if strings.TrimSpace(u.URL) == "" {
			return fmt.Errorf("config error: upstream[%d] (id %d) url cannot be empty", i, u.ID)
		}
		uURL, err := url.Parse(u.URL)
		if err != nil || uURL.Host == "" {
			return fmt.Errorf("config error: upstream[%d] (id %d) invalid url %q", i, u.ID, u.URL)
		}
		scheme := strings.ToLower(uURL.Scheme)
		if scheme != "http" && scheme != "https" {
			return fmt.Errorf("config error: upstream[%d] (id %d) url scheme must be http or https (got %q)", i, u.ID, u.URL)
		}
		if strings.TrimSpace(u.Capacity) == "" {
			return fmt.Errorf("config error: upstream[%d] (id %d) capacity must be specified", i, u.ID)
		}
		if _, err := ParseCapacity(u.Capacity); err != nil {
			return fmt.Errorf("config error: upstream[%d] (id %d) invalid capacity %q: %w", i, u.ID, u.Capacity, err)
		}
	}

	return nil
}

// CheckDuplicateIDs checks if there are duplicate IDs in the upstream list.
func (c *Config) CheckDuplicateIDs() error {
	seen := make(map[int]bool, len(c.Upstream))
	for _, u := range c.Upstream {
		if seen[u.ID] {
			return fmt.Errorf("duplicate upstream id: %d", u.ID)
		}
		seen[u.ID] = true
	}
	return nil
}

// NextAvailableID returns the next available unique incrementing ID.
// If there are no existing upstreams, it returns 1.
// Otherwise it returns the maximum existing ID plus 1.
func (c *Config) NextAvailableID() int {
	maxID := 0
	for _, u := range c.Upstream {
		if u.ID > maxID {
			maxID = u.ID
		}
	}
	return maxID + 1
}

// AddUpstream adds a new upstream node to the configuration.
// If node.ID <= 0, it automatically assigns NextAvailableID().
func (c *Config) AddUpstream(node UpstreamConfig) (*UpstreamConfig, error) {
	if node.ID <= 0 {
		node.ID = c.NextAvailableID()
	} else {
		for _, u := range c.Upstream {
			if u.ID == node.ID {
				return nil, fmt.Errorf("upstream id %d already exists", node.ID)
			}
		}
	}

	if strings.TrimSpace(node.URL) == "" {
		return nil, errors.New("upstream url cannot be empty")
	}
	parsedURL, err := url.Parse(node.URL)
	if err != nil || parsedURL.Host == "" {
		return nil, fmt.Errorf("invalid upstream url %q", node.URL)
	}
	scheme := strings.ToLower(parsedURL.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil, fmt.Errorf("upstream url scheme must be http or https (got %q)", node.URL)
	}

	if strings.TrimSpace(node.Capacity) != "" {
		if _, err := ParseCapacity(node.Capacity); err != nil {
			return nil, fmt.Errorf("invalid upstream capacity %q: %w", node.Capacity, err)
		}
	}

	c.Upstream = append(c.Upstream, node)
	return &c.Upstream[len(c.Upstream)-1], nil
}

// RemoveUpstream removes an upstream by ID. Returns true if removed, false if not found.
func (c *Config) RemoveUpstream(id int) bool {
	for i, u := range c.Upstream {
		if u.ID == id {
			c.Upstream = append(c.Upstream[:i], c.Upstream[i+1:]...)
			return true
		}
	}
	return false
}

// GetUpstream retrieves an upstream configuration by ID.
func (c *Config) GetUpstream(id int) (*UpstreamConfig, bool) {
	for i := range c.Upstream {
		if c.Upstream[i].ID == id {
			return &c.Upstream[i], true
		}
	}
	return nil, false
}

// MarkUpstreamType sets the Type field for the upstream with the given ID (e.g. "del").
func (c *Config) MarkUpstreamType(id int, nodeType string) error {
	for i := range c.Upstream {
		if c.Upstream[i].ID == id {
			c.Upstream[i].Type = nodeType
			return nil
		}
	}
	return fmt.Errorf("upstream id %d not found", id)
}

// Save writes the configuration back to the file it was loaded from.
func (c *Config) Save() error {
	if c.filePath == "" {
		return errors.New("config file path is not set")
	}
	return c.SaveToFile(c.filePath)
}

// SaveToFile writes the configuration to the specified file path.
func (c *Config) SaveToFile(filePath string) error {
	if err := c.Validate(); err != nil {
		return fmt.Errorf("cannot save invalid config: %w", err)
	}

	data, err := c.ToYAML()
	if err != nil {
		return fmt.Errorf("failed to serialize config: %w", err)
	}

	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %q: %w", dir, err)
	}

	tmpFile, err := os.CreateTemp(dir, "config-*.tmp")
	if err != nil {
		return os.WriteFile(filePath, data, 0644)
	}
	tmpName := tmpFile.Name()

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}

	if err := os.Rename(tmpName, filePath); err != nil {
		_ = os.Remove(filePath)
		if err := os.Rename(tmpName, filePath); err != nil {
			_ = os.Remove(tmpName)
			return os.WriteFile(filePath, data, 0644)
		}
	}

	c.filePath = filePath
	return nil
}

// ToYAML serializes the configuration to YAML bytes, preserving structure and comments when possible.
func (c *Config) ToYAML() ([]byte, error) {
	if c.docNode != nil {
		if err := syncDocNode(c.docNode, c); err != nil {
			return nil, err
		}
		var buf bytes.Buffer
		enc := yaml.NewEncoder(&buf)
		enc.SetIndent(2)
		if err := enc.Encode(c.docNode); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func syncDocNode(doc *yaml.Node, cfg *Config) error {
	if doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 {
		return doc.Encode(cfg)
	}

	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return doc.Encode(cfg)
	}

	setRootScalar(root, "listen", cfg.Listen)
	setRootInt(root, "retry", cfg.Retry)
	setRootScalar(root, "proxy", cfg.Proxy)
	if cfg.CacheExpire != "" {
		setRootScalar(root, "cache_expire", cfg.CacheExpire)
	}

	var upstreamSeq *yaml.Node
	for i := 0; i < len(root.Content); i += 2 {
		if root.Content[i].Value == "upstream" {
			upstreamSeq = root.Content[i+1]
			break
		}
	}

	if upstreamSeq == nil {
		keyNode := &yaml.Node{Kind: yaml.ScalarNode, Value: "upstream", Tag: "!!str"}
		upstreamSeq = &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
		root.Content = append(root.Content, keyNode, upstreamSeq)
	}

	oldNodesByID := make(map[int]*yaml.Node)
	for _, itemNode := range upstreamSeq.Content {
		if itemNode.Kind == yaml.MappingNode {
			for j := 0; j < len(itemNode.Content); j += 2 {
				if itemNode.Content[j].Value == "id" {
					if idVal, err := strconv.Atoi(itemNode.Content[j+1].Value); err == nil {
						oldNodesByID[idVal] = itemNode
					}
					break
				}
			}
		}
	}

	newSeqContent := make([]*yaml.Node, 0, len(cfg.Upstream))
	for _, u := range cfg.Upstream {
		if oldNode, ok := oldNodesByID[u.ID]; ok {
			setMappingScalar(oldNode, "url", u.URL)
			setMappingScalar(oldNode, "user", u.User)
			setMappingScalar(oldNode, "pass", u.Pass)
			if u.Capacity != "" {
				setMappingScalar(oldNode, "capacity", u.Capacity)
			} else {
				deleteMappingKey(oldNode, "capacity")
			}
			if u.Type != "" {
				setMappingScalar(oldNode, "type", u.Type)
			} else {
				deleteMappingKey(oldNode, "type")
			}
			newSeqContent = append(newSeqContent, oldNode)
		} else {
			var encDoc yaml.Node
			if err := encDoc.Encode(u); err != nil {
				return err
			}
			if encDoc.Kind == yaml.DocumentNode && len(encDoc.Content) > 0 {
				newSeqContent = append(newSeqContent, encDoc.Content[0])
			} else {
				newSeqContent = append(newSeqContent, &encDoc)
			}
		}
	}

	upstreamSeq.Content = newSeqContent
	return nil
}

func setRootScalar(root *yaml.Node, key, val string) {
	for i := 0; i < len(root.Content); i += 2 {
		if root.Content[i].Value == key {
			root.Content[i+1].Value = val
			root.Content[i+1].Tag = "!!str"
			return
		}
	}
	keyNode := &yaml.Node{Kind: yaml.ScalarNode, Value: key, Tag: "!!str"}
	valNode := &yaml.Node{Kind: yaml.ScalarNode, Value: val, Tag: "!!str"}
	root.Content = append(root.Content, keyNode, valNode)
}

func setRootInt(root *yaml.Node, key string, val int) {
	for i := 0; i < len(root.Content); i += 2 {
		if root.Content[i].Value == key {
			root.Content[i+1].Value = strconv.Itoa(val)
			root.Content[i+1].Tag = "!!int"
			return
		}
	}
	keyNode := &yaml.Node{Kind: yaml.ScalarNode, Value: key, Tag: "!!str"}
	valNode := &yaml.Node{Kind: yaml.ScalarNode, Value: strconv.Itoa(val), Tag: "!!int"}
	root.Content = append(root.Content, keyNode, valNode)
}

func setMappingScalar(m *yaml.Node, key, val string) {
	for i := 0; i < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			m.Content[i+1].Value = val
			m.Content[i+1].Tag = "!!str"
			return
		}
	}

	keyNode := &yaml.Node{Kind: yaml.ScalarNode, Value: key, Tag: "!!str"}
	valNode := &yaml.Node{Kind: yaml.ScalarNode, Value: val, Tag: "!!str"}

	if key == "type" {
		for i := 0; i < len(m.Content); i += 2 {
			if m.Content[i].Value == "id" {
				insertIdx := i + 2
				m.Content = append(m.Content[:insertIdx], append([]*yaml.Node{keyNode, valNode}, m.Content[insertIdx:]...)...)
				return
			}
		}
	}

	m.Content = append(m.Content, keyNode, valNode)
}

func deleteMappingKey(m *yaml.Node, key string) {
	for i := 0; i < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			m.Content = append(m.Content[:i], m.Content[i+2:]...)
			return
		}
	}
}
