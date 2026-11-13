package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"webdav-agg/src/client"
	"webdav-agg/src/config"
)

type mockWebDAVServer struct {
	server       *httptest.Server
	mu           sync.Mutex
	files        map[string][]byte
	modTimes     map[string]time.Time
	dirs         map[string]bool
	availBytes   int64
	usedBytes    int64
	supportQuota bool
	putFailCount int
	getFailCount int
	deletedFiles []string
}

func newMockWebDAVServer(avail, used int64, supportQuota bool) *mockWebDAVServer {
	m := &mockWebDAVServer{
		files:        make(map[string][]byte),
		modTimes:     make(map[string]time.Time),
		dirs:         map[string]bool{"/": true},
		availBytes:   avail,
		usedBytes:    used,
		supportQuota: supportQuota,
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()

		davPath := path.Clean(r.URL.Path)
		if !strings.HasPrefix(davPath, "/") {
			davPath = "/" + davPath
		}

		switch r.Method {
		case "PROPFIND":
			depth := r.Header.Get("Depth")
			if depth == "" {
				depth = "0"
			}

			// Quota query check: depth 0 on "/" with quota body
			bodyBytes, _ := io.ReadAll(r.Body)
			if string(bodyBytes) != "" && strings.Contains(string(bodyBytes), "quota-available-bytes") {
				if !m.supportQuota {
					w.WriteHeader(http.StatusMultiStatus)
					w.Write([]byte(`<?xml version="1.0" encoding="utf-8"?><D:multistatus xmlns:D="DAV:"></D:multistatus>`))
					return
				}
				w.Header().Set("Content-Type", "application/xml; charset=utf-8")
				w.WriteHeader(http.StatusMultiStatus)
				xml := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/</D:href>
    <D:propstat>
      <D:status>HTTP/1.1 200 OK</D:status>
      <D:prop>
        <D:quota-available-bytes>%d</D:quota-available-bytes>
        <D:quota-used-bytes>%d</D:quota-used-bytes>
      </D:prop>
    </D:propstat>
  </D:response>
</D:multistatus>`, m.availBytes, m.usedBytes)
				w.Write([]byte(xml))
				return
			}

			// File / Dir listing PROPFIND
			if davPath == "/" || m.dirs[davPath] {
				w.Header().Set("Content-Type", "application/xml; charset=utf-8")
				w.WriteHeader(http.StatusMultiStatus)

				var buf bytes.Buffer
				buf.WriteString(`<?xml version="1.0" encoding="utf-8"?><D:multistatus xmlns:D="DAV:">`)

				// Self
				buf.WriteString(fmt.Sprintf(`
<D:response>
  <D:href>%s</D:href>
  <D:propstat>
    <D:status>HTTP/1.1 200 OK</D:status>
    <D:prop>
      <D:resourcetype><D:collection/></D:resourcetype>
      <D:displayname>%s</D:displayname>
    </D:prop>
  </D:propstat>
</D:response>`, davPath, path.Base(davPath)))

				if depth == "1" {
					// Add children
					prefix := davPath
					if prefix != "/" {
						prefix += "/"
					}

					// Subdirs
					for d := range m.dirs {
						if d != davPath && strings.HasPrefix(d, prefix) {
							rel := strings.TrimPrefix(d, prefix)
							if !strings.Contains(rel, "/") {
								buf.WriteString(fmt.Sprintf(`
<D:response>
  <D:href>%s</D:href>
  <D:propstat>
    <D:status>HTTP/1.1 200 OK</D:status>
    <D:prop>
      <D:resourcetype><D:collection/></D:resourcetype>
      <D:displayname>%s</D:displayname>
    </D:prop>
  </D:propstat>
</D:response>`, d, path.Base(d)))
							}
						}
					}

					// Files
					for fPath, content := range m.files {
						if strings.HasPrefix(fPath, prefix) {
							rel := strings.TrimPrefix(fPath, prefix)
							if !strings.Contains(rel, "/") {
								mod := m.modTimes[fPath].UTC().Format(http.TimeFormat)
								buf.WriteString(fmt.Sprintf(`
<D:response>
  <D:href>%s</D:href>
  <D:propstat>
    <D:status>HTTP/1.1 200 OK</D:status>
    <D:prop>
      <D:resourcetype/>
      <D:getcontentlength>%d</D:getcontentlength>
      <D:getlastmodified>%s</D:getlastmodified>
      <D:displayname>%s</D:displayname>
    </D:prop>
  </D:propstat>
</D:response>`, fPath, len(content), mod, path.Base(fPath)))
							}
						}
					}
				}

				buf.WriteString(`</D:multistatus>`)
				w.Write(buf.Bytes())
				return
			}

			// Single file PROPFIND
			if content, ok := m.files[davPath]; ok {
				w.Header().Set("Content-Type", "application/xml; charset=utf-8")
				w.WriteHeader(http.StatusMultiStatus)
				mod := m.modTimes[davPath].UTC().Format(http.TimeFormat)
				xml := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>%s</D:href>
    <D:propstat>
      <D:status>HTTP/1.1 200 OK</D:status>
      <D:prop>
        <D:resourcetype/>
        <D:getcontentlength>%d</D:getcontentlength>
        <D:getlastmodified>%s</D:getlastmodified>
        <D:displayname>%s</D:displayname>
      </D:prop>
    </D:propstat>
  </D:response>
</D:multistatus>`, davPath, len(content), mod, path.Base(davPath))
				w.Write([]byte(xml))
				return
			}

			w.WriteHeader(http.StatusNotFound)

		case "GET":
			if m.getFailCount > 0 {
				m.getFailCount--
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			content, ok := m.files[davPath]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
			w.WriteHeader(http.StatusOK)
			w.Write(content)

		case "PUT":
			if m.putFailCount > 0 {
				m.putFailCount--
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			data, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			m.files[davPath] = data
			m.modTimes[davPath] = time.Now()
			// Ensure parent dirs
			dir := path.Dir(davPath)
			for dir != "/" && dir != "." {
				m.dirs[dir] = true
				dir = path.Dir(dir)
			}
			w.WriteHeader(http.StatusCreated)

		case "MKCOL":
			m.dirs[davPath] = true
			w.WriteHeader(http.StatusCreated)

		case "DELETE":
			delete(m.files, davPath)
			delete(m.dirs, davPath)
			m.deletedFiles = append(m.deletedFiles, davPath)
			w.WriteHeader(http.StatusNoContent)

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	m.server = httptest.NewServer(handler)
	return m
}

func (m *mockWebDAVServer) setFile(filePath string, content []byte, modTime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	clean := path.Clean(filePath)
	if !strings.HasPrefix(clean, "/") {
		clean = "/" + clean
	}
	m.files[clean] = content
	m.modTimes[clean] = modTime
	dir := path.Dir(clean)
	for dir != "/" && dir != "." {
		m.dirs[dir] = true
		dir = path.Dir(dir)
	}
}

func (m *mockWebDAVServer) Close() {
	m.server.Close()
}

func TestNode_QuotaOperations(t *testing.T) {
	srv := newMockWebDAVServer(500, 200, true)
	defer srv.Close()

	u := config.UpstreamConfig{
		ID:       1,
		URL:      srv.server.URL,
		User:     "u",
		Pass:     "p",
		Capacity: "700B",
	}

	cli, err := client.NewClient(u.URL, u.User, u.Pass, "", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	node := NewNode(u, cli)
	if node.ID() != 1 {
		t.Errorf("expected ID 1, got %d", node.ID())
	}

	ctx := context.Background()
	if err := node.UpdateQuota(ctx); err != nil {
		t.Fatalf("UpdateQuota failed: %v", err)
	}
	node.SetQuota(200, 0)
	avail, used, total := node.Quota()
	if avail != 500 || used != 200 || total != 700 {
		t.Errorf("unexpected quota: avail=%d, used=%d, total=%d", avail, used, total)
	}

	// Test RecordWrite
	node.RecordWrite(150)
	avail, used, total = node.Quota()
	if avail != 350 || used != 350 || total != 700 {
		t.Errorf("after RecordWrite unexpected quota: avail=%d, used=%d, total=%d", avail, used, total)
	}

	// Test RecordDelete
	node.RecordDelete(100)
	avail, used, total = node.Quota()
	if avail != 450 || used != 250 || total != 700 {
		t.Errorf("after RecordDelete unexpected quota: avail=%d, used=%d, total=%d", avail, used, total)
	}

	// Test Draining
	if node.Draining() {
		t.Error("expected node not draining")
	}
	node.SetDraining(true)
	if !node.Draining() {
		t.Error("expected node draining")
	}
}

func TestNode_ManualCapacityFallback(t *testing.T) {
	srv := newMockWebDAVServer(0, 0, false) // Quota not supported
	defer srv.Close()

	u := config.UpstreamConfig{
		ID:       2,
		URL:      srv.server.URL,
		User:     "u",
		Pass:     "p",
		Capacity: "100MB",
	}

	cli, err := client.NewClient(u.URL, u.User, u.Pass, "", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	node := NewNode(u, cli)
	expectedBytes := int64(100 * 1024 * 1024)
	if node.TotalBytes != expectedBytes || node.AvailableBytes != expectedBytes {
		t.Errorf("expected initial capacity %d, got total=%d avail=%d", expectedBytes, node.TotalBytes, node.AvailableBytes)
	}

	ctx := context.Background()
	// UpdateQuota should succeed using configured capacity
	if err := node.UpdateQuota(ctx); err != nil {
		t.Fatalf("UpdateQuota fallback failed: %v", err)
	}

	avail, used, total := node.Quota()
	if total != expectedBytes || avail != expectedBytes || used != 0 {
		t.Errorf("expected total=%d, avail=%d, used=0; got %d, %d, %d", expectedBytes, expectedBytes, avail, used, total)
	}
}

func TestCluster_LoadBalancing_MostFreeSpace(t *testing.T) {
	s1 := newMockWebDAVServer(1000, 200, true)
	defer s1.Close()
	s2 := newMockWebDAVServer(3000, 500, true)
	defer s2.Close()
	s3 := newMockWebDAVServer(2000, 300, true)
	defer s3.Close()

	cfg := &config.Config{
		Retry: 2,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: s1.server.URL, User: "u", Pass: "p", Capacity: "1000B"},
			{ID: 2, URL: s2.server.URL, User: "u", Pass: "p", Capacity: "3000B"},
			{ID: 3, URL: s3.server.URL, User: "u", Pass: "p", Capacity: "2000B"},
		},
	}

	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	ctx := context.Background()
	if err := c.RefreshAllQuotas(ctx); err != nil {
		t.Fatalf("RefreshAllQuotas failed: %v", err)
	}

	// 1. Most Free Space: Node 2 has 3000, Node 3 has 2000, Node 1 has 1000
	node, err := c.SelectWriteNode(500)
	if err != nil {
		t.Fatalf("SelectWriteNode failed: %v", err)
	}
	if node.ID() != 2 {
		t.Fatalf("expected node 2, got %d", node.ID())
	}

	// 2. Simulate write on Node 2 of 2500 bytes -> Node 2 available becomes 500
	node.RecordWrite(2500)

	// Now: Node 3 (2000) > Node 1 (1000) > Node 2 (500)
	node, err = c.SelectWriteNode(500)
	if err != nil {
		t.Fatalf("SelectWriteNode failed: %v", err)
	}
	if node.ID() != 3 {
		t.Fatalf("expected node 3, got %d", node.ID())
	}

	// 3. Request size larger than any single node available (e.g. 5000)
	_, err = c.SelectWriteNode(5000)
	if !errors.Is(err, ErrInsufficientStorage) {
		t.Fatalf("expected ErrInsufficientStorage, got %v", err)
	}

	// 4. Mark node 3 as draining -> SelectWriteNode must skip it
	n3, _ := c.GetNode(3)
	n3.SetDraining(true)

	// Now healthy: Node 1 (1000), Node 2 (500)
	node, err = c.SelectWriteNode(600)
	if err != nil {
		t.Fatalf("SelectWriteNode failed: %v", err)
	}
	if node.ID() != 1 {
		t.Fatalf("expected node 1, got %d", node.ID())
	}
}

func TestCluster_TotalQuota(t *testing.T) {
	s1 := newMockWebDAVServer(1000, 200, true) // avail 1000, used 200, tot 1200
	defer s1.Close()
	s2 := newMockWebDAVServer(2000, 400, true) // avail 2000, used 400, tot 2400
	defer s2.Close()
	s3 := newMockWebDAVServer(3000, 600, true) // del node: will be draining
	defer s3.Close()

	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: s1.server.URL, User: "u", Pass: "p", Capacity: "1200B"},
			{ID: 2, URL: s2.server.URL, User: "u", Pass: "p", Capacity: "2400B"},
			{ID: 3, URL: s3.server.URL, User: "u", Pass: "p", Capacity: "3600B", Type: "del"},
		},
	}

	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	ctx := context.Background()
	if err := c.RefreshAllQuotas(ctx); err != nil {
		t.Fatalf("RefreshAllQuotas failed: %v", err)
	}
	n1, _ := c.GetNode(1)
	n1.SetQuota(200, 0)
	n2, _ := c.GetNode(2)
	n2.SetQuota(400, 0)
	n3, _ := c.GetNode(3)
	n3.SetQuota(600, 0)
	avail, used, total := c.TotalQuota()
	// Node 3 is type: "del" (IsDraining = true), must be skipped!
	// Total available = 1000 + 2000 = 3000
	// Total used = 200 + 400 = 600
	// Total total = 1200 + 2400 = 3600
	if avail != 3000 || used != 600 || total != 3600 {
		t.Errorf("TotalQuota mismatch: got avail=%d, used=%d, total=%d; want 3000, 600, 3600", avail, used, total)
	}
}

func TestCluster_MigrateNode_PreflightInsufficient(t *testing.T) {
	s1 := newMockWebDAVServer(5000, 5000, true)
	defer s1.Close()
	s2 := newMockWebDAVServer(100, 0, true) // target node only has 100 bytes available
	defer s2.Close()

	s1.setFile("/bigfile.bin", make([]byte, 500), time.Now())

	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: s1.server.URL, User: "u", Pass: "p", Capacity: "10000B"},
			{ID: 2, URL: s2.server.URL, User: "u", Pass: "p", Capacity: "100B"},
		},
	}

	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	ctx := context.Background()
	if err := c.RefreshAllQuotas(ctx); err != nil {
		t.Fatalf("RefreshAllQuotas failed: %v", err)
	}

	err = c.MigrateNode(ctx, 1, nil)
	if err == nil {
		t.Fatal("expected preflight check to fail with ErrInsufficientStorage")
	}
	if !errors.Is(err, ErrInsufficientStorage) {
		t.Fatalf("expected ErrInsufficientStorage, got %v", err)
	}
}

func TestCluster_MigrateNode_Success(t *testing.T) {
	s1 := newMockWebDAVServer(5000, 500, true)
	defer s1.Close()
	s2 := newMockWebDAVServer(10000, 0, true)
	defer s2.Close()
	s3 := newMockWebDAVServer(10000, 0, true)
	defer s3.Close()

	content1 := []byte("hello file 1")
	content2 := []byte("hello nested file 2 content")
	s1.setFile("/file1.txt", content1, time.Now())
	s1.setFile("/sub/file2.txt", content2, time.Now())

	cfg := &config.Config{
		Retry: 2,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: s1.server.URL, User: "u", Pass: "p", Capacity: "5000B"},
			{ID: 2, URL: s2.server.URL, User: "u", Pass: "p", Capacity: "10000B"},
			{ID: 3, URL: s3.server.URL, User: "u", Pass: "p", Capacity: "10000B"},
		},
	}

	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	ctx := context.Background()
	if err := c.RefreshAllQuotas(ctx); err != nil {
		t.Fatalf("RefreshAllQuotas failed: %v", err)
	}

	var progressReports []string
	progressFunc := func(current, total int, p string, targetID int, size int64) {
		progressReports = append(progressReports, fmt.Sprintf("%d/%d: %s -> node %d (%d bytes)", current, total, p, targetID, size))
	}

	err = c.MigrateNode(ctx, 1, progressFunc)
	if err != nil {
		t.Fatalf("MigrateNode failed: %v", err)
	}

	if len(progressReports) != 2 {
		t.Fatalf("expected 2 progress reports, got %d: %v", len(progressReports), progressReports)
	}

	// Verify source node deleted both files
	s1.mu.Lock()
	if len(s1.files) != 0 {
		t.Errorf("expected source node to have 0 files, got %d", len(s1.files))
	}
	s1.mu.Unlock()

	// Verify target nodes received the files
	s2.mu.Lock()
	s3.mu.Lock()
	s2Files := len(s2.files)
	s3Files := len(s3.files)
	s2.mu.Unlock()
	s3.mu.Unlock()

	if s2Files+s3Files != 2 {
		t.Errorf("expected target nodes to have 2 files total, got %d + %d", s2Files, s3Files)
	}
}

func TestCluster_MigrateNode_WithRetry(t *testing.T) {
	s1 := newMockWebDAVServer(5000, 100, true)
	defer s1.Close()
	s2 := newMockWebDAVServer(10000, 0, true)
	defer s2.Close()

	// Simulate target failing PUT once then succeeding
	s2.putFailCount = 1

	s1.setFile("/retry.txt", []byte("retry content"), time.Now())

	cfg := &config.Config{
		Retry: 2,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: s1.server.URL, User: "u", Pass: "p", Capacity: "5000B"},
			{ID: 2, URL: s2.server.URL, User: "u", Pass: "p", Capacity: "10000B"},
		},
	}

	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	ctx := context.Background()
	if err := c.RefreshAllQuotas(ctx); err != nil {
		t.Fatalf("RefreshAllQuotas failed: %v", err)
	}

	err = c.MigrateNode(ctx, 1, nil)
	if err != nil {
		t.Fatalf("MigrateNode with retry failed: %v", err)
	}

	s2.mu.Lock()
	data, ok := s2.files["/retry.txt"]
	s2.mu.Unlock()
	if !ok || string(data) != "retry content" {
		t.Errorf("file was not correctly migrated to target: %s", string(data))
	}
}

func TestDeduplicateFiles(t *testing.T) {
	s1 := newMockWebDAVServer(5000, 100, true)
	defer s1.Close()
	s2 := newMockWebDAVServer(5000, 200, true)
	defer s2.Close()
	s3 := newMockWebDAVServer(5000, 300, true)
	defer s3.Close()

	cli1, _ := client.NewClient(s1.server.URL, "u", "p", "", 1, 5*time.Second)
	cli2, _ := client.NewClient(s2.server.URL, "u", "p", "", 1, 5*time.Second)
	cli3, _ := client.NewClient(s3.server.URL, "u", "p", "", 1, 5*time.Second)

	n1 := NewNode(config.UpstreamConfig{ID: 1}, cli1)
	n2 := NewNode(config.UpstreamConfig{ID: 2}, cli2)
	n3 := NewNode(config.UpstreamConfig{ID: 3}, cli3)

	s1.setFile("/test.txt", []byte("oldest"), time.Now().Add(-2*time.Hour))
	s2.setFile("/test.txt", []byte("newest"), time.Now())
	s3.setFile("/test.txt", []byte("middle"), time.Now().Add(-1*time.Hour))

	instances := []FileInfoWithNode{
		{
			FileInfo: client.FileInfo{
				Path:    "/test.txt",
				Size:    6,
				ModTime: time.Now().Add(-2 * time.Hour),
			},
			Node: n1,
		},
		{
			FileInfo: client.FileInfo{
				Path:    "/test.txt",
				Size:    6,
				ModTime: time.Now(),
			},
			Node: n2,
		},
		{
			FileInfo: client.FileInfo{
				Path:    "/test.txt",
				Size:    6,
				ModTime: time.Now().Add(-1 * time.Hour),
			},
			Node: n3,
		},
	}

	ctx := context.Background()
	chosen, err := DeduplicateFiles(ctx, "/test.txt", instances)
	if err != nil {
		t.Fatalf("DeduplicateFiles failed: %v", err)
	}

	if chosen.Node.ID() != 2 {
		t.Errorf("expected newest node 2 to be chosen, got %d", chosen.Node.ID())
	}

	// Verify old files deleted on Node 1 and Node 3
	s1.mu.Lock()
	_, f1Exists := s1.files["/test.txt"]
	s1.mu.Unlock()
	if f1Exists {
		t.Error("expected /test.txt to be deleted on node 1")
	}

	s3.mu.Lock()
	_, f3Exists := s3.files["/test.txt"]
	s3.mu.Unlock()
	if f3Exists {
		t.Error("expected /test.txt to be deleted on node 3")
	}

	// Verify newest file kept on Node 2
	s2.mu.Lock()
	_, f2Exists := s2.files["/test.txt"]
	s2.mu.Unlock()
	if !f2Exists {
		t.Error("expected /test.txt to be kept on node 2")
	}

	// Test single instance
	single, err := DeduplicateFiles(ctx, "/test.txt", instances[1:2])
	if err != nil || single.Node.ID() != 2 {
		t.Errorf("single instance dedup failed: %v", err)
	}

	// Test directory instances (should not delete)
	dirInstances := []FileInfoWithNode{
		{
			FileInfo: client.FileInfo{Path: "/dir", IsDir: true, ModTime: time.Now().Add(-1 * time.Hour)},
			Node:     n1,
		},
		{
			FileInfo: client.FileInfo{Path: "/dir", IsDir: true, ModTime: time.Now()},
			Node:     n2,
		},
	}
	s1.dirs["/dir"] = true
	s2.dirs["/dir"] = true

	chosenDir, err := DeduplicateFiles(ctx, "/dir", dirInstances)
	if err != nil {
		t.Fatalf("dedup dir failed: %v", err)
	}
	if chosenDir.Node.ID() != 2 {
		t.Errorf("expected dir node 2, got %d", chosenDir.Node.ID())
	}
	s1.mu.Lock()
	dir1Exists := s1.dirs["/dir"]
	s1.mu.Unlock()
	if !dir1Exists {
		t.Error("directory should not be deleted on node 1")
	}
}

func TestCluster_AddRemoveNode(t *testing.T) {
	s1 := newMockWebDAVServer(1000, 100, true)
	defer s1.Close()

	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: s1.server.URL, User: "u", Pass: "p"},
		},
	}

	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	if len(c.AllNodes()) != 1 {
		t.Fatalf("expected 1 node, got %d", len(c.AllNodes()))
	}

	s2 := newMockWebDAVServer(2000, 200, true)
	defer s2.Close()

	added, err := c.AddNode(config.UpstreamConfig{
		ID:   2,
		URL:  s2.server.URL,
		User: "u",
		Pass: "p",
	})
	if err != nil {
		t.Fatalf("AddNode failed: %v", err)
	}
	if added.ID() != 2 {
		t.Errorf("expected added node ID 2, got %d", added.ID())
	}

	if len(c.AllNodes()) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(c.AllNodes()))
	}

	// Remove node 1
	if !c.RemoveNode(1) {
		t.Error("expected RemoveNode(1) to return true")
	}
	if _, ok := c.GetNode(1); ok {
		t.Error("node 1 should not exist")
	}
	if len(c.AllNodes()) != 1 {
		t.Errorf("expected 1 node, got %d", len(c.AllNodes()))
	}
}
func TestCluster_EdgeCases(t *testing.T) {
	// NewCluster(nil, nil)
	if _, err := NewCluster(nil, nil); err == nil {
		t.Error("expected error for nil config")
	}

	s1 := newMockWebDAVServer(1000, 100, true)
	defer s1.Close()

	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: s1.server.URL, User: "u", Pass: "p"},
		},
	}
	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	// AddNode with duplicate ID
	if _, err := c.AddNode(config.UpstreamConfig{ID: 1, URL: "http://example.com"}); err == nil {
		t.Error("expected error adding duplicate ID")
	}

	// RemoveNode with nonexistent ID
	if c.RemoveNode(999) {
		t.Error("expected false removing non-existent node")
	}

	// MigrateNode with non-existent source
	ctx := context.Background()
	if err := c.MigrateNode(ctx, 999, nil); err == nil {
		t.Error("expected error migrating non-existent node")
	}

	// MigrateNode with empty source node
	s2 := newMockWebDAVServer(1000, 100, true)
	defer s2.Close()
	_, _ = c.AddNode(config.UpstreamConfig{ID: 2, URL: s2.server.URL, User: "u", Pass: "p"})
	if err := c.MigrateNode(ctx, 2, nil); err != nil {
		t.Errorf("expected nil migrating empty source node, got %v", err)
	}

	// DeduplicateFiles on Cluster with empty instances
	if _, err := c.DeduplicateFiles(ctx, "/none", nil); !errors.Is(err, client.ErrNotFound) {
		t.Errorf("expected ErrNotFound for empty dedup, got %v", err)
	}

	// DeduplicateFiles on Cluster with 1 instance
	n1, _ := c.GetNode(1)
	single := []FileInfoWithNode{{FileInfo: client.FileInfo{Path: "/one"}, Node: n1}}
	if res, err := c.DeduplicateFiles(ctx, "/one", single); err != nil || res.Path != "/one" {
		t.Errorf("expected single instance returned, got %v, err=%v", res, err)
	}

	// Node nil client UpdateQuota
	bareNode := &Node{}
	if err := bareNode.UpdateQuota(ctx); err == nil {
		t.Error("expected error updating quota with nil client")
	}

	// RecordWrite and RecordDelete boundary checks
	testNode := NewNode(config.UpstreamConfig{ID: 10, Capacity: "100B"}, nil)
	testNode.AvailableBytes = 50
	testNode.UsedBytes = 50
	testNode.TotalBytes = 100

	testNode.RecordWrite(0)
	testNode.RecordWrite(-10)
	if testNode.AvailableBytes != 50 || testNode.UsedBytes != 50 {
		t.Errorf("non-positive write changed quota: %+v", testNode)
	}

	testNode.RecordWrite(100)
	if testNode.AvailableBytes != 0 || testNode.UsedBytes != 100 {
		t.Errorf("expected clamped quota: avail=0 used=100, got avail=%d used=%d", testNode.AvailableBytes, testNode.UsedBytes)
	}

	testNode.RecordDelete(0)
	testNode.RecordDelete(-10)
	testNode.RecordDelete(200)
	if testNode.UsedBytes != 0 || testNode.AvailableBytes != 100 {
		t.Errorf("expected clamped quota: used=0 avail=100, got avail=%d used=%d", testNode.AvailableBytes, testNode.UsedBytes)
	}

	// RefreshAllQuotas with failing node
	badServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer badServer.Close()

	badCfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: badServer.URL, User: "u", Pass: "p"},
		},
	}
	badCluster, err := NewCluster(badCfg, nil)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}
	if err := badCluster.RefreshAllQuotas(ctx); err == nil {
		t.Error("expected RefreshAllQuotas to return error when node fails")
	}
}
func TestPathLocker_TryLock(t *testing.T) {
	pl := NewPathLocker()
	path := "/test/conflict.txt"

	// 1. 成功加锁
	unlock1, ok := pl.TryLock(path)
	if !ok || unlock1 == nil {
		t.Fatalf("expected TryLock to succeed on unlocked path, got ok=%v", ok)
	}

	// 2. 并发/重复 TryLock 应当失败
	unlock2, ok2 := pl.TryLock(path)
	if ok2 || unlock2 != nil {
		t.Fatalf("expected TryLock to fail on already locked path, got ok=%v", ok2)
	}

	// 统一路径规范化测试 (e.g. "test/conflict.txt" vs "/test/conflict.txt")
	unlock3, ok3 := pl.TryLock("test/conflict.txt")
	if ok3 || unlock3 != nil {
		t.Fatalf("expected TryLock on unnormalized path to fail when normalized path is locked")
	}

	// 3. 释放锁
	unlock1()

	// 4. 释放后重新加锁应当成功
	unlock4, ok4 := pl.TryLock(path)
	if !ok4 || unlock4 == nil {
		t.Fatalf("expected TryLock to succeed after unlock, got ok=%v", ok4)
	}
	unlock4()

	// 5. 并发冲突测试
	var (
		wg         sync.WaitGroup
		successCnt int
		failCnt    int
		mu         sync.Mutex
	)
	goroutines := 10
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			u, ok := pl.TryLock("/concurrent.dat")
			if ok {
				mu.Lock()
				successCnt++
				mu.Unlock()
				time.Sleep(20 * time.Millisecond)
				u()
			} else {
				mu.Lock()
				failCnt++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if successCnt < 1 {
		t.Errorf("expected at least one goroutine to acquire lock, got %d", successCnt)
	}
	if failCnt == 0 {
		t.Errorf("expected concurrent TryLocks to experience conflicts")
	}

	// 6. Cluster.PathLocker() 访问验证
	c := &Cluster{pathLocker: pl}
	if c.PathLocker() != pl {
		t.Errorf("Cluster.PathLocker() did not return expected instance")
	}
}
func TestCluster_SelectWriteNode_WithOverwriteContext(t *testing.T) {
	// Node 1: avail 100, tot 1000
	s1 := newMockWebDAVServer(1000, 900, true)
	defer s1.Close()
	// Node 2: avail 5000, tot 10000
	s2 := newMockWebDAVServer(10000, 5000, true)
	defer s2.Close()

	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: s1.server.URL, User: "u", Pass: "p"},
			{ID: 2, URL: s2.server.URL, User: "u", Pass: "p"},
		},
	}

	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	n1, _ := c.GetNode(1)
	n1.AvailableBytes = 100
	n1.UsedBytes = 900
	n1.TotalBytes = 1000

	n2, _ := c.GetNode(2)
	n2.AvailableBytes = 5000
	n2.UsedBytes = 5000
	n2.TotalBytes = 10000

	// Case 1: Small file (50 bytes) on Node 1 overwritten with large file (2000 bytes)
	// Node 1 virtually has 100 + 50 = 150 < 2000, cannot accommodate.
	// Node 2 has 5000 >= 2000, so Node 2 must be selected!
	node, err := c.SelectWriteNode(2000, OverwriteContext{OldNodeID: 1, OldSize: 50})
	if err != nil {
		t.Fatalf("SelectWriteNode error: %v", err)
	}
	if node.ID() != 2 {
		t.Fatalf("expected node 2 to be selected for 2000 bytes, got node %d", node.ID())
	}

	// Case 2: In-place overwrite when Node 1 has more space after virtual credit
	// Make Node 1 have 6000 avail, Node 2 have 5000 avail
	n1.AvailableBytes = 6000
	nodeInPlace, err := c.SelectWriteNode(500, OverwriteContext{OldNodeID: 1, OldSize: 200})
	if err != nil {
		t.Fatalf("SelectWriteNode error: %v", err)
	}
	if nodeInPlace.ID() != 1 {
		t.Fatalf("expected node 1 to be selected in-place, got node %d", nodeInPlace.ID())
	}
}
func TestCluster_TotalQuota_AllLimit(t *testing.T) {
	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: "http://127.0.0.1:18001", Capacity: "2T"},
			{ID: 2, URL: "http://127.0.0.1:18002", Capacity: "500G"},
		},
	}
	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	n1, _ := c.GetNode(1)
	n2, _ := c.GetNode(2)
	n1.SetQuota(100*1024*1024*1024, 100) // 100GB used
	n2.SetQuota(50*1024*1024*1024, 50)   // 50GB used

	avail, used, total := c.TotalQuota()
	expectedTotal := int64(2)*1024*1024*1024*1024 + int64(500)*1024*1024*1024
	expectedUsed := int64(150) * 1024 * 1024 * 1024
	expectedAvail := expectedTotal - expectedUsed

	if total != expectedTotal {
		t.Errorf("expected total %d, got %d", expectedTotal, total)
	}
	if used != expectedUsed {
		t.Errorf("expected used %d, got %d", expectedUsed, used)
	}
	if avail != expectedAvail {
		t.Errorf("expected avail %d, got %d", expectedAvail, avail)
	}
}

func TestCluster_TotalQuota_AllRatio(t *testing.T) {
	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: "http://127.0.0.1:18001", Capacity: "1.00"},
			{ID: 2, URL: "http://127.0.0.1:18002", Capacity: "2.00"},
		},
	}
	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	n1, _ := c.GetNode(1)
	n2, _ := c.GetNode(2)
	n1.SetQuota(10*1024*1024*1024, 10) // 10GB used
	n2.SetQuota(20*1024*1024*1024, 20) // 20GB used

	avail, used, total := c.TotalQuota()
	expectedUsed := int64(30) * 1024 * 1024 * 1024
	expectedTotal := DefaultMaxCapacity
	expectedAvail := expectedTotal - expectedUsed

	if total != expectedTotal {
		t.Errorf("expected total %d (1 PiB), got %d", expectedTotal, total)
	}
	if used != expectedUsed {
		t.Errorf("expected used %d, got %d", expectedUsed, used)
	}
	if avail != expectedAvail {
		t.Errorf("expected avail %d, got %d", expectedAvail, avail)
	}
}

func TestCluster_TotalQuota_Mixed(t *testing.T) {
	// One limit node (1TB) and one ratio node (1.00)
	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: "http://127.0.0.1:18001", Capacity: "1T"},
			{ID: 2, URL: "http://127.0.0.1:18002", Capacity: "1.00"},
		},
	}
	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	n1, _ := c.GetNode(1)
	n2, _ := c.GetNode(2)
	n1.SetQuota(200*1024*1024*1024, 200) // 200GB used
	n2.SetQuota(100*1024*1024*1024, 100) // 100GB used

	avail, used, total := c.TotalQuota()
	expectedUsed := int64(300) * 1024 * 1024 * 1024
	expectedTotal := DefaultMaxCapacity
	expectedAvail := expectedTotal - expectedUsed

	if total != expectedTotal {
		t.Errorf("expected mixed pool total %d (1 PiB), got %d", expectedTotal, total)
	}
	if used != expectedUsed {
		t.Errorf("expected mixed pool used %d, got %d", expectedUsed, used)
	}
	if avail != expectedAvail {
		t.Errorf("expected mixed pool avail %d, got %d", expectedAvail, avail)
	}
}

func TestCluster_TotalQuota_Overflow(t *testing.T) {
	// If actual used bytes exceed 1 PiB
	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: "http://127.0.0.1:18001", Capacity: "1.00"},
		},
	}
	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	n1, _ := c.GetNode(1)
	hugeUsed := DefaultMaxCapacity + int64(50)*1024*1024*1024 // 1 PiB + 50 GB
	n1.SetQuota(hugeUsed, 5000)

	avail, used, total := c.TotalQuota()
	expectedTotal := hugeUsed + DefaultMaxCapacity
	expectedAvail := DefaultMaxCapacity

	if total != expectedTotal {
		t.Errorf("expected extended total %d, got %d", expectedTotal, total)
	}
	if used != hugeUsed {
		t.Errorf("expected used %d, got %d", hugeUsed, used)
	}
	if avail != expectedAvail {
		t.Errorf("expected avail %d, got %d", expectedAvail, avail)
	}
}
