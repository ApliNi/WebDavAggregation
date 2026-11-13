package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"webdav-agg/src/config"
	"webdav-agg/src/data"
)

// createDelayedServer returns a mock WebDAV HTTP server that simulates a specific latency or failure.
func createDelayedServer(filename string, size int64, delay time.Duration, isFailure bool, canceled *atomic.Bool, requestCount *atomic.Int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requestCount != nil {
			requestCount.Add(1)
		}
		if isFailure {
			http.Error(w, "upstream failure", http.StatusForbidden)
			return
		}
		if r.Method == "PROPFIND" {
			if r.Header.Get("Depth") == "0" {
				w.Header().Set("Content-Type", "application/xml; charset=utf-8")
				w.WriteHeader(http.StatusMultiStatus)
				_, _ = w.Write([]byte(`<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/</D:href>
    <D:propstat>
      <D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>`))
				return
			}

			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				if canceled != nil {
					canceled.Store(true)
				}
				return
			}
			w.Header().Set("Content-Type", "application/xml; charset=utf-8")
			w.WriteHeader(http.StatusMultiStatus)
			xmlResp := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/</D:href>
    <D:propstat>
      <D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/%s</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype/>
        <D:getcontentlength>%d</D:getcontentlength>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>`, filename, size)
			_, _ = w.Write([]byte(xmlResp))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
}

// TestInitData_ConcurrentWalkAndSerialDBWrite verifies:
// 1. Concurrent Walk executes in parallel across nodes: 3 nodes with 150ms latency each complete in ~150ms rather than ~450ms.
// 2. Sequential in-memory DB lock ensures all nodes' metadata, capacity and last_sync are persisted to SQLite without locks/conflicts.
func TestInitData_ConcurrentWalkAndSerialDBWrite(t *testing.T) {
	s1 := createDelayedServer("file1.bin", 1000, 150*time.Millisecond, false, nil, nil)
	defer s1.Close()
	s2 := createDelayedServer("file2.bin", 2000, 150*time.Millisecond, false, nil, nil)
	defer s2.Close()
	s3 := createDelayedServer("file3.bin", 3000, 150*time.Millisecond, false, nil, nil)
	defer s3.Close()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "data.db")
	dm, err := data.NewManager(dbPath)
	if err != nil {
		t.Fatalf("failed to create data.Manager: %v", err)
	}
	defer dm.Close()

	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: s1.URL, Capacity: "10G"},
			{ID: 2, URL: s2.URL, Capacity: "10G"},
			{ID: 3, URL: s3.URL, Capacity: "10G"},
		},
	}

	c, err := NewCluster(cfg, dm)
	if err != nil {
		t.Fatalf("failed to create Cluster: %v", err)
	}

	start := time.Now()
	if err := c.InitData(context.Background()); err != nil {
		t.Fatalf("InitData failed: %v", err)
	}
	elapsed := time.Since(start)

	t.Logf("InitData elapsed time: %v (serial would take >= 450ms)", elapsed)

	// In serial execution, 3 nodes * 150ms = 450ms.
	// With concurrent walk, total time should be close to 150ms and well under 400ms.
	if elapsed >= 380*time.Millisecond {
		t.Errorf("InitData took %v, expected concurrent execution < 380ms", elapsed)
	}

	// Verify in-memory node stats
	expectedStats := map[int]struct {
		used  int64
		files int64
	}{
		1: {used: 1000, files: 1},
		2: {used: 2000, files: 1},
		3: {used: 3000, files: 1},
	}

	for id, exp := range expectedStats {
		node, ok := c.GetNode(id)
		if !ok {
			t.Errorf("node %d not found in cluster", id)
			continue
		}
		_, used, _ := node.Quota()
		files := node.Files()
		if used != exp.used || files != exp.files {
			t.Errorf("node %d in-memory stats: used=%d, files=%d; expected used=%d, files=%d",
				id, used, files, exp.used, exp.files)
		}
	}

	// Verify SQLite storages table
	for id, exp := range expectedStats {
		st, err := dm.GetStorage(id)
		if err != nil {
			t.Errorf("dm.GetStorage(%d) failed: %v", id, err)
			continue
		}
		if st == nil {
			t.Errorf("dm.GetStorage(%d) returned nil", id)
			continue
		}
		if st.UsedBytes != exp.used || st.FileCount != exp.files {
			t.Errorf("storage %d in DB: used=%d, files=%d; expected used=%d, files=%d",
				id, st.UsedBytes, st.FileCount, exp.used, exp.files)
		}
		if st.LastSync == nil || st.LastSync.IsZero() {
			t.Errorf("storage %d has nil or zero LastSync in DB", id)
		}
	}

	// Verify SQLite files table
	expectedFiles := map[string]struct {
		storageID int
		size      int64
	}{
		"/file1.bin": {storageID: 1, size: 1000},
		"/file2.bin": {storageID: 2, size: 2000},
		"/file3.bin": {storageID: 3, size: 3000},
	}

	for path, exp := range expectedFiles {
		file, err := dm.GetFile(exp.storageID, path)
		if err != nil {
			t.Errorf("dm.GetFile(%d, %q) failed: %v", exp.storageID, path, err)
			continue
		}
		if file == nil {
			t.Errorf("file %q not found in DB", path)
			continue
		}
		if file.StorageID != exp.storageID || file.Size != exp.size {
			t.Errorf("file %q: storageID=%d, size=%d; expected storageID=%d, size=%d",
				path, file.StorageID, file.Size, exp.storageID, exp.size)
		}
	}
}

// TestInitData_FailFastAndQuickCancellation verifies that if any upstream node fails,
// InitData returns an error immediately and cancels other in-flight Walks.
func TestInitData_FailFastAndQuickCancellation(t *testing.T) {
	var canceled2, canceled3 atomic.Bool
	node2Started := make(chan struct{}, 1)
	node3Started := make(chan struct{}, 1)

	// Node 2 & 3 simulate long walks (1000ms) but notify when their request starts
	s2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case node2Started <- struct{}{}:
		default:
		}
		select {
		case <-time.After(1000 * time.Millisecond):
		case <-r.Context().Done():
			canceled2.Store(true)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer s2.Close()

	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case node3Started <- struct{}{}:
		default:
		}
		select {
		case <-time.After(1000 * time.Millisecond):
		case <-r.Context().Done():
			canceled3.Store(true)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer s3.Close()

	// Node 1 waits until nodes 2 & 3 have started their walk, then fails with 403
	s1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Wait for both node 2 and node 3 to be actively in flight
		select {
		case <-node2Started:
		case <-time.After(300 * time.Millisecond):
		}
		select {
		case <-node3Started:
		case <-time.After(300 * time.Millisecond):
		}
		http.Error(w, "upstream failure", http.StatusForbidden)
	}))
	defer s1.Close()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "data.db")
	dm, err := data.NewManager(dbPath)
	if err != nil {
		t.Fatalf("failed to create data.Manager: %v", err)
	}
	defer dm.Close()

	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: s1.URL, Capacity: "10G"},
			{ID: 2, URL: s2.URL, Capacity: "10G"},
			{ID: 3, URL: s3.URL, Capacity: "10G"},
		},
	}

	c, err := NewCluster(cfg, dm)
	if err != nil {
		t.Fatalf("failed to create Cluster: %v", err)
	}

	start := time.Now()
	err = c.InitData(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected InitData to fail, got nil")
	}

	t.Logf("InitData failed fast in %v with error: %v", elapsed, err)

	// Verify root cause error from upstream 1 is returned (not overwritten by context.Canceled)
	if !strings.Contains(err.Error(), "upstream 1") {
		t.Errorf("expected error to mention upstream 1, got: %v", err)
	}

	// Fail-fast must terminate well before the 1000ms slow walk timeout
	if elapsed >= 600*time.Millisecond {
		t.Errorf("expected fail-fast cancellation under 600ms, took %v", elapsed)
	}

	// Verify slow nodes 2 and 3 did not complete and write to SQLite
	for _, id := range []int{2, 3} {
		if _, err := dm.GetStorage(id); !errors.Is(err, data.ErrNotFound) {
			t.Errorf("expected storage %d not to be persisted after cancellation, got err=%v", id, err)
		}
	}

	// Give HTTP server goroutines a moment to observe context cancellation
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if canceled2.Load() && canceled3.Load() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !canceled2.Load() || !canceled3.Load() {
		t.Errorf("expected slow walks on nodes 2 and 3 to be canceled, got canceled2=%v canceled3=%v",
			canceled2.Load(), canceled3.Load())
	}
}

// TestInitData_SkipAlreadySyncedNodes verifies that nodes with an existing snapshot (LastSync != nil)
// are directly restored from DB and not walked again, while un-synced nodes are scanned concurrently.
func TestInitData_SkipAlreadySyncedNodes(t *testing.T) {
	var reqCount1, reqCount2 atomic.Int32

	s1 := createDelayedServer("file1.bin", 1000, 50*time.Millisecond, false, nil, &reqCount1)
	defer s1.Close()
	s2 := createDelayedServer("file2.bin", 2000, 50*time.Millisecond, false, nil, &reqCount2)
	defer s2.Close()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "data.db")
	dm, err := data.NewManager(dbPath)
	if err != nil {
		t.Fatalf("failed to create data.Manager: %v", err)
	}
	defer dm.Close()

	// Pre-populate Node 1 in DB with LastSync set
	now := time.Now()
	preRec := []data.FileRecord{
		{StorageID: 1, Path: "/existing.bin", Size: 500, IsDir: false, ModTime: now},
	}
	if err := dm.BatchInsertFiles(1, preRec); err != nil {
		t.Fatalf("pre-populating storage 1 failed: %v", err)
	}

	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: s1.URL, Capacity: "10G"},
			{ID: 2, URL: s2.URL, Capacity: "10G"},
		},
	}

	c, err := NewCluster(cfg, dm)
	if err != nil {
		t.Fatalf("failed to create Cluster: %v", err)
	}

	if err := c.InitData(context.Background()); err != nil {
		t.Fatalf("InitData failed: %v", err)
	}

	// Node 1 was already synced, so it should NOT have been walked
	if count := reqCount1.Load(); count != 0 {
		t.Errorf("node 1 was already synced, expected 0 requests, got %d", count)
	}

	// Node 2 was not synced, so it must have been walked
	if count := reqCount2.Load(); count == 0 {
		t.Errorf("node 2 was not synced, expected >0 requests, got %d", count)
	}

	// Check quotas: node 1 restored from DB (500 bytes, 1 file)
	n1, _ := c.GetNode(1)
	_, used1, _ := n1.Quota()
	if used1 != 500 || n1.Files() != 1 {
		t.Errorf("node 1 quota: used=%d, files=%d; expected 500, 1", used1, n1.Files())
	}

	// Node 2 synced from remote walk (2000 bytes, 1 file)
	n2, _ := c.GetNode(2)
	_, used2, _ := n2.Quota()
	if used2 != 2000 || n2.Files() != 1 {
		t.Errorf("node 2 quota: used=%d, files=%d; expected 2000, 1", used2, n2.Files())
	}
}
