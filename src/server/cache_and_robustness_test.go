package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"webdav-agg/src/config"
	"webdav-agg/src/data"
)

// TestGhostFileCacheExpiration verifies:
// 1. When cache_expire: 0 (or expired), GET/PROPFIND/HEAD probes upstream node.
// 2. If upstream physically removed the file, metadata is immediately evicted and returns 404.
// 3. If upstream still has the file, last_probed_at is updated.
// 4. When cache_expire: -1 (never expire), no upstream probe occurs.
// 5. When cache_expire: 24h and last_probed_at is fresh, no upstream probe occurs; when older than 24h, upstream is probed.
func TestGhostFileCacheExpiration(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()

	// Case A: cache_expire = 0s (always probe upstream)
	cfgAlways := &config.Config{
		Listen:      "127.0.0.1:0",
		CacheExpire: "0s",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "100M"},
		},
	}
	srvAlways := newTestServer(t, cfgAlways)
	dmAlways := srvAlways.Cluster().DataManager()

	content := []byte("payload for ghost probe test")
	req := httptest.NewRequest("PUT", "/dav/probe_test.txt", bytes.NewReader(content))
	w := httptest.NewRecorder()
	srvAlways.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("PUT failed: %d", w.Code)
	}

	rec, err := dmAlways.GetFile(1, "/probe_test.txt")
	if err != nil || rec == nil {
		t.Fatalf("file not found in DB: %v", err)
	}
	initialProbeTime := rec.LastProbedAt

	// Reset PROPFIND counter
	up.mu.Lock()
	up.reqCounts["PROPFIND"] = 0
	up.mu.Unlock()

	time.Sleep(15 * time.Millisecond)
	req = httptest.NewRequest("HEAD", "/dav/probe_test.txt", nil)
	w = httptest.NewRecorder()
	srvAlways.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("HEAD failed: %d", w.Code)
	}

	up.mu.Lock()
	propfindCount := up.reqCounts["PROPFIND"]
	up.mu.Unlock()
	if propfindCount == 0 {
		t.Errorf("expected PROPFIND probe on upstream when cache_expire=0s, got 0")
	}

	recAfterHead, err := dmAlways.GetFile(1, "/probe_test.txt")
	if err != nil || recAfterHead == nil {
		t.Fatalf("file not found in DB after HEAD: %v", err)
	}
	if !recAfterHead.LastProbedAt.After(initialProbeTime) {
		t.Errorf("expected LastProbedAt to be updated, initial=%v, after=%v", initialProbeTime, recAfterHead.LastProbedAt)
	}

	// Simulate physical backend deletion behind the scenes (ghost file condition)
	up.mu.Lock()
	delete(up.resources, "/probe_test.txt")
	up.mu.Unlock()

	// PROPFIND depth 0 detects missing upstream and returns 404 + evicts DB
	req = httptest.NewRequest("PROPFIND", "/dav/probe_test.txt", nil)
	req.Header.Set("Depth", "0")
	w = httptest.NewRecorder()
	srvAlways.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for ghost file on PROPFIND, got %d", w.Code)
	}

	_, errEvicted := dmAlways.GetFile(1, "/probe_test.txt")
	if errEvicted != data.ErrNotFound {
		t.Errorf("expected ErrNotFound from DB, got %v", errEvicted)
	}

	// Case B: cache_expire = -1 (never expire -> never probe upstream)
	cfgNever := &config.Config{
		Listen:      "127.0.0.1:0",
		CacheExpire: "-1",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "100M"},
		},
	}
	srvNever := newTestServer(t, cfgNever)
	dmNever := srvNever.Cluster().DataManager()

	req = httptest.NewRequest("PUT", "/dav/never_probe.txt", bytes.NewReader(content))
	w = httptest.NewRecorder()
	srvNever.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("PUT never_probe failed: %d", w.Code)
	}

	// Manually set LastProbedAt to 10 days ago
	_ = dmNever.UpdateLastProbed(1, "/never_probe.txt", time.Now().Add(-240*time.Hour))

	// Remove physical file on upstream
	up.mu.Lock()
	delete(up.resources, "/never_probe.txt")
	up.reqCounts["PROPFIND"] = 0
	up.mu.Unlock()

	// HEAD must still return 200 without probing upstream because cache_expire is -1
	req = httptest.NewRequest("HEAD", "/dav/never_probe.txt", nil)
	w = httptest.NewRecorder()
	srvNever.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("HEAD never_probe expected 200 (cache never expires), got %d", w.Code)
	}
	up.mu.Lock()
	neverPropf := up.reqCounts["PROPFIND"]
	up.mu.Unlock()
	if neverPropf != 0 {
		t.Errorf("expected 0 PROPFIND probes when cache_expire=-1, got %d", neverPropf)
	}

	// Case C: cache_expire = 24h (fresh cache skips probe, stale cache triggers probe and evicts ghost)
	cfg24h := &config.Config{
		Listen:      "127.0.0.1:0",
		CacheExpire: "24h",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "100M"},
		},
	}
	srv24h := newTestServer(t, cfg24h)
	dm24h := srv24h.Cluster().DataManager()

	req = httptest.NewRequest("PUT", "/dav/ttl_test.txt", bytes.NewReader(content))
	w = httptest.NewRecorder()
	srv24h.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("PUT ttl_test failed: %d", w.Code)
	}

	// Delete physically behind the scenes while cache is still fresh (< 24h)
	up.mu.Lock()
	delete(up.resources, "/ttl_test.txt")
	up.reqCounts["PROPFIND"] = 0
	up.mu.Unlock()

	// Fresh cache: HEAD does NOT probe upstream
	req = httptest.NewRequest("HEAD", "/dav/ttl_test.txt", nil)
	w = httptest.NewRecorder()
	srv24h.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected cached 200 when within 24h TTL, got %d", w.Code)
	}
	up.mu.Lock()
	freshPropf := up.reqCounts["PROPFIND"]
	up.mu.Unlock()
	if freshPropf != 0 {
		t.Errorf("expected 0 PROPFIND probes within 24h TTL, got %d", freshPropf)
	}

	// Age LastProbedAt past 24h (25 hours ago)
	_ = dm24h.UpdateLastProbed(1, "/ttl_test.txt", time.Now().Add(-25*time.Hour))

	// Expired cache: HEAD probes upstream, detects 404, evicts record, returns 404
	req = httptest.NewRequest("HEAD", "/dav/ttl_test.txt", nil)
	w = httptest.NewRecorder()
	srv24h.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 once 24h TTL expired and physical file missing, got %d", w.Code)
	}
	if _, err := dm24h.GetFile(1, "/ttl_test.txt"); err != data.ErrNotFound {
		t.Errorf("expected evicted DB record after expired probe 404, got %v", err)
	}
}

// TestDirectoryDeleteFallbackWithUntrackedRemnants tests:
// Simulates a strict cloud drive (like 189) that rejects DELETE on any non-empty directory with 500.
// Verifies that when untracked physical files/subdirs exist inside a directory on upstream,
// handleDelete falls back to cleanNodeDirRecursive to purge everything bottom-up and succeeds with 204.
func TestDirectoryDeleteFallbackWithUntrackedRemnants(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()

	// Wrap upstream handler to strictly reject DELETE on non-empty directories (like 189 Cloud)
	var rejectedNonEmpty int32
	strictHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			davPath := path.Clean(r.URL.Path)
			if !strings.HasPrefix(davPath, "/") {
				davPath = "/" + davPath
			}
			up.mu.Lock()
			res, exists := up.resources[davPath]
			hasChildren := false
			if exists && res.isDir {
				prefix := davPath + "/"
				for k := range up.resources {
					if strings.HasPrefix(k, prefix) {
						hasChildren = true
						break
					}
				}
			}
			up.mu.Unlock()
			if hasChildren {
				atomic.AddInt32(&rejectedNonEmpty, 1)
				http.Error(w, "Directory not empty", http.StatusInternalServerError)
				return
			}
		}
		up.server.Config.Handler.ServeHTTP(w, r)
	})
	strictServer := httptest.NewServer(strictHandler)
	defer strictServer.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Retry:  0,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: strictServer.URL, Capacity: "100M"},
		},
	}
	srv := newTestServer(t, cfg)

	// 1. Create a directory /dav/untracked_dir/
	req := httptest.NewRequest("MKCOL", "/dav/untracked_dir/", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("MKCOL failed: %d", w.Code)
	}

	// 2. Inject untracked physical subdirs and files directly on upstream (e.g. orphan .openlist-copy-* fragments)
	up.mu.Lock()
	up.resources["/untracked_dir/.openlist-copy-1234"] = &mockResource{isDir: true, modTime: time.Now()}
	up.resources["/untracked_dir/.openlist-copy-1234/deep"] = &mockResource{isDir: true, modTime: time.Now()}
	up.resources["/untracked_dir/.openlist-copy-1234/deep/orphan.bin"] = &mockResource{isDir: false, content: []byte("orphan"), modTime: time.Now()}
	up.mu.Unlock()

	// 3. Issue DELETE on /dav/untracked_dir
	// Direct DELETE on /untracked_dir will hit strictHandler and get 500 because of untracked remnants,
	// forcing handleDelete to trigger cleanNodeDirRecursive!
	req = httptest.NewRequest("DELETE", "/dav/untracked_dir", nil)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("DELETE failed: %d, body: %s", w.Code, w.Body.String())
	}

	if atomic.LoadInt32(&rejectedNonEmpty) == 0 {
		t.Errorf("expected direct non-empty DELETE to be rejected at least once before recursive fallback")
	}

	// 4. Verify upstream physical node is completely clean
	up.mu.Lock()
	defer up.mu.Unlock()
	for p := range up.resources {
		if strings.HasPrefix(p, "/untracked_dir") {
			t.Errorf("orphan physical resource was not cleaned up: %s", p)
		}
	}
}

// TestCrossNodeDirectoryMoveRollback tests:
// When cross-node directory move transfers multiple files to a target node and fails mid-way
// (after creating directories and uploading the first file), the defer rollback mechanism
// automatically purges all partially uploaded files and created directories on the target node.
func TestCrossNodeDirectoryMoveRollback(t *testing.T) {
	up1 := newTestWebDAVUpstream(1000, 100, true)
	defer up1.Close()

	up2 := newTestWebDAVUpstream(1000, 0, true)
	defer up2.Close()

	// Configure up2 to fail PUT on file2.bin to simulate mid-migration failure
	up2.mu.Lock()
	up2.failPutPath = "/dest_tree/sub/file2.bin"
	up2.mu.Unlock()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Retry:  0,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up1.server.URL, Capacity: "100M"},
			{ID: 2, URL: up2.server.URL, Capacity: "100M"},
		},
	}
	srv := newTestServer(t, cfg)
	dm := srv.Cluster().DataManager()

	// Create /src_tree/sub/ and upload 2 files so source lives on node 1 (Wait: let's guarantee source lives on node 1)
	// First fill node 2 slightly in DB or put files directly on node 1
	// Let's upload via WebDAV, then make sure /src_tree is on node 1, and destination selects node 2.
	// Better yet: ensure node 1 has higher used bytes ONLY after creating /src_tree so ReserveWrite picks node 2 for dest_tree!
	for _, p := range []string{"/src_tree", "/src_tree/sub"} {
		_ = up1.putDir(p)
		_ = dm.PutFile(data.FileRecord{StorageID: 1, Path: p, IsDir: true, ModTime: time.Now()})
	}
	fileData := []byte("cross node move payload 123456")
	up1.mu.Lock()
	up1.resources["/src_tree/sub/file1.bin"] = &mockResource{isDir: false, content: fileData, modTime: time.Now()}
	up1.resources["/src_tree/sub/file2.bin"] = &mockResource{isDir: false, content: fileData, modTime: time.Now()}
	up1.mu.Unlock()
	_ = dm.PutFile(data.FileRecord{StorageID: 1, Path: "/src_tree/sub/file1.bin", Size: int64(len(fileData)), ModTime: time.Now()})
	_ = dm.PutFile(data.FileRecord{StorageID: 1, Path: "/src_tree/sub/file2.bin", Size: int64(len(fileData)), ModTime: time.Now()})

	// Force node 1 to have more used bytes in memory so ReserveWrite selects node 2 (least loaded)
	n1, _ := srv.Cluster().GetNode(1)
	n2, _ := srv.Cluster().GetNode(2)
	n1.SetQuota(50*1024*1024, 10)
	n2.SetQuota(0, 0)

	// Perform MOVE /dav/src_tree -> /dav/dest_tree
	// Native MOVE on node 1 will work if same node, wait: in handleMoveCopy, if dest doesn't exist, canSameUpstream=true!
	// Wait: let's force native MOVE to fail on node 1 (e.g. like 189 Cloud that does not support MOVE),
	// OR let dest exist on node 2 so it goes through cross-node pipelineFallback!
	// Let's create an empty /dav/dest_tree on node 2!
	_ = up2.putDir("/dest_tree")
	_ = dm.PutFile(data.FileRecord{StorageID: 2, Path: "/dest_tree", IsDir: true, ModTime: time.Now()})

	req := httptest.NewRequest("MOVE", "/dav/src_tree", nil)
	req.Header.Set("Destination", "/dav/dest_tree")
	req.Header.Set("Overwrite", "T")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	// Because up2 fails PUT /dest_tree/sub/file2.bin, MOVE must fail with 500
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 Internal Server Error on partial migration failure, got %d (body: %s)", w.Code, w.Body.String())
	}

	// Wait briefly if any async cleanup occurs (defer cleanup is synchronous)
	var mu sync.Mutex
	mu.Lock()
	mu.Unlock()

	// Verify node 2 does NOT have orphan /dest_tree/sub/file1.bin or /dest_tree/sub
	up2.mu.Lock()
	_, hasFile1 := up2.resources["/dest_tree/sub/file1.bin"]
	_, hasSub := up2.resources["/dest_tree/sub"]
	up2.mu.Unlock()

	if hasFile1 {
		t.Errorf("orphan file /dest_tree/sub/file1.bin remained on target node after rollback")
	}
	if hasSub {
		t.Errorf("orphan dir /dest_tree/sub remained on target node after rollback")
	}

	// Verify source tree in DB is completely intact
	srcRecs, err := dm.ListSubtree("/src_tree")
	if err != nil || len(srcRecs) != 4 {
		t.Errorf("expected all 4 source records to remain intact, got %d (err: %v)", len(srcRecs), err)
	}
}

func (u *testWebDAVUpstream) putDir(p string) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.resources[p] = &mockResource{isDir: true, modTime: time.Now()}
	return nil
}
