package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"webdav-agg/src/client"
	"webdav-agg/src/config"
)

// TestServer_CrossNodeDirectoryMovePipeline tests the two-phase topological and concurrent file pipeline for directory moves.
func TestServer_CrossNodeDirectoryMovePipeline(t *testing.T) {
	up1 := newTestWebDAVUpstream(50000, 100, true)
	defer up1.Close()

	// Populate up1 with nested directory structure and multiple files
	const numFiles = 10
	for i := range numFiles {
		p := fmt.Sprintf("/large_tree/file_%d.txt", i)
		content := fmt.Sprintf("content_%d", i)
		up1.setFile(p, []byte(content), time.Now())
	}
	up1.setFile("/large_tree/nested/sub1/deep.txt", []byte("deep_content"), time.Now())

	// Upstream 2 destination
	up2 := newTestWebDAVUpstream(50000, 100, true)
	defer up2.Close()
	up2.setFile("/dest_archive/marker.txt", []byte("marker"), time.Now())

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up1.server.URL},
			{ID: 2, URL: up2.server.URL},
		},
	}
	srv := newTestServer(t, cfg)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	c, err := client.NewClient(ts.URL+"/dav", "", "", "", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	// Execute cross-node MOVE /large_tree -> /dest_archive
	req, _ := http.NewRequest("MOVE", ts.URL+"/dav/large_tree", nil)
	req.Header.Set("Destination", ts.URL+"/dav/dest_archive")
	req.Header.Set("Overwrite", "T")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("MOVE failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected status 204 or 201, got %d", resp.StatusCode)
	}

	// Verify all files moved to /dest_archive and contents match
	for i := range numFiles {
		p := fmt.Sprintf("/dest_archive/file_%d.txt", i)
		rc, _, err := c.Get(context.Background(), p, nil)
		if err != nil {
			t.Fatalf("Get %s failed: %v", p, err)
		}
		data, _ := io.ReadAll(rc)
		_ = rc.Close()
		expected := fmt.Sprintf("content_%d", i)
		if string(data) != expected {
			t.Fatalf("file %s expected %q, got %q", p, expected, string(data))
		}
	}

	// Verify deep nested file
	rc, _, err := c.Get(context.Background(), "/dest_archive/nested/sub1/deep.txt", nil)
	if err != nil {
		t.Fatalf("Get deep file failed: %v", err)
	}
	deepData, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(deepData) != "deep_content" {
		t.Fatalf("expected deep_content, got %q", string(deepData))
	}

	// Verify source files removed from up1
	for i := range numFiles {
		p := fmt.Sprintf("/large_tree/file_%d.txt", i)
		if _, ok := up1.resources[p]; ok {
			t.Fatalf("source file %s still exists on up1 after MOVE", p)
		}
	}
}

// TestServer_DeleteMultiStatusOnPartialFailure tests RFC 4918 207 Multi-Status response on partial delete failures.
func TestServer_DeleteMultiStatusOnPartialFailure(t *testing.T) {
	up1 := newTestWebDAVUpstream(50000, 100, true)
	defer up1.Close()
	up1.setFile("/shared_dir/doc1.txt", []byte("doc1"), time.Now())

	up2 := newTestWebDAVUpstream(50000, 100, true)
	defer up2.Close()
	up2.setFile("/shared_dir/doc2.txt", []byte("doc2"), time.Now())
	// Make upstream 2 fail when deleting /shared_dir
	up2.failDeletePath = "/shared_dir"

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up1.server.URL},
			{ID: 2, URL: up2.server.URL},
		},
	}
	srv := newTestServer(t, cfg)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Send DELETE /dav/shared_dir
	req, _ := http.NewRequest("DELETE", ts.URL+"/dav/shared_dir", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMultiStatus {
		t.Fatalf("expected status 207 Multi-Status, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body failed: %v", err)
	}
	xmlBody := string(body)

	if !strings.Contains(xmlBody, "<D:multistatus") {
		t.Fatalf("expected XML to contain <D:multistatus, got: %s", xmlBody)
	}
	if !strings.Contains(xmlBody, "502") {
		t.Fatalf("expected XML to contain 502 status for upstream failure, got: %s", xmlBody)
	}

	// Verify up1 deleted successfully
	if _, ok := up1.resources["/shared_dir/doc1.txt"]; ok {
		t.Fatalf("up1 file doc1.txt should have been deleted")
	}
}

// TestServer_MoveWithDuplicateSourceInstances_NoDeadlock verifies that MOVE on a path with duplicate instances
// executes DeduplicateFilesLocked without self-deadlocking under PathLocker.
func TestServer_MoveWithDuplicateSourceInstances_NoDeadlock(t *testing.T) {
	now := time.Now()
	up1 := newTestWebDAVUpstream(50000, 100, true)
	defer up1.Close()
	up1.setFile("/conflict.txt", []byte("old_content"), now.Add(-10*time.Minute))

	up2 := newTestWebDAVUpstream(50000, 100, true)
	defer up2.Close()
	up2.setFile("/conflict.txt", []byte("new_content"), now)

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up1.server.URL},
			{ID: 2, URL: up2.server.URL},
		},
	}
	srv := newTestServer(t, cfg)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	c, err := client.NewClient(ts.URL+"/dav", "", "", "", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		req, _ := http.NewRequest("MOVE", ts.URL+"/dav/conflict.txt", nil)
		req.Header.Set("Destination", ts.URL+"/dav/resolved.txt")
		req.Header.Set("Overwrite", "T")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Errorf("MOVE failed: %v", err)
			return
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
			t.Errorf("expected 201 or 204, got %d", resp.StatusCode)
		}
	}()

	select {
	case <-done:
		// Succeeded without deadlock
	case <-time.After(3 * time.Second):
		t.Fatal("MOVE self-deadlocked while holding PathLocker during deduplication!")
	}

	rc, _, err := c.Get(context.Background(), "/resolved.txt", nil)
	if err != nil {
		t.Fatalf("Get /resolved.txt failed: %v", err)
	}
	content, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(content) != "new_content" {
		t.Fatalf("expected newer content %q, got %q", "new_content", string(content))
	}
}

func TestServer_ConcurrentWorkerPoolAndHierarchicalDirectoryDelete(t *testing.T) {
	up := newTestWebDAVUpstream(500000, 100, true)
	defer up.Close()

	// Strict handler that rejects DELETE on non-empty directories (simulating strict cloud backends)
	strictHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			davPath := r.URL.Path
			if !strings.HasPrefix(davPath, "/") {
				davPath = "/" + davPath
			}
			up.mu.Lock()
			hasChildren := false
			prefix := strings.TrimSuffix(davPath, "/") + "/"
			for k := range up.resources {
				if strings.HasPrefix(k, prefix) {
					hasChildren = true
					break
				}
			}
			up.mu.Unlock()
			if hasChildren {
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
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: strictServer.URL, Capacity: "100M"},
		},
	}
	srv := newTestServer(t, cfg)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	c, err := client.NewClient(ts.URL+"/dav", "", "", "", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	// Populate directory structure with multiple levels and many files
	const topFiles = 25
	for i := range topFiles {
		fPath := fmt.Sprintf("/batch_del/top_file_%d.txt", i)
		if err := c.Put(context.Background(), fPath, strings.NewReader("data"), 4, "text/plain"); err != nil {
			t.Fatalf("Put %s failed: %v", fPath, err)
		}
	}

	for sub := range 3 {
		for i := range 5 {
			fPath := fmt.Sprintf("/batch_del/sub_%d/sub_file_%d.txt", sub, i)
			if err := c.Put(context.Background(), fPath, strings.NewReader("subdata"), 7, "text/plain"); err != nil {
				t.Fatalf("Put %s failed: %v", fPath, err)
			}
			deepPath := fmt.Sprintf("/batch_del/sub_%d/deep/deep_file_%d.txt", sub, i)
			if err := c.Put(context.Background(), deepPath, strings.NewReader("deepdata"), 8, "text/plain"); err != nil {
				t.Fatalf("Put %s failed: %v", deepPath, err)
			}
		}
	}

	// Delete root collection /batch_del
	if err := c.Delete(context.Background(), "/batch_del"); err != nil {
		t.Fatalf("Delete /batch_del failed: %v", err)
	}

	// Verify PROPFIND on /batch_del returns 404
	_, err = c.Stat(context.Background(), "/batch_del")
	if err == nil {
		t.Fatalf("expected 404 for /batch_del after deletion, but stat succeeded")
	}

	// Verify physical upstream is completely clean
	up.mu.Lock()
	defer up.mu.Unlock()
	for p := range up.resources {
		if strings.HasPrefix(p, "/batch_del") {
			t.Errorf("orphan resource %s still exists on upstream", p)
		}
	}
}

func TestServer_FastDirectDeleteAttempt(t *testing.T) {
	up := newTestWebDAVUpstream(500000, 100, true)
	defer up.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "100M"},
		},
	}
	srv := newTestServer(t, cfg)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	c, err := client.NewClient(ts.URL+"/dav", "", "", "", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	// Upload 20 files
	for i := range 20 {
		fPath := fmt.Sprintf("/fast_del/file_%d.txt", i)
		if err := c.Put(context.Background(), fPath, strings.NewReader("payload"), 7, "text/plain"); err != nil {
			t.Fatalf("Put %s failed: %v", fPath, err)
		}
	}

	// Record DELETE count on upstream before deleting /fast_del
	up.mu.Lock()
	deletesBefore := up.reqCounts["DELETE"]
	up.mu.Unlock()

	// Delete collection /fast_del
	if err := c.Delete(context.Background(), "/fast_del"); err != nil {
		t.Fatalf("Delete /fast_del failed: %v", err)
	}

	// Since mock upstream supports recursive DELETE, fast direct delete should have succeeded in exactly 1 DELETE request
	up.mu.Lock()
	deletesAfter := up.reqCounts["DELETE"]
	up.mu.Unlock()

	diff := deletesAfter - deletesBefore
	if diff != 1 {
		t.Errorf("expected fast direct delete in 1 DELETE request, got %d", diff)
	}

	// Verify /fast_del is gone
	_, err = c.Stat(context.Background(), "/fast_del")
	if err == nil {
		t.Fatalf("expected 404 for /fast_del after deletion")
	}
}
