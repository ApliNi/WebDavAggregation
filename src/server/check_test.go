package server

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"webdav-agg/src/client"
	"webdav-agg/src/config"
	"webdav-agg/src/data"
	"webdav-agg/src/logger"
)

func captureLogs(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	logger.SetOutput(&buf)
	logger.SetLevel(logger.LevelDebug)
	defer func() {
		logger.SetOutput(nil)
		logger.SetLevel(logger.LevelInfo)
	}()

	fn()
	return strings.ReplaceAll(buf.String(), "\r\n", "\n")
}

func TestCheckUpstreams_Success(t *testing.T) {
	up1 := newTestWebDAVUpstream(2*1024*1024*1024*1024, 0, true) // 2TB available
	defer up1.server.Close()

	up2 := newTestWebDAVUpstream(500*1024*1024*1024, 0, true) // 500GB available
	defer up2.server.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Retry:  1,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up1.server.URL, Capacity: "2T"},
			{ID: 2, URL: up2.server.URL, Capacity: "500G"},
		},
	}

	srv, err := NewServer(cfg, nil)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	var checkErr error
	logs := captureLogs(t, func() {
		checkErr = srv.CheckUpstreams(context.Background())
	})

	if checkErr != nil {
		t.Fatalf("expected CheckUpstreams to succeed, got: %v", checkErr)
	}

	// Self-check passed without error; should not print node details/capacity
	if strings.Contains(logs, "capacity:") || strings.Contains(logs, "Upstream[") {
		t.Errorf("expected no node capacity details logged on success, got:\n%s", logs)
	}
}

func TestCheckUpstreams_UnavailableFailsAndNoListen(t *testing.T) {
	up1 := newTestWebDAVUpstream(1024*1024*1024, 0, true)
	defer up1.server.Close()

	// Upstream 2 points to an invalid/non-listening port
	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Retry:  1,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up1.server.URL, Capacity: "1G"},
			{ID: 2, URL: "http://127.0.0.1:59998", Capacity: "1G"},
		},
	}

	srv, err := NewServer(cfg, nil)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	var startErr error
	logs := captureLogs(t, func() {
		startErr = srv.Start()
	})

	if startErr == nil {
		t.Fatalf("expected srv.Start() to fail when upstream is unavailable")
	}

	if srv.listener != nil {
		t.Fatalf("expected no listener to be bound when upstream check fails")
	}

	if !strings.Contains(logs, "ERROR]: [Main] Upstream[2] http://127.0.0.1:59998 check failed") {
		t.Errorf("expected error log for upstream 2 failure, got:\n%s", logs)
	}
	if !strings.Contains(logs, "ERROR]: [Main] Startup check failed") {
		t.Errorf("expected startup check failed summary error log, got:\n%s", logs)
	}
}

func TestCheckUpstreams_NoQuotaWithoutCapacityFails(t *testing.T) {
	// Upstream does not support RFC 4331 Quota
	up := newTestWebDAVUpstream(0, 0, false)
	defer up.server.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Retry:  1,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL}, // Capacity not set
		},
	}

	srv, err := NewServer(cfg, nil)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	var checkErr error
	logs := captureLogs(t, func() {
		checkErr = srv.CheckUpstreams(context.Background())
	})

	if checkErr == nil {
		t.Fatalf("expected failure for upstream without quota support and no configured capacity")
	}

	if !strings.Contains(logs, "ERROR]: [Main] Upstream[1]") {
		t.Errorf("expected error log for upstream 1, got:\n%s", logs)
	}
}

func TestCheckUpstreams_NoQuotaWithCapacitySucceeds(t *testing.T) {
	// Upstream does not support RFC 4331 Quota, but capacity is configured
	up := newTestWebDAVUpstream(0, 0, false)
	defer up.server.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Retry:  1,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "2T"},
		},
	}

	srv, err := NewServer(cfg, nil)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	var checkErr error
	logs := captureLogs(t, func() {
		checkErr = srv.CheckUpstreams(context.Background())
	})

	if checkErr != nil {
		t.Fatalf("expected success when capacity is explicitly configured, got: %v", checkErr)
	}

	if strings.Contains(logs, "capacity:") || strings.Contains(logs, "Upstream[") {
		t.Errorf("expected no node details on success, got:\n%s", logs)
	}
}

func TestHTTPLogging(t *testing.T) {
	up := newTestWebDAVUpstream(1024*1024*1024, 0, true)
	defer up.server.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Retry:  1,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "1G"},
		},
	}

	srv, err := NewServer(cfg, nil)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	handler := srv.Handler()

	t.Run("OPTIONS /dav basic logging", func(t *testing.T) {
		logs := captureLogs(t, func() {
			req := httptest.NewRequest("OPTIONS", "/dav", nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
		})

		pattern := `\[\d{2}:\d{2}:\d{2} INFO\]: \[HTTP\] OPTIONS 200 "/dav" \d+\.\d{2}s 0\.00B - 192\.0\.2\.1`
		matched, err := regexp.MatchString(pattern, logs)
		if err != nil || !matched {
			t.Errorf("expected HTTP access log matching %q, got:\n%s", pattern, logs)
		}
	})

	t.Run("X-Forwarded-For client IP and 404", func(t *testing.T) {
		logs := captureLogs(t, func() {
			req := httptest.NewRequest("GET", "/not-allowed", nil)
			req.Header.Set("X-Forwarded-For", "203.0.113.10:12345, 10.0.0.1")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
		})

		pattern := `\[\d{2}:\d{2}:\d{2} INFO\]: \[HTTP\] GET 404 "/not-allowed" \d+\.\d{2}s 0\.00B - 203\.0\.113\.10`
		matched, err := regexp.MatchString(pattern, logs)
		if err != nil || !matched {
			t.Errorf("expected HTTP access log matching %q, got:\n%s", pattern, logs)
		}
	})
}

func TestTargetWebDAVServiceLiveCheck(t *testing.T) {
	targetURL := "http://127.0.0.1:5244/dav/189_193_2_home/agg"
	c, err := client.NewClient(targetURL, "test", "abc", "", 1, 8*time.Second)
	if err != nil {
		t.Skipf("cannot create client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	if err := c.Ping(ctx); err != nil {
		t.Skipf("target service %s is unreachable: %v", targetURL, err)
	}

	// Ping succeeds! Verify that GetQuota returns ErrQuotaNotSupported because
	// RFC 4331 quota properties are 404 on this server.
	_, quotaErr := c.GetQuota(ctx)
	if quotaErr == nil {
		t.Errorf("expected quota to be unsupported on target service, but GetQuota succeeded")
	} else if !errors.Is(quotaErr, client.ErrQuotaNotSupported) {
		t.Skipf("target service returned error (skipping live check): %v", quotaErr)
	}

	// 1. Without capacity configured: startup check must fail
	cfgNoCap := &config.Config{
		Listen: "127.0.0.1:0",
		Retry:  1,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: targetURL, User: "test", Pass: "abc"},
		},
	}
	srvNoCap, err := NewServer(cfgNoCap, nil)
	if err == nil {
		err = srvNoCap.CheckUpstreams(context.Background())
		if err == nil {
			t.Errorf("expected CheckUpstreams to fail without capacity configuration")
		}
	}

	// 2. With capacity configured: startup check must succeed
	cfgWithCap := &config.Config{
		Listen: "127.0.0.1:0",
		Retry:  1,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: targetURL, User: "test", Pass: "abc", Capacity: "100G"},
		},
	}
	srvWithCap, err := NewServer(cfgWithCap, nil)
	if err != nil {
		t.Fatalf("NewServer with capacity error: %v", err)
	}

	logs := captureLogs(t, func() {
		err = srvWithCap.CheckUpstreams(context.Background())
	})
	if err != nil {
		t.Fatalf("expected CheckUpstreams to succeed with capacity configured, got: %v", err)
	}
	if strings.Contains(logs, "capacity:") || strings.Contains(logs, "Upstream[") {
		t.Errorf("expected no node capacity details logged on success, got:\n%s", logs)
	}
}

func TestStartup_InitialScanLogs_And_WaitsBeforeListening(t *testing.T) {
	up := newTestWebDAVUpstream(1024*1024*1024, 0, true)
	defer up.server.Close()
	up.setFile("/init/test.txt", []byte("12345"), time.Now())

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Retry:  1,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "100MB"},
		},
	}

	srv, err := NewServer(cfg, nil)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var startErr error
	started := make(chan struct{})

	logs := captureLogs(t, func() {
		go func() {
			close(started)
			startErr = srv.StartWithContext(ctx)
		}()

		// Wait for listener to become ready
		for range 50 {
			time.Sleep(20 * time.Millisecond)
			if srv.Addr() != "127.0.0.1:0" && srv.Addr() != "" {
				break
			}
		}
		cancel()
	})

	<-started
	if startErr != nil && !errors.Is(startErr, context.Canceled) {
		t.Fatalf("StartWithContext unexpected error: %v", startErr)
	}

	if !strings.Contains(logs, "Starting initial scan for upstream node 1") {
		t.Errorf("expected log 'Starting initial scan...', got:\n%s", logs)
	}
	if !strings.Contains(logs, "Initial scan completed for upstream node 1") {
		t.Errorf("expected log 'Initial scan completed...', got:\n%s", logs)
	}
}

func TestStartup_InitialScanError_ExitsAndRefusesToListen(t *testing.T) {
	up := newTestWebDAVUpstream(1024*1024*1024, 0, true)
	defer up.server.Close()

	// Set up mock to fail on PROPFIND depth 1 during walk
	up.mu.Lock()
	up.failPropfindDepth1 = true
	up.mu.Unlock()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Retry:  1,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "100MB"},
		},
	}

	srv, err := NewServer(cfg, nil)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}
	defer srv.Close()

	var startErr error
	logs := captureLogs(t, func() {
		startErr = srv.StartWithContext(context.Background())
	})

	if startErr == nil {
		t.Fatalf("expected StartWithContext to fail on initial scan error")
	}

	if srv.listener != nil {
		t.Fatalf("expected listener to be nil when initial scan fails")
	}

	if !strings.Contains(logs, "Initial scan failed for upstream node 1") {
		t.Errorf("expected log 'Initial scan failed...', got:\n%s", logs)
	}
}
func TestStartup_CleanupObsoleteUpstreamIDs(t *testing.T) {
	up := newTestWebDAVUpstream(1024*1024*1024, 0, true)
	defer up.server.Close()
	up.setFile("/init/test.txt", []byte("12345"), time.Now())

	dbPath := filepath.Join(t.TempDir(), "data.db")

	// Pre-seed SQLite database with obsolete upstream ID 888
	initDM, err := data.NewManager(dbPath)
	if err != nil {
		t.Fatalf("data.NewManager error: %v", err)
	}
	if err := initDM.EnsureStorage(888); err != nil {
		t.Fatalf("EnsureStorage(888) error: %v", err)
	}
	if err := initDM.PutFile(data.FileRecord{StorageID: 888, Path: "/old/file.txt", Size: 456}); err != nil {
		t.Fatalf("PutFile error: %v", err)
	}
	if err := initDM.Close(); err != nil {
		t.Fatalf("initDM.Close error: %v", err)
	}

	cfg := &config.Config{
		Listen:   "127.0.0.1:0",
		Retry:    1,
		DataFile: dbPath,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "100MB"},
		},
	}

	srv, err := NewServer(cfg, nil)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var startErr error
	started := make(chan struct{})

	logs := captureLogs(t, func() {
		go func() {
			close(started)
			startErr = srv.StartWithContext(ctx)
		}()

		// Wait for listener to become ready
		for range 50 {
			time.Sleep(20 * time.Millisecond)
			if srv.Addr() != "127.0.0.1:0" && srv.Addr() != "" {
				break
			}
		}
		cancel()
	})

	<-started
	if startErr != nil && !errors.Is(startErr, context.Canceled) {
		t.Fatalf("StartWithContext unexpected error: %v", startErr)
	}

	if !strings.Contains(logs, "Detected obsolete upstream node id 888 in database (not in configuration), deleting all associated data...") {
		t.Errorf("expected obsolete detection log, got:\n%s", logs)
	}
	if !strings.Contains(logs, "Successfully deleted all data for obsolete upstream node id 888") {
		t.Errorf("expected obsolete deletion success log, got:\n%s", logs)
	}
	if !strings.Contains(logs, "Starting initial scan for upstream node 1") {
		t.Errorf("expected initial scan log for node 1, got:\n%s", logs)
	}

	// Verify database content via server's data manager or a fresh data manager
	srvDM := srv.Cluster().DataManager()
	if srvDM == nil {
		t.Fatalf("expected server cluster to have data manager")
	}

	if _, err := srvDM.GetStorage(888); !errors.Is(err, data.ErrNotFound) {
		t.Errorf("expected storage 888 to be not found, got %v", err)
	}
	if _, err := srvDM.GetFile(888, "/old/file.txt"); !errors.Is(err, data.ErrNotFound) {
		t.Errorf("expected file for storage 888 to be not found, got %v", err)
	}

	st1, err := srvDM.GetStorage(1)
	if err != nil || st1 == nil {
		t.Fatalf("expected storage 1 to exist: %v", err)
	}
	if st1.FileCount != 1 || st1.UsedBytes != 5 {
		t.Errorf("expected storage 1 stats files=1, used=5; got files=%d, used=%d", st1.FileCount, st1.UsedBytes)
	}
}
func TestStartup_CleanupObsoleteUpstreamIDs_Error_RefusesToListen(t *testing.T) {
	up := newTestWebDAVUpstream(1024*1024*1024, 0, true)
	defer up.server.Close()

	dbPath := filepath.Join(t.TempDir(), "data.db")

	// Pre-seed SQLite database with obsolete upstream ID 888 and a trigger preventing deletion
	initDM, err := data.NewManager(dbPath)
	if err != nil {
		t.Fatalf("data.NewManager error: %v", err)
	}
	if err := initDM.EnsureStorage(888); err != nil {
		t.Fatalf("EnsureStorage(888) error: %v", err)
	}
	// Add trigger to abort deletion on storages table for id 888
	if _, err := initDM.DB().Exec("CREATE TRIGGER prevent_del_888 BEFORE DELETE ON storages WHEN OLD.id = 888 BEGIN SELECT RAISE(ABORT, 'cannot delete storage 888'); END;"); err != nil {
		t.Fatalf("create trigger prevent_del_888 error: %v", err)
	}
	if err := initDM.Close(); err != nil {
		t.Fatalf("initDM.Close error: %v", err)
	}

	cfg := &config.Config{
		Listen:   "127.0.0.1:0",
		Retry:    1,
		DataFile: dbPath,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "100MB"},
		},
	}

	srv, err := NewServer(cfg, nil)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}
	defer srv.Close()

	var startErr error
	logs := captureLogs(t, func() {
		startErr = srv.StartWithContext(context.Background())
	})

	if startErr == nil {
		t.Fatalf("expected StartWithContext to fail when obsolete upstream cleanup fails")
	}

	if srv.listener != nil {
		t.Fatalf("expected listener to be nil when obsolete cleanup fails")
	}

	if !strings.Contains(logs, "Detected obsolete upstream node id 888") {
		t.Errorf("expected detection log, got:\n%s", logs)
	}
	if !strings.Contains(logs, "Failed to delete data for obsolete upstream node id 888") {
		t.Errorf("expected deletion failure log, got:\n%s", logs)
	}
}
