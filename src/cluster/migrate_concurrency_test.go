package cluster

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"webdav-agg/src/config"
	"webdav-agg/src/data"
)

func wrapMockWithPutDelay(m *mockWebDAVServer, delay time.Duration, inFlight, maxInFlight *atomic.Int32) *httptest.Server {
	origHandler := m.server.Config.Handler
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" && delay > 0 {
			cur := inFlight.Add(1)
			for {
				prev := maxInFlight.Load()
				if cur <= prev || maxInFlight.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(delay)
			defer inFlight.Add(-1)
		}
		origHandler.ServeHTTP(w, r)
	}))
}

func TestMigrate_Concurrency_BalancedNoOverAllocation(t *testing.T) {
	const (
		totalFiles  = 20
		fileSize    = int64(1024) // 1 KiB per file, total 20 KiB
		existingSz2 = int64(5120) // 5 KiB pre-existing on node 2
	)

	s1 := newMockWebDAVServer(100*1024, totalFiles*fileSize, true)
	defer s1.Close()
	s2 := newMockWebDAVServer(10*1024, existingSz2, true)
	defer s2.Close()
	s3 := newMockWebDAVServer(10*1024, 0, true)
	defer s3.Close()

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32

	ws1 := wrapMockWithPutDelay(s1, 0, &inFlight, &maxInFlight)
	defer ws1.Close()
	ws2 := wrapMockWithPutDelay(s2, 15*time.Millisecond, &inFlight, &maxInFlight)
	defer ws2.Close()
	ws3 := wrapMockWithPutDelay(s3, 15*time.Millisecond, &inFlight, &maxInFlight)
	defer ws3.Close()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "migrate_concurrency.db")
	dm, err := data.NewManager(dbPath)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer dm.Close()

	for _, sid := range []int{1, 2, 3} {
		if err := dm.EnsureStorage(sid); err != nil {
			t.Fatalf("EnsureStorage(%d) failed: %v", sid, err)
		}
	}

	// Pre-populate existing file on node 2 so remaining space is exactly 10 KiB (10 files)
	existingData := bytes.Repeat([]byte("E"), int(existingSz2))
	s2.setFile("/existing_on_2.bin", existingData, time.Now())
	if err := dm.PutFile(data.FileRecord{
		StorageID: 2,
		Path:      "/existing_on_2.bin",
		Size:      existingSz2,
		IsDir:     false,
		ModTime:   time.Now(),
	}); err != nil {
		t.Fatalf("PutFile existing_on_2 failed: %v", err)
	}

	// Populate 20 files on source node 1
	expectedContents := make(map[string][]byte, totalFiles)
	for i := range totalFiles {
		p := fmt.Sprintf("/batch/file_%02d.bin", i)
		content := bytes.Repeat([]byte{byte('A' + (i % 26))}, int(fileSize))
		expectedContents[p] = content
		s1.setFile(p, content, time.Now())
		if err := dm.PutFile(data.FileRecord{
			StorageID: 1,
			Path:      p,
			Size:      fileSize,
			IsDir:     false,
			ModTime:   time.Now(),
		}); err != nil {
			t.Fatalf("PutFile %s failed: %v", p, err)
		}
	}

	cfg := &config.Config{
		Retry: 2,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: ws1.URL, User: "u", Pass: "p", Capacity: "50KiB"},
			{ID: 2, URL: ws2.URL, User: "u", Pass: "p", Capacity: "15KiB"}, // 15 KiB - 5 KiB = 10 KiB remaining
			{ID: 3, URL: ws3.URL, User: "u", Pass: "p", Capacity: "10KiB"}, // 10 KiB remaining
		},
	}

	c, err := NewCluster(cfg, dm)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	n1, _ := c.GetNode(1)
	n2, _ := c.GetNode(2)
	n3, _ := c.GetNode(3)
	n1.SetQuota(totalFiles*fileSize, totalFiles)
	n2.SetQuota(existingSz2, 1)
	n3.SetQuota(0, 0)

	var progressMu sync.Mutex
	seenCurr := make(map[int]bool)
	seenPaths := make(map[string]int)

	err = c.MigrateNode(context.Background(), 1, func(current, total int, path string, targetID int, size int64) {
		progressMu.Lock()
		defer progressMu.Unlock()
		if total != totalFiles {
			t.Errorf("expected total %d, got %d", totalFiles, total)
		}
		if seenCurr[current] {
			t.Errorf("duplicate progress current=%d", current)
		}
		seenCurr[current] = true
		seenPaths[path] = targetID
		if size != fileSize {
			t.Errorf("expected size %d for %s, got %d", fileSize, path, size)
		}
	})
	if err != nil {
		t.Fatalf("MigrateNode failed: %v", err)
	}

	// Verify concurrency actually occurred
	if maxInFlight.Load() < 2 {
		t.Errorf("expected concurrent PUT execution (maxInFlight >= 2), got %d", maxInFlight.Load())
	}

	// Verify progress reports
	if len(seenCurr) != totalFiles || len(seenPaths) != totalFiles {
		t.Fatalf("expected %d progress reports, got curr=%d paths=%d", totalFiles, len(seenCurr), len(seenPaths))
	}

	// Verify source node physically empty
	s1.mu.Lock()
	if len(s1.files) != 0 {
		t.Errorf("expected source node s1 to have 0 physical files, got %d", len(s1.files))
	}
	s1.mu.Unlock()

	// Verify target physical files: strictly 10 migrated on s2 (+1 existing = 11), 10 on s3
	s2.mu.Lock()
	s2TotalFiles := len(s2.files)
	var s2TotalBytes int64
	for _, b := range s2.files {
		s2TotalBytes += int64(len(b))
	}
	s2.mu.Unlock()

	s3.mu.Lock()
	s3TotalFiles := len(s3.files)
	var s3TotalBytes int64
	for _, b := range s3.files {
		s3TotalBytes += int64(len(b))
	}
	s3.mu.Unlock()

	if s2TotalFiles != 11 {
		t.Errorf("expected s2 to have 11 physical files (1 existing + 10 migrated), got %d", s2TotalFiles)
	}
	if s2TotalBytes != 15*1024 {
		t.Errorf("node 2 capacity exceeded or under-filled: got %d bytes, expected %d", s2TotalBytes, 15*1024)
	}
	if s3TotalFiles != 10 {
		t.Errorf("expected s3 to have 10 physical files, got %d", s3TotalFiles)
	}
	if s3TotalBytes != 10*1024 {
		t.Errorf("node 3 capacity exceeded or under-filled: got %d bytes, expected %d", s3TotalBytes, 10*1024)
	}

	// Verify file contents on targets
	for p, expected := range expectedContents {
		s2.mu.Lock()
		b2, ok2 := s2.files[p]
		s2.mu.Unlock()
		s3.mu.Lock()
		b3, ok3 := s3.files[p]
		s3.mu.Unlock()

		if ok2 == ok3 {
			t.Fatalf("file %s exists on both or neither target: ok2=%v ok3=%v", p, ok2, ok3)
		}
		if ok2 && !bytes.Equal(b2, expected) {
			t.Errorf("content mismatch on node 2 for %s", p)
		}
		if ok3 && !bytes.Equal(b3, expected) {
			t.Errorf("content mismatch on node 3 for %s", p)
		}
	}

	// Verify in-memory quota tracking
	avail1, used1, total1 := n1.Quota()
	if used1 != 0 || avail1 != total1 || n1.Files() != 0 || n1.Reserved() != 0 {
		t.Errorf("n1 quota mismatch: avail=%d used=%d total=%d files=%d reserved=%d", avail1, used1, total1, n1.Files(), n1.Reserved())
	}

	avail2, used2, total2 := n2.Quota()
	if used2 != 15*1024 || avail2 != 0 || total2 != 15*1024 || n2.Files() != 11 || n2.Reserved() != 0 {
		t.Errorf("n2 quota mismatch: avail=%d used=%d total=%d files=%d reserved=%d", avail2, used2, total2, n2.Files(), n2.Reserved())
	}

	avail3, used3, total3 := n3.Quota()
	if used3 != 10*1024 || avail3 != 0 || total3 != 10*1024 || n3.Files() != 10 || n3.Reserved() != 0 {
		t.Errorf("n3 quota mismatch: avail=%d used=%d total=%d files=%d reserved=%d", avail3, used3, total3, n3.Files(), n3.Reserved())
	}

	// Verify SQLite metadata accuracy
	st1, err := dm.GetStorage(1)
	if err != nil {
		t.Fatalf("GetStorage(1) failed: %v", err)
	}
	if st1.UsedBytes != 0 || st1.FileCount != 0 {
		t.Errorf("SQLite storage 1 mismatch: used=%d count=%d", st1.UsedBytes, st1.FileCount)
	}

	st2, err := dm.GetStorage(2)
	if err != nil {
		t.Fatalf("GetStorage(2) failed: %v", err)
	}
	if st2.UsedBytes != 15*1024 || st2.FileCount != 11 {
		t.Errorf("SQLite storage 2 mismatch: used=%d count=%d", st2.UsedBytes, st2.FileCount)
	}

	st3, err := dm.GetStorage(3)
	if err != nil {
		t.Fatalf("GetStorage(3) failed: %v", err)
	}
	if st3.UsedBytes != 10*1024 || st3.FileCount != 10 {
		t.Errorf("SQLite storage 3 mismatch: used=%d count=%d", st3.UsedBytes, st3.FileCount)
	}

	for p := range expectedContents {
		if _, err := dm.GetFile(1, p); err == nil {
			t.Errorf("expected file %s to be deleted from SQLite storage 1", p)
		}
		records, err := dm.GetFilesByPath(p)
		if err != nil {
			t.Fatalf("GetFilesByPath(%s) failed: %v", p, err)
		}
		if len(records) != 1 || (records[0].StorageID != 2 && records[0].StorageID != 3) {
			t.Errorf("unexpected SQLite records for %s: %+v", p, records)
		}
	}
}

func TestMigrate_Concurrency_TransientRetryAndCleanup(t *testing.T) {
	const (
		totalFiles = 10
		fileSize   = int64(512)
	)

	s1 := newMockWebDAVServer(50*1024, totalFiles*fileSize, true)
	defer s1.Close()
	s2 := newMockWebDAVServer(5*fileSize, 0, true)
	defer s2.Close()
	s3 := newMockWebDAVServer(5*fileSize, 0, true)
	defer s3.Close()

	// Inject transient PUT failures
	s2.putFailCount = 2
	s3.putFailCount = 1

	tempDir := t.TempDir()
	dm, err := data.NewManager(filepath.Join(tempDir, "retry.db"))
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer dm.Close()

	for _, sid := range []int{1, 2, 3} {
		_ = dm.EnsureStorage(sid)
	}

	for i := range totalFiles {
		p := fmt.Sprintf("/retry_%02d.dat", i)
		content := bytes.Repeat([]byte{byte('0' + i)}, int(fileSize))
		s1.setFile(p, content, time.Now())
		_ = dm.PutFile(data.FileRecord{StorageID: 1, Path: p, Size: fileSize})
	}

	cfg := &config.Config{
		Retry: 3,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: s1.server.URL, User: "u", Pass: "p", Capacity: "50KiB"},
			{ID: 2, URL: s2.server.URL, User: "u", Pass: "p", Capacity: fmt.Sprintf("%dB", 5*fileSize)},
			{ID: 3, URL: s3.server.URL, User: "u", Pass: "p", Capacity: fmt.Sprintf("%dB", 5*fileSize)},
		},
	}

	c, err := NewCluster(cfg, dm)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	n1, _ := c.GetNode(1)
	n2, _ := c.GetNode(2)
	n3, _ := c.GetNode(3)
	n1.SetQuota(totalFiles*fileSize, totalFiles)
	n2.SetQuota(0, 0)
	n3.SetQuota(0, 0)

	if err := c.MigrateNode(context.Background(), 1, nil); err != nil {
		t.Fatalf("MigrateNode with transient retries failed: %v", err)
	}

	if n2.Reserved() != 0 || n3.Reserved() != 0 {
		t.Errorf("expected 0 reserved bytes after retries, got n2=%d n3=%d", n2.Reserved(), n3.Reserved())
	}

	s1.mu.Lock()
	s1Count := len(s1.files)
	s1.mu.Unlock()
	if s1Count != 0 {
		t.Errorf("expected s1 empty, got %d files", s1Count)
	}

	s2.mu.Lock()
	s2Count := len(s2.files)
	s2.mu.Unlock()
	s3.mu.Lock()
	s3Count := len(s3.files)
	s3.mu.Unlock()
	if s2Count != 5 || s3Count != 5 {
		t.Errorf("expected 5 files each on s2 and s3, got s2=%d s3=%d", s2Count, s3Count)
	}
}

func TestMigrate_Concurrency_ErrorCancelsWorkersAndRollsBack(t *testing.T) {
	const (
		totalFiles = 12
		fileSize   = int64(256)
	)

	s1 := newMockWebDAVServer(50*1024, totalFiles*fileSize, true)
	defer s1.Close()
	s2 := newMockWebDAVServer(50*1024, 0, true)
	defer s2.Close()
	s3 := newMockWebDAVServer(50*1024, 0, true)
	defer s3.Close()

	// Make target nodes fail permanently
	s2.putFailCount = 100
	s3.putFailCount = 100

	for i := range totalFiles {
		s1.setFile(fmt.Sprintf("/fail_%02d.bin", i), make([]byte, fileSize), time.Now())
	}

	cfg := &config.Config{
		Retry: 1,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: s1.server.URL, User: "u", Pass: "p", Capacity: "50KiB"},
			{ID: 2, URL: s2.server.URL, User: "u", Pass: "p", Capacity: "50KiB"},
			{ID: 3, URL: s3.server.URL, User: "u", Pass: "p", Capacity: "50KiB"},
		},
	}

	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	n1, _ := c.GetNode(1)
	n2, _ := c.GetNode(2)
	n3, _ := c.GetNode(3)
	n1.SetQuota(totalFiles*fileSize, totalFiles)

	err = c.MigrateNode(context.Background(), 1, nil)
	if err == nil {
		t.Fatal("expected MigrateNode to fail when target PUTs permanently fail")
	}

	if n2.Reserved() != 0 || n3.Reserved() != 0 {
		t.Errorf("expected all reservations rolled back on error, got n2=%d n3=%d", n2.Reserved(), n3.Reserved())
	}

	// Source files must remain intact
	s1.mu.Lock()
	s1Count := len(s1.files)
	s1.mu.Unlock()
	if s1Count != totalFiles {
		t.Errorf("expected all %d source files to remain intact, got %d", totalFiles, s1Count)
	}
}

func TestMigrate_Concurrency_NoDB(t *testing.T) {
	const (
		totalFiles = 12
		fileSize   = int64(512)
	)

	s1 := newMockWebDAVServer(50*1024, totalFiles*fileSize, true)
	defer s1.Close()
	s2 := newMockWebDAVServer(6*fileSize, 0, true)
	defer s2.Close()
	s3 := newMockWebDAVServer(6*fileSize, 0, true)
	defer s3.Close()

	for i := range totalFiles {
		s1.setFile(fmt.Sprintf("/nodb_%02d.txt", i), bytes.Repeat([]byte("X"), int(fileSize)), time.Now())
	}

	cfg := &config.Config{
		Retry: 1,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: s1.server.URL, User: "u", Pass: "p", Capacity: "50KiB"},
			{ID: 2, URL: s2.server.URL, User: "u", Pass: "p", Capacity: fmt.Sprintf("%dB", 6*fileSize)},
			{ID: 3, URL: s3.server.URL, User: "u", Pass: "p", Capacity: fmt.Sprintf("%dB", 6*fileSize)},
		},
	}

	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	n1, _ := c.GetNode(1)
	n2, _ := c.GetNode(2)
	n3, _ := c.GetNode(3)
	n1.SetQuota(totalFiles*fileSize, totalFiles)

	if err := c.MigrateNode(context.Background(), 1, nil); err != nil {
		t.Fatalf("MigrateNode without DB failed: %v", err)
	}

	_, used1, _ := n1.Quota()
	avail2, used2, _ := n2.Quota()
	avail3, used3, _ := n3.Quota()

	if used1 != 0 || n1.Files() != 0 {
		t.Errorf("n1 expected 0 used/files, got used=%d files=%d", used1, n1.Files())
	}
	if used2 != 6*fileSize || avail2 != 0 || n2.Reserved() != 0 {
		t.Errorf("n2 quota mismatch: used=%d avail=%d reserved=%d", used2, avail2, n2.Reserved())
	}
	if used3 != 6*fileSize || avail3 != 0 || n3.Reserved() != 0 {
		t.Errorf("n3 quota mismatch: used=%d avail=%d reserved=%d", used3, avail3, n3.Reserved())
	}
}
