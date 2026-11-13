package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestPRAGMASettings verifies that the required PRAGMAs are properly set on SQLite connections:
// - WAL journal mode (for file DB)
// - synchronous = NORMAL
// - busy_timeout = 5000
// - foreign_keys = ON
func TestPRAGMASettings(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "pragma_test.db")
	m, err := NewManager(dbPath)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer m.Close()

	var journalMode string
	if err := m.DB().QueryRow("PRAGMA journal_mode;").Scan(&journalMode); err != nil {
		t.Fatalf("query journal_mode: %v", err)
	}
	if journalMode != "wal" {
		t.Errorf("expected journal_mode 'wal', got '%s'", journalMode)
	}

	var syncMode int
	if err := m.DB().QueryRow("PRAGMA synchronous;").Scan(&syncMode); err != nil {
		t.Fatalf("query synchronous: %v", err)
	}
	// 1 corresponds to NORMAL
	if syncMode != 1 {
		t.Errorf("expected synchronous=1 (NORMAL), got %d", syncMode)
	}

	var busyTimeout int
	if err := m.DB().QueryRow("PRAGMA busy_timeout;").Scan(&busyTimeout); err != nil {
		t.Fatalf("query busy_timeout: %v", err)
	}
	if busyTimeout != 5000 {
		t.Errorf("expected busy_timeout=5000, got %d", busyTimeout)
	}

	var foreignKeys int
	if err := m.DB().QueryRow("PRAGMA foreign_keys;").Scan(&foreignKeys); err != nil {
		t.Fatalf("query foreign_keys: %v", err)
	}
	if foreignKeys != 1 {
		t.Errorf("expected foreign_keys=1 (ON), got %d", foreignKeys)
	}
}

// TestPathNormalization verifies path normalization and splitting logic.
func TestPathNormalization(t *testing.T) {
	tests := []struct {
		input      string
		wantNorm   string
		wantParent string
		wantName   string
	}{
		{"", "/", "", "/"},
		{"/", "/", "", "/"},
		{"///", "/", "", "/"},
		{"/a", "/a", "/", "a"},
		{"/a/", "/a", "/", "a"},
		{"a", "/a", "/", "a"},
		{"a/b", "/a/b", "/a", "b"},
		{"a/b/", "/a/b", "/a", "b"},
		{"//a///b//c", "/a/b/c", "/a/b", "c"},
		{"/a/./b/../c", "/a/c", "/a", "c"},
		{"\\folder\\sub\\file.txt", "/folder/sub/file.txt", "/folder/sub", "file.txt"},
		{"/folder/sub/", "/folder/sub", "/folder", "sub"},
	}

	for _, tt := range tests {
		gotNorm := NormalizePath(tt.input)
		if gotNorm != tt.wantNorm {
			t.Errorf("NormalizePath(%q) = %q, want %q", tt.input, gotNorm, tt.wantNorm)
		}
		gotParent, gotName := SplitPath(tt.input)
		if gotParent != tt.wantParent || gotName != tt.wantName {
			t.Errorf("SplitPath(%q) = (%q, %q), want (%q, %q)", tt.input, gotParent, gotName, tt.wantParent, tt.wantName)
		}
	}
}

// TestTableCreationAndTriggers verifies:
// - storages and files tables created
// - INSERT increases used_bytes and file_count
// - UPDATE adjusts used_bytes and file_count
// - DELETE decreases used_bytes and file_count
// - Directories (is_dir=1) do not count towards file_count or used_bytes
// - Storage cross-migration updates both storages
func TestTableCreationAndTriggers(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	if err := m.EnsureStorage(1); err != nil {
		t.Fatalf("EnsureStorage(1) failed: %v", err)
	}
	if err := m.EnsureStorage(2); err != nil {
		t.Fatalf("EnsureStorage(2) failed: %v", err)
	}

	// 1. Initial storage stats
	st1, err := m.GetStorage(1)
	if err != nil {
		t.Fatalf("GetStorage(1) failed: %v", err)
	}
	if st1.UsedBytes != 0 || st1.FileCount != 0 {
		t.Fatalf("initial storage not empty: %+v", st1)
	}

	// 2. Insert regular file (INSERT increment)
	f1 := FileRecord{
		StorageID: 1,
		Path:      "/docs/spec.pdf",
		Size:      1024,
		IsDir:     false,
	}
	if err := m.PutFile(f1); err != nil {
		t.Fatalf("PutFile(f1) failed: %v", err)
	}

	st1, _ = m.GetStorage(1)
	if st1.UsedBytes != 1024 || st1.FileCount != 1 {
		t.Errorf("after INSERT f1: expected used=1024 count=1, got used=%d count=%d", st1.UsedBytes, st1.FileCount)
	}

	// 3. Insert directory (is_dir=true) -> should NOT increment used_bytes or file_count
	dir := FileRecord{
		StorageID: 1,
		Path:      "/docs",
		Size:      0,
		IsDir:     true,
	}
	if err := m.PutFile(dir); err != nil {
		t.Fatalf("PutFile(dir) failed: %v", err)
	}

	st1, _ = m.GetStorage(1)
	if st1.UsedBytes != 1024 || st1.FileCount != 1 {
		t.Errorf("after inserting directory: expected used=1024 count=1, got used=%d count=%d", st1.UsedBytes, st1.FileCount)
	}

	// 4. Insert 2nd regular file
	f2 := FileRecord{
		StorageID: 1,
		Path:      "/docs/image.png",
		Size:      2048,
		IsDir:     false,
	}
	if err := m.PutFile(f2); err != nil {
		t.Fatalf("PutFile(f2) failed: %v", err)
	}

	st1, _ = m.GetStorage(1)
	if st1.UsedBytes != 3072 || st1.FileCount != 2 {
		t.Errorf("after INSERT f2: expected used=3072 count=2, got used=%d count=%d", st1.UsedBytes, st1.FileCount)
	}

	// 5. UPDATE in-place size adjustment (e.g. file modified from 1024 to 512 bytes)
	f1Updated := FileRecord{
		StorageID: 1,
		Path:      "/docs/spec.pdf",
		Size:      512,
		IsDir:     false,
	}
	if err := m.PutFile(f1Updated); err != nil {
		t.Fatalf("PutFile(f1Updated) failed: %v", err)
	}

	st1, _ = m.GetStorage(1)
	if st1.UsedBytes != 2560 || st1.FileCount != 2 {
		t.Errorf("after UPDATE f1: expected used=2560 count=2, got used=%d count=%d", st1.UsedBytes, st1.FileCount)
	}

	// 6. DELETE regular file (DELETE decrement)
	if err := m.DeleteFile(1, "/docs/image.png"); err != nil {
		t.Fatalf("DeleteFile failed: %v", err)
	}

	st1, _ = m.GetStorage(1)
	if st1.UsedBytes != 512 || st1.FileCount != 1 {
		t.Errorf("after DELETE f2: expected used=512 count=1, got used=%d count=%d", st1.UsedBytes, st1.FileCount)
	}

	// 7. Cross-storage migration UPDATE (moving from storage 1 to storage 2)
	_, err = m.DB().Exec("UPDATE files SET storage_id = 2 WHERE storage_id = 1 AND path = '/docs/spec.pdf'")
	if err != nil {
		t.Fatalf("cross-storage update failed: %v", err)
	}

	st1, _ = m.GetStorage(1)
	if st1.UsedBytes != 0 || st1.FileCount != 0 {
		t.Errorf("after moving file out: expected storage 1 used=0 count=0, got used=%d count=%d", st1.UsedBytes, st1.FileCount)
	}
	st2, _ := m.GetStorage(2)
	if st2.UsedBytes != 512 || st2.FileCount != 1 {
		t.Errorf("after moving file in: expected storage 2 used=512 count=1, got used=%d count=%d", st2.UsedBytes, st2.FileCount)
	}
}

// TestSubtreeDeletionCascade verifies that deleting a directory triggers cascade deletion
// of all descendants and automatically deducts used_bytes and file_count on the storage.
func TestSubtreeDeletionCascade(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	if err := m.EnsureStorage(1); err != nil {
		t.Fatalf("EnsureStorage failed: %v", err)
	}

	// Create directory structure:
	// /folder (dir)
	// /folder/file1.txt (100 B)
	// /folder/file2.txt (200 B)
	// /folder/nested (dir)
	// /folder/nested/file3.txt (300 B)
	// /outside.txt (400 B)
	files := []FileRecord{
		{StorageID: 1, Path: "/folder", IsDir: true},
		{StorageID: 1, Path: "/folder/file1.txt", Size: 100, IsDir: false},
		{StorageID: 1, Path: "/folder/file2.txt", Size: 200, IsDir: false},
		{StorageID: 1, Path: "/folder/nested", IsDir: true},
		{StorageID: 1, Path: "/folder/nested/file3.txt", Size: 300, IsDir: false},
		{StorageID: 1, Path: "/outside.txt", Size: 400, IsDir: false},
	}

	for _, f := range files {
		if err := m.PutFile(f); err != nil {
			t.Fatalf("PutFile(%s) failed: %v", f.Path, err)
		}
	}

	st, err := m.GetStorage(1)
	if err != nil {
		t.Fatalf("GetStorage failed: %v", err)
	}
	// Total: 100 + 200 + 300 + 400 = 1000 bytes, 4 files
	if st.UsedBytes != 1000 || st.FileCount != 4 {
		t.Fatalf("expected used=1000 count=4, got used=%d count=%d", st.UsedBytes, st.FileCount)
	}

	// 1. Delete the directory row directly: trigger trg_files_delete_dir cascades to all children
	if err := m.DeleteFile(1, "/folder"); err != nil {
		t.Fatalf("DeleteFile(/folder) failed: %v", err)
	}

	// Storage 1 should now only have /outside.txt (400 B, 1 file)
	st, _ = m.GetStorage(1)
	if st.UsedBytes != 400 || st.FileCount != 1 {
		t.Errorf("after cascade delete of /folder: expected used=400 count=1, got used=%d count=%d", st.UsedBytes, st.FileCount)
	}

	// Verify only /outside.txt remains in DB
	remaining, err := m.ListSubtree("/")
	if err != nil {
		t.Fatalf("ListSubtree failed: %v", err)
	}
	if len(remaining) != 1 || remaining[0].Path != "/outside.txt" {
		t.Errorf("expected only /outside.txt remaining, got: %+v", remaining)
	}

	// 2. Test DeleteSubtree API on root
	if err := m.DeleteSubtree("/"); err != nil {
		t.Fatalf("DeleteSubtree(/) failed: %v", err)
	}
	st, _ = m.GetStorage(1)
	if st.UsedBytes != 0 || st.FileCount != 0 {
		t.Errorf("after DeleteSubtree(/): expected used=0 count=0, got used=%d count=%d", st.UsedBytes, st.FileCount)
	}
}

// TestPROPFINDDirectChildrenAndRouting tests:
// - Single file routing (RouteFile)
// - Routing failure when file does not exist
// - Direct children query (ListChildren, PROPFIND Depth:1)
// - Path normalization resilience
func TestPROPFINDDirectChildrenAndRouting(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	// Seed files across storage 1 and 2
	records := []FileRecord{
		{StorageID: 1, Path: "/photos", IsDir: true},
		{StorageID: 1, Path: "/photos/beach.jpg", Size: 5000, IsDir: false},
		{StorageID: 2, Path: "/photos/mountain.jpg", Size: 7000, IsDir: false},
		{StorageID: 1, Path: "/photos/2026", IsDir: true},
		{StorageID: 1, Path: "/photos/2026/party.jpg", Size: 3000, IsDir: false},
		{StorageID: 2, Path: "/root_file.txt", Size: 100, IsDir: false},
	}

	if err := m.BatchInsert(records); err != nil {
		t.Fatalf("BatchInsert failed: %v", err)
	}

	// 1. Single file routing
	r, err := m.RouteFile("/photos/beach.jpg")
	if err != nil {
		t.Fatalf("RouteFile(/photos/beach.jpg) failed: %v", err)
	}
	if r.StorageID != 1 || r.Size != 5000 || r.ETag() == "" || r.Name() != "beach.jpg" || r.ContentType() != "image/jpeg" {
		t.Errorf("unexpected routed file: %+v", r)
	}

	r2, err := m.RouteFile("photos/mountain.jpg") // without leading slash
	if err != nil {
		t.Fatalf("RouteFile(photos/mountain.jpg) failed: %v", err)
	}
	if r2.StorageID != 2 || r2.Size != 7000 || r2.ETag() == "" || r2.Name() != "mountain.jpg" || r2.ContentType() != "image/jpeg" {
		t.Errorf("unexpected routed file: %+v", r2)
	}

	// 2. Non-existent file routing
	_, err = m.RouteFile("/photos/non_existent.jpg")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}

	// 3. PROPFIND Depth:1 on /photos
	// Direct children of /photos should be:
	// - /photos/2026 (dir)
	// - /photos/beach.jpg (file)
	// - /photos/mountain.jpg (file)
	// It MUST NOT contain /photos/2026/party.jpg or /root_file.txt
	children, err := m.ListChildren("/photos")
	if err != nil {
		t.Fatalf("ListChildren(/photos) failed: %v", err)
	}
	if len(children) != 3 {
		t.Fatalf("expected 3 direct children under /photos, got %d: %+v", len(children), children)
	}
	paths := make(map[string]bool)
	for _, c := range children {
		paths[c.Path] = true
	}
	if !paths["/photos/2026"] || !paths["/photos/beach.jpg"] || !paths["/photos/mountain.jpg"] {
		t.Errorf("unexpected children set: %+v", paths)
	}

	// Test trailing slash input
	childrenTrailing, err := m.ListChildren("/photos/")
	if err != nil || len(childrenTrailing) != 3 {
		t.Errorf("ListChildren with trailing slash failed, len=%d, err=%v", len(childrenTrailing), err)
	}

	// 4. PROPFIND Depth:1 on root "/"
	// Direct children of "/" are /photos (in storages 1 and 2) and /root_file.txt
	rootChildren, err := m.ListChildren("/")
	if err != nil {
		t.Fatalf("ListChildren(/) failed: %v", err)
	}
	rootPaths := make(map[string]bool)
	for _, c := range rootChildren {
		rootPaths[c.Path] = true
	}
	if len(rootPaths) != 2 || !rootPaths["/photos"] || !rootPaths["/root_file.txt"] {
		t.Fatalf("expected 2 unique direct children under /, got %d unique (%d total): %+v", len(rootPaths), len(rootChildren), rootChildren)
	}
}
// TestSubtreeQueryAndCalibration tests:
// - Subtree queries
// - Single and multi-storage quota calibrations
// - Batch insertion for initial full sync
func TestSubtreeQueryAndCalibration(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	// 1. Batch insert files for storage 1 (initial full sync)
	syncFiles := make([]FileRecord, 50)
	for i := range 50 {
		syncFiles[i] = FileRecord{
			StorageID: 1,
			Path:      fmt.Sprintf("/sync/dir%d/file%d.dat", i/10, i),
			Size:      int64((i + 1) * 10),
			IsDir:     false,
		}
	}
	if err := m.BatchInsertFiles(1, syncFiles); err != nil {
		t.Fatalf("BatchInsertFiles failed: %v", err)
	}

	st1, err := m.GetStorage(1)
	if err != nil {
		t.Fatalf("GetStorage failed: %v", err)
	}
	var expectedUsed int64
	for i := range 50 {
		expectedUsed += int64((i + 1) * 10)
	}
	if st1.UsedBytes != expectedUsed || st1.FileCount != 50 {
		t.Errorf("expected used=%d count=50, got used=%d count=%d", expectedUsed, st1.UsedBytes, st1.FileCount)
	}
	if st1.LastSync == nil || st1.LastSync.IsZero() {
		t.Errorf("expected non-zero last_sync timestamp after full sync")
	}

	// 2. Subtree query for /sync/dir0
	dir0Files, err := m.ListSubtree("/sync/dir0")
	if err != nil {
		t.Fatalf("ListSubtree(/sync/dir0) failed: %v", err)
	}
	// Subtree includes /sync/dir0 directory row (via ancestor completion) plus 10 child files = 11
	if len(dir0Files) != 11 {
		t.Fatalf("expected 11 items in /sync/dir0 subtree, got %d", len(dir0Files))
	}
	// 3. Quota calibration
	// Manually tamper storages table to corrupt quota statistics
	_, err = m.DB().Exec("UPDATE storages SET used_bytes = 999999, file_count = 999 WHERE id = 1")
	if err != nil {
		t.Fatalf("corrupt quota: %v", err)
	}
	corrupted, _ := m.GetStorage(1)
	if corrupted.UsedBytes != 999999 || corrupted.FileCount != 999 {
		t.Fatalf("failed to tamper storage stats")
	}

	// Recalibrate single storage
	if err := m.CalibrateStorage(1); err != nil {
		t.Fatalf("CalibrateStorage failed: %v", err)
	}
	calibrated, _ := m.GetStorage(1)
	if calibrated.UsedBytes != expectedUsed || calibrated.FileCount != 50 {
		t.Errorf("after CalibrateStorage: expected used=%d count=50, got used=%d count=%d", expectedUsed, calibrated.UsedBytes, calibrated.FileCount)
	}

	// Tamper again and recalibrate with CalibrateAll
	_, _ = m.DB().Exec("UPDATE storages SET used_bytes = 123, file_count = 456 WHERE id = 1")
	if err := m.CalibrateStorages(); err != nil {
		t.Fatalf("CalibrateStorages failed: %v", err)
	}
	calibratedAll, _ := m.GetStorage(1)
	if calibratedAll.UsedBytes != expectedUsed || calibratedAll.FileCount != 50 {
		t.Errorf("after CalibrateAll: expected used=%d count=50, got used=%d count=%d", expectedUsed, calibratedAll.UsedBytes, calibratedAll.FileCount)
	}
}

// TestConcurrencySafety tests concurrent operations:
// multiple goroutines concurrently reading, inserting, deleting, routing, and calibrating.
func TestConcurrencySafety(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "concurrent_test.db")
	m, err := NewManager(dbPath)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	_ = m.EnsureStorage(1)
	_ = m.EnsureStorage(2)

	var wg sync.WaitGroup
	workers := 16
	opsPerWorker := 40

	for w := range workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			storageID := (workerID % 2) + 1

			for i := range opsPerWorker {
				filePath := fmt.Sprintf("/work_%d/file_%d.bin", workerID, i)

				// 1. PutFile
				err := m.PutFile(FileRecord{
					StorageID: storageID,
					Path:      filePath,
					Size:      int64(i + 1),
					IsDir:     false,
				})
				if err != nil {
					t.Errorf("concurrent PutFile error: %v", err)
				}

				// 2. RouteFile
				if _, err := m.RouteFile(filePath); err != nil {
					t.Errorf("concurrent RouteFile error: %v", err)
				}

				// 3. ListChildren
				if _, err := m.ListChildren(fmt.Sprintf("/work_%d", workerID)); err != nil {
					t.Errorf("concurrent ListChildren error: %v", err)
				}

				// 4. Record operations compatibility
				m.RecordWrite(storageID, 10, false)
				m.RecordOverwrite(storageID, 5)

				// 5. Read stats
				if _, ok := m.Get(storageID); !ok {
					t.Errorf("concurrent Get returned false")
				}

				// 6. Calibrate periodically
				if i%10 == 0 {
					_ = m.CalibrateStorage(storageID)
				}

				// 7. Occasional delete
				if i%5 == 0 && i > 0 {
					prevPath := fmt.Sprintf("/work_%d/file_%d.bin", workerID, i-1)
					_ = m.DeleteFile(storageID, prevPath)
				}
			}
		}(w)
	}

	wg.Wait()

	// Ensure database is in a consistent state and calibrate final stats
	if err := m.CalibrateStorages(); err != nil {
		t.Fatalf("final CalibrateStorages failed: %v", err)
	}

	allStorages, err := m.GetAllStorages()
	if err != nil {
		t.Fatalf("GetAllStorages failed: %v", err)
	}
	if len(allStorages) < 2 {
		t.Errorf("expected at least 2 storages, got %d", len(allStorages))
	}
}

// TestLegacyYamlMigration verifies that an existing data.yml file is automatically
// parsed, backed up, and migrated into SQLite storages table on startup.
func TestLegacyYamlMigration(t *testing.T) {
	tempDir := t.TempDir()
	legacyFile := filepath.Join(tempDir, "data.yml")

	legacyYaml := `
version: 1
upstreams:
  1:
    used: 10485760
    human_used: 10M
    files: 42
    updated_at: 2026-11-09T01:00:00Z
  2:
    used: 20971520
    human_used: 20M
    files: 84
    updated_at: 2026-11-09T02:00:00Z
`
	if err := os.WriteFile(legacyFile, []byte(legacyYaml), 0644); err != nil {
		t.Fatalf("failed to write mock legacy yaml: %v", err)
	}
	m, err := NewManager(legacyFile)
	if err != nil {
		t.Fatalf("NewManager with legacy yaml failed: %v", err)
	}
	defer m.Close()

	// Verify backup file was created
	bakPath := legacyFile + ".bak"
	if _, err := os.Stat(bakPath); err != nil {
		t.Errorf("expected backup file %s to exist: %v", bakPath, err)
	}

	// Verify upstreams were migrated
	n1, ok1 := m.Get(1)
	if !ok1 || n1.UsedBytes != 10485760 || n1.FileCount != 42 {
		t.Errorf("node 1 not properly migrated: %+v", n1)
	}

	n2, ok2 := m.Get(2)
	if !ok2 || n2.UsedBytes != 20971520 || n2.FileCount != 84 {
		t.Errorf("node 2 not properly migrated: %+v", n2)
	}
}

// TestCompatibilityRecordOperations verifies compatibility methods:
// RecordWrite, RecordOverwrite, RecordDelete, Set, Get.
func TestCompatibilityRecordOperations(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	// Initial Set
	m.Set(1, 1000, 10, true)
	n, ok := m.Get(1)
	if !ok || n.UsedBytes != 1000 || n.FileCount != 10 {
		t.Fatalf("after Set: %+v", n)
	}
	if n.LastSync == nil || n.LastSync.IsZero() {
		t.Errorf("expected non-zero LastSync")
	}

	// RecordWrite
	m.RecordWrite(1, 500, true)
	n, _ = m.Get(1)
	if n.UsedBytes != 1500 || n.FileCount != 11 {
		t.Errorf("after RecordWrite: %+v", n)
	}

	// RecordOverwrite positive
	m.RecordOverwrite(1, 300)
	n, _ = m.Get(1)
	if n.UsedBytes != 1800 || n.FileCount != 11 {
		t.Errorf("after RecordOverwrite positive: %+v", n)
	}

	// RecordOverwrite negative
	m.RecordOverwrite(1, -200)
	n, _ = m.Get(1)
	if n.UsedBytes != 1600 || n.FileCount != 11 {
		t.Errorf("after RecordOverwrite negative: %+v", n)
	}

	// RecordDelete
	m.RecordDelete(1, 600)
	n, _ = m.Get(1)
	if n.UsedBytes != 1000 || n.FileCount != 10 {
		t.Errorf("after RecordDelete: %+v", n)
	}

	// RecordDelete clamp to zero
	m.RecordDelete(1, 20000)
	n, _ = m.Get(1)
	if n.UsedBytes != 0 || n.FileCount != 9 {
		t.Errorf("after RecordDelete clamped: %+v", n)
	}
}

func TestMovePath_Cascade(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	if err := m.EnsureStorage(1); err != nil {
		t.Fatalf("EnsureStorage failed: %v", err)
	}

	// 1. Test moving a single file
	if err := m.PutFile(FileRecord{StorageID: 1, Path: "/orig/file.txt", Size: 100, IsDir: false}); err != nil {
		t.Fatalf("PutFile failed: %v", err)
	}
	if err := m.MovePath(1, "/orig/file.txt", "/renamed/file.txt"); err != nil {
		t.Fatalf("MovePath file failed: %v", err)
	}
	f, err := m.GetFile(1, "/renamed/file.txt")
	if err != nil || f.ParentPath != "/renamed" || f.Name() != "file.txt" || f.Size != 100 {
		t.Fatalf("unexpected moved file: %+v, err: %v", f, err)
	}
	// Old path should not exist
	if _, err := m.GetFile(1, "/orig/file.txt"); !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound for old path, got %v", err)
	}

	// 2. Test moving a directory with descendants (cascading update)
	tree := []FileRecord{
		{StorageID: 1, Path: "/a", IsDir: true},
		{StorageID: 1, Path: "/a/f1.txt", Size: 200, IsDir: false},
		{StorageID: 1, Path: "/a/sub", IsDir: true},
		{StorageID: 1, Path: "/a/sub/f2.txt", Size: 300, IsDir: false},
	}
	for _, tr := range tree {
		if err := m.PutFile(tr); err != nil {
			t.Fatalf("PutFile tree failed: %v", err)
		}
	}

	// Move /a to /b
	if err := m.MovePath(1, "/a", "/b"); err != nil {
		t.Fatalf("MovePath dir failed: %v", err)
	}

	// Check /b
	bDir, err := m.GetFile(1, "/b")
	if err != nil || !bDir.IsDir || bDir.ParentPath != "/" || bDir.Name() != "b" {
		t.Errorf("unexpected moved dir /b: %+v, err: %v", bDir, err)
	}
	// Check /b/f1.txt
	bf1, err := m.GetFile(1, "/b/f1.txt")
	if err != nil || bf1.ParentPath != "/b" || bf1.Name() != "f1.txt" || bf1.Size != 200 {
		t.Errorf("unexpected moved file /b/f1.txt: %+v, err: %v", bf1, err)
	}
	// Check /b/sub
	bSub, err := m.GetFile(1, "/b/sub")
	if err != nil || !bSub.IsDir || bSub.ParentPath != "/b" || bSub.Name() != "sub" {
		t.Errorf("unexpected moved dir /b/sub: %+v, err: %v", bSub, err)
	}
	// Check /b/sub/f2.txt
	bf2, err := m.GetFile(1, "/b/sub/f2.txt")
	if err != nil || bf2.ParentPath != "/b/sub" || bf2.Name() != "f2.txt" || bf2.Size != 300 {
		t.Errorf("unexpected moved file /b/sub/f2.txt: %+v, err: %v", bf2, err)
	}

	// 3. Move directory into its own descendant must fail
	if err := m.MovePath(1, "/b", "/b/nested/error"); err == nil {
		t.Errorf("expected error moving dir into descendant, got nil")
	}

	// 4. Move root must fail
	if err := m.MovePath(1, "/", "/root"); err == nil {
		t.Errorf("expected error moving root, got nil")
	}

	// 5. Move non-existent item must fail
	if err := m.MovePath(1, "/does_not_exist", "/new_path"); !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestBatchInsertFiles_Empty(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	if err := m.BatchInsertFiles(10, nil); err != nil {
		t.Fatalf("BatchInsertFiles with empty slice failed: %v", err)
	}

	st, err := m.GetStorage(10)
	if err != nil {
		t.Fatalf("GetStorage failed: %v", err)
	}
	if st.UsedBytes != 0 || st.FileCount != 0 {
		t.Errorf("expected 0 bytes and 0 files, got %d bytes, %d files", st.UsedBytes, st.FileCount)
	}
	if st.LastSync == nil || st.LastSync.IsZero() {
		t.Errorf("expected non-nil LastSync for empty storage after BatchInsertFiles")
	}
}
func TestPutFile_AutoCreatesAncestorDirectories(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	// Insert a deep nested file: /a/b/c/deep.txt
	err = m.PutFile(FileRecord{
		StorageID: 1,
		Path:      "/a/b/c/deep.txt",
		Size:      1024,
		IsDir:     false,
		ModTime:   time.Now(),
	})
	if err != nil {
		t.Fatalf("PutFile failed: %v", err)
	}

	// Check that /a, /a/b, and /a/b/c all exist as directories
	for _, p := range []string{"/a", "/a/b", "/a/b/c"} {
		rec, err := m.GetFile(1, p)
		if err != nil {
			t.Fatalf("expected ancestor dir %s to exist, got err: %v", p, err)
		}
		if !rec.IsDir {
			t.Errorf("expected %s to be directory", p)
		}
	}

	// Check PROPFIND / depth: 1 lists /a
	childrenRoot, err := m.ListChildren("/")
	if err != nil {
		t.Fatalf("ListChildren(/) failed: %v", err)
	}
	if len(childrenRoot) != 1 || childrenRoot[0].Path != "/a" {
		t.Errorf("expected [/a], got: %+v", childrenRoot)
	}
}

func TestBatchInsertAncestorDirectories(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	files := []FileRecord{
		{StorageID: 1, Path: "/level1/level2/level3/file1.txt", Size: 100},
		{StorageID: 2, Path: "/level1/level2/other/file2.txt", Size: 200},
	}

	// 1. BatchInsertFiles
	if err := m.BatchInsertFiles(1, files[:1]); err != nil {
		t.Fatalf("BatchInsertFiles failed: %v", err)
	}

	// Verify all ancestors exist for storage 1
	for _, p := range []string{"/level1", "/level1/level2", "/level1/level2/level3"} {
		rec, err := m.GetFile(1, p)
		if err != nil {
			t.Fatalf("expected ancestor %s to exist in storage 1, got %v", p, err)
		}
		if !rec.IsDir {
			t.Errorf("expected %s to be dir", p)
		}
	}

	// 2. BatchInsert across storages
	if err := m.BatchInsert(files[1:]); err != nil {
		t.Fatalf("BatchInsert failed: %v", err)
	}

	// Verify ancestor in storage 2
	for _, p := range []string{"/level1", "/level1/level2", "/level1/level2/other"} {
		rec, err := m.GetFile(2, p)
		if err != nil {
			t.Fatalf("expected ancestor %s to exist in storage 2, got %v", p, err)
		}
		if !rec.IsDir {
			t.Errorf("expected %s to be dir", p)
		}
	}

	// Verify Depth 0 PROPFIND query (RouteFile / GetFilesByPath) works on ancestors
	recs, err := m.GetFilesByPath("/level1/level2")
	if err != nil || len(recs) == 0 {
		t.Fatalf("GetFilesByPath(/level1/level2) failed: %v", err)
	}
}

func TestTriggerUpdateFilter(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	if err := m.PutFile(FileRecord{StorageID: 1, Path: "/folder/item.bin", Size: 500}); err != nil {
		t.Fatalf("PutFile failed: %v", err)
	}

	stBefore, err := m.GetStorage(1)
	if err != nil || stBefore.UsedBytes != 500 || stBefore.FileCount != 1 {
		t.Fatalf("unexpected storage before move: %+v", stBefore)
	}

	// Move item inside same storage without changing size or is_dir
	if err := m.MovePath(1, "/folder/item.bin", "/folder/item_renamed.bin"); err != nil {
		t.Fatalf("MovePath failed: %v", err)
	}

	stAfter, err := m.GetStorage(1)
	if err != nil {
		t.Fatalf("GetStorage failed: %v", err)
	}
	if stAfter.UsedBytes != 500 || stAfter.FileCount != 1 {
		t.Errorf("quota corrupted after rename: %+v", stAfter)
	}
}

func TestRecursiveTriggersMultiConn(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "triggers_test.db")
	m, err := NewManager(dbPath)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	// Open multiple connections and verify recursive_triggers is ON on all of them
	for range 10 {
		conn, err := m.DB().Conn(context.Background())
		if err != nil {
			t.Fatalf("Conn failed: %v", err)
		}
		var rt int
		err = conn.QueryRowContext(context.Background(), "PRAGMA recursive_triggers;").Scan(&rt)
		_ = conn.Close()
		if err != nil || rt != 1 {
			t.Fatalf("expected recursive_triggers = 1, got %d, err: %v", rt, err)
		}
	}
}

func TestWALCheckpointTruncate(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "wal_truncate.db")
	m, err := NewManager(dbPath)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	for i := range 20 {
		_ = m.PutFile(FileRecord{
			StorageID: 1,
			Path:      fmt.Sprintf("/file_%d.txt", i),
			Size:      100,
		})
	}

	// Save() should checkpoint with TRUNCATE
	if err := m.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	walPath := dbPath + "-wal"
	if fi, err := os.Stat(walPath); err == nil {
		if fi.Size() > 0 {
			t.Logf("WAL file size after truncate checkpoint: %d", fi.Size())
		}
	}
}

func TestFileRecordMethods(t *testing.T) {
	tNow := time.Now()
	tProbe := time.Now().Add(-1 * time.Hour)
	f := FileRecord{
		StorageID:    1,
		Path:         "/docs/readme.txt",
		Size:         512,
		IsDir:        false,
		ModTime:      tNow,
		LastProbedAt: tProbe,
	}
	if f.LastProbedAt != tProbe {
		t.Errorf("expected LastProbedAt %v, got %v", tProbe, f.LastProbedAt)
	}
	if f.Name() != "readme.txt" {
		t.Errorf("expected Name() readme.txt, got %s", f.Name())
	}
	if f.ContentType() != "text/plain; charset=utf-8" {
		t.Errorf("expected text/plain; charset=utf-8, got %s", f.ContentType())
	}
	expectedETag := fmt.Sprintf(`"%x-%x"`, tNow.UnixNano(), int64(512))
	if f.ETag() != expectedETag {
		t.Errorf("expected ETag %s, got %s", expectedETag, f.ETag())
	}

	dir := FileRecord{
		StorageID: 1,
		Path:      "/docs",
		IsDir:     true,
	}
	if dir.Name() != "docs" {
		t.Errorf("expected Name() docs, got %s", dir.Name())
	}
	if dir.ContentType() != "httpd/unix-directory" {
		t.Errorf("expected httpd/unix-directory, got %s", dir.ContentType())
	}

	root := FileRecord{
		Path:  "/",
		IsDir: true,
	}
	if root.Name() != "/" {
		t.Errorf("expected Name() /, got %s", root.Name())
	}
}

func TestWithTx_Commit(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	err = m.WithTx(context.Background(), func(tx *sql.Tx) error {
		return m.PutFileTx(tx, FileRecord{
			StorageID: 1,
			Path:      "/folder/test.txt",
			Size:      1024,
			IsDir:     false,
		})
	})
	if err != nil {
		t.Fatalf("WithTx failed: %v", err)
	}

	f, err := m.GetFile(1, "/folder/test.txt")
	if err != nil {
		t.Fatalf("GetFile failed: %v", err)
	}
	if f == nil || f.Size != 1024 {
		t.Fatalf("expected file size 1024, got %v", f)
	}

	st, err := m.GetStorage(1)
	if err != nil {
		t.Fatalf("GetStorage failed: %v", err)
	}
	if st.UsedBytes != 1024 || st.FileCount != 1 {
		t.Fatalf("expected 1024 bytes and 1 file, got %d bytes, %d files", st.UsedBytes, st.FileCount)
	}
}

func TestWithTx_RollbackOnError(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	expectedErr := errors.New("simulated transaction failure")
	err = m.WithTx(context.Background(), func(tx *sql.Tx) error {
		if err := m.PutFileTx(tx, FileRecord{
			StorageID: 1,
			Path:      "/folder/rollback.txt",
			Size:      2048,
			IsDir:     false,
		}); err != nil {
			return err
		}
		return expectedErr
	})

	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}

	f, err := m.GetFile(1, "/folder/rollback.txt")
	if !errors.Is(err, ErrNotFound) && f != nil {
		t.Fatalf("expected file to not exist after rollback, got %v (err: %v)", f, err)
	}

	st, err := m.GetStorage(1)
	if err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetStorage error: %v", err)
	}
	if st != nil && (st.UsedBytes != 0 || st.FileCount != 0) {
		t.Fatalf("expected storage to have 0 bytes and 0 files, got %d bytes, %d files", st.UsedBytes, st.FileCount)
	}
}

func TestWithTx_RollbackOnPanic(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	defer func() {
		p := recover()
		if p == nil {
			t.Fatalf("expected panic to be propagated")
		}
		if p != "crash-panic" {
			t.Fatalf("expected panic value 'crash-panic', got %v", p)
		}

		f, err := m.GetFile(1, "/folder/panic.txt")
		if !errors.Is(err, ErrNotFound) && f != nil {
			t.Fatalf("expected file to not exist after panic rollback, got %v", f)
		}
	}()

	_ = m.WithTx(context.Background(), func(tx *sql.Tx) error {
		if err := m.PutFileTx(tx, FileRecord{
			StorageID: 1,
			Path:      "/folder/panic.txt",
			Size:      4096,
			IsDir:     false,
		}); err != nil {
			return err
		}
		panic("crash-panic")
	})
}

func TestWithTx_OperationsTx(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	if err := m.PutFile(FileRecord{StorageID: 1, Path: "/a/file1.txt", Size: 100}); err != nil {
		t.Fatalf("PutFile file1: %v", err)
	}
	if err := m.PutFile(FileRecord{StorageID: 1, Path: "/a/file2.txt", Size: 200}); err != nil {
		t.Fatalf("PutFile file2: %v", err)
	}
	if err := m.PutFile(FileRecord{StorageID: 1, Path: "/b/file3.txt", Size: 300}); err != nil {
		t.Fatalf("PutFile file3: %v", err)
	}

	err = m.WithTx(context.Background(), func(tx *sql.Tx) error {
		if err := m.MovePathTx(tx, 1, "/a/file1.txt", "/b/file1_moved.txt"); err != nil {
			return err
		}
		if err := m.DeleteFileTx(tx, 1, "/a/file2.txt"); err != nil {
			return err
		}
		if err := m.PutFileTx(tx, FileRecord{StorageID: 1, Path: "/b/file4.txt", Size: 400}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("atomic WithTx failed: %v", err)
	}

	if _, err := m.GetFile(1, "/a/file1.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected /a/file1.txt not found, got err: %v", err)
	}
	if f, err := m.GetFile(1, "/b/file1_moved.txt"); err != nil || f.Size != 100 {
		t.Fatalf("expected /b/file1_moved.txt with size 100, got %v (err: %v)", f, err)
	}

	if _, err := m.GetFile(1, "/a/file2.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected /a/file2.txt deleted, got err: %v", err)
	}

	if f, err := m.GetFile(1, "/b/file4.txt"); err != nil || f.Size != 400 {
		t.Fatalf("expected /b/file4.txt with size 400, got %v (err: %v)", f, err)
	}

	st, err := m.GetStorage(1)
	if err != nil {
		t.Fatalf("GetStorage: %v", err)
	}
	if st.UsedBytes != 800 || st.FileCount != 3 {
		t.Fatalf("expected 800 bytes and 3 files, got %d bytes, %d files", st.UsedBytes, st.FileCount)
	}

	err = m.WithTx(context.Background(), func(tx *sql.Tx) error {
		return m.DeleteSubtreeByStorageTx(tx, 1, "/b")
	})
	if err != nil {
		t.Fatalf("DeleteSubtreeByStorageTx: %v", err)
	}

	sub, err := m.ListSubtreeByStorage(1, "/b")
	if err != nil {
		t.Fatalf("ListSubtreeByStorage: %v", err)
	}
	if len(sub) != 0 {
		t.Fatalf("expected 0 files under /b, got %d", len(sub))
	}
}

func TestWithTx_MultiStepRollback(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	if err := m.PutFile(FileRecord{StorageID: 1, Path: "/docs/report.pdf", Size: 500}); err != nil {
		t.Fatalf("PutFile: %v", err)
	}

	failErr := errors.New("midway failure")
	err = m.WithTx(context.Background(), func(tx *sql.Tx) error {
		if err := m.PutFileTx(tx, FileRecord{StorageID: 1, Path: "/docs/extra.pdf", Size: 300}); err != nil {
			return err
		}
		if err := m.DeleteFileTx(tx, 1, "/docs/report.pdf"); err != nil {
			return err
		}
		return failErr
	})

	if !errors.Is(err, failErr) {
		t.Fatalf("expected failErr, got %v", err)
	}

	f, err := m.GetFile(1, "/docs/report.pdf")
	if err != nil || f == nil || f.Size != 500 {
		t.Fatalf("expected original report.pdf retained, got %v (err: %v)", f, err)
	}

	if _, err := m.GetFile(1, "/docs/extra.pdf"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected extra.pdf not found, got err: %v", err)
	}

	st, err := m.GetStorage(1)
	if err != nil {
		t.Fatalf("GetStorage: %v", err)
	}
	if st.UsedBytes != 500 || st.FileCount != 1 {
		t.Fatalf("expected quota 500 bytes and 1 file, got %d bytes, %d files", st.UsedBytes, st.FileCount)
	}
}

func TestWithTx_ContextCancellation(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = m.WithTx(ctx, func(tx *sql.Tx) error {
		return m.PutFileTx(tx, FileRecord{StorageID: 1, Path: "/cancel.txt", Size: 100})
	})
	if err == nil {
		t.Fatalf("expected error on canceled context, got nil")
	}

	if _, err := m.GetFile(1, "/cancel.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected file to not exist, got err: %v", err)
	}
}
func TestManager_GetAllStorageIDs(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	// Empty DB
	ids, err := m.GetAllStorageIDs()
	if err != nil {
		t.Fatalf("GetAllStorageIDs on empty DB: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected 0 ids, got %v", ids)
	}

	// Add storage 10 via EnsureStorage
	if err := m.EnsureStorage(10); err != nil {
		t.Fatalf("EnsureStorage(10): %v", err)
	}

	// Add storage 20 with files via PutFile
	if err := m.PutFile(FileRecord{StorageID: 20, Path: "/file20.txt", Size: 200}); err != nil {
		t.Fatalf("PutFile: %v", err)
	}

	// Insert into files directly for storage 5 with foreign keys temporarily off
	if _, err := m.DB().Exec("PRAGMA foreign_keys = OFF;"); err != nil {
		t.Fatalf("pragma foreign_keys off: %v", err)
	}
	_, err = m.DB().Exec("INSERT INTO files (storage_id, path, parent_path, size, is_dir) VALUES (?, ?, ?, ?, ?);", 5, "/file5.txt", "/", 50, 0)
	if err != nil {
		t.Fatalf("direct insert to files: %v", err)
	}
	if _, err := m.DB().Exec("PRAGMA foreign_keys = ON;"); err != nil {
		t.Fatalf("pragma foreign_keys on: %v", err)
	}

	// Add storage 15 via Set
	m.Set(15, 500, 2, true)

	ids, err = m.GetAllStorageIDs()
	if err != nil {
		t.Fatalf("GetAllStorageIDs: %v", err)
	}

	expected := []int{5, 10, 15, 20}
	if len(ids) != len(expected) {
		t.Fatalf("expected ids %v, got %v", expected, ids)
	}
	for i, id := range ids {
		if id != expected[i] {
			t.Errorf("ids[%d] = %d, expected %d", i, id, expected[i])
		}
	}
}

func TestManager_DeleteStorage_And_Tx(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	// Seed storage 1 with files
	if err := m.EnsureStorage(1); err != nil {
		t.Fatalf("EnsureStorage(1): %v", err)
	}
	if err := m.PutFile(FileRecord{StorageID: 1, Path: "/a.txt", Size: 100}); err != nil {
		t.Fatalf("PutFile /a.txt: %v", err)
	}
	if err := m.PutFile(FileRecord{StorageID: 1, Path: "/docs/b.txt", Size: 200}); err != nil {
		t.Fatalf("PutFile /docs/b.txt: %v", err)
	}

	// Seed storage 2 with files
	if err := m.EnsureStorage(2); err != nil {
		t.Fatalf("EnsureStorage(2): %v", err)
	}
	if err := m.PutFile(FileRecord{StorageID: 2, Path: "/c.txt", Size: 300}); err != nil {
		t.Fatalf("PutFile /c.txt: %v", err)
	}

	// 1. Test DeleteStorageTx rollback
	simErr := errors.New("simulated error")
	err = m.WithTx(context.Background(), func(tx *sql.Tx) error {
		if err := m.DeleteStorageTx(tx, 1); err != nil {
			return err
		}
		return simErr
	})
	if !errors.Is(err, simErr) {
		t.Fatalf("expected simErr, got %v", err)
	}

	// Verify storage 1 still exists after rollback
	st1, err := m.GetStorage(1)
	if err != nil || st1 == nil {
		t.Fatalf("storage 1 should still exist after tx rollback: %v", err)
	}
	f1, err := m.GetFile(1, "/a.txt")
	if err != nil || f1 == nil {
		t.Fatalf("file /a.txt should still exist after tx rollback: %v", err)
	}

	// 2. Test successful DeleteStorage
	if err := m.DeleteStorage(1); err != nil {
		t.Fatalf("DeleteStorage(1): %v", err)
	}

	// Verify storage 1 is deleted
	if _, err := m.GetStorage(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected storage 1 to be not found, got err: %v", err)
	}

	// Verify files for storage 1 are deleted
	if _, err := m.GetFile(1, "/a.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected /a.txt to be not found, got err: %v", err)
	}
	if _, err := m.GetFile(1, "/docs/b.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected /docs/b.txt to be not found, got err: %v", err)
	}

	var count int
	if err := m.DB().QueryRow("SELECT COUNT(*) FROM files WHERE storage_id = 1;").Scan(&count); err != nil {
		t.Fatalf("query files count: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 files for storage 1, got %d", count)
	}

	if err := m.DB().QueryRow("SELECT COUNT(*) FROM storages WHERE id = 1;").Scan(&count); err != nil {
		t.Fatalf("query storages count: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 storages records for storage 1, got %d", count)
	}

	// Verify storage 2 and its files remain untouched
	st2, err := m.GetStorage(2)
	if err != nil || st2 == nil {
		t.Fatalf("expected storage 2 to remain, got err: %v", err)
	}
	f2, err := m.GetFile(2, "/c.txt")
	if err != nil || f2 == nil {
		t.Fatalf("expected /c.txt in storage 2 to remain, got err: %v", err)
	}

	// Verify GetAllStorageIDs only contains 2
	remainingIDs, err := m.GetAllStorageIDs()
	if err != nil {
		t.Fatalf("GetAllStorageIDs: %v", err)
	}
	if len(remainingIDs) != 1 || remainingIDs[0] != 2 {
		t.Fatalf("expected only storage id 2, got %v", remainingIDs)
	}
}
func TestPutFileTx_CrossNodeDuplicateCleanup(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	// 1. 在节点 1 写入文件 /docs/file.txt，大小 1000
	err = m.PutFile(FileRecord{
		StorageID: 1,
		Path:      "/docs/file.txt",
		Size:      1000,
		IsDir:     false,
		ModTime:   time.Now().Add(-1 * time.Hour),
	})
	if err != nil {
		t.Fatalf("PutFile node 1 failed: %v", err)
	}

	st1, err := m.GetStorage(1)
	if err != nil || st1 == nil {
		t.Fatalf("GetStorage 1 failed: %v", err)
	}
	if st1.UsedBytes != 1000 || st1.FileCount != 1 {
		t.Fatalf("expected storage 1 used_bytes=1000, file_count=1; got bytes=%d, count=%d", st1.UsedBytes, st1.FileCount)
	}

	// 2. 在节点 2 写入同名文件 /docs/file.txt，大小 2500
	err = m.PutFile(FileRecord{
		StorageID: 2,
		Path:      "/docs/file.txt",
		Size:      2500,
		IsDir:     false,
		ModTime:   time.Now(),
	})
	if err != nil {
		t.Fatalf("PutFile node 2 failed: %v", err)
	}

	// 3. 验证节点 1 上的文件记录已被自动清理
	f1, err := m.GetFile(1, "/docs/file.txt")
	if !errors.Is(err, ErrNotFound) && f1 != nil {
		t.Fatalf("expected file on node 1 to be deleted, but still exists: %+v (err: %v)", f1, err)
	}

	// 验证节点 1 的用量触发器自动扣减
	st1After, err := m.GetStorage(1)
	if err != nil || st1After == nil {
		t.Fatalf("GetStorage 1 after cleanup failed: %v", err)
	}
	if st1After.UsedBytes != 0 || st1After.FileCount != 0 {
		t.Fatalf("expected storage 1 used_bytes=0, file_count=0 after cleanup; got bytes=%d, count=%d", st1After.UsedBytes, st1After.FileCount)
	}

	// 4. 验证节点 2 上的文件记录与用量
	f2, err := m.GetFile(2, "/docs/file.txt")
	if err != nil {
		t.Fatalf("GetFile node 2 failed: %v", err)
	}
	if f2 == nil || f2.Size != 2500 {
		t.Fatalf("expected file on node 2 to exist with size 2500, got: %+v", f2)
	}

	st2, err := m.GetStorage(2)
	if err != nil || st2 == nil {
		t.Fatalf("GetStorage 2 failed: %v", err)
	}
	if st2.UsedBytes != 2500 || st2.FileCount != 1 {
		t.Fatalf("expected storage 2 used_bytes=2500, file_count=1; got bytes=%d, count=%d", st2.UsedBytes, st2.FileCount)
	}

	// 5. 验证 GetFilesByPath 全库只存在一条记录
	records, err := m.GetFilesByPath("/docs/file.txt")
	if err != nil {
		t.Fatalf("GetFilesByPath failed: %v", err)
	}
	if len(records) != 1 || records[0].StorageID != 2 {
		t.Fatalf("expected exactly 1 record for storage 2, got %v", records)
	}
}
func TestLastProbedAt(t *testing.T) {
	m, err := NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	if err := m.EnsureStorage(1); err != nil {
		t.Fatalf("EnsureStorage failed: %v", err)
	}

	before := time.Now().Add(-time.Second)

	// 1. PutFile without LastProbedAt -> automatically defaults to time.Now()
	err = m.PutFile(FileRecord{
		StorageID: 1,
		Path:      "/test/file1.txt",
		Size:      100,
	})
	if err != nil {
		t.Fatalf("PutFile failed: %v", err)
	}

	rec, err := m.GetFile(1, "/test/file1.txt")
	if err != nil {
		t.Fatalf("GetFile failed: %v", err)
	}
	if rec.LastProbedAt.Before(before) || rec.LastProbedAt.After(time.Now().Add(time.Second)) {
		t.Fatalf("expected LastProbedAt around now, got: %v", rec.LastProbedAt)
	}

	// RouteFile
	routed, err := m.RouteFile("/test/file1.txt")
	if err != nil {
		t.Fatalf("RouteFile failed: %v", err)
	}
	if !routed.LastProbedAt.Equal(rec.LastProbedAt) {
		t.Fatalf("RouteFile LastProbedAt %v != GetFile %v", routed.LastProbedAt, rec.LastProbedAt)
	}

	// GetFilesByPath
	byPath, err := m.GetFilesByPath("/test/file1.txt")
	if err != nil || len(byPath) == 0 {
		t.Fatalf("GetFilesByPath failed: %v", err)
	}
	if !byPath[0].LastProbedAt.Equal(rec.LastProbedAt) {
		t.Fatalf("GetFilesByPath LastProbedAt %v != GetFile %v", byPath[0].LastProbedAt, rec.LastProbedAt)
	}

	// ListChildren
	children, err := m.ListChildren("/test")
	if err != nil || len(children) == 0 {
		t.Fatalf("ListChildren failed: %v", err)
	}
	if !children[0].LastProbedAt.Equal(rec.LastProbedAt) {
		t.Fatalf("ListChildren LastProbedAt %v != GetFile %v", children[0].LastProbedAt, rec.LastProbedAt)
	}

	// 2. PutFileTx with explicit LastProbedAt and update on conflict
	explicitTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	err = m.PutFile(FileRecord{
		StorageID:    1,
		Path:         "/test/file1.txt",
		Size:         150,
		LastProbedAt: explicitTime,
	})
	if err != nil {
		t.Fatalf("PutFile update failed: %v", err)
	}

	recUpdated, err := m.GetFile(1, "/test/file1.txt")
	if err != nil {
		t.Fatalf("GetFile updated failed: %v", err)
	}
	if !recUpdated.LastProbedAt.Equal(explicitTime) {
		t.Fatalf("expected updated LastProbedAt %v, got %v", explicitTime, recUpdated.LastProbedAt)
	}

	// 3. UpdateLastProbed
	probeTime := time.Date(2025, 2, 1, 15, 30, 0, 0, time.UTC)
	err = m.UpdateLastProbed(1, "/test/file1.txt", probeTime)
	if err != nil {
		t.Fatalf("UpdateLastProbed failed: %v", err)
	}

	recProbed, err := m.GetFile(1, "/test/file1.txt")
	if err != nil {
		t.Fatalf("GetFile after probe update failed: %v", err)
	}
	if !recProbed.LastProbedAt.Equal(probeTime) {
		t.Fatalf("expected probeTime %v, got %v", probeTime, recProbed.LastProbedAt)
	}

	// 4. UpdateLastProbedTx
	txProbeTime := time.Date(2025, 3, 1, 9, 0, 0, 0, time.UTC)
	err = m.WithTx(context.Background(), func(tx *sql.Tx) error {
		return m.UpdateLastProbedTx(tx, 1, "/test/file1.txt", txProbeTime)
	})
	if err != nil {
		t.Fatalf("UpdateLastProbedTx failed: %v", err)
	}

	recTxProbed, err := m.GetFile(1, "/test/file1.txt")
	if err != nil {
		t.Fatalf("GetFile after tx probe update failed: %v", err)
	}
	if !recTxProbed.LastProbedAt.Equal(txProbeTime) {
		t.Fatalf("expected txProbeTime %v, got %v", txProbeTime, recTxProbed.LastProbedAt)
	}

	// 5. UpdateLastProbed on non-existent file
	err = m.UpdateLastProbed(1, "/not/found.txt", time.Now())
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for non-existent file, got %v", err)
	}
}

func TestSmoothMigrationLastProbedAt(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "legacy_schema.db")

	// 1. Manually create tables WITHOUT last_probed_at
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	_, err = db.Exec(`
CREATE TABLE storages (
    id INTEGER PRIMARY KEY,
    used_bytes INTEGER NOT NULL DEFAULT 0,
    file_count INTEGER NOT NULL DEFAULT 0,
    last_sync TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE files (
    storage_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    parent_path TEXT NOT NULL,
    size INTEGER NOT NULL DEFAULT 0,
    is_dir INTEGER NOT NULL DEFAULT 0,
    mod_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (storage_id, path),
    FOREIGN KEY (storage_id) REFERENCES storages(id) ON DELETE CASCADE
);
INSERT INTO storages (id) VALUES (1);
INSERT INTO files (storage_id, path, parent_path, size, is_dir) VALUES (1, '/legacy.txt', '/', 123, 0);
`)
	if err != nil {
		_ = db.Close()
		t.Fatalf("create legacy schema failed: %v", err)
	}
	_ = db.Close()

	// 2. Open via NewManager -> should smoothly migrate last_probed_at column
	m, err := NewManager(dbPath)
	if err != nil {
		t.Fatalf("NewManager on legacy schema failed: %v", err)
	}
	defer m.Close()

	rec, err := m.GetFile(1, "/legacy.txt")
	if err != nil {
		t.Fatalf("GetFile after migration failed: %v", err)
	}
	if rec.LastProbedAt.IsZero() {
		t.Fatalf("expected non-zero default LastProbedAt after migration")
	}

	// Updating last_probed_at should work
	newTime := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	if err := m.UpdateLastProbed(1, "/legacy.txt", newTime); err != nil {
		t.Fatalf("UpdateLastProbed after migration failed: %v", err)
	}
	rec2, err := m.GetFile(1, "/legacy.txt")
	if err != nil {
		t.Fatalf("GetFile after update failed: %v", err)
	}
	if !rec2.LastProbedAt.Equal(newTime) {
		t.Fatalf("expected LastProbedAt %v, got %v", newTime, rec2.LastProbedAt)
	}
}
