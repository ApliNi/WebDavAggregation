package cluster

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"webdav-agg/src/config"
	"webdav-agg/src/data"
)

func TestCluster_SQLite_ZeroDelayStartup_And_Sync(t *testing.T) {
	var propfindCount atomic.Int32

	mockUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PROPFIND" {
			propfindCount.Add(1)
			p := r.URL.Path
			w.Header().Set("Content-Type", "application/xml; charset=utf-8")
			w.WriteHeader(http.StatusMultiStatus)

			switch p {
			case "/", "":
				_, _ = w.Write([]byte(`<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/</D:href>
    <D:propstat>
      <D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/docs</D:href>
    <D:propstat>
      <D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/root.txt</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype/>
        <D:getcontentlength>500</D:getcontentlength>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>`))
			case "/docs", "/docs/":
				_, _ = w.Write([]byte(`<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/docs</D:href>
    <D:propstat>
      <D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/docs/manual.pdf</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype/>
        <D:getcontentlength>1500</D:getcontentlength>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer mockUpstream.Close()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "data.db")

	dm1, err := data.NewManager(dbPath)
	if err != nil {
		t.Fatalf("NewManager 1 failed: %v", err)
	}

	cfg1 := &config.Config{
		Upstream: []config.UpstreamConfig{
			{
				ID:       1,
				URL:      mockUpstream.URL,
				Capacity: "10G",
			},
		},
	}

	c1, err := NewCluster(cfg1, dm1)
	if err != nil {
		t.Fatalf("NewCluster 1 failed: %v", err)
	}

	// 1. First encounter: ID 1 is not yet in SQLite storages table with last_sync.
	// InitData MUST walk the upstream, convert to FileRecords, and batch insert to SQLite.
	if err := c1.InitData(context.Background()); err != nil {
		t.Fatalf("InitData 1 failed: %v", err)
	}

	if propfindCount.Load() == 0 {
		t.Fatalf("expected remote PROPFIND walk on first boot, got 0")
	}

	n1, ok := c1.GetNode(1)
	if !ok {
		t.Fatalf("GetNode(1) not found")
	}
	_, used1, _ := n1.Quota()
	if used1 != 2000 || n1.Files() != 2 {
		t.Errorf("expected node 1 used=2000, files=2, got used=%d, files=%d", used1, n1.Files())
	}

	// Verify SQLite files table contains the files
	st1, err := dm1.GetStorage(1)
	if err != nil || st1 == nil {
		t.Fatalf("GetStorage 1 failed: %v", err)
	}
	if st1.UsedBytes != 2000 || st1.FileCount != 2 {
		t.Errorf("SQLite storage record mismatch: used=%d, count=%d", st1.UsedBytes, st1.FileCount)
	}
	if st1.LastSync == nil || st1.LastSync.IsZero() {
		t.Errorf("expected LastSync to be set in SQLite storages table")
	}

	// Verify SQLite files table has the records
	fManual, err := dm1.GetFile(1, "/docs/manual.pdf")
	if err != nil || fManual.Size != 1500 {
		t.Errorf("expected /docs/manual.pdf in files table with size 1500, got %+v, err: %v", fManual, err)
	}
	fRoot, err := dm1.GetFile(1, "/root.txt")
	if err != nil || fRoot.Size != 500 {
		t.Errorf("expected /root.txt in files table with size 500, got %+v, err: %v", fRoot, err)
	}

	dm1.Close()

	// 2. Second encounter (Service Restart): Zero-delay startup
	propfindCount.Store(0)

	dm2, err := data.NewManager(dbPath)
	if err != nil {
		t.Fatalf("NewManager 2 failed: %v", err)
	}
	defer dm2.Close()

	c2, err := NewCluster(cfg1, dm2)
	if err != nil {
		t.Fatalf("NewCluster 2 failed: %v", err)
	}

	if err := c2.InitData(context.Background()); err != nil {
		t.Fatalf("InitData 2 failed: %v", err)
	}

	// MUST be 0 PROPFIND requests because SQLite already has LastSync != nil
	if propfindCount.Load() != 0 {
		t.Errorf("expected 0 PROPFIND requests on second boot, got %d", propfindCount.Load())
	}

	n2, _ := c2.GetNode(1)
	_, used2, _ := n2.Quota()
	if used2 != 2000 || n2.Files() != 2 {
		t.Errorf("expected node 2 used=2000, files=2 from SQLite cache, got used=%d, files=%d", used2, n2.Files())
	}
}

func TestCluster_SQLite_RecordOperations(t *testing.T) {
	dm, err := data.NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer dm.Close()

	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: "http://127.0.0.1:5243", Capacity: "1000B"},
		},
	}

	c, err := NewCluster(cfg, dm)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	n, _ := c.GetNode(1)

	// 1. Path-based RecordWrite: new file
	c.RecordWrite(n, 200, true, "/folder/file.txt")
	avail, used, _ := n.Quota()
	if used != 200 || avail != 800 || n.Files() != 1 {
		t.Errorf("in-memory quota mismatch after RecordWrite: used=%d avail=%d files=%d", used, avail, n.Files())
	}

	// Verify in SQLite
	f, err := dm.GetFile(1, "/folder/file.txt")
	if err != nil || f.Size != 200 {
		t.Errorf("SQLite GetFile failed: %+v, err: %v", f, err)
	}
	st, _ := dm.GetStorage(1)
	if st.UsedBytes != 200 || st.FileCount != 1 {
		t.Errorf("SQLite storage stats mismatch: %+v", st)
	}

	// 2. Path-based RecordOverwrite
	c.RecordOverwrite(n, 50, "/folder/file.txt")
	_, used, _ = n.Quota()
	if used != 250 {
		t.Errorf("in-memory used mismatch after Overwrite: %d", used)
	}
	f, _ = dm.GetFile(1, "/folder/file.txt")
	if f.Size != 250 {
		t.Errorf("SQLite file size mismatch after Overwrite: %d", f.Size)
	}
	st, _ = dm.GetStorage(1)
	if st.UsedBytes != 250 {
		t.Errorf("SQLite used_bytes mismatch after Overwrite: %d", st.UsedBytes)
	}

	// 3. RecordMovePath
	if err := c.RecordMovePath(n, "/folder/file.txt", "/folder/moved.txt"); err != nil {
		t.Fatalf("RecordMovePath failed: %v", err)
	}
	fMoved, err := dm.GetFile(1, "/folder/moved.txt")
	if err != nil || fMoved.Size != 250 {
		t.Errorf("SQLite moved file mismatch: %+v, err: %v", fMoved, err)
	}

	// 4. Path-based RecordDelete
	c.RecordDelete(n, 250, "/folder/moved.txt")
	_, used, _ = n.Quota()
	if used != 0 || n.Files() != 0 {
		t.Errorf("in-memory quota mismatch after Delete: used=%d files=%d", used, n.Files())
	}
	st, _ = dm.GetStorage(1)
	if st.UsedBytes != 0 || st.FileCount != 0 {
		t.Errorf("SQLite storage stats mismatch after Delete: %+v", st)
	}

	// 5. Usage-only RecordWrite / RecordDelete without path
	c.RecordWrite(n, 300, true)
	_, used, _ = n.Quota()
	if used != 300 || n.Files() != 1 {
		t.Errorf("in-memory usage-only write mismatch: used=%d files=%d", used, n.Files())
	}
	st, _ = dm.GetStorage(1)
	if st.UsedBytes != 300 || st.FileCount != 1 {
		t.Errorf("SQLite storage stats mismatch after usage-only write: %+v", st)
	}

	c.RecordDelete(n, 300)
	_, used, _ = n.Quota()
	if used != 0 || n.Files() != 0 {
		t.Errorf("in-memory usage-only delete mismatch: used=%d files=%d", used, n.Files())
	}
	st, _ = dm.GetStorage(1)
	if st.UsedBytes != 0 || st.FileCount != 0 {
		t.Errorf("SQLite storage stats mismatch after usage-only delete: %+v", st)
	}
}
func TestCluster_SQLite_CleanupObsoleteStorageIDs(t *testing.T) {
	mockUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PROPFIND" {
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
  <D:response>
    <D:href>/node1_file.txt</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype/>
        <D:getcontentlength>1024</D:getcontentlength>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer mockUpstream.Close()

	dm, err := data.NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer dm.Close()

	// Pre-insert obsolete node 999 storage and files records
	if err := dm.EnsureStorage(999); err != nil {
		t.Fatalf("EnsureStorage(999): %v", err)
	}
	if err := dm.PutFile(data.FileRecord{StorageID: 999, Path: "/obsolete/file.txt", Size: 500}); err != nil {
		t.Fatalf("PutFile: %v", err)
	}
	if err := dm.PutFile(data.FileRecord{StorageID: 999, Path: "/obsolete_root.txt", Size: 300}); err != nil {
		t.Fatalf("PutFile: %v", err)
	}

	// Verify ID 999 is present initially
	preIDs, err := dm.GetAllStorageIDs()
	if err != nil {
		t.Fatalf("GetAllStorageIDs: %v", err)
	}
	has999 := false
	for _, id := range preIDs {
		if id == 999 {
			has999 = true
			break
		}
	}
	if !has999 {
		t.Fatalf("expected storage id 999 to be present initially")
	}

	// Cluster is configured with only node 1
	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{
				ID:       1,
				URL:      mockUpstream.URL,
				Capacity: "10G",
			},
		},
	}

	c, err := NewCluster(cfg, dm)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	// Execute InitData
	if err := c.InitData(context.Background()); err != nil {
		t.Fatalf("InitData failed: %v", err)
	}

	// Assert ID 999 was completely cleaned up from storages and files
	if _, err := dm.GetStorage(999); !errors.Is(err, data.ErrNotFound) {
		t.Errorf("expected storage 999 to be deleted, got err: %v", err)
	}
	if _, err := dm.GetFile(999, "/obsolete/file.txt"); !errors.Is(err, data.ErrNotFound) {
		t.Errorf("expected file /obsolete/file.txt to be deleted, got err: %v", err)
	}
	if _, err := dm.GetFile(999, "/obsolete_root.txt"); !errors.Is(err, data.ErrNotFound) {
		t.Errorf("expected file /obsolete_root.txt to be deleted, got err: %v", err)
	}

	var obsoleteFileCount int
	if err := dm.DB().QueryRow("SELECT COUNT(*) FROM files WHERE storage_id = 999;").Scan(&obsoleteFileCount); err != nil {
		t.Fatalf("query obsolete files count: %v", err)
	}
	if obsoleteFileCount != 0 {
		t.Errorf("expected 0 files for storage 999, got %d", obsoleteFileCount)
	}

	var obsoleteStorageCount int
	if err := dm.DB().QueryRow("SELECT COUNT(*) FROM storages WHERE id = 999;").Scan(&obsoleteStorageCount); err != nil {
		t.Fatalf("query obsolete storages count: %v", err)
	}
	if obsoleteStorageCount != 0 {
		t.Errorf("expected 0 storages records for 999, got %d", obsoleteStorageCount)
	}

	// Assert 999 is no longer in GetAllStorageIDs
	postIDs, err := dm.GetAllStorageIDs()
	if err != nil {
		t.Fatalf("GetAllStorageIDs: %v", err)
	}
	for _, id := range postIDs {
		if id == 999 {
			t.Errorf("found obsolete id 999 in post-InitData GetAllStorageIDs: %v", postIDs)
		}
	}

	// Assert node 1 was processed normally
	n1, ok := c.GetNode(1)
	if !ok || n1 == nil {
		t.Fatalf("expected node 1 to exist in cluster")
	}
	st1, err := dm.GetStorage(1)
	if err != nil || st1 == nil {
		t.Fatalf("expected storage record for node 1: %v", err)
	}
	if st1.LastSync == nil {
		t.Errorf("expected node 1 LastSync to be set")
	}
	if st1.UsedBytes != 1024 || st1.FileCount != 1 {
		t.Errorf("expected node 1 stats used=1024, files=1; got used=%d, files=%d", st1.UsedBytes, st1.FileCount)
	}
	_, used1, _ := n1.Quota()
	if used1 != 1024 || n1.Files() != 1 {
		t.Errorf("expected in-memory node 1 quota used=1024, files=1; got used=%d, files=%d", used1, n1.Files())
	}
}
func TestCluster_SQLite_CleanupObsoleteStorageIDs_Error(t *testing.T) {
	dm, err := data.NewManager(":memory:")
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	if err := dm.EnsureStorage(999); err != nil {
		t.Fatalf("EnsureStorage(999): %v", err)
	}

	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{
				ID:       1,
				URL:      "http://127.0.0.1:5243",
				Capacity: "10G",
			},
		},
	}

	c, err := NewCluster(cfg, dm)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	_ = dm.Close()

	err = c.InitData(context.Background())
	if err == nil {
		t.Fatalf("expected InitData to fail when DB is closed")
	}
}
