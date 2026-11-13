package cluster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"webdav-agg/src/config"
	"webdav-agg/src/data"
)

func TestCluster_InitData_FirstEncounterVsPersisted(t *testing.T) {
	var propfindCount atomic.Int32
	mockUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PROPFIND" {
			propfindCount.Add(1)
			w.Header().Set("Content-Type", "application/xml; charset=utf-8")
			w.WriteHeader(http.StatusMultiStatus)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/></D:resourcetype>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
  <D:response>
    <D:href>/hello.txt</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype/>
        <D:getcontentlength>123</D:getcontentlength>
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

	tempDir := t.TempDir()
	dataFile := filepath.Join(tempDir, "data.yml")

	dm1, err := data.NewManager(dataFile)
	if err != nil {
		t.Fatalf("NewManager error: %v", err)
	}

	cfg1 := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: mockUpstream.URL, Capacity: "100M"},
		},
	}

	c1, err := NewCluster(cfg1, dm1)
	if err != nil {
		t.Fatalf("NewCluster 1 error: %v", err)
	}

	// 1. First encounter: ID 1 is not in data.yml, InitData MUST walk the upstream
	if err := c1.InitData(context.Background()); err != nil {
		t.Fatalf("InitData 1 error: %v", err)
	}

	if propfindCount.Load() == 0 {
		t.Error("expected PROPFIND requests to walk remote storage on first encounter")
	}

	n1, _ := c1.GetNode(1)
	_, used1, _ := n1.Quota()
	if used1 != 123 || n1.Files() != 1 {
		t.Errorf("expected node 1 used=123, files=1, got used=%d, files=%d", used1, n1.Files())
	}

	// Verify data.yml persisted
	entry, ok := dm1.Get(1)
	if !ok || entry.UsedBytes != 123 || entry.FileCount != 1 {
		t.Errorf("expected persisted used=123, files=1, got %+v", entry)
	}
	dm1.Close()

	// 2. Second encounter: reload cluster with existing data.yml
	// Reset propfind counter to 0
	propfindCount.Store(0)

	dm2, err := data.NewManager(dataFile)
	if err != nil {
		t.Fatalf("NewManager 2 error: %v", err)
	}
	defer dm2.Close()

	c2, err := NewCluster(cfg1, dm2)
	if err != nil {
		t.Fatalf("NewCluster 2 error: %v", err)
	}

	if err := c2.InitData(context.Background()); err != nil {
		t.Fatalf("InitData 2 error: %v", err)
	}

	// Should NOT have walked the upstream because node 1 exists in data.yml
	if propfindCount.Load() != 0 {
		t.Errorf("expected 0 PROPFIND requests since node 1 was already in data.yml, got %d", propfindCount.Load())
	}

	n2, _ := c2.GetNode(1)
	_, used2, _ := n2.Quota()
	if used2 != 123 || n2.Files() != 1 {
		t.Errorf("expected node 1 used=123, files=1 from data.yml, got used=%d, files=%d", used2, n2.Files())
	}

	_ = os.Remove(dataFile)
}
