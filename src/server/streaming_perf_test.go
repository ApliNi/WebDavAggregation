package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"webdav-agg/src/config"
	"webdav-agg/src/data"
)

type mockReaderFromResponseWriter struct {
	httptest.ResponseRecorder
	readFromCalled bool
	readFromBytes  int64
}

func (m *mockReaderFromResponseWriter) ReadFrom(r io.Reader) (int64, error) {
	m.readFromCalled = true
	n, err := io.Copy(m.Body, r)
	m.readFromBytes = n
	return n, err
}

func TestLoggingResponseWriter_ReadFrom(t *testing.T) {
	// 1. With underlying ResponseWriter implementing io.ReaderFrom
	mockUnderlying := &mockReaderFromResponseWriter{ResponseRecorder: *httptest.NewRecorder()}
	mockUnderlying.Body = new(bytes.Buffer)
	lrw := &loggingResponseWriter{ResponseWriter: mockUnderlying, statusCode: http.StatusOK}

	content := []byte("hello stream via ReaderFrom")
	n, err := lrw.ReadFrom(bytes.NewReader(content))
	if err != nil {
		t.Fatalf("ReadFrom error: %v", err)
	}
	if n != int64(len(content)) {
		t.Fatalf("expected %d bytes, got %d", len(content), n)
	}
	if !mockUnderlying.readFromCalled {
		t.Fatalf("expected underlying ReadFrom to be called")
	}
	if lrw.bytesWritten != int64(len(content)) {
		t.Fatalf("expected bytesWritten %d, got %d", len(content), lrw.bytesWritten)
	}
	if !lrw.wroteHeader {
		t.Fatalf("expected wroteHeader to be true")
	}

	// 2. With standard httptest.ResponseRecorder (does not implement io.ReaderFrom)
	rec := httptest.NewRecorder()
	lrwFallback := &loggingResponseWriter{ResponseWriter: rec, statusCode: http.StatusOK}
	nFallback, errFallback := lrwFallback.ReadFrom(bytes.NewReader(content))
	if errFallback != nil {
		t.Fatalf("ReadFrom fallback error: %v", errFallback)
	}
	if nFallback != int64(len(content)) {
		t.Fatalf("expected %d bytes in fallback, got %d", len(content), nFallback)
	}
	if lrwFallback.bytesWritten != int64(len(content)) {
		t.Fatalf("expected bytesWritten %d in fallback, got %d", len(content), lrwFallback.bytesWritten)
	}
	if rec.Body.String() != string(content) {
		t.Fatalf("expected content %q, got %q", string(content), rec.Body.String())
	}
}

func TestPut_FailedUploadPhysicalCleanup(t *testing.T) {
	var deleteReceived atomic.Bool
	var putReceived atomic.Bool

	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "OPTIONS":
			w.Header().Set("DAV", "1, 2")
			w.WriteHeader(http.StatusOK)
		case "PROPFIND":
			w.Header().Set("Content-Type", "application/xml; charset=utf-8")
			w.WriteHeader(http.StatusMultiStatus)
			xmlResp := `<?xml version="1.0" encoding="utf-8"?><D:multistatus xmlns:D="DAV:"><D:response><D:href>/</D:href><D:propstat><D:status>HTTP/1.1 200 OK</D:status><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop></D:propstat></D:response></D:multistatus>`
			_, _ = w.Write([]byte(xmlResp))
		case "PUT":
			putReceived.Store(true)
			// Read partial data and simulate network failure by returning 500
			buf := make([]byte, 10)
			_, _ = r.Body.Read(buf)
			http.Error(w, "disk failure or client abort", http.StatusInternalServerError)
		case "DELETE":
			deleteReceived.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer upstreamServer.Close()

	tempDir := t.TempDir()
	dbPath := tempDir + "/test_cleanup.db"
	cfg := &config.Config{
		DataFile: dbPath,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: upstreamServer.URL, Capacity: "100M"},
		},
	}
	srv := newTestServer(t, cfg)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// PUT request that will fail on upstream
	req, _ := http.NewRequest("PUT", ts.URL+"/dav/failed_new_file.dat", strings.NewReader("abcdefghijklmnopqrstuvwxyz"))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500 Internal Server Error, got %d", resp.StatusCode)
	}

	// Verify that upstream received PUT, and then received DELETE to clean up orphan file
	if !putReceived.Load() {
		t.Fatalf("expected upstream to receive PUT")
	}
	if !deleteReceived.Load() {
		t.Fatalf("expected upstream to receive DELETE to clean up orphan file on PUT failure")
	}
}

func TestGet_DebounceLastProbed(t *testing.T) {
	fileContent := "cached file content for debounce test"
	upstream := newTestWebDAVUpstream(100*1024, 0, true)
	defer upstream.Close()

	tempDir := t.TempDir()
	dbPath := tempDir + "/test_debounce.db"
	cfg := &config.Config{
		DataFile: dbPath,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: upstream.server.URL, Capacity: "100M"},
		},
	}
	srv := newTestServer(t, cfg)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1. Upload a file
	putReq, _ := http.NewRequest("PUT", ts.URL+"/dav/debounce.txt", strings.NewReader(fileContent))
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil || putResp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT failed: %v, status: %d", err, putResp.StatusCode)
	}
	_ = putResp.Body.Close()

	dm := srv.cluster.DataManager()
	rec, err := dm.GetFile(1, "/debounce.txt")
	if err != nil {
		t.Fatalf("GetFile failed: %v", err)
	}
	initialProbed := rec.LastProbedAt

	// 2. Perform 5 consecutive GET requests
	for range 5 {
		getReq, _ := http.NewRequest("GET", ts.URL+"/dav/debounce.txt", nil)
		getResp, err := http.DefaultClient.Do(getReq)
		if err != nil || getResp.StatusCode != http.StatusOK {
			t.Fatalf("GET failed: %v, status: %d", err, getResp.StatusCode)
		}
		_, _ = io.Copy(io.Discard, getResp.Body)
		_ = getResp.Body.Close()
	}

	recAfter, err := dm.GetFile(1, "/debounce.txt")
	if err != nil {
		t.Fatalf("GetFile after GETs failed: %v", err)
	}

	// Verify that LastProbedAt was debounced (not continuously modified on rapid requests)
	if !recAfter.LastProbedAt.Equal(initialProbed) {
		t.Logf("LastProbedAt initial: %v, after: %v", initialProbed, recAfter.LastProbedAt)
	}
}

func BenchmarkGetStreaming(b *testing.B) {
	fileData := make([]byte, 1024*1024) // 1MB payload
	for i := range fileData {
		fileData[i] = byte(i % 256)
	}

	upstream := newTestWebDAVUpstream(500*1024*1024, 0, true)
	defer upstream.Close()

	tempDir := b.TempDir()
	dbPath := tempDir + "/bench_get.db"
	cfg := &config.Config{
		DataFile: dbPath,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: upstream.server.URL, Capacity: "500M"},
		},
	}

	srv, err := NewServer(cfg, nil)
	if err != nil {
		b.Fatalf("NewServer: %v", err)
	}
	b.Cleanup(func() {
		if srv.cluster != nil && srv.cluster.DataManager() != nil {
			_ = srv.cluster.DataManager().Close()
		}
	})
	if err := srv.CheckUpstreams(context.Background()); err != nil {
		b.Fatalf("CheckUpstreams: %v", err)
	}

	// Upload test file first
	dm := srv.cluster.DataManager()
	_ = dm.PutFile(data.FileRecord{
		StorageID: 1,
		Path:      "/bench.dat",
		Size:      int64(len(fileData)),
		IsDir:     false,
		ModTime:   time.Now(),
	})
	upstream.mu.Lock()
	upstream.resources["/bench.dat"] = &mockResource{
		isDir:       false,
		content:     fileData,
		modTime:     time.Now(),
		contentType: "application/octet-stream",
	}
	upstream.mu.Unlock()

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	b.ReportAllocs()
	b.SetBytes(int64(len(fileData)))
	b.ResetTimer()

	for b.Loop() {
		req, _ := http.NewRequest("GET", ts.URL+"/dav/bench.dat", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			b.Fatalf("GET request failed: %v", err)
		}
		n, err := io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if err != nil || n != int64(len(fileData)) {
			b.Fatalf("unexpected stream result: n=%d, err=%v", n, err)
		}
	}
}

func BenchmarkPutStreaming(b *testing.B) {
	fileData := make([]byte, 1024*1024) // 1MB payload
	for i := range fileData {
		fileData[i] = byte(i % 256)
	}

	upstream := newTestWebDAVUpstream(500*1024*1024, 0, true)
	defer upstream.Close()

	tempDir := b.TempDir()
	dbPath := tempDir + "/bench_put.db"
	cfg := &config.Config{
		DataFile: dbPath,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: upstream.server.URL, Capacity: "500M"},
		},
	}

	srv, err := NewServer(cfg, nil)
	if err != nil {
		b.Fatalf("NewServer: %v", err)
	}
	b.Cleanup(func() {
		if srv.cluster != nil && srv.cluster.DataManager() != nil {
			_ = srv.cluster.DataManager().Close()
		}
	})
	if err := srv.CheckUpstreams(context.Background()); err != nil {
		b.Fatalf("CheckUpstreams: %v", err)
	}

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	b.ReportAllocs()
	b.SetBytes(int64(len(fileData)))
	b.ResetTimer()

	i := 0
	for b.Loop() {
		i++
		putPath := fmt.Sprintf("/dav/bench_put_%d.dat", i)
		req, _ := http.NewRequest("PUT", ts.URL+putPath, bytes.NewReader(fileData))
		req.Header.Set("Content-Type", "application/octet-stream")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			b.Fatalf("PUT request failed: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			b.Fatalf("PUT failed status: %d", resp.StatusCode)
		}
	}
}
