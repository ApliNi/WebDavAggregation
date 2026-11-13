package server

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"webdav-agg/src/config"
)

func TestConditionalGetHead304(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "100M"},
		},
	}
	srv := newTestServer(t, cfg)

	// 1. Upload a file
	content := []byte("hello conditional world 12345")
	req := httptest.NewRequest("PUT", "/dav/conditional.txt", bytes.NewReader(content))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("PUT failed: %d", w.Code)
	}

	// 2. GET to retrieve ETag and Last-Modified
	req = httptest.NewRequest("GET", "/dav/conditional.txt", nil)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET failed: %d", w.Code)
	}
	etag := w.Header().Get("ETag")
	lastMod := w.Header().Get("Last-Modified")
	if etag == "" {
		t.Fatalf("expected non-empty ETag")
	}

	// 3. GET with matching If-None-Match -> expect 304 Not Modified
	req = httptest.NewRequest("GET", "/dav/conditional.txt", nil)
	req.Header.Set("If-None-Match", etag)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNotModified {
		t.Fatalf("expected 304 for matching If-None-Match, got %d", w.Code)
	}
	if w.Body.Len() > 0 {
		t.Fatalf("expected empty body for 304, got %d bytes", w.Body.Len())
	}
	if w.Header().Get("ETag") == "" {
		t.Fatalf("expected ETag header in 304 response")
	}

	// 4. GET with matching If-Modified-Since -> expect 304 Not Modified
	req = httptest.NewRequest("GET", "/dav/conditional.txt", nil)
	req.Header.Set("If-Modified-Since", lastMod)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNotModified {
		t.Fatalf("expected 304 for matching If-Modified-Since, got %d", w.Code)
	}
	if w.Body.Len() > 0 {
		t.Fatalf("expected empty body for 304, got %d bytes", w.Body.Len())
	}
}

func TestRangeNotSatisfiable416(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "100M"},
		},
	}
	srv := newTestServer(t, cfg)

	content := []byte("short content")
	req := httptest.NewRequest("PUT", "/dav/range_test.txt", bytes.NewReader(content))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("PUT failed: %d", w.Code)
	}

	// Request Range beyond EOF (start = 99999 > len(content))
	req = httptest.NewRequest("GET", "/dav/range_test.txt", nil)
	req.Header.Set("Range", "bytes=99999-")
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("expected 416 Range Not Satisfiable, got %d", w.Code)
	}
	expectedCR := fmt.Sprintf("bytes */%d", len(content))
	if cr := w.Header().Get("Content-Range"); cr != expectedCR {
		t.Fatalf("expected Content-Range %q, got %q", expectedCR, cr)
	}
}

func TestMoveFallbackCopyDelete(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "100M"},
		},
	}
	srv := newTestServer(t, cfg)

	node, ok := srv.cluster.GetNode(1)
	if !ok || node == nil {
		t.Fatalf("failed to get node 1")
	}

	content := []byte("move fallback content")
	req := httptest.NewRequest("PUT", "/dav/src_file.txt", bytes.NewReader(content))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("PUT failed: %d", w.Code)
	}

	// Inject custom HTTP client to upstream node that fails on MOVE with 501
	origClient := node.Client
	customHTTP := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method == "MOVE" {
				return &http.Response{
					StatusCode: http.StatusNotImplemented,
					Status:     "501 Not Implemented",
					Body:       io.NopCloser(strings.NewReader("native move not supported")),
					Header:     make(http.Header),
				}, nil
			}
			return http.DefaultTransport.RoundTrip(r)
		}),
	}
	origClient.SetHTTPClient(customHTTP)

	// Issue MOVE request -> should fallback to GET+PUT+DELETE and succeed with 201 Created
	req = httptest.NewRequest("MOVE", "/dav/src_file.txt", nil)
	req.Header.Set("Destination", "/dav/dest_file.txt")
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created from fallback MOVE, got %d (body: %s)", w.Code, w.Body.String())
	}

	// Verify dest_file exists and has correct content
	req = httptest.NewRequest("GET", "/dav/dest_file.txt", nil)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET dest_file failed: %d", w.Code)
	}
	if w.Body.String() != string(content) {
		t.Fatalf("expected %q, got %q", string(content), w.Body.String())
	}

	// Verify src_file is gone
	req = httptest.NewRequest("GET", "/dav/src_file.txt", nil)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for deleted src_file, got %d", w.Code)
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestProppatchRFC4918(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "100M"},
		},
	}
	srv := newTestServer(t, cfg)

	content := []byte("proppatch test file")
	req := httptest.NewRequest("PUT", "/dav/prop_file.txt", bytes.NewReader(content))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("PUT failed: %d", w.Code)
	}

	t.Run("Protected property returns 403 and other 424", func(t *testing.T) {
		xmlBody := `<?xml version="1.0" encoding="utf-8" ?>
<D:propertyupdate xmlns:D="DAV:" xmlns:custom="http://example.com/ns">
  <D:set>
    <D:prop>
      <D:getcontentlength>12345</D:getcontentlength>
      <custom:author>Alice</custom:author>
    </D:prop>
  </D:set>
</D:propertyupdate>`

		req := httptest.NewRequest("PROPPATCH", "/dav/prop_file.txt", strings.NewReader(xmlBody))
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, req)

		if w.Code != http.StatusMultiStatus {
			t.Fatalf("expected 207 Multi-Status, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, "403 Forbidden") {
			t.Fatalf("expected 403 Forbidden in body, got: %s", body)
		}
		if !strings.Contains(body, "cannot modify protected property") {
			t.Fatalf("expected cannot modify protected property tag, got: %s", body)
		}
		if !strings.Contains(body, "424 Failed Dependency") {
			t.Fatalf("expected 424 Failed Dependency for custom author, got: %s", body)
		}
	})

	t.Run("Custom properties return 200 OK", func(t *testing.T) {
		xmlBody := `<?xml version="1.0" encoding="utf-8" ?>
<D:propertyupdate xmlns:D="DAV:" xmlns:custom="http://example.com/ns">
  <D:set>
    <D:prop>
      <custom:tag>val1</custom:tag>
      <custom:rating>5</custom:rating>
    </D:prop>
  </D:set>
</D:propertyupdate>`

		req := httptest.NewRequest("PROPPATCH", "/dav/prop_file.txt", strings.NewReader(xmlBody))
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, req)

		if w.Code != http.StatusMultiStatus {
			t.Fatalf("expected 207 Multi-Status, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, "200 OK") {
			t.Fatalf("expected 200 OK in body, got: %s", body)
		}
		if strings.Contains(body, "403 Forbidden") || strings.Contains(body, "424") {
			t.Fatalf("unexpected failure status in body: %s", body)
		}
	})
}

func TestMkcolBroadcastPrewarming(t *testing.T) {
	up1 := newTestWebDAVUpstream(1000, 100, true)
	defer up1.Close()
	up2 := newTestWebDAVUpstream(1000, 100, true)
	defer up2.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up1.server.URL, Capacity: "100M"},
			{ID: 2, URL: up2.server.URL, Capacity: "100M"},
		},
	}
	srv := newTestServer(t, cfg)

	// Send MKCOL to create /dav/prewarm_coll/
	req := httptest.NewRequest("MKCOL", "/dav/prewarm_coll/", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created from MKCOL, got %d", w.Code)
	}

	// Verify both upstreams received the directory creation (allow async goroutine to complete)
	deadline := time.Now().Add(2 * time.Second)
	up2Has := false
	for time.Now().Before(deadline) {
		up2.mu.Lock()
		_, up2Has = up2.resources["/prewarm_coll"]
		up2.mu.Unlock()
		if up2Has {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	up1.mu.Lock()
	_, up1Has := up1.resources["/prewarm_coll"]
	up1.mu.Unlock()

	if !up1Has || !up2Has {
		t.Fatalf("expected directory to be broadcast to all nodes: up1=%v, up2=%v", up1Has, up2Has)
	}
}
func TestIfMatchPreconditionFailed412(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "100M"},
		},
	}
	srv := newTestServer(t, cfg)

	content := []byte("content for if-match test")
	req := httptest.NewRequest("PUT", "/dav/ifmatch.txt", bytes.NewReader(content))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("PUT failed: %d", w.Code)
	}

	req = httptest.NewRequest("HEAD", "/dav/ifmatch.txt", nil)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	etag := w.Header().Get("ETag")
	if etag == "" {
		t.Fatalf("expected non-empty ETag")
	}

	// 1. GET with mismatched If-Match -> expect 412
	req = httptest.NewRequest("GET", "/dav/ifmatch.txt", nil)
	req.Header.Set("If-Match", `"invalid-etag"`)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusPreconditionFailed {
		t.Errorf("expected 412 for mismatched If-Match on GET, got %d", w.Code)
	}

	// 2. HEAD with mismatched If-Match -> expect 412
	req = httptest.NewRequest("HEAD", "/dav/ifmatch.txt", nil)
	req.Header.Set("If-Match", `"invalid-etag"`)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusPreconditionFailed {
		t.Errorf("expected 412 for mismatched If-Match on HEAD, got %d", w.Code)
	}

	// 3. GET with matching If-Match -> expect 200 OK
	req = httptest.NewRequest("GET", "/dav/ifmatch.txt", nil)
	req.Header.Set("If-Match", etag)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for matching If-Match on GET, got %d", w.Code)
	}

	// 4. GET with wildcard If-Match: * -> expect 200 OK
	req = httptest.NewRequest("GET", "/dav/ifmatch.txt", nil)
	req.Header.Set("If-Match", "*")
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for wildcard If-Match on GET, got %d", w.Code)
	}

	// 5. GET with weak tag -> strong comparison fails, expect 412
	req = httptest.NewRequest("GET", "/dav/ifmatch.txt", nil)
	req.Header.Set("If-Match", "W/"+etag)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusPreconditionFailed {
		t.Errorf("expected 412 for weak If-Match on GET, got %d", w.Code)
	}

	// 6. GET with If-Unmodified-Since in the past -> expect 412
	req = httptest.NewRequest("GET", "/dav/ifmatch.txt", nil)
	pastDate := time.Now().Add(-24 * time.Hour).UTC().Format(http.TimeFormat)
	req.Header.Set("If-Unmodified-Since", pastDate)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusPreconditionFailed {
		t.Errorf("expected 412 for past If-Unmodified-Since on GET, got %d", w.Code)
	}
}

func TestETagConsistencyHeadGetPropfind(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "100M"},
		},
	}
	srv := newTestServer(t, cfg)

	content := []byte("consistent etag test payload")
	req := httptest.NewRequest("PUT", "/dav/etag_test.txt", bytes.NewReader(content))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("PUT failed: %d", w.Code)
	}

	// HEAD
	req = httptest.NewRequest("HEAD", "/dav/etag_test.txt", nil)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	headETag := w.Header().Get("ETag")

	// GET
	req = httptest.NewRequest("GET", "/dav/etag_test.txt", nil)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	getETag := w.Header().Get("ETag")

	// PROPFIND
	req = httptest.NewRequest("PROPFIND", "/dav/etag_test.txt", nil)
	req.Header.Set("Depth", "0")
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	propfindBody := w.Body.String()

	if headETag == "" || getETag == "" {
		t.Fatalf("empty ETag from HEAD (%q) or GET (%q)", headETag, getETag)
	}
	if headETag != getETag {
		t.Fatalf("ETag mismatch between HEAD (%s) and GET (%s)", headETag, getETag)
	}
	expectedPropSnippet := fmt.Sprintf("<D:getetag>%s</D:getetag>", headETag)
	if !strings.Contains(propfindBody, expectedPropSnippet) {
		t.Fatalf("PROPFIND did not contain consistent ETag %s, got body: %s", headETag, propfindBody)
	}
}

func TestCascadingDeleteBottomUp(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "100M"},
		},
	}
	srv := newTestServer(t, cfg)

	// Build directory tree: /tree/sub1/sub2/file.txt
	for _, p := range []string{"/dav/tree/", "/dav/tree/sub1/", "/dav/tree/sub1/sub2/"} {
		req := httptest.NewRequest("MKCOL", p, nil)
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, req)
		if w.Code != http.StatusCreated {
			t.Fatalf("MKCOL %s failed: %d", p, w.Code)
		}
	}

	content := []byte("nested file data")
	req := httptest.NewRequest("PUT", "/dav/tree/sub1/sub2/file.txt", bytes.NewReader(content))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("PUT nested file failed: %d", w.Code)
	}

	// Issue DELETE on root collection /dav/tree
	req = httptest.NewRequest("DELETE", "/dav/tree", nil)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("DELETE collection failed: %d, body: %s", w.Code, w.Body.String())
	}

	// Verify all subpaths return 404
	for _, checkPath := range []string{"/dav/tree", "/dav/tree/sub1", "/dav/tree/sub1/sub2/file.txt"} {
		req = httptest.NewRequest("PROPFIND", checkPath, nil)
		w = httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, req)
		if w.Code != http.StatusNotFound {
			t.Errorf("expected 404 for %s after delete, got %d", checkPath, w.Code)
		}
	}
}

func TestCopyFallbackAndDestinationNormalization(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "100M"},
		},
	}
	srv := newTestServer(t, cfg)

	content := []byte("copy test payload")
	req := httptest.NewRequest("PUT", "/dav/src_copy.txt", bytes.NewReader(content))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("PUT failed: %d", w.Code)
	}

	// 1. Destination parent does not exist -> expect 409 Conflict
	req = httptest.NewRequest("COPY", "/dav/src_copy.txt", nil)
	req.Header.Set("Destination", "/dav/nonexistent_folder/dest.txt")
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Errorf("expected 409 Conflict when destination parent does not exist, got %d", w.Code)
	}

	// 2. Destination with reverse proxy mount prefix (/dav/WebDavAggregation/dst_copy.txt) -> normalized and succeeds
	req = httptest.NewRequest("COPY", "/dav/src_copy.txt", nil)
	req.Header.Set("Destination", "http://127.0.0.1:5244/dav/WebDavAggregation/dst_copy.txt")
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created for COPY with proxy prefix, got %d (body: %s)", w.Code, w.Body.String())
	}

	// Verify dest content matches
	req = httptest.NewRequest("GET", "/dav/dst_copy.txt", nil)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), content) {
		t.Fatalf("GET copied file mismatch: status=%d, content=%q", w.Code, w.Body.String())
	}
}
