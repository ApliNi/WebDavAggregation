package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"webdav-agg/src/client"
	"webdav-agg/src/config"
	"webdav-agg/src/data"
)

type mockResource struct {
	isDir       bool
	content     []byte
	modTime     time.Time
	contentType string
}

type testWebDAVUpstream struct {
	server       *httptest.Server
	mu           sync.Mutex
	resources    map[string]*mockResource
	reqCounts    map[string]int
	availBytes   int64
	usedBytes    int64
	supportQuota bool
	deletedPaths []string
	ignoreRange  bool
	failPutPath        string
	failDeletePath     string
	failPropfindDepth1 bool
}

func newTestWebDAVUpstream(avail, used int64, supportQuota bool) *testWebDAVUpstream {
	u := &testWebDAVUpstream{
		resources:    make(map[string]*mockResource),
		reqCounts:    make(map[string]int),
		availBytes:   avail,
		usedBytes:    used,
		supportQuota: supportQuota,
	}
	u.resources["/"] = &mockResource{isDir: true, modTime: time.Now()}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.mu.Lock()
		defer u.mu.Unlock()
		u.reqCounts[r.Method]++

		davPath := path.Clean(r.URL.Path)
		if !strings.HasPrefix(davPath, "/") {
			davPath = "/" + davPath
		}

		switch r.Method {
		case "OPTIONS":
			w.Header().Set("DAV", "1, 2")
			w.Header().Set("Allow", "OPTIONS, GET, HEAD, PUT, DELETE, MKCOL, MOVE, COPY, PROPFIND, PROPPATCH")
			w.WriteHeader(http.StatusOK)

		case "PROPFIND":
			depth := r.Header.Get("Depth")
			if depth == "" {
				depth = "0"
			}
			if u.failPropfindDepth1 && depth == "1" {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			bodyBytes, _ := io.ReadAll(r.Body)
			bodyStr := string(bodyBytes)

			// Quota PROPFIND
			if strings.Contains(bodyStr, "quota-available-bytes") {
				if !u.supportQuota {
					w.WriteHeader(http.StatusMultiStatus)
					_, _ = w.Write([]byte(`<?xml version="1.0" encoding="utf-8"?><D:multistatus xmlns:D="DAV:"></D:multistatus>`))
					return
				}
				w.Header().Set("Content-Type", "application/xml; charset=utf-8")
				w.WriteHeader(http.StatusMultiStatus)
				xmlResp := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/</D:href>
    <D:propstat>
      <D:status>HTTP/1.1 200 OK</D:status>
      <D:prop>
        <D:quota-available-bytes>%d</D:quota-available-bytes>
        <D:quota-used-bytes>%d</D:quota-used-bytes>
      </D:prop>
    </D:propstat>
  </D:response>
</D:multistatus>`, u.availBytes, u.usedBytes)
				_, _ = w.Write([]byte(xmlResp))
				return
			}

			// Resource listing
			res, exists := u.resources[davPath]
			if !exists {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			w.Header().Set("Content-Type", "application/xml; charset=utf-8")
			w.WriteHeader(http.StatusMultiStatus)

			var buf bytes.Buffer
			buf.WriteString(`<?xml version="1.0" encoding="utf-8"?><D:multistatus xmlns:D="DAV:">`)

			// Self entry
			href := davPath
			if res.isDir && href != "/" && !strings.HasSuffix(href, "/") {
				href += "/"
			}
			buf.WriteString(fmt.Sprintf(`
<D:response>
  <D:href>%s</D:href>
  <D:propstat>
    <D:status>HTTP/1.1 200 OK</D:status>
    <D:prop>`, href))
			if res.isDir {
				buf.WriteString(`<D:resourcetype><D:collection/></D:resourcetype>`)
			} else {
				buf.WriteString(fmt.Sprintf(`<D:resourcetype/><D:getcontentlength>%d</D:getcontentlength>`, len(res.content)))
			}
			buf.WriteString(fmt.Sprintf(`<D:getlastmodified>%s</D:getlastmodified>`, res.modTime.UTC().Format(http.TimeFormat)))
			buf.WriteString(fmt.Sprintf(`<D:displayname>%s</D:displayname></D:prop></D:propstat></D:response>`, path.Base(davPath)))

			// Child entries if Depth 1
			if depth == "1" && res.isDir {
				for itemPath, itemRes := range u.resources {
					if itemPath == davPath {
						continue
					}
					if path.Dir(itemPath) == davPath {
						itemHref := itemPath
						if itemRes.isDir && !strings.HasSuffix(itemHref, "/") {
							itemHref += "/"
						}
						buf.WriteString(fmt.Sprintf(`
<D:response>
  <D:href>%s</D:href>
  <D:propstat>
    <D:status>HTTP/1.1 200 OK</D:status>
    <D:prop>`, itemHref))
						if itemRes.isDir {
							buf.WriteString(`<D:resourcetype><D:collection/></D:resourcetype>`)
						} else {
							buf.WriteString(fmt.Sprintf(`<D:resourcetype/><D:getcontentlength>%d</D:getcontentlength>`, len(itemRes.content)))
						}
						buf.WriteString(fmt.Sprintf(`<D:getlastmodified>%s</D:getlastmodified>`, itemRes.modTime.UTC().Format(http.TimeFormat)))
						buf.WriteString(fmt.Sprintf(`<D:displayname>%s</D:displayname></D:prop></D:propstat></D:response>`, path.Base(itemPath)))
					}
				}
			}

			buf.WriteString(`</D:multistatus>`)
			_, _ = w.Write(buf.Bytes())

		case "GET":
			res, exists := u.resources[davPath]
			if !exists || res.isDir {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Type", "application/octet-stream")
			if res.contentType != "" {
				w.Header().Set("Content-Type", res.contentType)
			}
			w.Header().Set("Last-Modified", res.modTime.UTC().Format(http.TimeFormat))

			// Handle Range header
			rangeHeader := r.Header.Get("Range")
			if !u.ignoreRange && rangeHeader != "" && strings.HasPrefix(rangeHeader, "bytes=") {
				spec := strings.TrimPrefix(rangeHeader, "bytes=")
				parts := strings.Split(spec, "-")
				start, _ := strconv.Atoi(parts[0])
				end := len(res.content) - 1
				if len(parts) > 1 && parts[1] != "" {
					end, _ = strconv.Atoi(parts[1])
				}
				if start > end || start >= len(res.content) {
					w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(res.content)))
					w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
					return
				}
				if end >= len(res.content) {
					end = len(res.content) - 1
				}

				part := res.content[start : end+1]
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(res.content)))
				w.Header().Set("Content-Length", strconv.Itoa(len(part)))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(part)
				return
			}

			w.Header().Set("Content-Length", strconv.Itoa(len(res.content)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(res.content)

		case "HEAD":
			res, exists := u.resources[davPath]
			if !exists || res.isDir {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Length", strconv.Itoa(len(res.content)))
			w.Header().Set("Last-Modified", res.modTime.UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)

		case "PUT":
			if u.failPutPath != "" && davPath == u.failPutPath {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			data, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, existed := u.resources[davPath]
			u.resources[davPath] = &mockResource{
				isDir:       false,
				content:     data,
				modTime:     time.Now(),
				contentType: r.Header.Get("Content-Type"),
			}
			// Ensure parent dirs
			parent := path.Dir(davPath)
			for parent != "/" && parent != "." && parent != "" {
				if _, ok := u.resources[parent]; !ok {
					u.resources[parent] = &mockResource{isDir: true, modTime: time.Now()}
				}
				parent = path.Dir(parent)
			}
			if existed {
				w.WriteHeader(http.StatusNoContent)
			} else {
				w.WriteHeader(http.StatusCreated)
			}

		case "MKCOL":
			if _, exists := u.resources[davPath]; exists {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			u.resources[davPath] = &mockResource{isDir: true, modTime: time.Now()}
			w.WriteHeader(http.StatusCreated)

		case "DELETE":
			if u.failDeletePath != "" && (davPath == u.failDeletePath || strings.HasPrefix(davPath, u.failDeletePath)) {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			deleted := false
			for p := range u.resources {
				if p == davPath || strings.HasPrefix(p, davPath+"/") {
					delete(u.resources, p)
					deleted = true
				}
			}
			if deleted {
				u.deletedPaths = append(u.deletedPaths, davPath)
				w.WriteHeader(http.StatusNoContent)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}

		case "MOVE":
			dest := r.Header.Get("Destination")
			destURL, _ := url.Parse(dest)
			destPath := path.Clean("/" + destURL.Path)

			res, ok := u.resources[davPath]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, destExisted := u.resources[destPath]
			u.resources[destPath] = res
			delete(u.resources, davPath)

			if res.isDir {
				prefix := davPath + "/"
				for k, v := range u.resources {
					if strings.HasPrefix(k, prefix) {
						rel := strings.TrimPrefix(k, prefix)
						newK := path.Clean(destPath + "/" + rel)
						u.resources[newK] = v
						delete(u.resources, k)
					}
				}
			}

			if destExisted {
				w.WriteHeader(http.StatusNoContent)
			} else {
				w.WriteHeader(http.StatusCreated)
			}

		case "COPY":
			dest := r.Header.Get("Destination")
			destURL, _ := url.Parse(dest)
			destPath := path.Clean("/" + destURL.Path)

			res, ok := u.resources[davPath]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, destExisted := u.resources[destPath]
			cpy := *res
			u.resources[destPath] = &cpy

			if res.isDir {
				prefix := davPath + "/"
				for k, v := range u.resources {
					if strings.HasPrefix(k, prefix) {
						rel := strings.TrimPrefix(k, prefix)
						newK := path.Clean(destPath + "/" + rel)
						childCpy := *v
						u.resources[newK] = &childCpy
					}
				}
			}

			if destExisted {
				w.WriteHeader(http.StatusNoContent)
			} else {
				w.WriteHeader(http.StatusCreated)
			}
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	u.server = httptest.NewServer(handler)
	return u
}

func (u *testWebDAVUpstream) setFile(davPath string, content []byte, modTime time.Time) {
	u.mu.Lock()
	defer u.mu.Unlock()
	davPath = path.Clean("/" + davPath)
	u.resources[davPath] = &mockResource{
		isDir:   false,
		content: content,
		modTime: modTime,
	}
	parent := path.Dir(davPath)
	for parent != "/" && parent != "." && parent != "" {
		if _, ok := u.resources[parent]; !ok {
			u.resources[parent] = &mockResource{isDir: true, modTime: modTime}
		}
		parent = path.Dir(parent)
	}
}

func (u *testWebDAVUpstream) setDir(davPath string, modTime time.Time) {
	u.mu.Lock()
	defer u.mu.Unlock()
	davPath = path.Clean("/" + davPath)
	u.resources[davPath] = &mockResource{
		isDir:   true,
		modTime: modTime,
	}
}

func (u *testWebDAVUpstream) hasFile(davPath string) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	davPath = path.Clean("/" + davPath)
	res, ok := u.resources[davPath]
	return ok && !res.isDir
}

func (u *testWebDAVUpstream) wasDeleted(davPath string) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	davPath = path.Clean("/" + davPath)
	for _, p := range u.deletedPaths {
		if p == davPath {
			return true
		}
	}
	return false
}

func (u *testWebDAVUpstream) methodCount(method string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.reqCounts[method]
}

func (u *testWebDAVUpstream) resetCounts() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.reqCounts = make(map[string]int)
}

func (u *testWebDAVUpstream) Close() {
	u.server.Close()
}

func newTestServer(t *testing.T, cfg *config.Config) *Server {
	t.Helper()
	for i := range cfg.Upstream {
		if strings.TrimSpace(cfg.Upstream[i].Capacity) == "" {
			cfg.Upstream[i].Capacity = "100M"
		}
	}
	srv, err := NewServer(cfg, nil)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}
	t.Cleanup(func() {
		if srv.cluster != nil {
			if dm := srv.cluster.DataManager(); dm != nil {
				_ = dm.Close()
			}
		}
	})
	if err := srv.CheckUpstreams(context.Background()); err != nil {
		t.Fatalf("CheckUpstreams error: %v", err)
	}
	if err := srv.InitData(context.Background()); err != nil {
		t.Fatalf("InitData error: %v", err)
	}
	return srv
}
func TestOptions(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	req := httptest.NewRequest("OPTIONS", "/dav", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	davHeader := resp.Header.Get("DAV")
	if !strings.Contains(davHeader, "1") || !strings.Contains(davHeader, "2") {
		t.Errorf("expected DAV 1, 2, got %q", davHeader)
	}

	allow := resp.Header.Get("Allow")
	for _, m := range []string{"OPTIONS", "GET", "HEAD", "PUT", "DELETE", "MKCOL", "MOVE", "COPY", "PROPFIND", "LOCK", "UNLOCK"} {
		if !strings.Contains(allow, m) {
			t.Errorf("expected Allow to contain %s, got %q", m, allow)
		}
	}
}

func TestPropfindAggregationAndQuota(t *testing.T) {
	// Node 1: avail 1000, used 200. Files: /dir1/file1.txt
	up1 := newTestWebDAVUpstream(1000, 200, true)
	defer up1.Close()
	up1.setFile("/dir1/file1.txt", bytes.Repeat([]byte("A"), 200), time.Now())

	// Node 2: avail 2000, used 300. Files: /dir1/file2.txt, /dir2/file3.txt
	up2 := newTestWebDAVUpstream(2000, 300, true)
	defer up2.Close()
	up2.setFile("/dir1/file2.txt", bytes.Repeat([]byte("B"), 150), time.Now())
	up2.setFile("/dir2/file3.txt", bytes.Repeat([]byte("C"), 150), time.Now())

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up1.server.URL, Capacity: "1200B"},
			{ID: 2, URL: up2.server.URL, Capacity: "2300B"},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	c, err := client.NewClient(testServer.URL+"/dav", "", "", "", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}
	// 1. Check aggregate Quota (RFC 4331)
	quota, err := c.GetQuota(context.Background())
	if err != nil {
		t.Fatalf("GetQuota error: %v", err)
	}
	if quota.AvailableBytes != 3000 {
		t.Errorf("expected AvailableBytes 3000, got %d", quota.AvailableBytes)
	}
	if quota.UsedBytes != 500 {
		t.Errorf("expected UsedBytes 500, got %d", quota.UsedBytes)
	}

	// 2. PROPFIND Depth 1 on "/"
	rootItems, err := c.List(context.Background(), "/", 1)
	if err != nil {
		t.Fatalf("List / error: %v", err)
	}

	// Expect root self entry and child dirs /dir1, /dir2
	paths := make(map[string]bool)
	for _, it := range rootItems {
		paths[it.Path] = true
	}
	if !paths["/dir1"] {
		t.Errorf("expected /dir1 in root items, got %+v", paths)
	}
	if !paths["/dir2"] {
		t.Errorf("expected /dir2 in root items, got %+v", paths)
	}

	// 3. PROPFIND Depth 1 on "/dir1" (merges items across both upstreams)
	dir1Items, err := c.List(context.Background(), "/dir1", 1)
	if err != nil {
		t.Fatalf("List /dir1 error: %v", err)
	}

	dir1Paths := make(map[string]bool)
	for _, it := range dir1Items {
		dir1Paths[it.Path] = true
	}
	if !dir1Paths["/dir1/file1.txt"] {
		t.Errorf("expected /dir1/file1.txt in dir1 items, got %+v", dir1Paths)
	}
	if !dir1Paths["/dir1/file2.txt"] {
		t.Errorf("expected /dir1/file2.txt in dir1 items, got %+v", dir1Paths)
	}
}

func TestDuplicateFilesOldVersionDeleted(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-2 * time.Hour)
	newTime := now.Add(-10 * time.Minute)

	up1 := newTestWebDAVUpstream(1000, 100, true)
	defer up1.Close()
	up1.setFile("/shared/file.txt", []byte("old-content"), oldTime)

	up2 := newTestWebDAVUpstream(1000, 100, true)
	defer up2.Close()
	up2.setFile("/shared/file.txt", []byte("new-content"), newTime)

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up1.server.URL},
			{ID: 2, URL: up2.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	c, err := client.NewClient(testServer.URL+"/dav", "", "", "", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}

	// PROPFIND on /shared should trigger deduplication:
	// Older file on up1 must be deleted, newer file on up2 must be retained.
	items, err := c.List(context.Background(), "/shared", 1)
	if err != nil {
		t.Fatalf("List /shared error: %v", err)
	}

	fileCount := 0
	for _, it := range items {
		if it.Path == "/shared/file.txt" {
			fileCount++
			if it.ModTime.Before(oldTime.Add(time.Minute)) {
				t.Errorf("expected retained file to have newer ModTime, got %v", it.ModTime)
			}
		}
	}
	if fileCount != 1 {
		t.Errorf("expected exactly 1 /shared/file.txt, got %d", fileCount)
	}

	// Verify up1 had its old file deleted
	if !up1.wasDeleted("/shared/file.txt") {
		t.Errorf("expected up1 to have received DELETE for /shared/file.txt")
	}

	// Verify GET returns the newer content
	rc, _, err := c.Get(context.Background(), "/shared/file.txt", nil)
	if err != nil {
		t.Fatalf("Get /shared/file.txt error: %v", err)
	}
	defer rc.Close()

	content, _ := io.ReadAll(rc)
	if string(content) != "new-content" {
		t.Errorf("expected content 'new-content', got %q", string(content))
	}
}

func TestPutRouteToMostFreeSpaceNode(t *testing.T) {
	up1 := newTestWebDAVUpstream(500, 100, true) // avail 500
	defer up1.Close()

	up2 := newTestWebDAVUpstream(2000, 100, true) // avail 2000 (largest)
	defer up2.Close()

	up3 := newTestWebDAVUpstream(1000, 100, true) // avail 1000
	defer up3.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up1.server.URL, Capacity: "500B"},
			{ID: 2, URL: up2.server.URL, Capacity: "2000B"},
			{ID: 3, URL: up3.server.URL, Capacity: "1000B"},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	c, err := client.NewClient(testServer.URL+"/dav", "", "", "", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}

	// 1. Upload a 300-byte new file
	data := bytes.Repeat([]byte("A"), 300)
	err = c.Put(context.Background(), "/test/route.txt", bytes.NewReader(data), int64(len(data)), "text/plain")
	if err != nil {
		t.Fatalf("Put /test/route.txt error: %v", err)
	}

	// Verify it was routed to up2 (max free space)
	if !up2.hasFile("/test/route.txt") {
		t.Errorf("expected file to be written to up2 (avail 2000), but not found there")
	}
	if up1.hasFile("/test/route.txt") || up3.hasFile("/test/route.txt") {
		t.Errorf("file should only be written to up2")
	}

	// 2. Overwrite existing file: should directly overwrite up2, not migrate
	data2 := bytes.Repeat([]byte("B"), 400)
	err = c.Put(context.Background(), "/test/route.txt", bytes.NewReader(data2), int64(len(data2)), "text/plain")
	if err != nil {
		t.Fatalf("Overwrite Put error: %v", err)
	}
	if !up2.hasFile("/test/route.txt") {
		t.Errorf("expected overwritten file to stay on up2")
	}

	// 3. Test HTTP 507 Insufficient Storage when file exceeds all nodes' capacities
	largeData := bytes.Repeat([]byte("C"), 50000)
	err = c.Put(context.Background(), "/huge.bin", bytes.NewReader(largeData), int64(len(largeData)), "application/octet-stream")
	if err == nil {
		t.Fatalf("expected 507 Insufficient Storage error for 50000 bytes, got nil")
	}
}

func TestGetAndRangeStreamDownload(t *testing.T) {
	up := newTestWebDAVUpstream(5000, 100, true)
	defer up.Close()

	content := []byte("abcdefghijklmnopqrstuvwxyz")
	up.setFile("/alpha.txt", content, time.Now())

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	// 1. Full GET
	resp, err := http.Get(testServer.URL + "/dav/alpha.txt")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(body, content) {
		t.Errorf("expected %q, got %q", content, body)
	}

	// 2. Range GET (HTTP 206 Partial Content)
	req, _ := http.NewRequest("GET", testServer.URL+"/dav/alpha.txt", nil)
	req.Header.Set("Range", "bytes=0-4")
	rangeResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Range request error: %v", err)
	}
	defer rangeResp.Body.Close()

	if rangeResp.StatusCode != http.StatusPartialContent {
		t.Errorf("expected 206 Partial Content, got %d", rangeResp.StatusCode)
	}
	rangeBody, _ := io.ReadAll(rangeResp.Body)
	if string(rangeBody) != "abcde" {
		t.Errorf("expected 'abcde', got %q", string(rangeBody))
	}
	if rangeResp.Header.Get("Content-Range") != "bytes 0-4/26" {
		t.Errorf("expected Content-Range 'bytes 0-4/26', got %q", rangeResp.Header.Get("Content-Range"))
	}

	// 3. HEAD request
	headResp, err := http.Head(testServer.URL + "/dav/alpha.txt")
	if err != nil {
		t.Fatalf("Head error: %v", err)
	}
	defer headResp.Body.Close()

	if headResp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", headResp.StatusCode)
	}
	if headResp.Header.Get("Content-Length") != "26" {
		t.Errorf("expected Content-Length 26, got %s", headResp.Header.Get("Content-Length"))
	}
}

func TestDeleteBroadcast(t *testing.T) {
	// Virtual directory /vdir exists on both up1 and up2
	up1 := newTestWebDAVUpstream(1000, 100, true)
	defer up1.Close()
	up1.setFile("/vdir/f1.txt", []byte("f1"), time.Now())

	up2 := newTestWebDAVUpstream(1000, 100, true)
	defer up2.Close()
	up2.setFile("/vdir/f2.txt", []byte("f2"), time.Now())

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up1.server.URL},
			{ID: 2, URL: up2.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	c, err := client.NewClient(testServer.URL+"/dav", "", "", "", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}

	// Broadcast DELETE /vdir
	err = c.Delete(context.Background(), "/vdir")
	if err != nil {
		t.Fatalf("Delete /vdir error: %v", err)
	}

	// Verify both upstreams received delete for /vdir
	if !up1.wasDeleted("/vdir") {
		t.Errorf("expected up1 to receive delete for /vdir")
	}
	if !up2.wasDeleted("/vdir") {
		t.Errorf("expected up2 to receive delete for /vdir")
	}

	// DELETE on nonexistent resource should return 404
	err = c.Delete(context.Background(), "/nonexistent")
	if err == nil {
		t.Fatalf("expected error on deleting nonexistent file, got nil")
	}
}

func TestBasicAuth(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Auth: config.AuthConfig{
			User: "admin",
			Pass: "pass123",
		},
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	// 1. Without credentials -> 401 Unauthorized
	req, _ := http.NewRequest("OPTIONS", testServer.URL+"/dav/", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Request error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %d", resp.StatusCode)
	}
	if !strings.Contains(resp.Header.Get("WWW-Authenticate"), "Basic") {
		t.Errorf("expected WWW-Authenticate header, got %q", resp.Header.Get("WWW-Authenticate"))
	}

	// 2. Wrong credentials -> 401 Unauthorized
	req, _ = http.NewRequest("OPTIONS", testServer.URL+"/dav/", nil)
	req.SetBasicAuth("admin", "wrong")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Request error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for wrong password, got %d", resp.StatusCode)
	}

	// 3. Correct credentials -> 200 OK
	req, _ = http.NewRequest("OPTIONS", testServer.URL+"/dav/", nil)
	req.SetBasicAuth("admin", "pass123")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Request error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for correct auth, got %d", resp.StatusCode)
	}
}

func TestLockAndUnlock(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()
	up.setFile("/document.docx", []byte("initial text"), time.Now())

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	// 1. Send LOCK request
	lockXML := `<?xml version="1.0" encoding="utf-8" ?>
<D:lockinfo xmlns:D='DAV:'>
  <D:lockscope><D:exclusive/></D:lockscope>
  <D:locktype><D:write/></D:locktype>
  <D:owner><D:href>user1</D:href></D:owner>
</D:lockinfo>`

	req, _ := http.NewRequest("LOCK", testServer.URL+"/dav/document.docx", strings.NewReader(lockXML))
	req.Header.Set("Timeout", "Second-3600")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("LOCK request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on LOCK, got %d", resp.StatusCode)
	}

	lockTokenHeader := resp.Header.Get("Lock-Token")
	if !strings.HasPrefix(lockTokenHeader, "<urn:uuid:") {
		t.Fatalf("invalid Lock-Token header: %q", lockTokenHeader)
	}
	token := strings.Trim(lockTokenHeader, "<>")

	bodyBytes, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(bodyBytes), "activelock") || !strings.Contains(string(bodyBytes), token) {
		t.Errorf("LOCK response body missing lock info: %s", string(bodyBytes))
	}

	// 2. PUT without token -> 423 Locked
	req, _ = http.NewRequest("PUT", testServer.URL+"/dav/document.docx", strings.NewReader("attempt"))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT request error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusLocked {
		t.Errorf("expected 423 Locked without token, got %d", resp.StatusCode)
	}

	// 3. PUT with matching If header -> Success
	req, _ = http.NewRequest("PUT", testServer.URL+"/dav/document.docx", strings.NewReader("updated text"))
	req.Header.Set("If", fmt.Sprintf("(<%s>)", token))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT with If error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		t.Errorf("expected 200/204 on PUT with lock token, got %d", resp.StatusCode)
	}

	// 4. Lock Refresh with empty body and token
	req, _ = http.NewRequest("LOCK", testServer.URL+"/dav/document.docx", nil)
	req.Header.Set("If", fmt.Sprintf("(<%s>)", token))
	req.Header.Set("Timeout", "Second-1800")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Lock refresh error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK on lock refresh, got %d", resp.StatusCode)
	}

	// 5. UNLOCK
	req, _ = http.NewRequest("UNLOCK", testServer.URL+"/dav/document.docx", nil)
	req.Header.Set("Lock-Token", lockTokenHeader)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("UNLOCK error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("expected 204 No Content on UNLOCK, got %d", resp.StatusCode)
	}

	// 6. PUT after UNLOCK succeeds without token
	req, _ = http.NewRequest("PUT", testServer.URL+"/dav/document.docx", strings.NewReader("post-unlock text"))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT after unlock error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		t.Errorf("expected success after unlock, got %d", resp.StatusCode)
	}
}

func TestMkcol(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	c, err := client.NewClient(testServer.URL+"/dav", "", "", "", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}

	// 1. Create collection
	err = c.Mkcol(context.Background(), "/newcoll")
	if err != nil {
		t.Fatalf("Mkcol /newcoll error: %v", err)
	}

	// 2. Mkcol on existing resource -> 405 Method Not Allowed
	req, _ := http.NewRequest("MKCOL", testServer.URL+"/dav/newcoll", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("MKCOL error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 Method Not Allowed on existing coll, got %d", resp.StatusCode)
	}

	// 3. Mkcol when parent does not exist -> 409 Conflict
	req, _ = http.NewRequest("MKCOL", testServer.URL+"/dav/parent/child/coll", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("MKCOL missing parent error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("expected 409 Conflict when parent missing, got %d", resp.StatusCode)
	}
}

func TestMoveAndCopy(t *testing.T) {
	up1 := newTestWebDAVUpstream(2000, 100, true)
	defer up1.Close()
	up1.setFile("/file1.txt", []byte("file1 content"), time.Now())

	up2 := newTestWebDAVUpstream(2000, 100, true)
	defer up2.Close()
	up2.setFile("/dest/existing.txt", []byte("dest content"), time.Now())

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up1.server.URL},
			{ID: 2, URL: up2.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	c, err := client.NewClient(testServer.URL+"/dav", "", "", "", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}

	// 1. Same-node COPY /file1.txt -> /file1_copy.txt
	err = c.Copy(context.Background(), "/file1.txt", "/file1_copy.txt", true)
	if err != nil {
		t.Fatalf("Copy /file1.txt error: %v", err)
	}

	rc, _, err := c.Get(context.Background(), "/file1_copy.txt", nil)
	if err != nil {
		t.Fatalf("Get /file1_copy.txt error: %v", err)
	}
	copyContent, _ := io.ReadAll(rc)
	rc.Close()
	if string(copyContent) != "file1 content" {
		t.Errorf("expected 'file1 content', got %q", string(copyContent))
	}

	// 2. Cross-node MOVE to /dest/existing.txt (overwrite = true)
	err = c.Move(context.Background(), "/file1_copy.txt", "/dest/existing.txt", true)
	if err != nil {
		t.Fatalf("Move cross-node error: %v", err)
	}

	// Source /file1_copy.txt should be gone
	_, _, err = c.Get(context.Background(), "/file1_copy.txt", nil)
	if err == nil {
		t.Errorf("expected /file1_copy.txt to be deleted after MOVE")
	}

	// Destination /dest/existing.txt should have new content
	rc, _, err = c.Get(context.Background(), "/dest/existing.txt", nil)
	if err != nil {
		t.Fatalf("Get /dest/existing.txt error: %v", err)
	}
	newContent, _ := io.ReadAll(rc)
	rc.Close()
	if string(newContent) != "file1 content" {
		t.Errorf("expected 'file1 content' at destination, got %q", string(newContent))
	}
}

func TestProppatch(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	req := httptest.NewRequest("PROPPATCH", "/dav/file.txt", strings.NewReader(`<propfind/>`))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusMultiStatus {
		t.Errorf("expected 207 Multi-Status, got %d", resp.StatusCode)
	}

	var parsed struct {
		XMLName xml.Name `xml:"multistatus"`
	}
	body, _ := io.ReadAll(resp.Body)
	if err := xml.Unmarshal(body, &parsed); err != nil {
		t.Errorf("failed to unmarshal PROPPATCH response: %v", err)
	}
}

func TestServerLifecycle(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	if srv.Config() == nil || srv.Cluster() == nil || srv.LockManager() == nil {
		t.Fatalf("expected non-nil components")
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	// Wait for listener to be active
	var addr string
	for range 50 {
		time.Sleep(10 * time.Millisecond)
		a := srv.Addr()
		if a != "127.0.0.1:0" && a != "" {
			addr = a
			break
		}
	}
	if addr == "" {
		t.Fatalf("server failed to start listening")
	}

	resp, err := http.Get("http://" + addr + "/")
	if err != nil {
		t.Fatalf("GET / on server error: %v", err)
	}
	resp.Body.Close()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		t.Errorf("Shutdown error: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("unexpected Start error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("server did not exit in time")
	}
}

func TestRootPathReturns404NoBody(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.server.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "1000B"},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	pathsToTest := []string{"/", "/random", "/favicon.ico", "/api"}
	for _, p := range pathsToTest {
		resp, err := http.Get(testServer.URL + p)
		if err != nil {
			t.Fatalf("GET %s error: %v", p, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("expected 404 for %s, got %d", p, resp.StatusCode)
		}
		if len(body) != 0 {
			t.Errorf("expected empty body for %s, got %q", p, string(body))
		}
	}
}

func TestPut_InsufficientStorageReturns507(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.server.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "1000B"},
		},
	}
	srv := newTestServer(t, cfg)

	// Fill node to 950 bytes
	node, _ := srv.Cluster().GetNode(1)
	node.SetQuota(950, 5)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	// Try to upload 100 bytes (exceeding available 50 bytes)
	req, _ := http.NewRequest("PUT", testServer.URL+"/dav/largefile.bin", strings.NewReader(strings.Repeat("X", 100)))
	req.ContentLength = 100
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInsufficientStorage {
		t.Errorf("expected HTTP 507 Insufficient Storage, got %d", resp.StatusCode)
	}
}

func TestDeleteDirectoryRecoversUsedCapacity(t *testing.T) {
	up := newTestWebDAVUpstream(2000, 0, true)
	defer up.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL, Capacity: "2000B"},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	c, err := client.NewClient(testServer.URL+"/dav", "", "", "", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}

	// 1. Upload nested files
	data1 := bytes.Repeat([]byte("1"), 100)
	err = c.Put(context.Background(), "/tree/sub1/a.bin", bytes.NewReader(data1), int64(len(data1)), "application/octet-stream")
	if err != nil {
		t.Fatalf("Put /tree/sub1/a.bin failed: %v", err)
	}
	data2 := bytes.Repeat([]byte("2"), 200)
	err = c.Put(context.Background(), "/tree/sub2/b.bin", bytes.NewReader(data2), int64(len(data2)), "application/octet-stream")
	if err != nil {
		t.Fatalf("Put /tree/sub2/b.bin failed: %v", err)
	}

	// Verify used capacity
	avail, used, _ := srv.Cluster().TotalQuota()
	if used != 300 {
		t.Fatalf("expected used 300 after uploads, got %d", used)
	}
	if avail != 1700 {
		t.Fatalf("expected avail 1700 after uploads, got %d", avail)
	}

	// 2. Delete parent directory /tree
	err = c.Delete(context.Background(), "/tree")
	if err != nil {
		t.Fatalf("Delete /tree failed: %v", err)
	}

	// Verify used capacity is restored to 0 and available restored to 2000
	avail, used, _ = srv.Cluster().TotalQuota()
	if used != 0 {
		t.Errorf("expected used to be restored to 0 after deleting /tree, got %d", used)
	}
	if avail != 2000 {
		t.Errorf("expected avail to be restored to 2000 after deleting /tree, got %d", avail)
	}

	// Verify SQLite subtree is empty
	dm := srv.Cluster().DataManager()
	recs, err := dm.ListSubtree("/tree")
	if err != nil {
		t.Fatalf("ListSubtree error: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("expected 0 records under /tree after delete, got %d", len(recs))
	}

	// 3. Single file delete capacity recovery
	data3 := bytes.Repeat([]byte("3"), 150)
	err = c.Put(context.Background(), "/single.txt", bytes.NewReader(data3), int64(len(data3)), "text/plain")
	if err != nil {
		t.Fatalf("Put /single.txt failed: %v", err)
	}
	_, used, _ = srv.Cluster().TotalQuota()
	if used != 150 {
		t.Fatalf("expected used 150 after /single.txt upload, got %d", used)
	}
	err = c.Delete(context.Background(), "/single.txt")
	if err != nil {
		t.Fatalf("Delete /single.txt failed: %v", err)
	}
	avail, used, _ = srv.Cluster().TotalQuota()
	if used != 0 {
		t.Errorf("expected used to be 0 after deleting /single.txt, got %d", used)
	}
	if avail != 2000 {
		t.Errorf("expected avail to be 2000 after deleting /single.txt, got %d", avail)
	}

	// 4. Virtual directory spanning 2 upstreams
	up1 := newTestWebDAVUpstream(1000, 0, true)
	defer up1.Close()
	up2 := newTestWebDAVUpstream(1000, 0, true)
	defer up2.Close()

	up1.setFile("/multi/n1.bin", bytes.Repeat([]byte("X"), 100), time.Now())
	up2.setFile("/multi/n2.bin", bytes.Repeat([]byte("Y"), 200), time.Now())

	cfg2 := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up1.server.URL, Capacity: "1000B"},
			{ID: 2, URL: up2.server.URL, Capacity: "1000B"},
		},
	}
	srv2 := newTestServer(t, cfg2)
	testServer2 := httptest.NewServer(srv2.Handler())
	defer testServer2.Close()

	c2, err := client.NewClient(testServer2.URL+"/dav", "", "", "", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClient 2 error: %v", err)
	}

	_, usedMulti, _ := srv2.Cluster().TotalQuota()
	if usedMulti != 300 {
		t.Fatalf("expected initial used 300 for multi-node, got %d", usedMulti)
	}

	err = c2.Delete(context.Background(), "/multi")
	if err != nil {
		t.Fatalf("Delete /multi failed: %v", err)
	}

	availMulti, usedMulti, _ := srv2.Cluster().TotalQuota()
	if usedMulti != 0 {
		t.Errorf("expected used 0 after deleting multi-node /multi, got %d", usedMulti)
	}
	if availMulti != 2000 {
		t.Errorf("expected avail 2000 after deleting multi-node /multi, got %d", availMulti)
	}
}

func TestPropfindAndHeadZeroNetworkBroadcast(t *testing.T) {
	up1 := newTestWebDAVUpstream(2000, 100, true)
	defer up1.Close()
	up1.setFile("/docs/readme.txt", []byte("readme content 12345"), time.Now())

	up2 := newTestWebDAVUpstream(2000, 100, true)
	defer up2.Close()
	up2.setFile("/docs/guide.txt", []byte("guide content 1234567890"), time.Now())

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up1.server.URL},
			{ID: 2, URL: up2.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	c, err := client.NewClient(testServer.URL+"/dav", "", "", "", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}

	// Reset request counts after boot initialization
	up1.resetCounts()
	up2.resetCounts()

	// 1. PROPFIND / (Depth 1) should be served directly from SQLite
	items, err := c.List(context.Background(), "/", 1)
	if err != nil {
		t.Fatalf("List / error: %v", err)
	}
	if len(items) == 0 {
		t.Fatalf("expected non-empty root items")
	}
	if up1.methodCount("PROPFIND") != 0 || up2.methodCount("PROPFIND") != 0 {
		t.Errorf("expected 0 PROPFIND upstream requests for root List, got up1=%d up2=%d",
			up1.methodCount("PROPFIND"), up2.methodCount("PROPFIND"))
	}

	// 2. PROPFIND /docs (Depth 1) should also be served directly from SQLite
	docItems, err := c.List(context.Background(), "/docs", 1)
	if err != nil {
		t.Fatalf("List /docs error: %v", err)
	}
	if len(docItems) < 2 {
		t.Fatalf("expected at least 2 items under /docs, got %d", len(docItems))
	}
	if up1.methodCount("PROPFIND") != 0 || up2.methodCount("PROPFIND") != 0 {
		t.Errorf("expected 0 PROPFIND upstream requests for /docs List, got up1=%d up2=%d",
			up1.methodCount("PROPFIND"), up2.methodCount("PROPFIND"))
	}

	// 3. HEAD /docs/readme.txt should be served directly from SQLite
	headResp, err := http.Head(testServer.URL + "/dav/docs/readme.txt")
	if err != nil {
		t.Fatalf("HEAD request error: %v", err)
	}
	headResp.Body.Close()
	if headResp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for HEAD, got %d", headResp.StatusCode)
	}
	if headResp.Header.Get("Content-Length") != "20" {
		t.Errorf("expected Content-Length 20, got %s", headResp.Header.Get("Content-Length"))
	}
	if up1.methodCount("HEAD") != 0 || up2.methodCount("HEAD") != 0 {
		t.Errorf("expected 0 HEAD upstream requests, got up1=%d up2=%d",
			up1.methodCount("HEAD"), up2.methodCount("HEAD"))
	}

	// 4. GET /docs/readme.txt should route exclusively to up1 (holding the file), 0 requests to up2
	up1.resetCounts()
	up2.resetCounts()

	getResp, err := http.Get(testServer.URL + "/dav/docs/readme.txt")
	if err != nil {
		t.Fatalf("GET request error: %v", err)
	}
	body, _ := io.ReadAll(getResp.Body)
	getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for GET, got %d", getResp.StatusCode)
	}
	if string(body) != "readme content 12345" {
		t.Errorf("expected 'readme content 12345', got %q", string(body))
	}
	if up1.methodCount("GET") != 1 {
		t.Errorf("expected exactly 1 GET to up1, got %d", up1.methodCount("GET"))
	}
	if up2.methodCount("GET") != 0 {
		t.Errorf("expected 0 GET to up2, got %d", up2.methodCount("GET"))
	}
}
func TestDeleteRootForbidden(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	for _, targetURL := range []string{testServer.URL + "/dav", testServer.URL + "/dav/"} {
		req, _ := http.NewRequest("DELETE", targetURL, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("DELETE %s error: %v", targetURL, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("expected 403 Forbidden for DELETE %s, got %d", targetURL, resp.StatusCode)
		}
	}

	// Also directly calling ServeHTTP with davPath == "/"
	req, _ := http.NewRequest("DELETE", "/", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden on ServeHTTP DELETE /, got %d", rr.Code)
	}
}

func TestLockNewFileReturns201AndExistingReturns200(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()
	up.setFile("/existing.txt", []byte("hello"), time.Now())

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	lockXML := `<?xml version="1.0" encoding="utf-8" ?>
<D:lockinfo xmlns:D='DAV:'>
  <D:lockscope><D:exclusive/></D:lockscope>
  <D:locktype><D:write/></D:locktype>
  <D:owner><D:href>tester</D:href></D:owner>
</D:lockinfo>`

	// 1. Lock on non-existent file -> 201 Created
	req, _ := http.NewRequest("LOCK", testServer.URL+"/dav/brand_new.txt", strings.NewReader(lockXML))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("LOCK error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected 201 Created for new file LOCK, got %d", resp.StatusCode)
	}

	// 2. Lock on existing file -> 200 OK
	req2, _ := http.NewRequest("LOCK", testServer.URL+"/dav/existing.txt", strings.NewReader(lockXML))
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("LOCK error: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for existing file LOCK, got %d", resp2.StatusCode)
	}
}

func TestLockInheritanceDepthInfinity(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	lockXML := `<?xml version="1.0" encoding="utf-8" ?>
<D:lockinfo xmlns:D='DAV:'>
  <D:lockscope><D:exclusive/></D:lockscope>
  <D:locktype><D:write/></D:locktype>
  <D:owner><D:href>parent-holder</D:href></D:owner>
</D:lockinfo>`

	// 1. Lock /dav/locked_dir with Depth: infinity
	req, _ := http.NewRequest("LOCK", testServer.URL+"/dav/locked_dir", strings.NewReader(lockXML))
	req.Header.Set("Depth", "infinity")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("LOCK error: %v", err)
	}
	token := resp.Header.Get("Lock-Token")
	resp.Body.Close()

	// 2. Attempt to PUT into a subpath without token -> 423 Locked
	reqPut, _ := http.NewRequest("PUT", testServer.URL+"/dav/locked_dir/sub/file.txt", strings.NewReader("data"))
	respPut, err := http.DefaultClient.Do(reqPut)
	if err != nil {
		t.Fatalf("PUT error: %v", err)
	}
	respPut.Body.Close()
	if respPut.StatusCode != http.StatusLocked {
		t.Errorf("expected 423 Locked on subpath under Depth: infinity, got %d", respPut.StatusCode)
	}

	// 3. PUT with token -> 201 Created
	reqPut2, _ := http.NewRequest("PUT", testServer.URL+"/dav/locked_dir/sub/file.txt", strings.NewReader("data"))
	reqPut2.Header.Set("If", fmt.Sprintf("(%s)", token))
	respPut2, err := http.DefaultClient.Do(reqPut2)
	if err != nil {
		t.Fatalf("PUT with token error: %v", err)
	}
	respPut2.Body.Close()
	if respPut2.StatusCode != http.StatusCreated {
		t.Errorf("expected 201 Created with valid token, got %d", respPut2.StatusCode)
	}

	// 4. Lock /dav/shallow with Depth: 0
	reqShallow, _ := http.NewRequest("LOCK", testServer.URL+"/dav/shallow", strings.NewReader(lockXML))
	reqShallow.Header.Set("Depth", "0")
	respShallow, err := http.DefaultClient.Do(reqShallow)
	if err != nil {
		t.Fatalf("LOCK shallow error: %v", err)
	}
	respShallow.Body.Close()

	// Subpath under Depth: 0 should NOT be locked
	reqPut3, _ := http.NewRequest("PUT", testServer.URL+"/dav/shallow/sub/file.txt", strings.NewReader("data"))
	respPut3, err := http.DefaultClient.Do(reqPut3)
	if err != nil {
		t.Fatalf("PUT shallow child error: %v", err)
	}
	respPut3.Body.Close()
	if respPut3.StatusCode != http.StatusCreated {
		t.Errorf("expected 201 Created under Depth: 0 parent lock, got %d", respPut3.StatusCode)
	}
}

func TestUnlockInvalidTokenReturns409(t *testing.T) {
	up := newTestWebDAVUpstream(1000, 100, true)
	defer up.Close()

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	lockXML := `<?xml version="1.0" encoding="utf-8" ?>
<D:lockinfo xmlns:D='DAV:'>
  <D:lockscope><D:exclusive/></D:lockscope>
  <D:locktype><D:write/></D:locktype>
  <D:owner><D:href>user</D:href></D:owner>
</D:lockinfo>`

	// Lock resource
	reqLock, _ := http.NewRequest("LOCK", testServer.URL+"/dav/item.txt", strings.NewReader(lockXML))
	respLock, err := http.DefaultClient.Do(reqLock)
	if err != nil {
		t.Fatalf("LOCK error: %v", err)
	}
	realToken := respLock.Header.Get("Lock-Token")
	respLock.Body.Close()

	// 1. UNLOCK with invalid token -> 409 Conflict
	reqUnlockWrong, _ := http.NewRequest("UNLOCK", testServer.URL+"/dav/item.txt", nil)
	reqUnlockWrong.Header.Set("Lock-Token", "<urn:uuid:00000000-0000-0000-0000-000000000000>")
	respUnlockWrong, err := http.DefaultClient.Do(reqUnlockWrong)
	if err != nil {
		t.Fatalf("UNLOCK error: %v", err)
	}
	respUnlockWrong.Body.Close()
	if respUnlockWrong.StatusCode != http.StatusConflict {
		t.Errorf("expected 409 Conflict for invalid token UNLOCK, got %d", respUnlockWrong.StatusCode)
	}

	// 2. UNLOCK on resource that is not locked -> 409 Conflict
	reqUnlockNone, _ := http.NewRequest("UNLOCK", testServer.URL+"/dav/not_locked.txt", nil)
	reqUnlockNone.Header.Set("Lock-Token", "<urn:uuid:00000000-0000-0000-0000-000000000000>")
	respUnlockNone, err := http.DefaultClient.Do(reqUnlockNone)
	if err != nil {
		t.Fatalf("UNLOCK error: %v", err)
	}
	respUnlockNone.Body.Close()
	if respUnlockNone.StatusCode != http.StatusConflict {
		t.Errorf("expected 409 Conflict for non-locked resource UNLOCK, got %d", respUnlockNone.StatusCode)
	}

	// 3. UNLOCK with correct token -> 204 No Content
	reqUnlockCorrect, _ := http.NewRequest("UNLOCK", testServer.URL+"/dav/item.txt", nil)
	reqUnlockCorrect.Header.Set("Lock-Token", realToken)
	respUnlockCorrect, err := http.DefaultClient.Do(reqUnlockCorrect)
	if err != nil {
		t.Fatalf("UNLOCK error: %v", err)
	}
	respUnlockCorrect.Body.Close()
	if respUnlockCorrect.StatusCode != http.StatusNoContent {
		t.Errorf("expected 204 No Content for valid UNLOCK, got %d", respUnlockCorrect.StatusCode)
	}
}

func TestRangeStrictPassthrough(t *testing.T) {
	content := []byte("abcdefghijklmnopqrstuvwxyz")

	// Upstream that ignores Range requests and returns full 200 OK
	upNoRange := newTestWebDAVUpstream(2000, 100, true)
	upNoRange.ignoreRange = true
	defer upNoRange.Close()
	upNoRange.setFile("/full.txt", content, time.Now())

	// Standard upstream supporting Range
	upRange := newTestWebDAVUpstream(2000, 100, true)
	defer upRange.Close()
	upRange.setFile("/range.txt", content, time.Now())

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: upNoRange.server.URL},
			{ID: 2, URL: upRange.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	// 1. When upstream ignores Range and returns 200: downstream must return 200 OK with full content and length
	req1, _ := http.NewRequest("GET", testServer.URL+"/dav/full.txt", nil)
	req1.Header.Set("Range", "bytes=0-4")
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("GET /full.txt error: %v", err)
	}
	body1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()

	if resp1.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK when upstream returns 200, got %d", resp1.StatusCode)
	}
	if resp1.Header.Get("Content-Range") != "" {
		t.Errorf("expected no Content-Range header when returning 200, got %q", resp1.Header.Get("Content-Range"))
	}
	if resp1.Header.Get("Content-Length") != "26" {
		t.Errorf("expected Content-Length 26, got %s", resp1.Header.Get("Content-Length"))
	}
	if !bytes.Equal(body1, content) {
		t.Errorf("expected full body %q, got %q", content, body1)
	}

	// 2. When upstream supports Range and returns 206: downstream passes 206 and Content-Range
	req2, _ := http.NewRequest("GET", testServer.URL+"/dav/range.txt", nil)
	req2.Header.Set("Range", "bytes=0-4")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("GET /range.txt error: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()

	if resp2.StatusCode != http.StatusPartialContent {
		t.Errorf("expected 206 Partial Content when upstream returns 206, got %d", resp2.StatusCode)
	}
	if resp2.Header.Get("Content-Range") != "bytes 0-4/26" {
		t.Errorf("expected Content-Range 'bytes 0-4/26', got %q", resp2.Header.Get("Content-Range"))
	}
	if string(body2) != "abcde" {
		t.Errorf("expected 'abcde', got %q", string(body2))
	}
}

func TestCrossNodeMoveAndCopyDirectory(t *testing.T) {
	// Node 1: contains directory /tree with multiple files
	up1 := newTestWebDAVUpstream(5000, 100, true)
	defer up1.Close()
	up1.setFile("/tree/file1.txt", []byte("file1 content"), time.Now())
	up1.setFile("/tree/sub/file2.txt", []byte("file2 content"), time.Now())

	// Node 2: contains destination /backup so it's on Node 2
	up2 := newTestWebDAVUpstream(5000, 100, true)
	defer up2.Close()
	up2.setFile("/backup/marker.txt", []byte("mark"), time.Now())

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up1.server.URL},
			{ID: 2, URL: up2.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	c, _ := client.NewClient(testServer.URL+"/dav", "", "", "", 1, 5*time.Second)

	// 1. Cross-node MOVE /tree to /backup
	req, _ := http.NewRequest("MOVE", testServer.URL+"/dav/tree", nil)
	req.Header.Set("Destination", testServer.URL+"/dav/backup")
	req.Header.Set("Overwrite", "T")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("MOVE error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 204 or 201 on cross-node directory MOVE, got %d", resp.StatusCode)
	}

	// Verify files moved to destination
	rc1, _, err := c.Get(context.Background(), "/backup/file1.txt", nil)
	if err != nil {
		t.Fatalf("Get /backup/file1.txt error: %v", err)
	}
	b1, _ := io.ReadAll(rc1)
	rc1.Close()
	if string(b1) != "file1 content" {
		t.Errorf("expected 'file1 content', got %q", string(b1))
	}

	rc2, _, err := c.Get(context.Background(), "/backup/sub/file2.txt", nil)
	if err != nil {
		t.Fatalf("Get /backup/sub/file2.txt error: %v", err)
	}
	b2, _ := io.ReadAll(rc2)
	rc2.Close()
	if string(b2) != "file2 content" {
		t.Errorf("expected 'file2 content', got %q", string(b2))
	}

	// Source files on up1 must be deleted
	if _, ok := up1.resources["/tree/file1.txt"]; ok {
		t.Errorf("source file1.txt still exists on up1 after MOVE")
	}
	if _, ok := up1.resources["/tree/sub/file2.txt"]; ok {
		t.Errorf("source file2.txt still exists on up1 after MOVE")
	}
}

func TestCrossNodeDirectoryMoveFailureRollback(t *testing.T) {
	up1 := newTestWebDAVUpstream(5000, 100, true)
	defer up1.Close()
	up1.setFile("/srcdir/f1.txt", []byte("f1 data"), time.Now())
	up1.setFile("/srcdir/f2.txt", []byte("f2 data"), time.Now())

	up2 := newTestWebDAVUpstream(5000, 100, true)
	defer up2.Close()
	// Destination on up2 exists to trigger cross-node transfer
	up2.setFile("/destdir/marker.txt", []byte("existing"), time.Now())
	// Make up2 fail when putting /destdir/f2.txt
	up2.failPutPath = "/destdir/f2.txt"

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up1.server.URL},
			{ID: 2, URL: up2.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	req, _ := http.NewRequest("MOVE", testServer.URL+"/dav/srcdir", nil)
	req.Header.Set("Destination", testServer.URL+"/dav/destdir")
	req.Header.Set("Overwrite", "T")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("MOVE error: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500 Internal Server Error on failure, got %d", resp.StatusCode)
	}

	// Verify source files remain completely intact on up1!
	if _, ok := up1.resources["/srcdir/f1.txt"]; !ok {
		t.Errorf("source /srcdir/f1.txt was incorrectly deleted on failure!")
	}
	if _, ok := up1.resources["/srcdir/f2.txt"]; !ok {
		t.Errorf("source /srcdir/f2.txt was incorrectly deleted on failure!")
	}

	// Verify partially created files on up2 were cleaned up (rolled back)
	if _, ok := up2.resources["/destdir/f1.txt"]; ok {
		t.Errorf("target /destdir/f1.txt was not cleaned up during rollback!")
	}
}
func TestServer_ConcurrentUpload_LoadBalancing_And_NoQuotaLeak(t *testing.T) {
	u1 := newTestWebDAVUpstream(50*1024*1024, 0, true)
	defer u1.Close()
	u2 := newTestWebDAVUpstream(50*1024*1024, 0, true)
	defer u2.Close()

	cfg := &config.Config{
		Listen: ":0",
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: u1.server.URL, Capacity: "50MB"},
			{ID: 2, URL: u2.server.URL, Capacity: "50MB"},
		},
	}

	srv := newTestServer(t, cfg)

	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	const totalRequests = 20
	var wg sync.WaitGroup
	errCh := make(chan error, totalRequests)
	start := make(chan struct{})

	tr := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		MaxConnsPerHost:     100,
	}
	httpClient := &http.Client{Transport: tr}

	for i := range totalRequests {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			payload := bytes.Repeat([]byte(fmt.Sprintf("data_%02d_", idx)), 1024) // ~8KB
			filename := fmt.Sprintf("/dav/upload_%02d.dat", idx)

			var req *http.Request
			var reqErr error
			if idx%2 == 0 {
				// Known Content-Length upload
				req, reqErr = http.NewRequest("PUT", testServer.URL+filename, bytes.NewReader(payload))
				if reqErr != nil {
					errCh <- fmt.Errorf("create req %d: %w", idx, reqErr)
					return
				}
				req.ContentLength = int64(len(payload))
			} else {
				// Chunked upload (Content-Length = -1)
				req, reqErr = http.NewRequest("PUT", testServer.URL+filename, bytes.NewReader(payload))
				if reqErr != nil {
					errCh <- fmt.Errorf("create req %d: %w", idx, reqErr)
					return
				}
				req.ContentLength = -1
			}

			req.Header.Set("Content-Type", "application/octet-stream")
			resp, doErr := httpClient.Do(req)
			if doErr != nil {
				errCh <- fmt.Errorf("do req %d: %w", idx, doErr)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
				body, _ := io.ReadAll(resp.Body)
				errCh <- fmt.Errorf("req %d failed with status %d: %s", idx, resp.StatusCode, string(body))
				return
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errCh)

	for reqErr := range errCh {
		t.Fatalf("concurrent upload error: %v", reqErr)
	}

	// 1. Assert requests were distributed across both upstream nodes (load balanced)
	u1Puts := u1.methodCount("PUT")
	u2Puts := u2.methodCount("PUT")
	t.Logf("Upstream PUT distribution: u1=%d, u2=%d", u1Puts, u2Puts)
	if u1Puts == 0 || u2Puts == 0 {
		t.Fatalf("expected writes distributed across both nodes, got u1=%d, u2=%d", u1Puts, u2Puts)
	}
	if u1Puts < 4 || u2Puts < 4 {
		t.Fatalf("distribution too skewed: u1=%d, u2=%d (expected each node >= 4)", u1Puts, u2Puts)
	}

	// 2. Assert in-memory ReservedBytes are strictly 0 (no leaks)
	n1, _ := srv.Cluster().GetNode(1)
	n2, _ := srv.Cluster().GetNode(2)
	if n1.Reserved() != 0 || n2.Reserved() != 0 {
		t.Fatalf("reservation leak detected: n1.Reserved=%d, n2.Reserved=%d", n1.Reserved(), n2.Reserved())
	}

	// 3. Assert SQLite records match in-memory node usage
	dm := srv.Cluster().DataManager()
	st1, err := dm.GetStorage(1)
	if err != nil || st1 == nil {
		t.Fatalf("GetStorage(1) error: %v", err)
	}
	st2, err := dm.GetStorage(2)
	if err != nil || st2 == nil {
		t.Fatalf("GetStorage(2) error: %v", err)
	}

	if st1.UsedBytes != n1.UsedBytes {
		t.Fatalf("storage 1 mismatch: db.UsedBytes=%d, node.UsedBytes=%d", st1.UsedBytes, n1.UsedBytes)
	}
	if st2.UsedBytes != n2.UsedBytes {
		t.Fatalf("storage 2 mismatch: db.UsedBytes=%d, node.UsedBytes=%d", st2.UsedBytes, n2.UsedBytes)
	}
	if st1.FileCount != n1.FilesCount {
		t.Fatalf("storage 1 file count mismatch: db=%d, node=%d", st1.FileCount, n1.FilesCount)
	}
	if st2.FileCount != n2.FilesCount {
		t.Fatalf("storage 2 file count mismatch: db=%d, node=%d", st2.FileCount, n2.FilesCount)
	}

	totalFiles := st1.FileCount + st2.FileCount
	if totalFiles != totalRequests {
		t.Fatalf("expected total files %d, got %d", totalRequests, totalFiles)
	}

	// 4. Assert AvailableBytes is strictly consistent with TotalBytes - UsedBytes
	if n1.AvailableBytes != n1.TotalBytes-n1.UsedBytes {
		t.Fatalf("node 1 available mismatch: avail=%d, expected=%d", n1.AvailableBytes, n1.TotalBytes-n1.UsedBytes)
	}
	if n2.AvailableBytes != n2.TotalBytes-n2.UsedBytes {
		t.Fatalf("node 2 available mismatch: avail=%d, expected=%d", n2.AvailableBytes, n2.TotalBytes-n2.UsedBytes)
	}
}
func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Duration
		expected string
	}{
		{"zero", 0, "0.00s"},
		{"50 milliseconds", 50 * time.Millisecond, "0.05s"},
		{"12.34 seconds", 12*time.Second + 340*time.Millisecond, "12.34s"},
		{"59.99 seconds", 59*time.Second + 990*time.Millisecond, "59.99s"},
		{"boundary 60 seconds", 60 * time.Second, "1.00m"},
		{"90 seconds", 90 * time.Second, "1.50m"},
		{"3599 seconds", 3599 * time.Second, "59.98m"},
		{"boundary 3600 seconds", 3600 * time.Second, "1.00h"},
		{"7200 seconds", 7200 * time.Second, "2.00h"},
		{"25 hours", 25 * time.Hour, "25.00h"},
		{"100 hours", 100 * time.Hour, "100.00h"},
		{"negative duration", -5 * time.Second, "0.00s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatDuration(tt.input)
			if got != tt.expected {
				t.Errorf("formatDuration(%v) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		expected string
	}{
		{"zero", 0, "0.00B"},
		{"100 bytes", 100, "100.00B"},
		{"1023 bytes boundary", 1023, "1023.00B"},
		{"1024 bytes boundary", 1024, "1.00KB"},
		{"1260 bytes (1.23KB)", 1260, "1.23KB"},
		{"1024*1024 boundary (1MB)", 1024 * 1024, "1.00MB"},
		{"1.5MB", int64(1.5 * 1024 * 1024), "1.50MB"},
		{"1GB boundary", 1024 * 1024 * 1024, "1.00GB"},
		{"1TB boundary", 1024 * 1024 * 1024 * 1024, "1.00TB"},
		{"1PB boundary", 1024 * 1024 * 1024 * 1024 * 1024, "1.00PB"},
		{"1EB boundary", 1024 * 1024 * 1024 * 1024 * 1024 * 1024, "1.00EB"},
		{"2EB boundary", 2 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024, "2.00EB"},
		{"max int64", math.MaxInt64, "8.00EB"},
		{"negative bytes", -100, "0.00B"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatBytes(tt.input)
			if got != tt.expected {
				t.Errorf("formatBytes(%d) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestGetClientIP(t *testing.T) {
	tests := []struct {
		name       string
		xff        string
		xri        string
		remoteAddr string
		expected   string
	}{
		{
			name:       "X-Forwarded-For single with port",
			xff:        "203.0.113.195:8080",
			remoteAddr: "192.168.1.1:1234",
			expected:   "203.0.113.195",
		},
		{
			name:       "X-Forwarded-For multiple IPs",
			xff:        "203.0.113.195, 70.41.3.18, 150.172.238.178",
			remoteAddr: "192.168.1.1:1234",
			expected:   "203.0.113.195",
		},
		{
			name:       "X-Forwarded-For IPv6 bracket with port",
			xff:        "[2001:db8::1]:8080, 10.0.0.1",
			remoteAddr: "192.168.1.1:1234",
			expected:   "2001:db8::1",
		},
		{
			name:       "X-Forwarded-For IPv6 bare",
			xff:        "2001:db8::1",
			remoteAddr: "192.168.1.1:1234",
			expected:   "2001:db8::1",
		},
		{
			name:       "X-Real-IP fallback when XFF empty",
			xff:        "",
			xri:        "198.51.100.1:443",
			remoteAddr: "192.168.1.1:1234",
			expected:   "198.51.100.1",
		},
		{
			name:       "RemoteAddr fallback with port",
			xff:        "",
			xri:        "",
			remoteAddr: "192.168.1.100:54321",
			expected:   "192.168.1.100",
		},
		{
			name:       "RemoteAddr IPv6 with port",
			xff:        "",
			xri:        "",
			remoteAddr: "[::1]:54321",
			expected:   "::1",
		},
		{
			name:       "RemoteAddr bare IPv4 without port",
			xff:        "",
			xri:        "",
			remoteAddr: "127.0.0.1",
			expected:   "127.0.0.1",
		},
		{
			name:       "All empty",
			xff:        "",
			xri:        "",
			remoteAddr: "",
			expected:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/dav/test", nil)
			if tt.xff != "" {
				req.Header.Set("X-Forwarded-For", tt.xff)
			}
			if tt.xri != "" {
				req.Header.Set("X-Real-IP", tt.xri)
			}
			req.RemoteAddr = tt.remoteAddr

			got := getClientIP(req)
			if got != tt.expected {
				t.Errorf("getClientIP() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestLoggingResponseWriter(t *testing.T) {
	t.Run("default status 200 without WriteHeader", func(t *testing.T) {
		rec := httptest.NewRecorder()
		lrw := &loggingResponseWriter{ResponseWriter: rec, statusCode: http.StatusOK}
		n, err := lrw.Write([]byte("hello world"))
		if err != nil {
			t.Fatalf("Write error: %v", err)
		}
		if n != 11 {
			t.Fatalf("Write n = %d, want 11", n)
		}
		if lrw.statusCode != http.StatusOK {
			t.Errorf("statusCode = %d, want 200", lrw.statusCode)
		}
		if lrw.bytesWritten != 11 {
			t.Errorf("bytesWritten = %d, want 11", lrw.bytesWritten)
		}
	})

	t.Run("custom status code via WriteHeader and multiple writes", func(t *testing.T) {
		rec := httptest.NewRecorder()
		lrw := &loggingResponseWriter{ResponseWriter: rec, statusCode: http.StatusOK}
		lrw.WriteHeader(http.StatusCreated)
		// Second WriteHeader call should be ignored
		lrw.WriteHeader(http.StatusInternalServerError)
		lrw.Write([]byte("part1-"))
		lrw.Write([]byte("part2"))

		if lrw.statusCode != http.StatusCreated {
			t.Errorf("statusCode = %d, want 201", lrw.statusCode)
		}
		if lrw.bytesWritten != 11 {
			t.Errorf("bytesWritten = %d, want 11", lrw.bytesWritten)
		}
	})

	t.Run("unwrap returns underlying ResponseWriter", func(t *testing.T) {
		rec := httptest.NewRecorder()
		lrw := &loggingResponseWriter{ResponseWriter: rec, statusCode: http.StatusOK}
		if lrw.Unwrap() != rec {
			t.Errorf("Unwrap() = %v, want %v", lrw.Unwrap(), rec)
		}
	})
}

func TestCountingReadCloser(t *testing.T) {
	t.Run("read and close tracking", func(t *testing.T) {
		rc := io.NopCloser(bytes.NewReader([]byte("test data 12345")))
		crc := newCountingReadCloser(rc)
		if crc.Unwrap() != rc {
			t.Errorf("Unwrap() = %v, want %v", crc.Unwrap(), rc)
		}

		buf := make([]byte, 4)
		n, err := crc.Read(buf)
		if err != nil || n != 4 {
			t.Fatalf("Read n = %d, err = %v", n, err)
		}
		if crc.BytesRead() != 4 {
			t.Errorf("BytesRead = %d, want 4", crc.BytesRead())
		}

		rest, err := io.ReadAll(crc)
		if err != nil {
			t.Fatalf("ReadAll error: %v", err)
		}
		if len(rest) != 11 {
			t.Fatalf("ReadAll len = %d, want 11", len(rest))
		}
		if crc.BytesRead() != 15 {
			t.Errorf("BytesRead = %d, want 15", crc.BytesRead())
		}
		_ = crc.Close()
	})

	t.Run("nil reader falls back to http.NoBody", func(t *testing.T) {
		crc := newCountingReadCloser(nil)
		buf := make([]byte, 10)
		n, err := crc.Read(buf)
		if err != io.EOF || n != 0 {
			t.Errorf("Read = (%d, %v), want (0, EOF)", n, err)
		}
		if crc.BytesRead() != 0 {
			t.Errorf("BytesRead = %d, want 0", crc.BytesRead())
		}
	})
}

func TestHTTPLogFormatting_EndToEnd(t *testing.T) {
	up := newTestWebDAVUpstream(1024*1024*1024, 0, true)
	defer up.server.Close()
	up.setFile("/test.txt", []byte("1234567890"), time.Now())

	cfg := &config.Config{
		Listen: "127.0.0.1:0",
		Retry:  1,
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL},
		},
	}
	srv := newTestServer(t, cfg)

	handler := srv.Handler()

	// 1. Successful GET returning file content
	logs := captureLogs(t, func() {
		req := httptest.NewRequest("GET", "/dav/test.txt", nil)
		req.Header.Set("X-Forwarded-For", "203.0.113.88:8080")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	})

	// Format: [HH:MM:SS INFO]: [HTTP] GET 200 "/dav/test.txt" 0.00s 10.00B - 203.0.113.88
	getPattern := `\[\d{2}:\d{2}:\d{2} INFO\]: \[HTTP\] GET 200 "/dav/test\.txt" \d+\.\d{2}s 10\.00B - 203\.0\.113\.88`
	matched, err := regexp.MatchString(getPattern, logs)
	if err != nil || !matched {
		t.Errorf("expected GET log matching %q, got:\n%s", getPattern, logs)
	}

	// 2. Request outside /dav -> 404
	logs404 := captureLogs(t, func() {
		req := httptest.NewRequest("GET", "/not-dav", nil)
		req.Header.Set("X-Real-IP", "10.0.0.5")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	})

	pattern404 := `\[\d{2}:\d{2}:\d{2} INFO\]: \[HTTP\] GET 404 "/not-dav" \d+\.\d{2}s 0\.00B - 10\.0\.0\.5`
	matched, err = regexp.MatchString(pattern404, logs404)
	if err != nil || !matched {
		t.Errorf("expected 404 log matching %q, got:\n%s", pattern404, logs404)
	}

	// 3. Successful PUT upload creating a new file -> logs upload size
	putPayload := []byte("hello webdav upload 12345") // 25 bytes
	logsPut := captureLogs(t, func() {
		req := httptest.NewRequest("PUT", "/dav/uploaded.txt", bytes.NewReader(putPayload))
		req.Header.Set("X-Forwarded-For", "198.51.100.1")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Fatalf("PUT expected 201 Created, got %d", rec.Code)
		}
	})

	putPattern := `\[\d{2}:\d{2}:\d{2} INFO\]: \[HTTP\] PUT 201 "/dav/uploaded\.txt" \d+\.\d{2}s 25\.00B - 198\.51\.100\.1`
	matched, err = regexp.MatchString(putPattern, logsPut)
	if err != nil || !matched {
		t.Errorf("expected PUT log matching %q, got:\n%s", putPattern, logsPut)
	}

	// 4. Successful PUT overwrite -> logs upload size and 204
	logsPutOverwrite := captureLogs(t, func() {
		overwritePayload := []byte("new overwritten data") // 20 bytes
		req := httptest.NewRequest("PUT", "/dav/uploaded.txt", bytes.NewReader(overwritePayload))
		req.Header.Set("X-Forwarded-For", "198.51.100.1")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("PUT overwrite expected 204 No Content, got %d", rec.Code)
		}
	})

	putOverwritePattern := `\[\d{2}:\d{2}:\d{2} INFO\]: \[HTTP\] PUT 204 "/dav/uploaded\.txt" \d+\.\d{2}s 20\.00B - 198\.51\.100\.1`
	matched, err = regexp.MatchString(putOverwritePattern, logsPutOverwrite)
	if err != nil || !matched {
		t.Errorf("expected PUT overwrite log matching %q, got:\n%s", putOverwritePattern, logsPutOverwrite)
	}

	// 5. Successful PUT empty file -> logs 0.00B
	logsPutEmpty := captureLogs(t, func() {
		req := httptest.NewRequest("PUT", "/dav/empty.txt", bytes.NewReader([]byte{}))
		req.Header.Set("X-Forwarded-For", "198.51.100.2")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Fatalf("PUT empty expected 201 Created, got %d", rec.Code)
		}
	})

	putEmptyPattern := `\[\d{2}:\d{2}:\d{2} INFO\]: \[HTTP\] PUT 201 "/dav/empty\.txt" \d+\.\d{2}s 0\.00B - 198\.51\.100\.2`
	matched, err = regexp.MatchString(putEmptyPattern, logsPutEmpty)
	if err != nil || !matched {
		t.Errorf("expected PUT empty log matching %q, got:\n%s", putEmptyPattern, logsPutEmpty)
	}

	// 6. PROPFIND returning directory XML -> logs response XML size
	logsPropfind := captureLogs(t, func() {
		req := httptest.NewRequest("PROPFIND", "/dav/", nil)
		req.Header.Set("Depth", "1")
		req.Header.Set("X-Forwarded-For", "198.51.100.3")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusMultiStatus {
			t.Fatalf("PROPFIND expected 207 MultiStatus, got %d", rec.Code)
		}
	})

	propfindPattern := `\[\d{2}:\d{2}:\d{2} INFO\]: \[HTTP\] PROPFIND 207 "/dav/" \d+\.\d{2}s [1-9]\d*(\.\d{2})?[KMG]?B - 198\.51\.100\.3`
	matched, err = regexp.MatchString(propfindPattern, logsPropfind)
	if err != nil || !matched {
		t.Errorf("expected PROPFIND log matching %q, got:\n%s", propfindPattern, logsPropfind)
	}

	// 7. HEAD request -> logs 0.00B
	logsHead := captureLogs(t, func() {
		req := httptest.NewRequest("HEAD", "/dav/test.txt", nil)
		req.Header.Set("X-Forwarded-For", "198.51.100.4")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("HEAD expected 200 OK, got %d", rec.Code)
		}
	})

	headPattern := `\[\d{2}:\d{2}:\d{2} INFO\]: \[HTTP\] HEAD 200 "/dav/test\.txt" \d+\.\d{2}s 0\.00B - 198\.51\.100\.4`
	matched, err = regexp.MatchString(headPattern, logsHead)
	if err != nil || !matched {
		t.Errorf("expected HEAD log matching %q, got:\n%s", headPattern, logsHead)
	}

	// 8. DELETE request -> logs 0.00B
	logsDelete := captureLogs(t, func() {
		req := httptest.NewRequest("DELETE", "/dav/uploaded.txt", nil)
		req.Header.Set("X-Forwarded-For", "198.51.100.5")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("DELETE expected 204 No Content, got %d", rec.Code)
		}
	})

	deletePattern := `\[\d{2}:\d{2}:\d{2} INFO\]: \[HTTP\] DELETE 204 "/dav/uploaded\.txt" \d+\.\d{2}s 0\.00B - 198\.51\.100\.5`
	matched, err = regexp.MatchString(deletePattern, logsDelete)
	if err != nil || !matched {
		t.Errorf("expected DELETE log matching %q, got:\n%s", deletePattern, logsDelete)
	}

	// 9. Auth enabled server: failed auth logs 0.00B
	cfgAuth := &config.Config{
		Listen: "127.0.0.1:0",
		Auth: config.AuthConfig{
			User: "admin",
			Pass: "secret",
		},
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: up.server.URL},
		},
	}
	srvAuth := newTestServer(t, cfgAuth)
	logsAuthFail := captureLogs(t, func() {
		req := httptest.NewRequest("PUT", "/dav/secret.bin", strings.NewReader("some unauthenticated payload"))
		req.Header.Set("X-Real-IP", "192.0.2.99")
		rec := httptest.NewRecorder()
		srvAuth.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 Unauthorized, got %d", rec.Code)
		}
	})

	authPattern := `\[\d{2}:\d{2}:\d{2} INFO\]: \[HTTP\] PUT 401 "/dav/secret\.bin" \d+\.\d{2}s 0\.00B - 192\.0\.2\.99`
	matched, err = regexp.MatchString(authPattern, logsAuthFail)
	if err != nil || !matched {
		t.Errorf("expected auth 401 log matching %q, got:\n%s", authPattern, logsAuthFail)
	}
}

type callbackReader struct {
	io.Reader
	once        sync.Once
	onFirstRead func()
}

func (r *callbackReader) Read(p []byte) (int, error) {
	if r.onFirstRead != nil {
		r.once.Do(r.onFirstRead)
	}
	return r.Reader.Read(p)
}
func TestServer_UploadConflictAndPreconditions(t *testing.T) {
	up1 := newTestWebDAVUpstream(5000, 100, true)
	defer up1.Close()
	up2 := newTestWebDAVUpstream(3000, 100, true)
	defer up2.Close()

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

	client := &http.Client{Timeout: 10 * time.Second}

	// 1. 测试相同文件同时上传时的冲突保护：并发发起对同一路径的两个 PUT 请求，断言一个成功（201/204）、另一个收到 409 Conflict
	t.Run("concurrent upload of same file returns 409 Conflict", func(t *testing.T) {
		inFlight := make(chan struct{})
		finishGate := make(chan struct{})
		results := make(chan int, 1)

		body := bytes.Repeat([]byte("X"), 1024)
		pr, pw := io.Pipe()
		go func() {
			<-inFlight
			<-finishGate
			_, _ = pw.Write(body)
			_ = pw.Close()
		}()

		// 包装 reader 在服务端读取 body（说明 handlePut 已经完成 TryLock 并进入传输状态）时触发 inFlight
		cr := &callbackReader{
			Reader: pr,
			onFirstRead: func() {
				close(inFlight)
			},
		}

		go func() {
			req, err := http.NewRequest("PUT", ts.URL+"/dav/concurrent-test.bin", cr)
			if err != nil {
				results <- 0
				return
			}
			req.ContentLength = int64(len(body))
			resp, err := client.Do(req)
			if err != nil {
				results <- 0
				return
			}
			_ = resp.Body.Close()
			results <- resp.StatusCode
		}()

		select {
		case <-inFlight:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for initial upload to reach in-flight state")
		}

		// 发起并发的第二个同路径上传，应当立即被 PathLocker 拦截并返回 409 Conflict
		req2, err := http.NewRequest("PUT", ts.URL+"/dav/concurrent-test.bin", strings.NewReader("conflict-body"))
		if err != nil {
			t.Fatalf("NewRequest failed: %v", err)
		}
		resp2, err := client.Do(req2)
		if err != nil {
			t.Fatalf("second PUT failed: %v", err)
		}
		_ = resp2.Body.Close()

		if resp2.StatusCode != http.StatusConflict {
			t.Fatalf("expected concurrent upload to return 409 Conflict, got %d", resp2.StatusCode)
		}

		// 释放第一个上传，使其顺利完成
		close(finishGate)
		status1 := <-results

		if status1 != http.StatusCreated && status1 != http.StatusNoContent {
			t.Fatalf("expected initial upload to succeed (201/204), got %d", status1)
		}
	})

	// 2. 测试 If-None-Match: * 条件冲突保护
	t.Run("If-None-Match wildcard returns 412 on existing file", func(t *testing.T) {
		filePath := "/dav/precondition.txt"

		// 初始上传文件
		req, err := http.NewRequest("PUT", ts.URL+filePath, strings.NewReader("initial content"))
		if err != nil {
			t.Fatalf("NewRequest failed: %v", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("expected 201 Created, got %d", resp.StatusCode)
		}

		// 携带 If-None-Match: * 尝试覆写已有文件
		req2, err := http.NewRequest("PUT", ts.URL+filePath, strings.NewReader("overwrite content"))
		if err != nil {
			t.Fatalf("NewRequest failed: %v", err)
		}
		req2.Header.Set("If-None-Match", "*")
		resp2, err := client.Do(req2)
		if err != nil {
			t.Fatalf("PUT with If-None-Match failed: %v", err)
		}
		_ = resp2.Body.Close()
		if resp2.StatusCode != http.StatusPreconditionFailed {
			t.Fatalf("expected 412 Precondition Failed, got %d", resp2.StatusCode)
		}

		// 对不存在的文件携带 If-None-Match: * 应该成功写入 (201 Created)
		req3, err := http.NewRequest("PUT", ts.URL+"/dav/brand-new-file.txt", strings.NewReader("new content"))
		if err != nil {
			t.Fatalf("NewRequest failed: %v", err)
		}
		req3.Header.Set("If-None-Match", "*")
		resp3, err := client.Do(req3)
		if err != nil {
			t.Fatalf("PUT new file with If-None-Match failed: %v", err)
		}
		_ = resp3.Body.Close()
		if resp3.StatusCode != http.StatusCreated {
			t.Fatalf("expected 201 Created for new file with If-None-Match: *, got %d", resp3.StatusCode)
		}
	})

	// 3. 测试覆盖已有文件时进行全局负载均衡：当另一个节点空间更大时，自动转移并清理原节点旧文件
	t.Run("overwrite existing file rebalances to node with most free space", func(t *testing.T) {
		filePath := "/dav/rebalance-overwrite.bin"

		// 首次上传小文件
		req1, _ := http.NewRequest("PUT", ts.URL+filePath, strings.NewReader("small"))
		resp1, err := client.Do(req1)
		if err != nil {
			t.Fatalf("first PUT failed: %v", err)
		}
		_ = resp1.Body.Close()
		if resp1.StatusCode != http.StatusCreated {
			t.Fatalf("expected 201, got %d", resp1.StatusCode)
		}

		dm := srv.Cluster().DataManager()
		recs1, err := dm.GetFilesByPath("/rebalance-overwrite.bin")
		if err != nil || len(recs1) != 1 {
			t.Fatalf("expected 1 record in DB, got %v (err: %v)", recs1, err)
		}
		initialStorageID := recs1[0].StorageID

		var targetID int
		if initialStorageID == 1 {
			targetID = 2
		} else {
			targetID = 1
		}
		targetNode, _ := srv.Cluster().GetNode(targetID)
		targetNode.AvailableBytes = 500000
		origNode, _ := srv.Cluster().GetNode(initialStorageID)
		origNode.AvailableBytes = 100

		// 第二次上传：覆写为较大文件，全局负载均衡应当将文件写入更空闲的 targetNode
		req2, _ := http.NewRequest("PUT", ts.URL+filePath, strings.NewReader(strings.Repeat("X", 1000)))
		resp2, err := client.Do(req2)
		if err != nil {
			t.Fatalf("second PUT failed: %v", err)
		}
		_ = resp2.Body.Close()
		if resp2.StatusCode != http.StatusNoContent {
			t.Fatalf("expected 204 NoContent on overwrite, got %d", resp2.StatusCode)
		}

		// 验证数据库元数据已更新为 targetID，且只有 1 条记录
		recs2, err := dm.GetFilesByPath("/rebalance-overwrite.bin")
		if err != nil || len(recs2) != 1 {
			t.Fatalf("expected exactly 1 DB record after overwrite, got %v (err: %v)", recs2, err)
		}
		if recs2[0].StorageID != targetID {
			t.Fatalf("expected overwrite to rebalance to node %d, but was %d", targetID, recs2[0].StorageID)
		}

		time.Sleep(50 * time.Millisecond)

		var newUp, oldUp *testWebDAVUpstream
		if targetID == 1 {
			newUp, oldUp = up1, up2
		} else {
			newUp, oldUp = up2, up1
		}

		if !newUp.hasFile("/rebalance-overwrite.bin") {
			t.Fatalf("expected file to exist on new target node %d", targetID)
		}
		if oldUp.hasFile("/rebalance-overwrite.bin") {
			t.Fatalf("expected old file to be deleted from previous node %d", initialStorageID)
		}
	})

	// 4. 测试跨节点覆写失败时原文件安全保障
	t.Run("overwrite failure keeps original file intact", func(t *testing.T) {
		filePath := "/dav/fail-overwrite.bin"

		// 首次上传到正常节点
		req1, _ := http.NewRequest("PUT", ts.URL+filePath, strings.NewReader("original content"))
		resp1, err := client.Do(req1)
		if err != nil {
			t.Fatalf("first PUT failed: %v", err)
		}
		_ = resp1.Body.Close()

		dm := srv.Cluster().DataManager()
		recs, err := dm.GetFilesByPath("/fail-overwrite.bin")
		if err != nil || len(recs) != 1 {
			t.Fatalf("expected 1 record in DB, got %v", recs)
		}
		origStorageID := recs[0].StorageID

		// 调整空间使调度器选中另一个节点，并让另一个节点拒绝 PUT (模拟 500)
		var otherID int
		if origStorageID == 1 {
			otherID = 2
		} else {
			otherID = 1
		}
		otherNode, _ := srv.Cluster().GetNode(otherID)
		otherNode.AvailableBytes = 999999
		origNode, _ := srv.Cluster().GetNode(origStorageID)
		origNode.AvailableBytes = 50

		var otherUp *testWebDAVUpstream
		if otherID == 1 {
			otherUp = up1
		} else {
			otherUp = up2
		}

		// 模拟 otherUp 在接收 PUT 时返回 500
		otherUp.mu.Lock()
		otherUp.failPutPath = "/fail-overwrite.bin"
		otherUp.mu.Unlock()
		defer func() {
			otherUp.mu.Lock()
			otherUp.failPutPath = ""
			otherUp.mu.Unlock()
		}()
		// 发起覆写
		req2, _ := http.NewRequest("PUT", ts.URL+filePath, strings.NewReader(strings.Repeat("Y", 500)))
		resp2, err := client.Do(req2)
		if err != nil {
			t.Fatalf("second PUT error: %v", err)
		}
		_ = resp2.Body.Close()
		if resp2.StatusCode == http.StatusOK || resp2.StatusCode == http.StatusNoContent {
			t.Fatalf("expected failure status, got %d", resp2.StatusCode)
		}

		// 验证数据库元数据仍然是原节点 origStorageID
		afterRecs, err := dm.GetFilesByPath("/fail-overwrite.bin")
		if err != nil || len(afterRecs) != 1 {
			t.Fatalf("expected original record in DB, got %v", afterRecs)
		}
		if afterRecs[0].StorageID != origStorageID {
			t.Fatalf("expected record to stay on original node %d, got %d", origStorageID, afterRecs[0].StorageID)
		}

		// 验证原节点上的旧文件依然存在且内容未变
		var origUp *testWebDAVUpstream
		if origStorageID == 1 {
			origUp = up1
		} else {
			origUp = up2
		}
		origUp.mu.Lock()
		res, ok := origUp.resources["/fail-overwrite.bin"]
		origUp.mu.Unlock()
		if !ok || string(res.content) != "original content" {
			t.Fatalf("expected original file content intact, got ok=%v", ok)
		}
	})
	// 4. 测试当数据库中存在历史跨节点同名重复记录时，PUT 不会死锁并顺利完成去重与覆写
	t.Run("historical duplicate files deduplicated during PUT without deadlock", func(t *testing.T) {
		dupPath := "/dav/historical-dup.txt"
		dm := srv.Cluster().DataManager()

		// 模拟历史脏数据：在物理节点和数据库中分别写入 ID 1 与 ID 2
		up1.setFile("/historical-dup.txt", []byte("old-up1"), time.Now().Add(-10*time.Minute))
		up2.setFile("/historical-dup.txt", []byte("new-up2"), time.Now().Add(-5*time.Minute))

		_ = dm.WithTx(context.Background(), func(tx *sql.Tx) error {
			_ = dm.PutFileTx(tx, data.FileRecord{StorageID: 1, Path: "/historical-dup.txt", Size: 7, IsDir: false, ModTime: time.Now().Add(-10 * time.Minute)})
			// 直接插入第二条跨节点记录以模拟历史并存数据
			_, err := tx.Exec(`INSERT INTO files (storage_id, path, parent_path, size, is_dir, mod_time) VALUES (2, '/historical-dup.txt', '/', 7, 0, ?);`, time.Now().Add(-5*time.Minute))
			return err
		})

		dupRecs, _ := dm.GetFilesByPath("/historical-dup.txt")
		if len(dupRecs) != 2 {
			t.Fatalf("expected 2 duplicate records before test, got %d", len(dupRecs))
		}

		// 发起 PUT 覆写，验证不会发生 PathLocker 死锁
		req, _ := http.NewRequest("PUT", ts.URL+dupPath, strings.NewReader("new-overwritten-content"))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("PUT failed: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 204 or 200, got %d", resp.StatusCode)
		}

		// 验证去重后数据库中仅剩 1 条记录
		finalRecs, err := dm.GetFilesByPath("/historical-dup.txt")
		if err != nil || len(finalRecs) != 1 {
			t.Fatalf("expected exactly 1 record after deduplicated PUT, got %v", finalRecs)
		}
	})
}
