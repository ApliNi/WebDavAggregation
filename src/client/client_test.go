package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	t.Run("valid URL", func(t *testing.T) {
		c, err := NewClient("http://127.0.0.1:5243/webdav", "admin", "12345", "", 3, 5*time.Second)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.user != "admin" || c.pass != "12345" {
			t.Errorf("expected user admin and pass 12345, got %s:%s", c.user, c.pass)
		}
		if c.retry != 3 {
			t.Errorf("expected retry 3, got %d", c.retry)
		}
		if c.Endpoint().String() != "http://127.0.0.1:5243/webdav" {
			t.Errorf("unexpected endpoint: %s", c.Endpoint().String())
		}
	})

	t.Run("empty URL", func(t *testing.T) {
		_, err := NewClient("", "admin", "12345", "", 3, 5*time.Second)
		if err == nil {
			t.Fatalf("expected error for empty URL")
		}
	})

	t.Run("valid proxy URLs", func(t *testing.T) {
		proxies := []string{
			"http://127.0.0.1:8080",
			"https://proxy.example.com:8443",
			"socks5://127.0.0.1:1080",
			"socks5://user:pass@127.0.0.1:1080",
		}
		for _, p := range proxies {
			c, err := NewClient("http://127.0.0.1:5243", "", "", p, 1, time.Second)
			if err != nil {
				t.Fatalf("expected proxy %s to be valid, got: %v", p, err)
			}
			if c.proxyURL != p {
				t.Errorf("expected proxy %s, got %s", p, c.proxyURL)
			}
		}
	})

	t.Run("invalid proxy URL", func(t *testing.T) {
		_, err := NewClient("http://127.0.0.1:5243", "", "", "http://invalid port:80", 1, time.Second)
		if err == nil {
			t.Fatalf("expected error for invalid proxy url")
		}
	})

	t.Run("transport connection pool settings", func(t *testing.T) {
		c, err := NewClient("http://127.0.0.1:5243", "", "", "", 3, 5*time.Second)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		transport, ok := c.httpClient.Transport.(*http.Transport)
		if !ok {
			t.Fatalf("expected *http.Transport, got %T", c.httpClient.Transport)
		}
		if transport.MaxIdleConnsPerHost != 50 {
			t.Errorf("expected MaxIdleConnsPerHost 50, got %d", transport.MaxIdleConnsPerHost)
		}
		if transport.MaxConnsPerHost != 100 {
			t.Errorf("expected MaxConnsPerHost 100, got %d", transport.MaxConnsPerHost)
		}
	})
}

func TestResolveURLAndParseHref(t *testing.T) {
	c, err := NewClient("http://127.0.0.1:5243/webdav", "admin", "12345", "", 1, time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	testCases := []struct {
		davPath     string
		expectedURL string
	}{
		{"/", "http://127.0.0.1:5243/webdav"},
		{"/file.txt", "http://127.0.0.1:5243/webdav/file.txt"},
		{"file.txt", "http://127.0.0.1:5243/webdav/file.txt"},
		{"/a/b/c.txt", "http://127.0.0.1:5243/webdav/a/b/c.txt"},
		{"/dir/", "http://127.0.0.1:5243/webdav/dir/"},
		{"/dir with space/file.txt", "http://127.0.0.1:5243/webdav/dir%20with%20space/file.txt"},
	}

	for _, tc := range testCases {
		resolved := c.resolveURL(tc.davPath)
		if resolved != tc.expectedURL {
			t.Errorf("resolveURL(%q) = %q; want %q", tc.davPath, resolved, tc.expectedURL)
		}
	}

	hrefCases := []struct {
		href         string
		expectedPath string
	}{
		{"/webdav", "/"},
		{"/webdav/", "/"},
		{"/webdav/file.txt", "/file.txt"},
		{"/webdav/sub/file.txt", "/sub/file.txt"},
		{"/webdav/sub/dir/", "/sub/dir"},
		{"http://127.0.0.1:5243/webdav/hello%20world.txt", "/hello world.txt"},
		{"/webdav/%E6%B5%8B%E8%AF%95.txt", "/测试.txt"},
	}

	for _, tc := range hrefCases {
		parsed := c.parseHref(tc.href)
		if parsed != tc.expectedPath {
			t.Errorf("parseHref(%q) = %q; want %q", tc.href, parsed, tc.expectedPath)
		}
	}
}

// mockWebDAVServer creates a test HTTP server with in-memory WebDAV-like filesystem.
type mockServer struct {
	user        string
	pass        string
	files       map[string][]byte
	dirs        map[string]bool
	quotaAvail  int64
	quotaUsed   int64
	optionsFail bool
}

func newMockServer(user, pass string) *mockServer {
	return &mockServer{
		user:       user,
		pass:       pass,
		files:      make(map[string][]byte),
		dirs:       map[string]bool{"/": true},
		quotaAvail: 10737418240, // 10 GB
		quotaUsed:  104857600,   // 100 MB
	}
}

func (m *mockServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Basic auth check
	if m.user != "" || m.pass != "" {
		u, p, ok := r.BasicAuth()
		if !ok || u != m.user || p != m.pass {
			w.Header().Set("WWW-Authenticate", `Basic realm="webdav"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	}

	reqPath := r.URL.Path
	if unescaped, err := url.PathUnescape(reqPath); err == nil {
		reqPath = unescaped
	}
	reqPath = path.Clean(reqPath)
	if !strings.HasPrefix(reqPath, "/") {
		reqPath = "/" + reqPath
	}

	switch r.Method {
	case "OPTIONS":
		if m.optionsFail {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("DAV", "1, 2")
		w.Header().Set("Allow", "OPTIONS, GET, HEAD, POST, PUT, DELETE, TRACE, COPY, MOVE, MKCOL, PROPFIND, PROPPATCH")
		w.WriteHeader(http.StatusOK)

	case "PROPFIND":
		bodyBytes, _ := io.ReadAll(r.Body)
		bodyStr := string(bodyBytes)
		depth := r.Header.Get("Depth")
		if depth == "" {
			depth = "infinity"
		}

		// Handle Quota query
		if strings.Contains(bodyStr, "quota-available-bytes") {
			xmlResp := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8" ?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>%s</D:href>
    <D:propstat>
      <D:prop>
        <D:quota-available-bytes>%d</D:quota-available-bytes>
        <D:quota-used-bytes>%d</D:quota-used-bytes>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>`, reqPath, m.quotaAvail, m.quotaUsed)
			w.Header().Set("Content-Type", "application/xml; charset=utf-8")
			w.WriteHeader(http.StatusMultiStatus)
			_, _ = w.Write([]byte(xmlResp))
			return
		}

		// Check existence
		isDir := m.dirs[reqPath]
		_, isFile := m.files[reqPath]
		if !isDir && !isFile {
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}

		var b strings.Builder
		b.WriteString(`<?xml version="1.0" encoding="utf-8" ?>` + "\n")
		b.WriteString(`<D:multistatus xmlns:D="DAV:">` + "\n")

		// Helper to render entry
		renderEntry := func(p string, dir bool, size int64) {
			b.WriteString("  <D:response>\n")
			href := p
			if dir && href != "/" && !strings.HasSuffix(href, "/") {
				href += "/"
			}
			b.WriteString(fmt.Sprintf("    <D:href>%s</D:href>\n", href))
			b.WriteString("    <D:propstat>\n")
			b.WriteString("      <D:prop>\n")
			if dir {
				b.WriteString("        <D:resourcetype><D:collection/></D:resourcetype>\n")
				b.WriteString("        <D:getcontenttype>httpd/unix-directory</D:getcontenttype>\n")
			} else {
				b.WriteString("        <D:resourcetype/>\n")
				b.WriteString(fmt.Sprintf("        <D:getcontentlength>%d</D:getcontentlength>\n", size))
				b.WriteString("        <D:getcontenttype>text/plain</D:getcontenttype>\n")
				b.WriteString("        <D:getetag>\"etag-mock\"</D:getetag>\n")
			}
			b.WriteString("        <D:getlastmodified>Thu, 01 Jan 2026 00:00:00 GMT</D:getlastmodified>\n")
			b.WriteString(fmt.Sprintf("        <D:displayname>%s</D:displayname>\n", path.Base(p)))
			b.WriteString("      </D:prop>\n")
			b.WriteString("      <D:status>HTTP/1.1 200 OK</D:status>\n")
			b.WriteString("    </D:propstat>\n")
			b.WriteString("  </D:response>\n")
		}

		if isDir {
			renderEntry(reqPath, true, 0)
			if depth == "1" || depth == "infinity" {
				for d := range m.dirs {
					if d == "/" || d == reqPath {
						continue
					}
					if path.Dir(d) == reqPath {
						renderEntry(d, true, 0)
					}
				}
				for f, data := range m.files {
					if path.Dir(f) == reqPath {
						renderEntry(f, false, int64(len(data)))
					}
				}
			}
		} else {
			renderEntry(reqPath, false, int64(len(m.files[reqPath])))
		}

		b.WriteString(`</D:multistatus>`)
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.WriteHeader(http.StatusMultiStatus)
		_, _ = w.Write([]byte(b.String()))

	case "GET":
		data, exists := m.files[reqPath]
		if !exists {
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}

		rangeHdr := r.Header.Get("Range")
		if rangeHdr != "" && strings.HasPrefix(rangeHdr, "bytes=") {
			parts := strings.Split(strings.TrimPrefix(rangeHdr, "bytes="), "-")
			start, _ := strconv.Atoi(parts[0])
			end := len(data) - 1
			if len(parts) > 1 && parts[1] != "" {
				if e, err := strconv.Atoi(parts[1]); err == nil && e < len(data) {
					end = e
				}
			}
			sub := data[start : end+1]
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
			w.Header().Set("Content-Length", strconv.Itoa(len(sub)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(sub)
			return
		}

		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)

	case "PUT":
		parentDir := path.Dir(reqPath)
		if !m.dirs[parentDir] {
			http.Error(w, "Conflict: parent directory does not exist", http.StatusConflict)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		m.files[reqPath] = body
		w.WriteHeader(http.StatusCreated)

	case "MKCOL":
		if m.dirs[reqPath] || m.files[reqPath] != nil {
			http.Error(w, "Method Not Allowed: directory already exists", http.StatusMethodNotAllowed)
			return
		}
		parentDir := path.Dir(reqPath)
		if !m.dirs[parentDir] {
			http.Error(w, "Conflict: parent does not exist", http.StatusConflict)
			return
		}
		m.dirs[reqPath] = true
		w.WriteHeader(http.StatusCreated)

	case "DELETE":
		if m.dirs[reqPath] {
			delete(m.dirs, reqPath)
			// Delete child items
			for d := range m.dirs {
				if strings.HasPrefix(d, reqPath+"/") {
					delete(m.dirs, d)
				}
			}
			for f := range m.files {
				if strings.HasPrefix(f, reqPath+"/") {
					delete(m.files, f)
				}
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if _, exists := m.files[reqPath]; exists {
			delete(m.files, reqPath)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.Error(w, "Not Found", http.StatusNotFound)

	case "COPY", "MOVE":
		destHeader := r.Header.Get("Destination")
		if destHeader == "" {
			http.Error(w, "Bad Request: missing Destination", http.StatusBadRequest)
			return
		}
		destURL, err := url.Parse(destHeader)
		if err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
		destPath := path.Clean(destURL.Path)

		data, exists := m.files[reqPath]
		if !exists && !m.dirs[reqPath] {
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}

		overwrite := r.Header.Get("Overwrite") != "F"
		destExists := m.files[destPath] != nil || m.dirs[destPath]
		if destExists && !overwrite {
			http.Error(w, "Precondition Failed", http.StatusPreconditionFailed)
			return
		}

		if exists {
			m.files[destPath] = data
			if r.Method == "MOVE" {
				delete(m.files, reqPath)
			}
		}

		if destExists {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusCreated)
		}

	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

func TestPing(t *testing.T) {
	mock := newMockServer("user", "pass")
	ts := httptest.NewServer(mock)
	defer ts.Close()

	ctx := context.Background()

	t.Run("ping success with OPTIONS", func(t *testing.T) {
		c, err := NewClient(ts.URL, "user", "pass", "", 1, 2*time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		if err := c.Ping(ctx); err != nil {
			t.Fatalf("Ping failed: %v", err)
		}
	})

	t.Run("ping fallback to PROPFIND when OPTIONS fails", func(t *testing.T) {
		mock.optionsFail = true
		defer func() { mock.optionsFail = false }()

		c, err := NewClient(ts.URL, "user", "pass", "", 1, 2*time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		if err := c.Ping(ctx); err != nil {
			t.Fatalf("Ping fallback failed: %v", err)
		}
	})

	t.Run("ping auth failure", func(t *testing.T) {
		c, err := NewClient(ts.URL, "wronguser", "wrongpass", "", 0, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		if err := c.Ping(ctx); err == nil {
			t.Fatalf("expected ping error on wrong auth, got nil")
		}
	})
}

func TestGetQuota(t *testing.T) {
	mock := newMockServer("user", "pass")
	mock.quotaAvail = 5000000000
	mock.quotaUsed = 1000000000
	ts := httptest.NewServer(mock)
	defer ts.Close()

	ctx := context.Background()
	c, err := NewClient(ts.URL, "user", "pass", "", 1, 2*time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	c.SetBaseBackoff(time.Millisecond)

	quota, err := c.GetQuota(ctx)
	if err != nil {
		t.Fatalf("GetQuota failed: %v", err)
	}

	if quota.AvailableBytes != 5000000000 {
		t.Errorf("expected AvailableBytes 5000000000, got %d", quota.AvailableBytes)
	}
	if quota.UsedBytes != 1000000000 {
		t.Errorf("expected UsedBytes 1000000000, got %d", quota.UsedBytes)
	}
	if quota.TotalBytes != 6000000000 {
		t.Errorf("expected TotalBytes 6000000000, got %d", quota.TotalBytes)
	}
}

func TestListAndWalk(t *testing.T) {
	mock := newMockServer("user", "pass")
	mock.dirs["/folder1"] = true
	mock.dirs["/folder1/nested"] = true
	mock.files["/folder1/file1.txt"] = []byte("hello file 1")
	mock.files["/folder1/nested/file2.txt"] = []byte("hello file 2")
	mock.files["/root_file.txt"] = []byte("root content")

	ts := httptest.NewServer(mock)
	defer ts.Close()

	ctx := context.Background()
	c, err := NewClient(ts.URL, "user", "pass", "", 1, 2*time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	c.SetBaseBackoff(time.Millisecond)

	t.Run("List Depth 0 on file", func(t *testing.T) {
		infos, err := c.List(ctx, "/root_file.txt", 0)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(infos) != 1 {
			t.Fatalf("expected 1 info, got %d", len(infos))
		}
		f := infos[0]
		if f.Path != "/root_file.txt" || f.Name != "root_file.txt" || f.IsDir || f.Size != int64(len("root content")) {
			t.Errorf("unexpected file info: %+v", f)
		}
	})
	t.Run("Stat on existing file and directory", func(t *testing.T) {
		fInfo, err := c.Stat(ctx, "/root_file.txt")
		if err != nil {
			t.Fatalf("Stat file failed: %v", err)
		}
		if fInfo.Path != "/root_file.txt" || fInfo.IsDir || fInfo.Size != int64(len("root content")) {
			t.Errorf("unexpected stat file info: %+v", fInfo)
		}

		dInfo, err := c.Stat(ctx, "/folder1")
		if err != nil {
			t.Fatalf("Stat dir failed: %v", err)
		}
		if dInfo.Path != "/folder1" || !dInfo.IsDir {
			t.Errorf("unexpected stat dir info: %+v", dInfo)
		}

		_, errNon := c.Stat(ctx, "/non_existent.bin")
		if !errors.Is(errNon, ErrNotFound) {
			t.Fatalf("expected ErrNotFound for non-existent resource, got: %v", errNon)
		}
	})
	t.Run("List Depth 1 on directory", func(t *testing.T) {
		infos, err := c.List(ctx, "/folder1", 1)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(infos) < 3 {
			t.Fatalf("expected at least 3 infos (self, nested, file1), got %d", len(infos))
		}
		names := make(map[string]bool)
		for _, info := range infos {
			names[info.Path] = true
		}
		if !names["/folder1"] || !names["/folder1/nested"] || !names["/folder1/file1.txt"] {
			t.Errorf("missing expected entries in list: %+v", names)
		}
	})

	t.Run("Walk entire tree", func(t *testing.T) {
		var visited []string
		err := c.Walk(ctx, "/", func(info FileInfo) error {
			visited = append(visited, info.Path)
			return nil
		})
		if err != nil {
			t.Fatalf("Walk failed: %v", err)
		}

		expected := []string{"/", "/folder1", "/folder1/nested", "/folder1/nested/file2.txt", "/folder1/file1.txt", "/root_file.txt"}
		for _, exp := range expected {
			found := false
			for _, v := range visited {
				if v == exp {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Walk missed expected path %s, visited: %v", exp, visited)
			}
		}
	})

	t.Run("Walk with SkipDir", func(t *testing.T) {
		var visited []string
		err := c.Walk(ctx, "/", func(info FileInfo) error {
			visited = append(visited, info.Path)
			if info.Path == "/folder1" {
				return filepath.SkipDir
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Walk failed: %v", err)
		}

		for _, v := range visited {
			if strings.HasPrefix(v, "/folder1/") {
				t.Errorf("path under /folder1 should have been skipped, but visited: %s", v)
			}
		}
	})
}

func TestGetAndPut(t *testing.T) {
	mock := newMockServer("user", "pass")
	ts := httptest.NewServer(mock)
	defer ts.Close()

	ctx := context.Background()
	c, err := NewClient(ts.URL, "user", "pass", "", 1, 2*time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	c.SetBaseBackoff(time.Millisecond)

	t.Run("Put file with automatic EnsureDir", func(t *testing.T) {
		content := []byte("streaming upload content")
		err := c.Put(ctx, "/deep/sub/dir/upload.txt", bytes.NewReader(content), int64(len(content)), "text/plain")
		if err != nil {
			t.Fatalf("Put with EnsureDir failed: %v", err)
		}

		// Verify directories were created
		if !mock.dirs["/deep"] || !mock.dirs["/deep/sub"] || !mock.dirs["/deep/sub/dir"] {
			t.Errorf("expected parent directories to be created, dirs: %+v", mock.dirs)
		}

		// Verify file content was uploaded
		if string(mock.files["/deep/sub/dir/upload.txt"]) != "streaming upload content" {
			t.Errorf("uploaded file content mismatch: %s", string(mock.files["/deep/sub/dir/upload.txt"]))
		}
	})

	t.Run("Get file stream", func(t *testing.T) {
		body, hdr, err := c.Get(ctx, "/deep/sub/dir/upload.txt", nil)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		defer body.Close()

		data, err := io.ReadAll(body)
		if err != nil {
			t.Fatalf("ReadAll failed: %v", err)
		}
		if string(data) != "streaming upload content" {
			t.Errorf("read data mismatch: %s", string(data))
		}
		if hdr.Get("Content-Type") != "text/plain" {
			t.Errorf("expected Content-Type text/plain, got %s", hdr.Get("Content-Type"))
		}
	})

	t.Run("Get file with Range header", func(t *testing.T) {
		h := make(http.Header)
		h.Set("Range", "bytes=0-8")
		body, hdr, err := c.Get(ctx, "/deep/sub/dir/upload.txt", h)
		if err != nil {
			t.Fatalf("Get with Range failed: %v", err)
		}
		defer body.Close()

		data, err := io.ReadAll(body)
		if err != nil {
			t.Fatalf("ReadAll failed: %v", err)
		}
		if string(data) != "streaming" {
			t.Errorf("read range data mismatch: got %q, want 'streaming'", string(data))
		}
		if !strings.Contains(hdr.Get("Content-Range"), "bytes 0-8") {
			t.Errorf("expected Content-Range header, got: %s", hdr.Get("Content-Range"))
		}
	})

	t.Run("Get non-existent file", func(t *testing.T) {
		_, _, err := c.Get(ctx, "/non_existent.txt", nil)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got: %v", err)
		}
	})
}

func TestDelete(t *testing.T) {
	mock := newMockServer("user", "pass")
	mock.files["/file.txt"] = []byte("data")
	ts := httptest.NewServer(mock)
	defer ts.Close()

	ctx := context.Background()
	c, err := NewClient(ts.URL, "user", "pass", "", 1, time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	c.SetBaseBackoff(time.Millisecond)

	if err := c.Delete(ctx, "/file.txt"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if mock.files["/file.txt"] != nil {
		t.Errorf("file still exists after delete")
	}

	// Delete non-existent file
	if err := c.Delete(ctx, "/file.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for deleted file, got: %v", err)
	}
}

func TestCopyAndMove(t *testing.T) {
	mock := newMockServer("user", "pass")
	mock.files["/src.txt"] = []byte("copy source")
	ts := httptest.NewServer(mock)
	defer ts.Close()

	ctx := context.Background()
	c, err := NewClient(ts.URL, "user", "pass", "", 1, time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	c.SetBaseBackoff(time.Millisecond)

	t.Run("Copy resource", func(t *testing.T) {
		if err := c.Copy(ctx, "/src.txt", "/dst.txt", true); err != nil {
			t.Fatalf("Copy failed: %v", err)
		}
		if string(mock.files["/dst.txt"]) != "copy source" {
			t.Errorf("destination file content mismatch: %s", string(mock.files["/dst.txt"]))
		}
		if string(mock.files["/src.txt"]) != "copy source" {
			t.Errorf("source file should still exist after copy")
		}
	})

	t.Run("Copy with overwrite=false on existing destination", func(t *testing.T) {
		err := c.Copy(ctx, "/src.txt", "/dst.txt", false)
		if !errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("expected ErrPreconditionFailed, got: %v", err)
		}
	})

	t.Run("Move resource", func(t *testing.T) {
		if err := c.Move(ctx, "/dst.txt", "/moved.txt", true); err != nil {
			t.Fatalf("Move failed: %v", err)
		}
		if string(mock.files["/moved.txt"]) != "copy source" {
			t.Errorf("moved file content mismatch: %s", string(mock.files["/moved.txt"]))
		}
		if mock.files["/dst.txt"] != nil {
			t.Errorf("original file should not exist after move")
		}
	})
}

func TestMkcolAndEnsureDir(t *testing.T) {
	mock := newMockServer("user", "pass")
	ts := httptest.NewServer(mock)
	defer ts.Close()

	ctx := context.Background()
	c, err := NewClient(ts.URL, "user", "pass", "", 1, time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	c.SetBaseBackoff(time.Millisecond)

	t.Run("Mkcol success and already exists", func(t *testing.T) {
		if err := c.Mkcol(ctx, "/dir1"); err != nil {
			t.Fatalf("Mkcol failed: %v", err)
		}
		// Second call returns ErrAlreadyExists
		if err := c.Mkcol(ctx, "/dir1"); !errors.Is(err, ErrAlreadyExists) {
			t.Fatalf("expected ErrAlreadyExists, got: %v", err)
		}
	})

	t.Run("EnsureDir creates parent levels", func(t *testing.T) {
		if err := c.EnsureDir(ctx, "/level1/level2/level3"); err != nil {
			t.Fatalf("EnsureDir failed: %v", err)
		}
		if !mock.dirs["/level1"] || !mock.dirs["/level1/level2"] || !mock.dirs["/level1/level2/level3"] {
			t.Fatalf("expected all levels created, got: %+v", mock.dirs)
		}

		// Calling EnsureDir on already existing dir succeeds
		if err := c.EnsureDir(ctx, "/level1/level2/level3"); err != nil {
			t.Fatalf("EnsureDir on existing dir should succeed: %v", err)
		}
	})
}
type testReadCloser struct {
	io.Reader
	onClose func()
	onEOF   func()
}

func (tr *testReadCloser) Read(p []byte) (int, error) {
	n, err := tr.Reader.Read(p)
	if errors.Is(err, io.EOF) && tr.onEOF != nil {
		tr.onEOF()
	}
	return n, err
}

func (tr *testReadCloser) Close() error {
	if tr.onClose != nil {
		tr.onClose()
	}
	return nil
}
func TestRetryMechanism(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n <= 2 {
			http.Error(w, "Temporary Server Error", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("success after retry"))
	}))
	defer server.Close()

	ctx := context.Background()

	t.Run("retry succeeds after 503 errors", func(t *testing.T) {
		atomic.StoreInt32(&attempts, 0)
		c, err := NewClient(server.URL, "", "", "", 3, 2*time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		resp, err := c.doWithRetry(ctx, func() (*http.Response, error) {
			req, _ := http.NewRequestWithContext(ctx, "GET", server.URL, nil)
			return http.DefaultClient.Do(req)
		})
		if err != nil {
			t.Fatalf("doWithRetry failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected status 200, got %d", resp.StatusCode)
		}
		if atomic.LoadInt32(&attempts) != 3 {
			t.Errorf("expected 3 attempts, got %d", atomic.LoadInt32(&attempts))
		}
	})

	t.Run("retry exhausted on persistent 500", func(t *testing.T) {
		atomic.StoreInt32(&attempts, 0)
		c, err := NewClient(server.URL, "", "", "", 1, 2*time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		resp, err := c.doWithRetry(ctx, func() (*http.Response, error) {
			req, _ := http.NewRequestWithContext(ctx, "GET", server.URL, nil)
			return http.DefaultClient.Do(req)
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("expected 503 status code when retries exhausted, got %d", resp.StatusCode)
		}
		if atomic.LoadInt32(&attempts) != 2 { // attempt 0 + 1 retry
			t.Errorf("expected 2 attempts, got %d", atomic.LoadInt32(&attempts))
		}
	})

	t.Run("do not retry on 404 client errors", func(t *testing.T) {
		var reqCount int32
		s404 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&reqCount, 1)
			http.Error(w, "Not Found", http.StatusNotFound)
		}))
		defer s404.Close()

		c, err := NewClient(s404.URL, "", "", "", 3, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		resp, err := c.doWithRetry(ctx, func() (*http.Response, error) {
			req, _ := http.NewRequestWithContext(ctx, "GET", s404.URL, nil)
			return http.DefaultClient.Do(req)
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer resp.Body.Close()

		if atomic.LoadInt32(&reqCount) != 1 {
			t.Errorf("expected exactly 1 attempt on 404, got %d", atomic.LoadInt32(&reqCount))
		}
	})

	t.Run("respect context cancellation", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately

		c, err := NewClient(server.URL, "", "", "", 3, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Second)

		_, err = c.doWithRetry(cancelCtx, func() (*http.Response, error) {
			req, _ := http.NewRequestWithContext(cancelCtx, "GET", server.URL, nil)
			return http.DefaultClient.Do(req)
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled error, got: %v", err)
		}
	})

	t.Run("retry succeeds after 429 with Retry-After header", func(t *testing.T) {
		var reqAttempts int32
		s429 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&reqAttempts, 1)
			if n <= 2 {
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte("Rate limit exceeded"))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}))
		defer s429.Close()

		c, err := NewClient(s429.URL, "", "", "", 3, 2*time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		resp, err := c.doWithRetry(ctx, func() (*http.Response, error) {
			req, _ := http.NewRequestWithContext(ctx, "GET", s429.URL, nil)
			return http.DefaultClient.Do(req)
		})
		if err != nil {
			t.Fatalf("doWithRetry failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
		if atomic.LoadInt32(&reqAttempts) != 3 {
			t.Errorf("expected 3 attempts, got %d", atomic.LoadInt32(&reqAttempts))
		}
	})

	t.Run("retry succeeds after 429 with exponential backoff and jitter", func(t *testing.T) {
		var reqAttempts int32
		s429 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&reqAttempts, 1)
			if n == 1 {
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte("Rate limit exceeded"))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}))
		defer s429.Close()

		c, err := NewClient(s429.URL, "", "", "", 2, 2*time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		resp, err := c.doWithRetry(ctx, func() (*http.Response, error) {
			req, _ := http.NewRequestWithContext(ctx, "GET", s429.URL, nil)
			return http.DefaultClient.Do(req)
		})
		if err != nil {
			t.Fatalf("doWithRetry failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
		if atomic.LoadInt32(&reqAttempts) != 2 {
			t.Errorf("expected 2 attempts, got %d", atomic.LoadInt32(&reqAttempts))
		}
	})

	t.Run("retry succeeds after 429 with invalid Retry-After header", func(t *testing.T) {
		var reqAttempts int32
		s429 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&reqAttempts, 1)
			if n == 1 {
				w.Header().Set("Retry-After", "not-a-number")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte("Rate limit exceeded"))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}))
		defer s429.Close()

		c, err := NewClient(s429.URL, "", "", "", 2, 2*time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		resp, err := c.doWithRetry(ctx, func() (*http.Response, error) {
			req, _ := http.NewRequestWithContext(ctx, "GET", s429.URL, nil)
			return http.DefaultClient.Do(req)
		})
		if err != nil {
			t.Fatalf("doWithRetry failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
		if atomic.LoadInt32(&reqAttempts) != 2 {
			t.Errorf("expected 2 attempts, got %d", atomic.LoadInt32(&reqAttempts))
		}
	})

	t.Run("retry exhausted on persistent 429", func(t *testing.T) {
		var reqAttempts int32
		s429 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&reqAttempts, 1)
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("Rate limit exceeded"))
		}))
		defer s429.Close()

		c, err := NewClient(s429.URL, "", "", "", 2, 2*time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		resp, err := c.doWithRetry(ctx, func() (*http.Response, error) {
			req, _ := http.NewRequestWithContext(ctx, "GET", s429.URL, nil)
			return http.DefaultClient.Do(req)
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusTooManyRequests {
			t.Errorf("expected 429 status code when retries exhausted, got %d", resp.StatusCode)
		}
		if atomic.LoadInt32(&reqAttempts) != 3 {
			t.Errorf("expected 3 attempts, got %d", atomic.LoadInt32(&reqAttempts))
		}
	})

	t.Run("respect context cancellation during 429 retry backoff", func(t *testing.T) {
		s429 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "10")
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		defer s429.Close()

		cancelCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		c, err := NewClient(s429.URL, "", "", "", 2, 2*time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		_, err = c.doWithRetry(cancelCtx, func() (*http.Response, error) {
			req, _ := http.NewRequestWithContext(cancelCtx, "GET", s429.URL, nil)
			return http.DefaultClient.Do(req)
		})
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context deadline or canceled error, got: %v", err)
		}
	})

	t.Run("retry respects Retry-After seconds delay", func(t *testing.T) {
		var reqAttempts int32
		s429 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&reqAttempts, 1)
			if n == 1 {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer s429.Close()

		c, err := NewClient(s429.URL, "", "", "", 2, 5*time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}

		start := time.Now()
		resp, err := c.doWithRetry(ctx, func() (*http.Response, error) {
			req, _ := http.NewRequestWithContext(ctx, "GET", s429.URL, nil)
			return http.DefaultClient.Do(req)
		})
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("doWithRetry failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
		if elapsed < 900*time.Millisecond {
			t.Errorf("expected Retry-After delay around 1s, elapsed %v", elapsed)
		}
	})

	t.Run("verify body discarded and closed before retry on 429", func(t *testing.T) {
		attempt := 0
		c, err := NewClient("http://127.0.0.1:5243", "", "", "", 2, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		var firstBodyClosed, firstBodyDrained bool

		resp, err := c.doWithRetry(ctx, func() (*http.Response, error) {
			attempt++
			if attempt == 1 {
				readCloser := &testReadCloser{
					Reader: bytes.NewReader([]byte("test 429 body")),
					onClose: func() {
						firstBodyClosed = true
					},
					onEOF: func() {
						firstBodyDrained = true
					},
				}
				return &http.Response{
					StatusCode: http.StatusTooManyRequests,
					Header: http.Header{
						"Retry-After": []string{"0"},
					},
					Body: readCloser,
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte("ok"))),
			}, nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer resp.Body.Close()

		if !firstBodyDrained {
			t.Errorf("expected first response body to be drained/discarded")
		}
		if !firstBodyClosed {
			t.Errorf("expected first response body to be closed before retry")
		}
	})
}
func TestRetryAfterDelay(t *testing.T) {
	c, err := NewClient("http://127.0.0.1:5243", "", "", "", 3, time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	c.SetBaseBackoff(10 * time.Millisecond)

	t.Run("valid seconds string", func(t *testing.T) {
		resp := &http.Response{
			Header: http.Header{
				"Retry-After": []string{"2"},
			},
		}
		delay := c.retryAfterDelay(resp, 0)
		if delay != 2*time.Second {
			t.Errorf("expected 2s, got %v", delay)
		}
	})

	t.Run("zero seconds string", func(t *testing.T) {
		resp := &http.Response{
			Header: http.Header{
				"Retry-After": []string{"0"},
			},
		}
		delay := c.retryAfterDelay(resp, 0)
		if delay != 0 {
			t.Errorf("expected 0, got %v", delay)
		}
	})

	t.Run("invalid seconds string falls back to exponential backoff", func(t *testing.T) {
		resp := &http.Response{
			Header: http.Header{
				"Retry-After": []string{"not-a-number"},
			},
		}
		for attempt := 0; attempt < 4; attempt++ {
			delay := c.retryAfterDelay(resp, attempt)
			base := c.baseBackoff * (1 << attempt)
			if delay < base || delay > base+50*time.Millisecond {
				t.Errorf("attempt %d: expected delay in [%v, %v], got %v", attempt, base, base+50*time.Millisecond, delay)
			}
		}
	})

	t.Run("negative seconds string falls back to exponential backoff", func(t *testing.T) {
		resp := &http.Response{
			Header: http.Header{
				"Retry-After": []string{"-5"},
			},
		}
		delay := c.retryAfterDelay(resp, 1)
		base := c.baseBackoff * 2
		if delay < base || delay > base+50*time.Millisecond {
			t.Errorf("expected delay in [%v, %v], got %v", base, base+50*time.Millisecond, delay)
		}
	})

	t.Run("missing header falls back to exponential backoff", func(t *testing.T) {
		resp := &http.Response{
			Header: http.Header{},
		}
		for attempt := 0; attempt < 3; attempt++ {
			delay := c.retryAfterDelay(resp, attempt)
			base := c.baseBackoff * (1 << attempt)
			if delay < base || delay > base+50*time.Millisecond {
				t.Errorf("attempt %d: expected delay in [%v, %v], got %v", attempt, base, base+50*time.Millisecond, delay)
			}
		}
	})

	t.Run("nil response falls back to exponential backoff", func(t *testing.T) {
		delay := c.retryAfterDelay(nil, 0)
		if delay < c.baseBackoff || delay > c.baseBackoff+50*time.Millisecond {
			t.Errorf("expected delay in [%v, %v], got %v", c.baseBackoff, c.baseBackoff+50*time.Millisecond, delay)
		}
	})

	t.Run("HTTP date format support", func(t *testing.T) {
		future := time.Now().UTC().Add(5 * time.Second)
		resp := &http.Response{
			Header: http.Header{
				"Retry-After": []string{future.Format(http.TimeFormat)},
			},
		}
		delay := c.retryAfterDelay(resp, 0)
		if delay <= 0 || delay > 6*time.Second {
			t.Errorf("expected delay around 4-5s, got %v", delay)
		}
	})
}

func TestParseWebDAVTime(t *testing.T) {
	testCases := []struct {
		input    string
		expected bool
	}{
		{"", false},
		{"invalid date string", false},
		{"Mon, 02 Jan 2006 15:04:05 GMT", true},
		{"Monday, 02-Jan-06 15:04:05 GMT", true},
		{"Mon Jan  2 15:04:05 2006", true},
		{"2006-01-02T15:04:05Z", true},
		{"2006-01-02T15:04:05+08:00", true},
		{"2006-01-02T15:04:05-05:00", true},
		{"2006-01-02T15:04:05.123456Z", true},
		{"2006-01-02T15:04:05", true},
		{"2006-01-02 15:04:05", true},
		{"2006-01-02 15:04:05.123456", true},
		{"2006-01-02 15:04:05 +0800", true},
		{"Mon, 02 Jan 2006 15:04:05 -0700", true},
		{"\"2006-01-02 15:04:05\"", true},
	}

	for _, tc := range testCases {
		res := parseWebDAVTime(tc.input)
		if tc.expected {
			if res.IsZero() {
				t.Errorf("expected non-zero time for input %q", tc.input)
			}
			if res.Location() != time.UTC {
				t.Errorf("expected UTC location for input %q, got: %v", tc.input, res.Location())
			}
		} else {
			if !res.IsZero() {
				t.Errorf("expected zero time for input %q, got: %v", tc.input, res)
			}
		}
	}
}

func TestGetQuotaErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("server returns 500", func(t *testing.T) {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "server error", http.StatusInternalServerError)
		}))
		defer s.Close()

		c, err := NewClient(s.URL, "", "", "", 0, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		_, err = c.GetQuota(ctx)
		if err == nil {
			t.Fatalf("expected error on 500 response")
		}
	})

	t.Run("server returns invalid XML", func(t *testing.T) {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusMultiStatus)
			_, _ = w.Write([]byte("not valid xml <<<"))
		}))
		defer s.Close()

		c, err := NewClient(s.URL, "", "", "", 0, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		_, err = c.GetQuota(ctx)
		if err == nil {
			t.Fatalf("expected error on invalid XML")
		}
	})

	t.Run("server returns XML without quota properties", func(t *testing.T) {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusMultiStatus)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="utf-8" ?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/</D:href>
    <D:propstat>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>`))
		}))
		defer s.Close()

		c, err := NewClient(s.URL, "", "", "", 0, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		_, err = c.GetQuota(ctx)
		if !errors.Is(err, ErrQuotaNotSupported) {
			t.Fatalf("expected ErrQuotaNotSupported, got: %v", err)
		}
	})
}

func TestListErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("List returns 404", func(t *testing.T) {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Not Found", http.StatusNotFound)
		}))
		defer s.Close()

		c, err := NewClient(s.URL, "", "", "", 0, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		_, err = c.List(ctx, "/missing", 0)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("List returns invalid XML", func(t *testing.T) {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusMultiStatus)
			_, _ = w.Write([]byte("bad xml"))
		}))
		defer s.Close()

		c, err := NewClient(s.URL, "", "", "", 0, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		_, err = c.List(ctx, "/dir", 1)
		if err == nil {
			t.Fatalf("expected error on invalid xml")
		}
	})

	t.Run("List with depth infinity", func(t *testing.T) {
		var receivedDepth string
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedDepth = r.Header.Get("Depth")
			w.WriteHeader(http.StatusMultiStatus)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="utf-8" ?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/dir/</D:href>
    <D:propstat>
      <D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>`))
		}))
		defer s.Close()

		c, err := NewClient(s.URL, "", "", "", 0, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		_, err = c.List(ctx, "/dir", -1)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if receivedDepth != "infinity" {
			t.Errorf("expected Depth header infinity, got %s", receivedDepth)
		}
	})
}

func TestWalkEdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("Walk on non-existent path", func(t *testing.T) {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Not Found", http.StatusNotFound)
		}))
		defer s.Close()

		c, err := NewClient(s.URL, "", "", "", 0, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		err = c.Walk(ctx, "/missing", func(info FileInfo) error {
			return nil
		})
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("Walk on single file", func(t *testing.T) {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusMultiStatus)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="utf-8" ?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/file.txt</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype/>
        <D:getcontentlength>42</D:getcontentlength>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>`))
		}))
		defer s.Close()

		c, err := NewClient(s.URL, "", "", "", 0, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		var count int
		err = c.Walk(ctx, "/file.txt", func(info FileInfo) error {
			count++
			if info.Path != "/file.txt" || info.IsDir || info.Size != 42 {
				t.Errorf("unexpected file info: %+v", info)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Walk failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1 file visited, got %d", count)
		}
	})

	t.Run("Walk callback error aborts traversal", func(t *testing.T) {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusMultiStatus)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="utf-8" ?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/</D:href>
    <D:propstat>
      <D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>`))
		}))
		defer s.Close()

		c, err := NewClient(s.URL, "", "", "", 0, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		customErr := errors.New("abort walk")
		err = c.Walk(ctx, "/", func(info FileInfo) error {
			return customErr
		})
		if !errors.Is(err, customErr) {
			t.Fatalf("expected customErr, got: %v", err)
		}
	})
}

func TestPutEdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("Put with nil body", func(t *testing.T) {
		var receivedLength int64
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedLength = r.ContentLength
			w.WriteHeader(http.StatusCreated)
		}))
		defer s.Close()

		c, err := NewClient(s.URL, "", "", "", 0, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		err = c.Put(ctx, "/empty.txt", nil, 0, "")
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if receivedLength != 0 {
			t.Errorf("expected content length 0, got %d", receivedLength)
		}
	})

	t.Run("Put with server 500 error", func(t *testing.T) {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "server error", http.StatusInternalServerError)
		}))
		defer s.Close()

		c, err := NewClient(s.URL, "", "", "", 0, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		err = c.Put(ctx, "/fail.txt", bytes.NewReader([]byte("test")), 4, "text/plain")
		if err == nil {
			t.Fatalf("expected error on 500 response")
		}
	})

	t.Run("Put with seekable body retry", func(t *testing.T) {
		var attempts int32
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&attempts, 1)
			if n == 1 {
				http.Error(w, "temporary error", http.StatusBadGateway)
				return
			}
			body, _ := io.ReadAll(r.Body)
			if string(body) != "seekable content" {
				http.Error(w, "corrupted body", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
		}))
		defer s.Close()

		c, err := NewClient(s.URL, "", "", "", 2, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		content := bytes.NewReader([]byte("seekable content"))
		err = c.Put(ctx, "/retry.txt", content, int64(content.Len()), "text/plain")
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if atomic.LoadInt32(&attempts) != 2 {
			t.Errorf("expected 2 attempts, got %d", atomic.LoadInt32(&attempts))
		}
	})

	t.Run("Put with non-seekable streaming body does not retry on 500", func(t *testing.T) {
		var attempts int32
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&attempts, 1)
			http.Error(w, "server error", http.StatusInternalServerError)
		}))
		defer s.Close()

		c, err := NewClient(s.URL, "", "", "", 3, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		// Create a purely streaming reader that does NOT implement io.ReadSeeker
		stream := io.NopCloser(bytes.NewBufferString("stream content"))
		err = c.Put(ctx, "/stream_fail.txt", stream, 14, "text/plain")
		if err == nil {
			t.Fatalf("expected error on 500 response")
		}
		// Because body is non-seekable, it must NOT retry with a drained reader
		if n := atomic.LoadInt32(&attempts); n != 1 {
			t.Errorf("expected 1 attempt without retry for non-seekable body, got %d", n)
		}
	})

	t.Run("Put with non-seekable streaming body succeeds", func(t *testing.T) {
		var receivedBody string
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			receivedBody = string(b)
			w.WriteHeader(http.StatusCreated)
		}))
		defer s.Close()

		c, err := NewClient(s.URL, "", "", "", 2, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}

		stream := io.NopCloser(bytes.NewBufferString("hello streaming"))
		err = c.Put(ctx, "/stream_ok.txt", stream, 15, "text/plain")
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if receivedBody != "hello streaming" {
			t.Errorf("expected 'hello streaming', got %q", receivedBody)
		}
	})
}

func TestEnsureDirRoot(t *testing.T) {
	c, _ := NewClient("http://127.0.0.1:5243", "", "", "", 0, time.Second)
	if err := c.EnsureDir(context.Background(), "/"); err != nil {
		t.Fatalf("EnsureDir(/) should succeed immediately: %v", err)
	}
	if err := c.EnsureDir(context.Background(), ""); err != nil {
		t.Fatalf("EnsureDir(\"\") should succeed immediately: %v", err)
	}
}

func TestSetHTTPClient(t *testing.T) {
	c, _ := NewClient("http://127.0.0.1:5243", "", "", "", 0, time.Second)
	custom := &http.Client{Timeout: 42 * time.Second}
	c.SetHTTPClient(custom)
	if c.httpClient != custom {
		t.Fatalf("expected custom http client to be set")
	}
}
