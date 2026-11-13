package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// nonSeekableReader wraps an io.Reader to ensure it does not implement io.ReadSeeker.
type nonSeekableReader struct {
	r io.Reader
}

func (n *nonSeekableReader) Read(p []byte) (int, error) {
	return n.r.Read(p)
}

func TestPutRawVsPut_MethodsSent(t *testing.T) {
	ctx := context.Background()

	var mu sync.Mutex
	var recordedRequests []struct {
		Method string
		Path   string
	}

	propfindResp := `<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/nested/dir</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/></D:resourcetype>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>`

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		recordedRequests = append(recordedRequests, struct {
			Method string
			Path   string
		}{Method: r.Method, Path: r.URL.Path})
		mu.Unlock()

		switch r.Method {
		case "PROPFIND":
			w.Header().Set("Content-Type", "application/xml; charset=utf-8")
			w.WriteHeader(http.StatusMultiStatus)
			_, _ = w.Write([]byte(propfindResp))
		case http.MethodPut:
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer s.Close()

	c, err := NewClient(s.URL, "", "", "", 0, time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	// 1. Calling Put on a nested path should trigger PROPFIND (EnsureDir) + PUT
	mu.Lock()
	recordedRequests = nil
	mu.Unlock()

	data := []byte("put test data")
	err = c.Put(ctx, "/nested/dir/file.txt", bytes.NewReader(data), int64(len(data)), "text/plain")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	mu.Lock()
	putCalls := append([]struct {
		Method string
		Path   string
	}(nil), recordedRequests...)
	mu.Unlock()

	if len(putCalls) != 2 {
		t.Fatalf("expected 2 requests for Put (PROPFIND + PUT), got %d: %+v", len(putCalls), putCalls)
	}
	if putCalls[0].Method != "PROPFIND" {
		t.Errorf("expected first call to be PROPFIND, got %s", putCalls[0].Method)
	}
	if putCalls[1].Method != http.MethodPut {
		t.Errorf("expected second call to be PUT, got %s", putCalls[1].Method)
	}
	if putCalls[1].Path != "/nested/dir/file.txt" {
		t.Errorf("expected PUT path /nested/dir/file.txt, got %s", putCalls[1].Path)
	}

	// 2. Calling PutRaw on the same nested path should ONLY send PUT (no PROPFIND)
	mu.Lock()
	recordedRequests = nil
	mu.Unlock()

	dataRaw := []byte("put raw test data")
	err = c.PutRaw(ctx, "/nested/dir/file2.txt", bytes.NewReader(dataRaw), int64(len(dataRaw)), "text/plain")
	if err != nil {
		t.Fatalf("PutRaw failed: %v", err)
	}

	mu.Lock()
	putRawCalls := append([]struct {
		Method string
		Path   string
	}(nil), recordedRequests...)
	mu.Unlock()

	if len(putRawCalls) != 1 {
		t.Fatalf("expected exactly 1 request for PutRaw (PUT only), got %d: %+v", len(putRawCalls), putRawCalls)
	}
	if putRawCalls[0].Method != http.MethodPut {
		t.Errorf("expected call to be PUT, got %s", putRawCalls[0].Method)
	}
	if putRawCalls[0].Path != "/nested/dir/file2.txt" {
		t.Errorf("expected PUT path /nested/dir/file2.txt, got %s", putRawCalls[0].Path)
	}
}

func TestPutRaw_StreamingContent(t *testing.T) {
	ctx := context.Background()

	t.Run("large stream content integrity", func(t *testing.T) {
		const size = 256 * 1024 // 256 KB
		originalData := make([]byte, size)
		_, err := rand.Read(originalData)
		if err != nil {
			t.Fatalf("failed to generate random data: %v", err)
		}

		var receivedData []byte
		var receivedContentLength int64
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPut {
				http.Error(w, "bad method", http.StatusMethodNotAllowed)
				return
			}
			receivedContentLength = r.ContentLength
			var readErr error
			receivedData, readErr = io.ReadAll(r.Body)
			if readErr != nil {
				http.Error(w, "read error", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusCreated)
		}))
		defer s.Close()

		c, err := NewClient(s.URL, "", "", "", 1, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}

		err = c.PutRaw(ctx, "/stream.bin", bytes.NewReader(originalData), size, "application/octet-stream")
		if err != nil {
			t.Fatalf("PutRaw failed: %v", err)
		}

		if receivedContentLength != size {
			t.Errorf("expected Content-Length %d, got %d", size, receivedContentLength)
		}
		if !bytes.Equal(receivedData, originalData) {
			t.Errorf("received payload does not match uploaded payload")
		}
	})

	t.Run("nil body", func(t *testing.T) {
		var receivedLength int64
		var receivedBytes []byte
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedLength = r.ContentLength
			receivedBytes, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusCreated)
		}))
		defer s.Close()

		c, err := NewClient(s.URL, "", "", "", 0, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}

		err = c.PutRaw(ctx, "/empty.txt", nil, 0, "")
		if err != nil {
			t.Fatalf("PutRaw with nil body failed: %v", err)
		}
		if receivedLength != 0 {
			t.Errorf("expected content length 0, got %d", receivedLength)
		}
		if len(receivedBytes) != 0 {
			t.Errorf("expected empty body, got %d bytes", len(receivedBytes))
		}
	})

	t.Run("seekable body retry after temporary 503 error", func(t *testing.T) {
		var attempts int32
		expectedContent := "rewindable seekable data"
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&attempts, 1)
			if n == 1 {
				http.Error(w, "temporary unavailable", http.StatusServiceUnavailable)
				return
			}
			body, _ := io.ReadAll(r.Body)
			if string(body) != expectedContent {
				http.Error(w, "corrupted body on retry", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer s.Close()

		c, err := NewClient(s.URL, "", "", "", 2, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		reader := bytes.NewReader([]byte(expectedContent))
		err = c.PutRaw(ctx, "/retry.txt", reader, int64(len(expectedContent)), "text/plain")
		if err != nil {
			t.Fatalf("PutRaw retry failed: %v", err)
		}
		if atomic.LoadInt32(&attempts) != 2 {
			t.Errorf("expected 2 attempts, got %d", atomic.LoadInt32(&attempts))
		}
	})

	t.Run("non-seekable body should not retry on 503 error", func(t *testing.T) {
		var attempts int32
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&attempts, 1)
			http.Error(w, "temporary error", http.StatusServiceUnavailable)
		}))
		defer s.Close()

		c, err := NewClient(s.URL, "", "", "", 3, time.Second)
		if err != nil {
			t.Fatalf("NewClient failed: %v", err)
		}
		c.SetBaseBackoff(time.Millisecond)

		nonSeekable := &nonSeekableReader{r: strings.NewReader("stream data")}
		err = c.PutRaw(ctx, "/no-retry.txt", nonSeekable, 11, "text/plain")
		if err == nil {
			t.Fatalf("expected error for non-seekable body on 503")
		}
		if atomic.LoadInt32(&attempts) != 1 {
			t.Errorf("expected exactly 1 attempt for non-seekable body, got %d", atomic.LoadInt32(&attempts))
		}
	})
}

func TestPutRaw_Headers(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name                string
		contentTypeInput    string
		sizeInput           int64
		expectedContentType string
		expectedLength      int64
	}{
		{
			name:                "custom content type and explicit size",
			contentTypeInput:    "application/json",
			sizeInput:           42,
			expectedContentType: "application/json",
			expectedLength:      42,
		},
		{
			name:                "empty content type defaults to application/octet-stream",
			contentTypeInput:    "",
			sizeInput:           10,
			expectedContentType: "application/octet-stream",
			expectedLength:      10,
		},
		{
			name:                "negative size does not set Content-Length",
			contentTypeInput:    "text/plain",
			sizeInput:           -1,
			expectedContentType: "text/plain",
			expectedLength:      -1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var recordedContentType string
			var recordedLength int64
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				recordedContentType = r.Header.Get("Content-Type")
				recordedLength = r.ContentLength
				w.WriteHeader(http.StatusCreated)
			}))
			defer s.Close()

			c, err := NewClient(s.URL, "", "", "", 0, time.Second)
			if err != nil {
				t.Fatalf("NewClient failed: %v", err)
			}

			payload := bytes.Repeat([]byte("a"), 64)
			var r io.Reader
			if tc.sizeInput >= 0 {
				r = bytes.NewReader(payload[:tc.sizeInput])
			} else {
				// use nonSeekable to test chunked / negative size
				r = &nonSeekableReader{r: bytes.NewReader(payload)}
			}

			err = c.PutRaw(ctx, "/header-test.dat", r, tc.sizeInput, tc.contentTypeInput)
			if err != nil {
				t.Fatalf("PutRaw failed: %v", err)
			}

			if recordedContentType != tc.expectedContentType {
				t.Errorf("expected Content-Type %q, got %q", tc.expectedContentType, recordedContentType)
			}
			if tc.expectedLength >= 0 && recordedLength != tc.expectedLength {
				t.Errorf("expected Content-Length %d, got %d", tc.expectedLength, recordedLength)
			}
		})
	}
}

func TestPutRaw_StatusCodes(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		code      int
		shouldErr bool
	}{
		{http.StatusOK, false},
		{http.StatusCreated, false},
		{http.StatusNoContent, false},
		{http.StatusBadRequest, true},
		{http.StatusUnauthorized, true},
		{http.StatusForbidden, true},
		{http.StatusNotFound, true},
		{http.StatusConflict, true},
		{http.StatusInternalServerError, true},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("status_%d", tc.code), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.code)
			}))
			defer s.Close()

			c, err := NewClient(s.URL, "", "", "", 0, time.Second)
			if err != nil {
				t.Fatalf("NewClient failed: %v", err)
			}
			c.SetBaseBackoff(time.Millisecond)

			err = c.PutRaw(ctx, "/test.bin", strings.NewReader("sample"), 6, "application/octet-stream")
			if tc.shouldErr && err == nil {
				t.Errorf("expected error for status %d, got nil", tc.code)
			}
			if !tc.shouldErr && err != nil {
				t.Errorf("unexpected error for status %d: %v", tc.code, err)
			}
		})
	}
}

func TestPutRawAndPut_Consistency(t *testing.T) {
	ctx := context.Background()

	var putHeaders, putRawHeaders http.Header
	var putBody, putRawBody []byte

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.URL.Path == "/test-put.txt" {
			putHeaders = r.Header.Clone()
			putBody = body
		} else if r.URL.Path == "/test-putraw.txt" {
			putRawHeaders = r.Header.Clone()
			putRawBody = body
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer s.Close()

	c, err := NewClient(s.URL, "", "", "", 0, time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	content := []byte("consistent content 12345")

	// For root path, Put does not call EnsureDir either
	if err := c.Put(ctx, "/test-put.txt", bytes.NewReader(content), int64(len(content)), "text/plain"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := c.PutRaw(ctx, "/test-putraw.txt", bytes.NewReader(content), int64(len(content)), "text/plain"); err != nil {
		t.Fatalf("PutRaw failed: %v", err)
	}

	if !bytes.Equal(putBody, putRawBody) {
		t.Errorf("bodies differ between Put and PutRaw")
	}
	if putHeaders.Get("Content-Type") != putRawHeaders.Get("Content-Type") {
		t.Errorf("Content-Type differs: Put=%s, PutRaw=%s", putHeaders.Get("Content-Type"), putRawHeaders.Get("Content-Type"))
	}
}
