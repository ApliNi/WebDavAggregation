package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEnsureDirSingleFlight(t *testing.T) {
	var mkcolCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PROPFIND" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == "MKCOL" {
			mkcolCount.Add(1)
			time.Sleep(50 * time.Millisecond) // Simulate slow backend MKCOL
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c, err := NewClient(server.URL, "user", "pass", "", 0, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	const concurrency = 10
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)

	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.EnsureDir(context.Background(), "/concurrent_dir"); err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("EnsureDir concurrently failed: %v", err)
	}

	if count := mkcolCount.Load(); count != 1 {
		t.Fatalf("expected exactly 1 MKCOL due to singleflight, got %d", count)
	}
}

func TestEnsureDirContextCancellation(t *testing.T) {
	gate := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PROPFIND" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == "MKCOL" {
			<-gate // wait for cancellation signal
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c, err := NewClient(server.URL, "user", "pass", "", 0, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctxCancel, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var errCancel, errSuccess error

	// Caller 1 will be canceled while waiting
	wg.Add(1)
	go func() {
		defer wg.Done()
		errCancel = c.EnsureDir(ctxCancel, "/cancel_dir")
	}()

	// Caller 2 stays active
	wg.Add(1)
	go func() {
		defer wg.Done()
		errSuccess = c.EnsureDir(context.Background(), "/cancel_dir")
	}()

	// Give goroutines time to enter singleflight
	time.Sleep(30 * time.Millisecond)
	cancel() // cancel caller 1

	// Unblock server MKCOL
	close(gate)
	wg.Wait()

	if !errors.Is(errCancel, context.Canceled) {
		t.Errorf("expected context.Canceled for caller 1, got: %v", errCancel)
	}
	if errSuccess != nil {
		t.Errorf("expected caller 2 to succeed, got: %v", errSuccess)
	}
}

func TestEnsureDirErrorPropagation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PROPFIND" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == "MKCOL" {
			time.Sleep(20 * time.Millisecond)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c, err := NewClient(server.URL, "user", "pass", "", 0, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	const concurrency = 5
	var wg sync.WaitGroup
	errorsReceived := make([]error, concurrency)

	for i := range concurrency {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errorsReceived[idx] = c.EnsureDir(context.Background(), "/failing_dir")
		}(i)
	}

	wg.Wait()

	for i, err := range errorsReceived {
		if err == nil {
			t.Errorf("worker %d expected error, got nil", i)
		}
	}
}

func TestClientGetRangeNotSatisfiable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes */102400")
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
	}))
	defer server.Close()

	c, err := NewClient(server.URL, "user", "pass", "", 0, 5*time.Second)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	header := make(http.Header)
	header.Set("Range", "bytes=9999999-")
	rc, hdr, err := c.Get(context.Background(), "/file.txt", header)
	if rc != nil {
		rc.Close()
	}

	if err == nil {
		t.Fatalf("expected error for 416, got nil")
	}

	if !errors.Is(err, ErrRangeNotSatisfiable) {
		t.Fatalf("expected ErrRangeNotSatisfiable, got: %v", err)
	}

	if hdr == nil {
		t.Fatalf("expected response headers to be returned for 416")
	}
	if cr := hdr.Get("Content-Range"); cr != "bytes */102400" {
		t.Errorf("expected Content-Range 'bytes */102400', got: %q", cr)
	}
	if status := hdr.Get("X-Upstream-Status"); status != "416" {
		t.Errorf("expected X-Upstream-Status '416', got: %q", status)
	}
}
