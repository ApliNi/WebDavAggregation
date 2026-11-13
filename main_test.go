package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"webdav-agg/src/config"
	"webdav-agg/src/server"
)

func TestGracefulShutdown(t *testing.T) {
	mockUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			w.Header().Set("DAV", "1, 2")
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == "PROPFIND" {
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
</D:multistatus>`, 1024*1024*1024, 0)
			_, _ = w.Write([]byte(xmlResp))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer mockUpstream.Close()

	cfg := &config.Config{
		Listen:   "127.0.0.1:0",
		DataFile: ":memory:",
		Upstream: []config.UpstreamConfig{
			{
				ID:       1,
				URL:      mockUpstream.URL,
				Capacity: "100M",
			},
		},
	}

	srv, err := server.NewServer(cfg, nil)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- srv.StartWithContext(ctx)
	}()

	for i := 0; i < 100; i++ {
		if srv.Addr() != "127.0.0.1:0" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected start error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop in time")
	}
}

func TestConfigLoadFailure(t *testing.T) {
	_, err := config.Load("non_existent_config_file_for_test.yml")
	if err == nil {
		t.Fatal("expected error loading non-existent config file")
	}
}
