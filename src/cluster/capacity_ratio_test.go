package cluster

import (
	"errors"
	"testing"

	"webdav-agg/src/config"
)

func TestCluster_RatioLoadBalancing(t *testing.T) {
	// Node 1 ratio = 2.00, Node 2 ratio = 1.00
	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: "http://127.0.0.1:9001", Capacity: "2.00"},
			{ID: 2, URL: "http://127.0.0.1:9002", Capacity: "1.00"},
		},
	}

	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster error: %v", err)
	}

	// 1. Initial selection: both nodes used = 0. Node 1 has ID 1, selected first
	n1, err := c.SelectWriteNode(100)
	if err != nil {
		t.Fatalf("SelectWriteNode error: %v", err)
	}
	if n1.ID() != 1 {
		t.Fatalf("expected node 1, got node %d", n1.ID())
	}

	// Node 1 writes 10MB -> used = 10, load = 10 / 2.0 = 5.0
	c.RecordWrite(n1, 10*1024*1024, true)

	// Next selection: Node 2 has load 0.0, Node 1 has load 5.0 -> Node 2 must be selected
	n2, err := c.SelectWriteNode(100)
	if err != nil {
		t.Fatalf("SelectWriteNode error: %v", err)
	}
	if n2.ID() != 2 {
		t.Fatalf("expected node 2, got node %d", n2.ID())
	}

	// Node 2 writes 10MB -> used = 10, load = 10 / 1.0 = 10.0
	c.RecordWrite(n2, 10*1024*1024, true)

	// Next selection: Node 1 has load 5.0 < Node 2 load 10.0 -> Node 1 must be selected
	n3, err := c.SelectWriteNode(100)
	if err != nil {
		t.Fatalf("SelectWriteNode error: %v", err)
	}
	if n3.ID() != 1 {
		t.Fatalf("expected node 1, got node %d", n3.ID())
	}

	// Node 1 writes another 10MB -> used = 20MB, load = 20 / 2.0 = 10.0
	c.RecordWrite(n3, 10*1024*1024, true)

	// Now: Node 1 used = 20MB, Node 2 used = 10MB -> strictly 2:1 ratio
	node1, _ := c.GetNode(1)
	node2, _ := c.GetNode(2)
	_, u1, _ := node1.Quota()
	_, u2, _ := node2.Quota()
	if u1 != 20*1024*1024 || u2 != 10*1024*1024 {
		t.Errorf("expected 20MB and 10MB, got %d and %d", u1, u2)
	}

	// 2. Unrestricted size test: ratio mode should NOT return ErrInsufficientStorage for large files
	hugeSize := int64(100) * 1024 * 1024 * 1024 * 1024 // 100 TB
	nHuge, err := c.SelectWriteNode(hugeSize)
	if err != nil {
		t.Fatalf("expected ratio mode to allow huge file, got: %v", err)
	}
	if nHuge == nil {
		t.Fatal("expected non-nil node for huge size in ratio mode")
	}
}

func TestCluster_LimitMode_InsufficientStorage(t *testing.T) {
	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: "http://127.0.0.1:9001", Capacity: "100M"},
		},
	}

	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster error: %v", err)
	}

	n1, _ := c.GetNode(1)
	// Simulate existing used 90MB -> available 10MB
	n1.SetQuota(90*1024*1024, 10)

	// Write 5MB (under 10MB limit) -> should succeed
	node, err := c.SelectWriteNode(5 * 1024 * 1024)
	if err != nil {
		t.Fatalf("expected success for 5MB, got: %v", err)
	}
	if node.ID() != 1 {
		t.Errorf("expected node 1, got %d", node.ID())
	}

	// Write 15MB (exceeds available 10MB) -> must return ErrInsufficientStorage
	_, err = c.SelectWriteNode(15 * 1024 * 1024)
	if !errors.Is(err, ErrInsufficientStorage) {
		t.Fatalf("expected ErrInsufficientStorage, got: %v", err)
	}
}
