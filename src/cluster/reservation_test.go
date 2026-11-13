package cluster

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"webdav-agg/src/config"
)

func TestNode_CommitQuota_And_CommitReserve(t *testing.T) {
	u := config.UpstreamConfig{
		ID:       1,
		URL:      "http://127.0.0.1:9001",
		Capacity: "1000B",
	}
	node := NewNode(u, nil)

	// 1. Initial quota state
	avail, used, tot := node.Quota()
	if avail != 1000 || used != 0 || tot != 1000 {
		t.Fatalf("unexpected initial quota: avail=%d, used=%d, tot=%d", avail, used, tot)
	}

	// 2. Reserve 300 bytes
	node.Reserve(300)
	if node.Reserved() != 300 {
		t.Fatalf("expected reserved 300, got %d", node.Reserved())
	}
	if node.EffectiveAvailable() != 700 {
		t.Fatalf("expected effAvail 700, got %d", node.EffectiveAvailable())
	}
	if node.EffectiveUsed() != 300 {
		t.Fatalf("expected effUsed 300, got %d", node.EffectiveUsed())
	}

	// 3. CommitQuota: atomically unreserves 300, sets DB values (used=300, files=1)
	node.CommitQuota(300, 300, 1)
	if node.Reserved() != 0 {
		t.Fatalf("expected reserved 0 after CommitQuota, got %d", node.Reserved())
	}
	avail, used, tot = node.Quota()
	if avail != 700 || used != 300 || tot != 1000 {
		t.Fatalf("expected avail=700, used=300, got avail=%d, used=%d", avail, used)
	}
	if node.Files() != 1 {
		t.Fatalf("expected files 1, got %d", node.Files())
	}

	// 4. CommitReserve: in non-DB mode, unreserves and accumulates used
	node.Reserve(200)
	if node.Reserved() != 200 {
		t.Fatalf("expected reserved 200, got %d", node.Reserved())
	}
	node.CommitReserve(200, 200)
	if node.Reserved() != 0 {
		t.Fatalf("expected reserved 0 after CommitReserve, got %d", node.Reserved())
	}
	avail, used, _ = node.Quota()
	if avail != 500 || used != 500 {
		t.Fatalf("expected avail=500, used=500 after CommitReserve, got avail=%d, used=%d", avail, used)
	}
}

func TestCluster_ReserveWrite_Concurrent_Balance(t *testing.T) {
	// Two Limit nodes with 100MB each
	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: "http://127.0.0.1:9001", Capacity: "100MB"},
			{ID: 2, URL: "http://127.0.0.1:9002", Capacity: "100MB"},
		},
	}
	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster error: %v", err)
	}

	const concurrency = 40
	var counts [3]int64 // index 1 for node 1, index 2 for node 2
	var wg sync.WaitGroup
	var rollbacks []func()
	var mu sync.Mutex

	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			node, _, rollback, err := c.ReserveWrite(1 * 1024 * 1024)
			if err != nil {
				t.Errorf("ReserveWrite failed: %v", err)
				return
			}
			atomic.AddInt64(&counts[node.ID()], 1)
			mu.Lock()
			rollbacks = append(rollbacks, rollback)
			mu.Unlock()
		}()
	}
	wg.Wait()

	t.Logf("Limit nodes write distribution: Node 1 = %d, Node 2 = %d", counts[1], counts[2])
	if counts[1] == 0 || counts[2] == 0 {
		t.Fatalf("expected writes to be distributed across both nodes, got Node 1 = %d, Node 2 = %d", counts[1], counts[2])
	}
	if math.Abs(float64(counts[1]-counts[2])) > 4 {
		t.Fatalf("distribution too skewed: Node 1 = %d, Node 2 = %d", counts[1], counts[2])
	}

	// Clean up reservations
	for _, rb := range rollbacks {
		rb()
	}

	n1, _ := c.GetNode(1)
	n2, _ := c.GetNode(2)
	if n1.Reserved() != 0 || n2.Reserved() != 0 {
		t.Fatalf("expected all reservations released, got n1=%d, n2=%d", n1.Reserved(), n2.Reserved())
	}
}

func TestCluster_ReserveWrite_Ratio_Concurrent_Balance(t *testing.T) {
	// Two Ratio nodes with ratio 1.0 each
	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: "http://127.0.0.1:9001", Capacity: "1.0"},
			{ID: 2, URL: "http://127.0.0.1:9002", Capacity: "1.0"},
		},
	}
	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster error: %v", err)
	}

	const concurrency = 40
	var counts [3]int64
	var wg sync.WaitGroup
	var rollbacks []func()
	var mu sync.Mutex

	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			node, _, rollback, err := c.ReserveWrite(2 * 1024 * 1024)
			if err != nil {
				t.Errorf("ReserveWrite failed: %v", err)
				return
			}
			atomic.AddInt64(&counts[node.ID()], 1)
			mu.Lock()
			rollbacks = append(rollbacks, rollback)
			mu.Unlock()
		}()
	}
	wg.Wait()

	t.Logf("Ratio nodes write distribution: Node 1 = %d, Node 2 = %d", counts[1], counts[2])
	if counts[1] == 0 || counts[2] == 0 {
		t.Fatalf("expected ratio writes to be distributed across both nodes, got Node 1 = %d, Node 2 = %d", counts[1], counts[2])
	}
	if math.Abs(float64(counts[1]-counts[2])) > 4 {
		t.Fatalf("distribution too skewed: Node 1 = %d, Node 2 = %d", counts[1], counts[2])
	}

	for _, rb := range rollbacks {
		rb()
	}
}

func TestCluster_ReserveWrite_UnknownSize_NominalReserve(t *testing.T) {
	// 2 limit nodes with 100MB each
	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: "http://127.0.0.1:9001", Capacity: "100MB"},
			{ID: 2, URL: "http://127.0.0.1:9002", Capacity: "100MB"},
		},
	}
	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster error: %v", err)
	}

	// 1. Single unknown size call with reqSize = -1 (chunked)
	node1, _, rb1, err := c.ReserveWrite(-1)
	if err != nil {
		t.Fatalf("ReserveWrite(-1) failed: %v", err)
	}
	if node1.ID() != 1 {
		t.Fatalf("expected first node to be 1, got %d", node1.ID())
	}
	if node1.Reserved() != DefaultNominalReserve {
		t.Fatalf("expected nominal reserve %d, got %d", DefaultNominalReserve, node1.Reserved())
	}

	// 2. Second unknown size call with reqSize = 0: must pick Node 2 because Node 1 effAvail is lower
	node2, _, rb2, err := c.ReserveWrite(0)
	if err != nil {
		t.Fatalf("ReserveWrite(0) failed: %v", err)
	}
	if node2.ID() != 2 {
		t.Fatalf("expected second node to be 2 due to nominal reserve, got %d", node2.ID())
	}
	if node2.Reserved() != DefaultNominalReserve {
		t.Fatalf("expected nominal reserve %d, got %d", DefaultNominalReserve, node2.Reserved())
	}

	rb1()
	rb2()

	// 3. Concurrently invoke unknown size requests (-1 and 0 mixed)
	const concurrency = 20
	var counts [3]int64
	var wg sync.WaitGroup
	var rollbacks []func()
	var mu sync.Mutex

	for i := range concurrency {
		wg.Add(1)
		reqSize := int64(-1)
		if i%2 == 0 {
			reqSize = 0
		}
		go func(sz int64) {
			defer wg.Done()
			node, _, rollback, err := c.ReserveWrite(sz)
			if err != nil {
				t.Errorf("ReserveWrite(%d) failed: %v", sz, err)
				return
			}
			atomic.AddInt64(&counts[node.ID()], 1)
			mu.Lock()
			rollbacks = append(rollbacks, rollback)
			mu.Unlock()
		}(reqSize)
	}
	wg.Wait()

	t.Logf("Unknown-size write distribution: Node 1 = %d, Node 2 = %d", counts[1], counts[2])
	if counts[1] == 0 || counts[2] == 0 {
		t.Fatalf("expected unknown size writes distributed across both nodes, got Node 1 = %d, Node 2 = %d", counts[1], counts[2])
	}
	if math.Abs(float64(counts[1]-counts[2])) > 4 {
		t.Fatalf("unknown size distribution too skewed: Node 1 = %d, Node 2 = %d", counts[1], counts[2])
	}

	for _, rb := range rollbacks {
		rb()
	}

	// Verify all reserved bytes returned to 0
	n1, _ := c.GetNode(1)
	n2, _ := c.GetNode(2)
	if n1.Reserved() != 0 || n2.Reserved() != 0 {
		t.Fatalf("expected 0 reserved after rollbacks, got n1=%d, n2=%d", n1.Reserved(), n2.Reserved())
	}
}

func TestCluster_ReserveWrite_Rollback(t *testing.T) {
	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: "http://127.0.0.1:9001", Capacity: "10MB"},
		},
	}
	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster error: %v", err)
	}

	node, _, rollback, err := c.ReserveWrite(2 * 1024 * 1024)
	if err != nil {
		t.Fatalf("ReserveWrite failed: %v", err)
	}
	if node.Reserved() != 2*1024*1024 {
		t.Fatalf("expected 2MB reserved, got %d", node.Reserved())
	}
	if node.EffectiveAvailable() != 8*1024*1024 {
		t.Fatalf("expected 8MB effAvail, got %d", node.EffectiveAvailable())
	}

	// Rollback
	rollback()
	if node.Reserved() != 0 {
		t.Fatalf("expected 0 reserved after rollback, got %d", node.Reserved())
	}
	if node.EffectiveAvailable() != 10*1024*1024 {
		t.Fatalf("expected 10MB effAvail, got %d", node.EffectiveAvailable())
	}

	// Multiple rollback calls must be idempotent (sync.Once)
	rollback()
	rollback()
	if node.Reserved() != 0 {
		t.Fatalf("expected 0 reserved after multiple rollbacks, got %d", node.Reserved())
	}
}

func TestCluster_ReserveWrite_Commit(t *testing.T) {
	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: "http://127.0.0.1:9001", Capacity: "10MB"},
		},
	}
	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster error: %v", err)
	}

	node, commit, rollback, err := c.ReserveWrite(2 * 1024 * 1024)
	if err != nil {
		t.Fatalf("ReserveWrite failed: %v", err)
	}
	if node.Reserved() != 2*1024*1024 {
		t.Fatalf("expected 2MB reserved, got %d", node.Reserved())
	}

	// Commit with written = 2MB
	commit(2 * 1024 * 1024)
	if node.Reserved() != 0 {
		t.Fatalf("expected 0 reserved after commit, got %d", node.Reserved())
	}
	avail, used, tot := node.Quota()
	if used != 2*1024*1024 || avail != 8*1024*1024 || tot != 10*1024*1024 {
		t.Fatalf("quota mismatch after commit: avail=%d, used=%d, tot=%d", avail, used, tot)
	}

	// Subsequent rollback must be no-op due to sync.Once
	rollback()
	if node.Reserved() != 0 {
		t.Fatalf("expected 0 reserved, got %d", node.Reserved())
	}
	avail, used, _ = node.Quota()
	if used != 2*1024*1024 || avail != 8*1024*1024 {
		t.Fatalf("quota corrupted by rollback after commit: avail=%d, used=%d", avail, used)
	}
}

func TestCluster_ReserveWriteOnNode(t *testing.T) {
	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: "http://127.0.0.1:9001", Capacity: "10MB"},
		},
	}
	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster error: %v", err)
	}

	node, _ := c.GetNode(1)
	node.SetQuota(8*1024*1024, 1) // 8MB used, 2MB avail

	// 1. Overwrite with deltaSize <= 0: no-op
	commit, rollback, err := c.ReserveWriteOnNode(node, 7*1024*1024, 8*1024*1024) // shrinking
	if err != nil {
		t.Fatalf("expected no error for delta <= 0, got %v", err)
	}
	if node.Reserved() != 0 {
		t.Fatalf("expected 0 reserved for delta <= 0, got %d", node.Reserved())
	}
	commit(0)
	rollback()

	// 2. Overwrite exceeding available space (delta 3MB > avail 2MB) -> ErrInsufficientStorage
	_, _, err = c.ReserveWriteOnNode(node, 11*1024*1024, 8*1024*1024)
	if !errors.Is(err, ErrInsufficientStorage) {
		t.Fatalf("expected ErrInsufficientStorage, got %v", err)
	}

	// 3. Overwrite fitting available space (delta 1MB <= avail 2MB) -> Success
	commit, rollback, err = c.ReserveWriteOnNode(node, 9*1024*1024, 8*1024*1024)
	if err != nil {
		t.Fatalf("ReserveWriteOnNode failed: %v", err)
	}
	if node.Reserved() != 1*1024*1024 {
		t.Fatalf("expected 1MB reserved, got %d", node.Reserved())
	}
	if node.EffectiveAvailable() != 1*1024*1024 {
		t.Fatalf("expected 1MB effAvail, got %d", node.EffectiveAvailable())
	}

	// Commit overwrite
	commit(1 * 1024 * 1024)
	if node.Reserved() != 0 {
		t.Fatalf("expected 0 reserved after commit, got %d", node.Reserved())
	}
	avail, used, _ := node.Quota()
	if used != 9*1024*1024 || avail != 1*1024*1024 {
		t.Fatalf("expected used=9MB, avail=1MB, got used=%d, avail=%d", used, avail)
	}
}
func TestCluster_ReserveWrite_Overwrite_BalanceAndRollback(t *testing.T) {
	// Node 1: 10MB limit, 8MB used, 2MB avail
	// Node 2: 20MB limit, 5MB used, 15MB avail
	cfg := &config.Config{
		Upstream: []config.UpstreamConfig{
			{ID: 1, URL: "http://127.0.0.1:9001", Capacity: "10MB"},
			{ID: 2, URL: "http://127.0.0.1:9002", Capacity: "20MB"},
		},
	}
	c, err := NewCluster(cfg, nil)
	if err != nil {
		t.Fatalf("NewCluster error: %v", err)
	}

	n1, _ := c.GetNode(1)
	n1.SetQuota(8*1024*1024, 1)

	n2, _ := c.GetNode(2)
	n2.SetQuota(5*1024*1024, 1)

	// Small file on Node 1 (1MB) overwritten with 5MB.
	// Node 1 after credit: 2MB + 1MB = 3MB < 5MB -> cannot fit.
	// Node 2 has 15MB >= 5MB -> Selected!
	// Cross-node reservation must reserve full 5MB on Node 2.
	node, commit, rollback, err := c.ReserveWrite(5*1024*1024, OverwriteContext{OldNodeID: 1, OldSize: 1 * 1024 * 1024})
	if err != nil {
		t.Fatalf("ReserveWrite error: %v", err)
	}
	if node.ID() != 2 {
		t.Fatalf("expected node 2, got %d", node.ID())
	}
	if n2.Reserved() != 5*1024*1024 {
		t.Fatalf("expected 5MB reserved on node 2, got %d", n2.Reserved())
	}

	// Rollback test
	rollback()
	if n2.Reserved() != 0 {
		t.Fatalf("expected 0 reserved on node 2 after rollback, got %d", n2.Reserved())
	}

	// Commit test
	node, commit, _, err = c.ReserveWrite(5*1024*1024, OverwriteContext{OldNodeID: 1, OldSize: 1 * 1024 * 1024})
	if err != nil {
		t.Fatalf("ReserveWrite error: %v", err)
	}
	commit(5 * 1024 * 1024)
	if n2.Reserved() != 0 {
		t.Fatalf("expected 0 reserved on node 2 after commit, got %d", n2.Reserved())
	}
}
