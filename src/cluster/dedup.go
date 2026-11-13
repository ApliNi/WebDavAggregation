package cluster

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"

	"webdav-agg/src/client"
	"webdav-agg/src/data"
)

// FileInfoWithNode associates a file metadata entry with the upstream Node storing it.
type FileInfoWithNode struct {
	client.FileInfo
	Node *Node
}

// trieNode represents a node in the hierarchical path locking trie.
type trieNode struct {
	singleCount     int
	subtreeCount    int
	descendantCount int // total locks held in descendants
	children        map[string]*trieNode
}

// PathLocker coordinates concurrency-safe per-path and subtree locking using a prefix trie.
type PathLocker struct {
	mu   sync.Mutex
	cond *sync.Cond
	root *trieNode
}

// NewPathLocker creates an empty PathLocker.
func NewPathLocker() *PathLocker {
	pl := &PathLocker{
		root: &trieNode{
			children: make(map[string]*trieNode),
		},
	}
	pl.cond = sync.NewCond(&pl.mu)
	return pl
}

func splitPathParts(p string) []string {
	norm := data.NormalizePath(p)
	trimmed := strings.Trim(norm, "/")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "/")
}

func (pl *PathLocker) canLockSingleLocked(parts []string) bool {
	curr := pl.root
	for _, part := range parts {
		if curr.subtreeCount > 0 {
			return false
		}
		child := curr.children[part]
		if child == nil {
			return true
		}
		curr = child
	}
	if curr.subtreeCount > 0 || curr.singleCount > 0 {
		return false
	}
	return true
}

func (pl *PathLocker) canLockSubtreeLocked(parts []string) bool {
	curr := pl.root
	for _, part := range parts {
		if curr.subtreeCount > 0 {
			return false
		}
		child := curr.children[part]
		if child == nil {
			return true
		}
		curr = child
	}
	if curr.subtreeCount > 0 || curr.singleCount > 0 || curr.descendantCount > 0 {
		return false
	}
	return true
}

func (pl *PathLocker) acquireLocked(parts []string, isSubtree bool) {
	curr := pl.root
	for _, part := range parts {
		curr.descendantCount++
		child, ok := curr.children[part]
		if !ok {
			child = &trieNode{children: make(map[string]*trieNode)}
			curr.children[part] = child
		}
		curr = child
	}
	if isSubtree {
		curr.subtreeCount++
	} else {
		curr.singleCount++
	}
}

func (pl *PathLocker) releaseLocked(parts []string, isSubtree bool) {
	nodes := make([]*trieNode, 0, len(parts)+1)
	curr := pl.root
	nodes = append(nodes, curr)
	for _, part := range parts {
		child := curr.children[part]
		if child == nil {
			return
		}
		nodes = append(nodes, child)
		curr = child
	}

	target := nodes[len(nodes)-1]
	if isSubtree {
		target.subtreeCount--
	} else {
		target.singleCount--
	}

	for i := len(nodes) - 2; i >= 0; i-- {
		parent := nodes[i]
		parent.descendantCount--
		part := parts[i]
		child := parent.children[part]
		if child != nil && child.singleCount == 0 && child.subtreeCount == 0 && child.descendantCount == 0 && len(child.children) == 0 {
			delete(parent.children, part)
		}
	}
}

// Lock locks the specified path and returns an unlock function.
func (pl *PathLocker) Lock(p string) func() {
	parts := splitPathParts(p)
	pl.mu.Lock()
	defer pl.mu.Unlock()
	for !pl.canLockSingleLocked(parts) {
		pl.cond.Wait()
	}
	pl.acquireLocked(parts, false)
	var once sync.Once
	return func() {
		once.Do(func() {
			pl.mu.Lock()
			pl.releaseLocked(parts, false)
			pl.cond.Broadcast()
			pl.mu.Unlock()
		})
	}
}

// TryLock attempts to lock the specified path without blocking.
// If the path or an ancestor subtree is already locked, it returns (nil, false).
func (pl *PathLocker) TryLock(p string) (func(), bool) {
	parts := splitPathParts(p)
	pl.mu.Lock()
	defer pl.mu.Unlock()
	if !pl.canLockSingleLocked(parts) {
		return nil, false
	}
	pl.acquireLocked(parts, false)
	var once sync.Once
	return func() {
		once.Do(func() {
			pl.mu.Lock()
			pl.releaseLocked(parts, false)
			pl.cond.Broadcast()
			pl.mu.Unlock()
		})
	}, true
}

// LockSubtree exclusively locks the path and all its descendant paths, blocking until available.
func (pl *PathLocker) LockSubtree(p string) func() {
	parts := splitPathParts(p)
	pl.mu.Lock()
	defer pl.mu.Unlock()
	for !pl.canLockSubtreeLocked(parts) {
		pl.cond.Wait()
	}
	pl.acquireLocked(parts, true)
	var once sync.Once
	return func() {
		once.Do(func() {
			pl.mu.Lock()
			pl.releaseLocked(parts, true)
			pl.cond.Broadcast()
			pl.mu.Unlock()
		})
	}
}

// TryLockSubtree attempts to exclusively lock the path and all its descendant paths without blocking.
func (pl *PathLocker) TryLockSubtree(p string) (func(), bool) {
	parts := splitPathParts(p)
	pl.mu.Lock()
	defer pl.mu.Unlock()
	if !pl.canLockSubtreeLocked(parts) {
		return nil, false
	}
	pl.acquireLocked(parts, true)
	var once sync.Once
	return func() {
		once.Do(func() {
			pl.mu.Lock()
			pl.releaseLocked(parts, true)
			pl.cond.Broadcast()
			pl.mu.Unlock()
		})
	}, true
}
var defaultPathLocker = NewPathLocker()

// DeduplicateFiles resolves conflicts when duplicate files exist across multiple upstream nodes.
// It selects the instance with the newest ModTime (aligned to nanoseconds in UTC)
// and safely deletes older versions on their respective nodes.
func DeduplicateFiles(ctx context.Context, davPath string, instances []FileInfoWithNode) (*FileInfoWithNode, error) {
	unlock := defaultPathLocker.Lock(davPath)
	defer unlock()

	return deduplicateFilesInternal(ctx, davPath, instances, nil)
}

// DeduplicateFiles on Cluster executes path-locked, UTC-nanosecond deduplication,
// safely deleting duplicates from remote upstreams and removing records from SQLite.
func (c *Cluster) DeduplicateFiles(ctx context.Context, davPath string, instances []FileInfoWithNode) (*FileInfoWithNode, error) {
	locker := c.pathLocker
	if locker == nil {
		locker = defaultPathLocker
	}
	unlock := locker.Lock(davPath)
	defer unlock()

	return deduplicateFilesInternal(ctx, davPath, instances, c.DataManager())
}
// DeduplicateFilesLocked executes deduplication without acquiring PathLocker.
// It is intended for callers (such as handlePut) that have already acquired the exclusive path lock.
func (c *Cluster) DeduplicateFilesLocked(ctx context.Context, davPath string, instances []FileInfoWithNode) (*FileInfoWithNode, error) {
	return deduplicateFilesInternal(ctx, davPath, instances, c.DataManager())
}

func deduplicateFilesInternal(
	ctx context.Context,
	davPath string,
	instances []FileInfoWithNode,
	dm *data.Manager,
) (*FileInfoWithNode, error) {
	if len(instances) == 0 {
		return nil, client.ErrNotFound
	}
	if len(instances) == 1 {
		res := instances[0]
		return &res, nil
	}

	// Nanosecond-aligned UTC timestamp comparison with deterministic tie-breaking
	newestIdx := 0
	for i := 1; i < len(instances); i++ {
		tCur := instances[i].ModTime.UTC().UnixNano()
		tBest := instances[newestIdx].ModTime.UTC().UnixNano()
		if tCur > tBest {
			newestIdx = i
		} else if tCur == tBest {
			curID := math.MaxInt32
			if instances[i].Node != nil {
				curID = instances[i].Node.ID()
			}
			bestID := math.MaxInt32
			if instances[newestIdx].Node != nil {
				bestID = instances[newestIdx].Node.ID()
			}
			if curID < bestID {
				newestIdx = i
			}
		}
	}

	newest := instances[newestIdx]

	// Do not delete directories
	if newest.IsDir {
		return &newest, nil
	}

	var wg sync.WaitGroup
	for i, inst := range instances {
		if i == newestIdx {
			continue
		}
		wg.Add(1)
		go func(item FileInfoWithNode) {
			defer wg.Done()
			p := item.Path
			if p == "" {
				p = davPath
			}
			p = data.NormalizePath(p)

			if item.Node != nil && item.Node.Client != nil {
				delErr := item.Node.Client.Delete(ctx, p)
				if delErr == nil || errors.Is(delErr, client.ErrNotFound) {
					item.Node.RecordDelete(item.Size)
					if dm != nil {
						_ = dm.DeleteFile(item.Node.ID(), p)
						if st, err := dm.GetStorage(item.Node.ID()); err == nil && st != nil {
							item.Node.SetQuota(st.UsedBytes, st.FileCount)
						}
					}
				}
			}
		}(inst)
	}
	wg.Wait()

	return &newest, nil
}
