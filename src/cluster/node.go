package cluster

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"

	"webdav-agg/src/client"
	"webdav-agg/src/config"
)

// Node wraps an upstream WebDAV configuration and its client,
// tracking capacity, usage, and draining status.
type Node struct {
	mu sync.RWMutex

	Config         config.UpstreamConfig
	ParsedCapacity config.ParsedCapacity
	Client         *client.Client

	AvailableBytes int64
	UsedBytes      int64
	TotalBytes     int64
	ReservedBytes  int64
	FilesCount     int64
	IsDraining     bool
}

// NewNode initializes a new Node from an UpstreamConfig and WebDAV Client.
func NewNode(u config.UpstreamConfig, cli *client.Client) *Node {
	isDraining := strings.EqualFold(u.Type, "del")
	pc, _ := u.ParseCapacity()

	node := &Node{
		Config:         u,
		ParsedCapacity: pc,
		Client:         cli,
		IsDraining:     isDraining,
	}

	if pc.Mode == config.CapacityModeLimit && pc.Bytes > 0 {
		node.TotalBytes = int64(pc.Bytes)
		node.AvailableBytes = int64(pc.Bytes)
	} else if pc.Mode == config.CapacityModeRatio && pc.Ratio > 0 {
		// Ratio mode has no fixed total bytes, and available space is unrestricted
		node.TotalBytes = 0
		node.AvailableBytes = math.MaxInt64
	}

	return node
}

// ID returns the node's upstream ID.
func (n *Node) ID() int {
	return n.Config.ID
}

// Quota returns the currently tracked available, used, and total bytes thread-safely.
func (n *Node) Quota() (available, used, total int64) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.AvailableBytes, n.UsedBytes, n.TotalBytes
}

// Files returns the number of files tracked on this node.
func (n *Node) Files() int64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.FilesCount
}
// Reserve increments in-flight reserved bytes on this node.
func (n *Node) Reserve(bytes int64) {
	if bytes <= 0 {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.ReservedBytes += bytes
}

// Unreserve decrements in-flight reserved bytes on this node.
func (n *Node) Unreserve(bytes int64) {
	if bytes <= 0 {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.ReservedBytes -= bytes
	if n.ReservedBytes < 0 {
		n.ReservedBytes = 0
	}
}

// Reserved returns the current in-flight reserved bytes on this node.
func (n *Node) Reserved() int64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.ReservedBytes
}

// EffectiveAvailable returns available bytes minus reserved bytes.
// For ratio nodes, returns math.MaxInt64.
func (n *Node) EffectiveAvailable() int64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.ParsedCapacity.Mode == config.CapacityModeRatio {
		return math.MaxInt64
	}
	avail := n.AvailableBytes - n.ReservedBytes
	if avail < 0 {
		return 0
	}
	return avail
}

// EffectiveUsed returns used bytes plus reserved bytes.
func (n *Node) EffectiveUsed() int64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.UsedBytes + n.ReservedBytes
}

// Draining returns whether the node is draining (marked for deletion).
func (n *Node) Draining() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.IsDraining
}

// SetDraining updates the node's draining status.
func (n *Node) SetDraining(draining bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.IsDraining = draining
}

func (n *Node) recalculateQuotaLocked() {
	if n.ParsedCapacity.Mode == config.CapacityModeLimit && n.ParsedCapacity.Bytes > 0 {
		if n.TotalBytes <= 0 {
			n.TotalBytes = int64(n.ParsedCapacity.Bytes)
		}
		if n.TotalBytes > 0 && n.UsedBytes > n.TotalBytes {
			n.UsedBytes = n.TotalBytes
		}
		n.AvailableBytes = n.TotalBytes - n.UsedBytes
		if n.AvailableBytes < 0 {
			n.AvailableBytes = 0
		}
	} else if n.ParsedCapacity.Mode == config.CapacityModeRatio {
		n.TotalBytes = 0
		n.AvailableBytes = math.MaxInt64
	} else if n.TotalBytes > 0 {
		if n.UsedBytes > n.TotalBytes {
			n.UsedBytes = n.TotalBytes
		}
		n.AvailableBytes = n.TotalBytes - n.UsedBytes
		if n.AvailableBytes < 0 {
			n.AvailableBytes = 0
		}
	}
}

// SetQuota updates in-memory used bytes and recalculates available bytes from persisted data.
func (n *Node) SetQuota(used int64, files int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if used < 0 {
		used = 0
	}
	if files < 0 {
		files = 0
	}

	n.FilesCount = files
	n.UsedBytes = used
	n.recalculateQuotaLocked()
}

// CommitQuota atomically unreserves reservedBytes, updates UsedBytes and FilesCount
// to the latest authoritative database values, and recalculates AvailableBytes under the same lock,
// preventing quota gaps and capacity jitter during concurrent writes.
func (n *Node) CommitQuota(reservedBytes int64, newUsed int64, newFiles int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if reservedBytes > 0 {
		n.ReservedBytes -= reservedBytes
		if n.ReservedBytes < 0 {
			n.ReservedBytes = 0
		}
	}

	if newUsed < 0 {
		newUsed = 0
	}
	if newFiles < 0 {
		newFiles = 0
	}

	n.UsedBytes = newUsed
	n.FilesCount = newFiles
	n.recalculateQuotaLocked()
}

// CommitReserve atomically releases reservedBytes and accumulates actualWritten to UsedBytes
// in non-DB or local mode, recalculating AvailableBytes under the same lock.
func (n *Node) CommitReserve(reservedBytes int64, actualWritten int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if reservedBytes > 0 {
		n.ReservedBytes -= reservedBytes
		if n.ReservedBytes < 0 {
			n.ReservedBytes = 0
		}
	}

	if actualWritten != 0 {
		n.UsedBytes += actualWritten
		if n.UsedBytes < 0 {
			n.UsedBytes = 0
		}
	}

	n.recalculateQuotaLocked()
}

// Sync recursively walks the entire remote storage to calculate real used bytes and files count.
func (n *Node) Sync(ctx context.Context) (int64, int64, error) {
	if n.Client == nil {
		return 0, 0, errors.New("client is nil")
	}

	var totalUsed int64
	var totalFiles int64

	err := n.Client.Walk(ctx, "/", func(info client.FileInfo) error {
		if !info.IsDir {
			totalUsed += info.Size
			totalFiles++
		}
		return nil
	})
	if err != nil {
		return 0, 0, err
	}

	n.SetQuota(totalUsed, totalFiles)
	return totalUsed, totalFiles, nil
}

// UpdateQuota verifies upstream connectivity and updates quota statistics
// strictly based on configured capacity and locally tracked usage.
func (n *Node) UpdateQuota(ctx context.Context) error {
	if n.Client == nil {
		return errors.New("client is nil")
	}

	if strings.TrimSpace(n.Config.Capacity) == "" {
		if _, err := n.Client.GetQuota(ctx); err != nil {
			return err
		}
		return errors.New("capacity must be specified")
	}

	if err := n.Client.Ping(ctx); err != nil {
		return err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if strings.EqualFold(n.Config.Type, "del") {
		n.IsDraining = true
	}

	n.recalculateQuotaLocked()

	return nil
}

// RecordWrite updates in-memory quota tracking after writing bytes to this node.
func (n *Node) RecordWrite(size int64) {
	if size <= 0 {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	n.UsedBytes += size
	n.FilesCount++

	n.recalculateQuotaLocked()
}

// RecordOverwrite updates in-memory quota tracking after overwriting bytes in-place.
func (n *Node) RecordOverwrite(deltaSize int64) {
	if deltaSize == 0 {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	n.UsedBytes += deltaSize
	if n.UsedBytes < 0 {
		n.UsedBytes = 0
	}

	n.recalculateQuotaLocked()
}

// RecordDelete updates in-memory quota tracking after deleting bytes from this node.
func (n *Node) RecordDelete(size int64) {
	if size <= 0 {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	n.UsedBytes -= size
	if n.UsedBytes < 0 {
		n.UsedBytes = 0
	}
	n.FilesCount--
	if n.FilesCount < 0 {
		n.FilesCount = 0
	}

	n.recalculateQuotaLocked()
}
