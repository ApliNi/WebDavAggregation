package cluster

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"webdav-agg/src/client"
	"webdav-agg/src/config"
	"webdav-agg/src/data"
	"webdav-agg/src/logger"
)

// ErrInsufficientStorage indicates that no healthy upstream node has enough space for the request (HTTP 507).
var ErrInsufficientStorage = errors.New("insufficient storage")

// Cluster manages upstream nodes, load balancing, capacity aggregation, and migration.
type Cluster struct {
	mu         sync.RWMutex
	cfg        *config.Config
	dataMgr    *data.Manager
	nodes      []*Node
	nodeMap    map[int]*Node
	pathLocker *PathLocker
}

// NewCluster creates a new Cluster initialized with nodes from the provided Config and Data Manager.
func NewCluster(cfg *config.Config, dataMgr *data.Manager) (*Cluster, error) {
	if cfg == nil {
		return nil, errors.New("config cannot be nil")
	}

	retry := cfg.Retry
	if retry <= 0 {
		retry = config.DefaultRetry
	}

	c := &Cluster{
		cfg:        cfg,
		dataMgr:    dataMgr,
		nodes:      make([]*Node, 0, len(cfg.Upstream)),
		nodeMap:    make(map[int]*Node, len(cfg.Upstream)),
		pathLocker: NewPathLocker(),
	}

	for _, u := range cfg.Upstream {
		cli, err := client.NewClient(u.URL, u.User, u.Pass, cfg.Proxy, retry, 0)
		if err != nil {
			return nil, fmt.Errorf("create client for upstream %d: %w", u.ID, err)
		}
		node := NewNode(u, cli)
		c.nodes = append(c.nodes, node)
		c.nodeMap[u.ID] = node
	}

	return c, nil
}

// DataManager returns the associated data.Manager instance.
func (c *Cluster) DataManager() *data.Manager {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.dataMgr
}

// SetDataManager sets the data.Manager for this cluster.
func (c *Cluster) SetDataManager(dm *data.Manager) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dataMgr = dm
}

// PathLocker returns the path locker associated with this cluster.
func (c *Cluster) PathLocker() *PathLocker {
	return c.pathLocker
}

// InitData initializes each upstream node's quota statistics from SQLite.
func (c *Cluster) InitData(ctx context.Context) error {
	nodes := c.AllNodes()
	configIDs := make(map[int]bool, len(nodes))
	for _, n := range nodes {
		configIDs[n.ID()] = true
	}

	if c.dataMgr != nil {
		dbIDs, err := c.dataMgr.GetAllStorageIDs()
		if err != nil {
			logger.Error("Cluster", "Failed to get storage ids from database: %v", err)
			return fmt.Errorf("failed to get storage ids from database: %w", err)
		}
		for _, dbID := range dbIDs {
			if !configIDs[dbID] {
				logger.Info("Cluster", "Detected obsolete upstream node id %d in database (not in configuration), deleting all associated data...", dbID)
				if err := c.dataMgr.DeleteStorage(dbID); err != nil {
					logger.Error("Cluster", "Failed to delete data for obsolete upstream node id %d: %v", dbID, err)
					return fmt.Errorf("failed to delete data for obsolete upstream node id %d: %w", dbID, err)
				}
				logger.Info("Cluster", "Successfully deleted all data for obsolete upstream node id %d", dbID)
			}
		}
	}

	var nodesToWalk []*Node
	for _, n := range nodes {
		id := n.ID()
		if c.dataMgr != nil {
			st, err := c.dataMgr.GetStorage(id)
			if err == nil && st != nil && st.LastSync != nil {
				n.SetQuota(st.UsedBytes, st.FileCount)
				continue
			}
		}
		nodesToWalk = append(nodesToWalk, n)
	}

	if len(nodesToWalk) == 0 {
		return nil
	}

	walkCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg        sync.WaitGroup
		dbWriteMu sync.Mutex
		firstErr  error
		errOnce   sync.Once
	)

	setErr := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	for _, n := range nodesToWalk {
		wg.Add(1)
		go func(node *Node) {
			defer wg.Done()

			id := node.ID()
			if node.Client == nil {
				err := fmt.Errorf("client is nil")
				logger.Error("Cluster", "Initial scan failed for upstream node %d (%s): %v", id, node.Config.URL, err)
				setErr(fmt.Errorf("initial sync for upstream %d (%s) failed: %w", id, node.Config.URL, err))
				return
			}

			if walkCtx.Err() != nil {
				return
			}

			logger.Info("Cluster", "Starting initial scan for upstream node %d (%s)...", id, node.Config.URL)

			var records []data.FileRecord
			err := node.Client.Walk(walkCtx, "/", func(info client.FileInfo) error {
				if data.NormalizePath(info.Path) == "/" {
					return nil
				}
				records = append(records, data.FileRecord{
					StorageID: id,
					Path:      info.Path,
					Size:      info.Size,
					IsDir:     info.IsDir,
					ModTime:   info.ModTime,
				})
				return nil
			})
			if err != nil {
				if walkCtx.Err() == nil || ctx.Err() != nil {
					logger.Error("Cluster", "Initial scan failed for upstream node %d (%s): %v", id, node.Config.URL, err)
				}
				setErr(fmt.Errorf("initial sync for upstream %d (%s) failed: %w", id, node.Config.URL, err))
				return
			}

			if walkCtx.Err() != nil {
				return
			}

			func() {
				dbWriteMu.Lock()
				defer dbWriteMu.Unlock()

				if walkCtx.Err() != nil {
					return
				}

				if c.dataMgr != nil {
					if err := c.dataMgr.BatchInsertFiles(id, records); err != nil {
						logger.Error("Cluster", "Initial scan failed for upstream node %d (%s): %v", id, node.Config.URL, err)
						setErr(fmt.Errorf("failed to batch insert files for upstream %d: %w", id, err))
						return
					}
					st, err := c.dataMgr.GetStorage(id)
					if err != nil || st == nil {
						if err == nil {
							err = fmt.Errorf("storage record not found")
						}
						logger.Error("Cluster", "Initial scan failed for upstream node %d (%s): %v", id, node.Config.URL, err)
						setErr(fmt.Errorf("failed to get storage stats for upstream %d: %w", id, err))
						return
					}
					node.SetQuota(st.UsedBytes, st.FileCount)
					logger.Info("Cluster", "Initial scan completed for upstream node %d (%s): found %d files, used %s", id, node.Config.URL, st.FileCount, st.HumanUsed)
				} else {
					var totalUsed int64
					var totalFiles int64
					for _, r := range records {
						if !r.IsDir {
							totalUsed += r.Size
							totalFiles++
						}
					}
					node.SetQuota(totalUsed, totalFiles)
					humanUsed := config.FormatCapacity(uint64(totalUsed))
					logger.Info("Cluster", "Initial scan completed for upstream node %d (%s): found %d files, used %s", id, node.Config.URL, totalFiles, humanUsed)
				}
			}()
		}(n)
	}

	wg.Wait()
	return firstErr
}
// RefreshAllQuotas queries all upstream nodes concurrently to refresh their quota status.
func (c *Cluster) RefreshAllQuotas(ctx context.Context) error {
	nodes := c.AllNodes()
	var wg sync.WaitGroup
	errCh := make(chan error, len(nodes))

	for _, node := range nodes {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			if err := n.UpdateQuota(ctx); err != nil {
				errCh <- fmt.Errorf("node %d (%s): %w", n.ID(), n.Config.URL, err)
			}
		}(node)
	}

	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// DefaultMaxCapacity is the virtual maximum capacity (1 PiB) used when ratio nodes
// (unbounded capacity) are present, preventing 64-bit signed integer overflow.
const DefaultMaxCapacity = int64(1) << 50

// TotalQuota returns the aggregated RFC 4331 capacity (available, used, total)
// summed across all healthy, non-draining nodes.
func (c *Cluster) TotalQuota() (available int64, used int64, total int64) {
	hasRatioNode := false
	var limitTotal int64

	for _, n := range c.HealthyNodes() {
		n.mu.RLock()
		effUsed := n.UsedBytes + n.ReservedBytes
		if effUsed < 0 {
			effUsed = 0
		}
		tot := n.TotalBytes
		isRatio := n.ParsedCapacity.Mode == config.CapacityModeRatio
		n.mu.RUnlock()

		if isRatio {
			hasRatioNode = true
		} else {
			if tot > 0 {
				limitTotal += tot
			}
		}
		if effUsed > 0 {
			used += effUsed
		}
	}

	if hasRatioNode {
		total = DefaultMaxCapacity
		if used >= total {
			total = used + DefaultMaxCapacity
		}
		available = total - used
	} else {
		total = limitTotal
		available = total - used
		if available < 0 {
			available = 0
		}
		if total < used {
			total = used
		}
	}

	return available, used, total
}

// OverwriteContext carries original file storage node ID and size when overwriting an existing file.
type OverwriteContext struct {
	OldNodeID int
	OldSize   int64
}

// SelectWriteNode routes a write operation using normalized capacity load balancing.
// If overwrite context is provided, it virtually credits back the old file size to the original node
// before evaluating which node is best suited to hold the new file.
// 1. If reqSize <= 0, writes are balanced across all healthy candidate nodes.
// 2. Limit nodes are evaluated based on remaining available space and used/capacity load rate.
// 3. Ratio nodes are evaluated based on relative load used/ratio.
// 4. In a mixed pool, limit and ratio nodes are normalized and balanced according to their load.
//    Physical limit nodes with sufficient space and lower load are favored/balanced, while excess
//    automatically overflows to ratio nodes.
// 5. Returns ErrInsufficientStorage only when all limit nodes cannot accommodate size and no ratio nodes exist.
func (c *Cluster) SelectWriteNode(size int64, overwrite ...OverwriteContext) (*Node, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var oldNodeID int
	var oldSize int64
	if len(overwrite) > 0 {
		oldNodeID = overwrite[0].OldNodeID
		oldSize = overwrite[0].OldSize
	}

	return c.selectWriteNodeLocked(size, oldNodeID, oldSize)
}

// DefaultNominalReserve is the default reservation size for unknown-length writes (64MB).
const DefaultNominalReserve = int64(64 * 1024 * 1024)

func (c *Cluster) nominalReserveLocked(node *Node) int64 {
	if node.ParsedCapacity.Mode == config.CapacityModeLimit {
		effAvail := node.AvailableBytes - node.ReservedBytes
		reserveBytes := DefaultNominalReserve
		if effAvail < reserveBytes {
			reserveBytes = effAvail / 2
			if reserveBytes < 1 && effAvail >= 1 {
				reserveBytes = 1
			} else if effAvail <= 0 {
				reserveBytes = 0
			}
		}
		return reserveBytes
	}
	return DefaultNominalReserve
}

// ReserveWrite reserves space on a selected write node for concurrent safe writing.
// If overwrite context is provided, it performs load balancing by virtually crediting back
// oldSize on the original storage node before selecting the best target node.
// For unknown-size requests (reqSize <= 0, e.g. chunked transfers), it uses nominal reservation
// (DefaultNominalReserve = 64MB, or effAvail / 2 if remaining limit space is less than 64MB, at least 1 byte)
// to prevent tie-breaking skew on the lowest ID node.
// Returns the selected node, a commit function accepting actual written bytes,
// and a rollback function, both guarded by sync.Once.
func (c *Cluster) ReserveWrite(reqSize int64, overwrite ...OverwriteContext) (node *Node, commit func(written int64), rollback func(), err error) {
	var oldNodeID int
	var oldSize int64
	if len(overwrite) > 0 {
		oldNodeID = overwrite[0].OldNodeID
		oldSize = overwrite[0].OldSize
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	selectedNode, err := c.selectWriteNodeLocked(reqSize, oldNodeID, oldSize)
	if err != nil {
		return nil, nil, nil, err
	}

	isCrossNode := (oldNodeID > 0 && selectedNode.ID() != oldNodeID)
	var reserveBytes int64
	selectedNode.mu.Lock()
	if reqSize > 0 {
		if !isCrossNode && oldSize > 0 {
			delta := reqSize - oldSize
			if delta > 0 {
				reserveBytes = delta
			}
		} else {
			reserveBytes = reqSize
		}
	} else {
		reserveBytes = c.nominalReserveLocked(selectedNode)
	}
	if reserveBytes > 0 {
		selectedNode.ReservedBytes += reserveBytes
	}
	selectedNode.mu.Unlock()

	var once sync.Once
	commit = func(written int64) {
		once.Do(func() {
			c.mu.RLock()
			dm := c.dataMgr
			c.mu.RUnlock()
			if dm != nil {
				st, err := dm.GetStorage(selectedNode.ID())
				if err == nil && st != nil {
					selectedNode.CommitQuota(reserveBytes, st.UsedBytes, st.FileCount)
					return
				}
			}
			selectedNode.CommitReserve(reserveBytes, written)
		})
	}
	rollback = func() {
		once.Do(func() {
			selectedNode.Unreserve(reserveBytes)
		})
	}

	return selectedNode, commit, rollback, nil
}

// ReserveWriteOnNode reserves incremental space on a specific node for overwriting an existing file.
// If deltaSize > 0 and the node is in Limit mode, it checks whether EffectiveAvailable is sufficient.
// If insufficient, returns ErrInsufficientStorage. If sufficient, it atomically reserves deltaSize.
// If deltaSize <= 0, it is a no-op.
func (c *Cluster) ReserveWriteOnNode(node *Node, reqSize int64, oldSize int64) (commit func(written int64), rollback func(), err error) {
	if node == nil {
		return nil, nil, errors.New("node is nil")
	}

	deltaSize := reqSize - oldSize
	if deltaSize <= 0 {
		noop := func(int64) {}
		noopRollback := func() {}
		return noop, noopRollback, nil
	}

	node.mu.Lock()
	if node.ParsedCapacity.Mode == config.CapacityModeLimit {
		effAvail := node.AvailableBytes - node.ReservedBytes
		if effAvail < deltaSize {
			node.mu.Unlock()
			return nil, nil, ErrInsufficientStorage
		}
	}
	node.ReservedBytes += deltaSize
	node.mu.Unlock()

	var once sync.Once
	commit = func(written int64) {
		once.Do(func() {
			c.mu.RLock()
			dm := c.dataMgr
			c.mu.RUnlock()
			if dm != nil {
				st, err := dm.GetStorage(node.ID())
				if err == nil && st != nil {
					node.CommitQuota(deltaSize, st.UsedBytes, st.FileCount)
					return
				}
			}
			node.CommitReserve(deltaSize, written)
		})
	}
	rollback = func() {
		once.Do(func() {
			node.Unreserve(deltaSize)
		})
	}

	return commit, rollback, nil
}

func (c *Cluster) selectWriteNodeLocked(reqSize int64, oldNodeID int, oldSize int64) (*Node, error) {
	type candidate struct {
		node           *Node
		isRatio        bool
		effAvail       int64
		effUsed        int64
		capacity       int64
		ratio          float64
		id             int
		remainingAvail int64
		expectedUsed   int64
	}

	var limitCandidates []candidate
	var ratioCandidates []candidate

	for _, n := range c.nodes {
		n.mu.RLock()
		draining := n.IsDraining
		mode := n.ParsedCapacity.Mode
		ratio := n.ParsedCapacity.Ratio
		tot := n.TotalBytes
		avail := n.AvailableBytes
		used := n.UsedBytes
		res := n.ReservedBytes
		id := n.Config.ID
		n.mu.RUnlock()

		if draining {
			continue
		}

		effAvail := avail - res
		if effAvail < 0 {
			effAvail = 0
		}
		effUsed := used + res
		if effUsed < 0 {
			effUsed = 0
		}

		// Virtual deduction: credit back oldSize on the original storage node
		if oldNodeID > 0 && id == oldNodeID {
			effAvail += oldSize
			effUsed -= oldSize
			if effUsed < 0 {
				effUsed = 0
			}
		}

		if mode == config.CapacityModeRatio {
			if ratio <= 0 {
				ratio = 1.0
			}
			expectedUsed := effUsed
			if reqSize > 0 {
				expectedUsed += reqSize
			}
			ratioCandidates = append(ratioCandidates, candidate{
				node:         n,
				isRatio:      true,
				effAvail:     math.MaxInt64,
				effUsed:      effUsed,
				expectedUsed: expectedUsed,
				ratio:        ratio,
				id:           id,
			})
		} else {
			// Limit mode: check if this candidate node can accommodate reqSize
			if reqSize > 0 && effAvail < reqSize {
				continue
			}
			if reqSize <= 0 && effAvail <= 0 {
				continue
			}

			capBytes := tot
			if capBytes <= 0 {
				capBytes = effUsed + effAvail
			}
			if capBytes <= 0 {
				capBytes = 1
			}

			remainingAvail := effAvail
			if reqSize > 0 {
				remainingAvail -= reqSize
			}

			limitCandidates = append(limitCandidates, candidate{
				node:           n,
				isRatio:        false,
				effAvail:       effAvail,
				effUsed:        effUsed,
				capacity:       capBytes,
				id:             id,
				remainingAvail: remainingAvail,
			})
		}
	}

	// 1. Most Free Space on Limit nodes (largest remainingAvail)
	if len(limitCandidates) > 0 {
		var best candidate
		bestInit := false
		var maxAvail int64 = -1

		for _, cand := range limitCandidates {
			if !bestInit || cand.remainingAvail > maxAvail || (cand.remainingAvail == maxAvail && cand.id < best.id) {
				best = cand
				bestInit = true
				maxAvail = cand.remainingAvail
			}
		}
		if bestInit {
			return best.node, nil
		}
	}

	// 2. Lowest relative load on Ratio nodes (expectedUsed / ratio)
	if len(ratioCandidates) > 0 {
		var best candidate
		bestInit := false
		minLoad := math.MaxFloat64

		for _, cand := range ratioCandidates {
			load := float64(cand.expectedUsed) / cand.ratio
			const eps = 1e-9
			if !bestInit || load < minLoad-eps || (math.Abs(load-minLoad) <= eps && cand.id < best.id) {
				best = cand
				bestInit = true
				minLoad = load
			}
		}
		if bestInit {
			return best.node, nil
		}
	}

	return nil, ErrInsufficientStorage
}

// RecordWrite updates in-memory quota and persists to data.Manager.
// Supports both path-based updating (if filePath is provided) and usage-based updating.
func (c *Cluster) RecordWrite(node *Node, size int64, isNewFile bool, filePath ...string) {
	if node == nil {
		return
	}
	node.RecordWrite(size)
	c.mu.RLock()
	dm := c.dataMgr
	c.mu.RUnlock()
	if dm != nil {
		if len(filePath) > 0 && strings.TrimSpace(filePath[0]) != "" {
			p := strings.TrimSpace(filePath[0])
			_ = dm.PutFile(data.FileRecord{
				StorageID: node.ID(),
				Path:      p,
				Size:      size,
				IsDir:     false,
				ModTime:   time.Now(),
			})
		} else {
			dm.RecordWrite(node.ID(), size, isNewFile)
		}
	}
}

// RecordOverwrite updates in-memory quota and persists to data.Manager.
// Supports both path-based updating (if filePath is provided) and delta usage updating.
func (c *Cluster) RecordOverwrite(node *Node, deltaSize int64, filePath ...string) {
	if node == nil {
		return
	}
	node.RecordOverwrite(deltaSize)
	c.mu.RLock()
	dm := c.dataMgr
	c.mu.RUnlock()
	if dm != nil {
		if len(filePath) > 0 && strings.TrimSpace(filePath[0]) != "" {
			p := strings.TrimSpace(filePath[0])
			f, err := dm.GetFile(node.ID(), p)
			if err == nil && f != nil {
				newSize := f.Size + deltaSize
				if newSize < 0 {
					newSize = 0
				}
				f.Size = newSize
				f.ModTime = time.Now()
				_ = dm.PutFile(*f)
			} else {
				dm.RecordOverwrite(node.ID(), deltaSize)
			}
		} else {
			dm.RecordOverwrite(node.ID(), deltaSize)
		}
	}
}

// RecordDelete updates in-memory quota and persists to data.Manager.
// Supports both path-based deleting (if filePath is provided) and usage-based deleting.
func (c *Cluster) RecordDelete(node *Node, size int64, filePath ...string) {
	if node == nil {
		return
	}
	node.RecordDelete(size)
	c.mu.RLock()
	dm := c.dataMgr
	c.mu.RUnlock()
	if dm != nil {
		if len(filePath) > 0 && strings.TrimSpace(filePath[0]) != "" {
			p := strings.TrimSpace(filePath[0])
			f, err := dm.GetFile(node.ID(), p)
			if err == nil && f != nil {
				_ = dm.DeleteFile(node.ID(), p)
			} else {
				dm.RecordDelete(node.ID(), size)
			}
		} else {
			dm.RecordDelete(node.ID(), size)
		}
	}
}

// RecordMove moves/renames a file or directory path in data.Manager.
func (c *Cluster) RecordMove(node *Node, oldPath, newPath string) error {
	if node == nil {
		return nil
	}
	c.mu.RLock()
	dm := c.dataMgr
	c.mu.RUnlock()
	if dm != nil {
		return dm.MovePath(node.ID(), oldPath, newPath)
	}
	return nil
}

// RecordMovePath is an alias for RecordMove.
func (c *Cluster) RecordMovePath(node *Node, oldPath, newPath string) error {
	return c.RecordMove(node, oldPath, newPath)
}

// GetNode retrieves a node by its upstream ID.
func (c *Cluster) GetNode(id int) (*Node, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n, ok := c.nodeMap[id]
	return n, ok
}

// HealthyNodes returns all nodes that are not marked as draining.
func (c *Cluster) HealthyNodes() []*Node {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var healthy []*Node
	for _, n := range c.nodes {
		n.mu.RLock()
		draining := n.IsDraining
		n.mu.RUnlock()
		if !draining {
			healthy = append(healthy, n)
		}
	}
	return healthy
}

// AllNodes returns a copy of all managed nodes.
func (c *Cluster) AllNodes() []*Node {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]*Node, len(c.nodes))
	copy(out, c.nodes)
	return out
}

// AddNode creates and adds a new upstream node to the cluster.
func (c *Cluster) AddNode(u config.UpstreamConfig) (*Node, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.nodeMap[u.ID]; exists {
		return nil, fmt.Errorf("node %d already exists", u.ID)
	}

	retry := config.DefaultRetry
	proxy := ""
	if c.cfg != nil {
		if c.cfg.Retry > 0 {
			retry = c.cfg.Retry
		}
		proxy = c.cfg.Proxy
	}

	cli, err := client.NewClient(u.URL, u.User, u.Pass, proxy, retry, 0)
	if err != nil {
		return nil, fmt.Errorf("create client for upstream %d: %w", u.ID, err)
	}

	node := NewNode(u, cli)
	c.nodes = append(c.nodes, node)
	c.nodeMap[u.ID] = node
	return node, nil
}

// RemoveNode removes a node by ID from the cluster.
func (c *Cluster) RemoveNode(id int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.nodeMap[id]; !ok {
		return false
	}

	delete(c.nodeMap, id)
	for i, n := range c.nodes {
		if n.Config.ID == id {
			c.nodes = append(c.nodes[:i], c.nodes[i+1:]...)
			break
		}
	}
	return true
}
