package cluster

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"webdav-agg/src/client"
	"webdav-agg/src/config"
	"webdav-agg/src/data"
)

const defaultMigrateWorkers = 4

// MigrateNode evacuates all files from sourceID to other healthy nodes in the cluster.
// It performs:
// 1) Mark source node as draining;
// 2) Recursive walk of all files on the source node;
// 3) Preflight check that other healthy nodes have sufficient total available space;
// 4) Concurrent streaming transfer using a worker pool (GET source -> ReserveWrite target -> PUT target -> WithTx atomic metadata switch -> source DELETE);
// 5) Calls onProgress safely per completed file.
func (c *Cluster) MigrateNode(
	ctx context.Context,
	sourceID int,
	onProgress func(current, total int, path string, targetID int, size int64),
) error {
	sourceNode, ok := c.GetNode(sourceID)
	if !ok {
		return fmt.Errorf("source node %d not found", sourceID)
	}

	// Mark source node as draining so SelectWriteNode avoids selecting it
	sourceNode.SetDraining(true)

	// Step 1: Walk all files on source node
	var files []client.FileInfo
	err := sourceNode.Client.Walk(ctx, "/", func(info client.FileInfo) error {
		if !info.IsDir {
			files = append(files, info)
		}
		return nil
	})
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		return fmt.Errorf("walk source node %d failed: %w", sourceID, err)
	}

	if len(files) == 0 {
		return nil
	}

	// Step 2: Preflight check available space on target nodes
	var totalSourceSize int64
	for _, f := range files {
		totalSourceSize += f.Size
	}

	var totalTargetAvail int64
	for _, n := range c.HealthyNodes() {
		if n.Config.ID == sourceID {
			continue
		}
		n.mu.RLock()
		avail := n.AvailableBytes
		n.mu.RUnlock()
		if avail > 0 {
			totalTargetAvail += avail
		}
	}

	if totalTargetAvail < totalSourceSize {
		return fmt.Errorf("%w: target nodes have %d bytes available, required %d bytes", ErrInsufficientStorage, totalTargetAvail, totalSourceSize)
	}

	// Step 3 & 4: Concurrent worker pool migration
	totalFiles := len(files)
	maxRetries := config.DefaultRetry
	if c.cfg != nil && c.cfg.Retry > 0 {
		maxRetries = c.cfg.Retry
	}

	dm := c.DataManager()
	mCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	numWorkers := defaultMigrateWorkers
	if len(files) < numWorkers {
		numWorkers = len(files)
	}
	if numWorkers <= 0 {
		numWorkers = 1
	}

	fileCh := make(chan client.FileInfo, len(files))
	for _, f := range files {
		fileCh <- f
	}
	close(fileCh)

	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	var completedFiles atomic.Int64
	var progressMu sync.Mutex

	setErr := func(err error) {
		if err != nil {
			errOnce.Do(func() {
				firstErr = err
				cancel()
			})
		}
	}

	migrateSingleFile := func(file client.FileInfo) error {
		var lastErr error
		var chosenTarget *Node
		var alreadyMigrated bool
		var reservedCommit func(written int64)
		var reservedRollback func()

		for attempt := 0; attempt <= maxRetries; attempt++ {
			if attempt > 0 {
				select {
				case <-mCtx.Done():
					return mCtx.Err()
				case <-time.After(time.Duration(attempt) * 50 * time.Millisecond):
				}
			}

			targetNode, commit, rollback, err := c.ReserveWrite(file.Size)
			if err != nil {
				// If target selection / reservation failed, check if the file was already migrated in a previous run
				for _, n := range c.HealthyNodes() {
					if n.ID() == sourceID || n.Client == nil {
						continue
					}
					items, statErr := n.Client.List(mCtx, file.Path, 0)
					if statErr == nil && len(items) > 0 && !items[0].IsDir && items[0].Size == file.Size {
						rc, _, getErr := sourceNode.Client.Get(mCtx, file.Path, nil)
						if getErr != nil {
							if errors.Is(getErr, client.ErrNotFound) || strings.Contains(getErr.Error(), "404") || errors.Is(getErr, os.ErrNotExist) {
								chosenTarget = n
								alreadyMigrated = true
								lastErr = nil
								break
							}
						} else if rc != nil {
							_ = rc.Close()
						}
					}
				}
				if alreadyMigrated {
					break
				}
				lastErr = err
				continue
			}

			chosenTarget = targetNode
			reservedCommit = commit
			reservedRollback = rollback

			// Fresh stream on every attempt
			rc, hdr, err := sourceNode.Client.Get(mCtx, file.Path, nil)
			if err != nil {
				rollback()
				if errors.Is(err, client.ErrNotFound) || strings.Contains(err.Error(), "404") || errors.Is(err, os.ErrNotExist) {
					items, statErr := chosenTarget.Client.List(mCtx, file.Path, 0)
					if statErr == nil && len(items) > 0 && !items[0].IsDir && items[0].Size == file.Size {
						alreadyMigrated = true
						lastErr = nil
						break
					}
				}
				lastErr = fmt.Errorf("get %s from source node %d: %w", file.Path, sourceID, err)
				continue
			}

			contentType := ""
			if hdr != nil {
				contentType = hdr.Get("Content-Type")
			}
			if contentType == "" {
				contentType = file.ContentType
			}

			putErr := chosenTarget.Client.Put(mCtx, file.Path, rc, file.Size, contentType)
			_ = rc.Close()
			if putErr != nil {
				rollback()
				delCtx, delCancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = chosenTarget.Client.Delete(delCtx, file.Path)
				delCancel()
				lastErr = fmt.Errorf("put %s to target node %d: %w", file.Path, chosenTarget.Config.ID, putErr)
				continue
			}

			lastErr = nil
			break
		}

		if lastErr != nil {
			return fmt.Errorf("migration of %s failed after %d retries: %w", file.Path, maxRetries, lastErr)
		}

		if !alreadyMigrated {
			if dm != nil {
				modTime := file.ModTime
				if modTime.IsZero() {
					modTime = time.Now()
				}
				txErr := dm.WithTx(mCtx, func(tx *sql.Tx) error {
					if err := dm.PutFileTx(tx, data.FileRecord{
						StorageID: chosenTarget.Config.ID,
						Path:      file.Path,
						Size:      file.Size,
						IsDir:     file.IsDir,
						ModTime:   modTime,
					}); err != nil {
						return err
					}
					return dm.DeleteFileTx(tx, sourceID, file.Path)
				})
				if txErr != nil {
					reservedRollback()
					delCtx, delCancel := context.WithTimeout(context.Background(), 5*time.Second)
					_ = chosenTarget.Client.Delete(delCtx, file.Path)
					delCancel()
					return fmt.Errorf("metadata tx for %s failed: %w", file.Path, txErr)
				}
			}

			reservedCommit(file.Size)
			sourceNode.RecordDelete(file.Size)
		} else {
			if dm != nil {
				modTime := file.ModTime
				if modTime.IsZero() {
					modTime = time.Now()
				}
				_ = dm.WithTx(mCtx, func(tx *sql.Tx) error {
					if err := dm.PutFileTx(tx, data.FileRecord{
						StorageID: chosenTarget.Config.ID,
						Path:      file.Path,
						Size:      file.Size,
						IsDir:     file.IsDir,
						ModTime:   modTime,
					}); err != nil {
						return err
					}
					return dm.DeleteFileTx(tx, sourceID, file.Path)
				})
			}
			sourceNode.RecordDelete(file.Size)
		}

		// Delete from source node with retry
		var delErr error
		for dAttempt := 0; dAttempt <= maxRetries; dAttempt++ {
			delErr = sourceNode.Client.Delete(mCtx, file.Path)
			if delErr == nil || errors.Is(delErr, client.ErrNotFound) || (delErr != nil && strings.Contains(delErr.Error(), "404")) || errors.Is(delErr, os.ErrNotExist) {
				delErr = nil
				break
			}
			select {
			case <-mCtx.Done():
				return mCtx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}
		if delErr != nil {
			return fmt.Errorf("file %s migrated to node %d but failed to delete from source node %d: %w", file.Path, chosenTarget.Config.ID, sourceID, delErr)
		}

		curr := int(completedFiles.Add(1))
		if onProgress != nil {
			progressMu.Lock()
			onProgress(curr, totalFiles, file.Path, chosenTarget.Config.ID, file.Size)
			progressMu.Unlock()
		}

		return nil
	}

	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for file := range fileCh {
				select {
				case <-mCtx.Done():
					return
				default:
				}

				if err := migrateSingleFile(file); err != nil {
					setErr(err)
					return
				}
			}
		}()
	}

	wg.Wait()

	if firstErr != nil {
		return firstErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if dm != nil {
		if st, err := dm.GetStorage(sourceID); err == nil && st != nil {
			sourceNode.SetQuota(st.UsedBytes, st.FileCount)
		}
	}

	return nil
}
