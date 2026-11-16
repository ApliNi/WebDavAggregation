package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
	_ "modernc.org/sqlite"

	"webdav-agg/src/config"
)

// ErrNotFound indicates the requested resource or file does not exist.
var ErrNotFound = os.ErrNotExist

// StorageRecord represents an upstream storage node entry in SQLite storages table.
type StorageRecord struct {
	ID        int        `json:"id" yaml:"id"`
	UsedBytes int64      `json:"used_bytes" yaml:"used_bytes"`
	FileCount int64      `json:"file_count" yaml:"file_count"`
	HumanUsed string     `json:"human_used" yaml:"human_used"`
	LastSync  *time.Time `json:"last_sync,omitempty" yaml:"last_sync,omitempty"`
	UpdatedAt time.Time  `json:"updated_at" yaml:"updated_at"`
}

// FileRecord represents a file or directory entry in SQLite files table.
type FileRecord struct {
	StorageID    int       `json:"storage_id" yaml:"storage_id"`
	Path         string    `json:"path" yaml:"path"`
	ParentPath   string    `json:"parent_path,omitempty" yaml:"parent_path,omitempty"`
	Size         int64     `json:"size" yaml:"size"`
	IsDir        bool      `json:"is_dir" yaml:"is_dir"`
	ModTime      time.Time `json:"mod_time" yaml:"mod_time"`
	LastProbedAt time.Time `json:"last_probed_at" yaml:"last_probed_at"`
}

// Name returns the base name of the file or directory.
func (r FileRecord) Name() string {
	if r.Path == "" || r.Path == "/" {
		return "/"
	}
	return path.Base(r.Path)
}

// ETag returns an entity tag generated from ModTime and Size.
func (r FileRecord) ETag() string {
	return fmt.Sprintf(`"%x-%x"`, r.ModTime.UnixNano(), r.Size)
}

// ContentType returns the MIME content type based on path extension or directory status.
func (r FileRecord) ContentType() string {
	if r.IsDir {
		return "httpd/unix-directory"
	}
	ct := mime.TypeByExtension(path.Ext(r.Path))
	if ct == "" {
		return "application/octet-stream"
	}
	return ct
}

// FileInfo is an alias for FileRecord.
type FileInfo = FileRecord

// legacyNodeData stores legacy capacity and file statistics from data.yml.
type legacyNodeData struct {
	Used      int64     `yaml:"used" json:"used"`
	HumanUsed string    `yaml:"human_used" json:"human_used"`
	Files     int64     `yaml:"files" json:"files"`
	LastSync  time.Time `yaml:"last_sync,omitempty" json:"last_sync,omitempty"`
	UpdatedAt time.Time `yaml:"updated_at" json:"updated_at"`
}

// legacyStoreData represents the legacy data structure persisted in data.yml.
type legacyStoreData struct {
	Version   int                     `yaml:"version" json:"version"`
	Upstreams map[int]*legacyNodeData `yaml:"upstreams" json:"upstreams"`
}

// Manager manages storage quotas and file metadata persisted in SQLite.
type Manager struct {
	filePath  string
	db        *sql.DB
	closeOnce sync.Once
}

const schemaDDL = `
CREATE TABLE IF NOT EXISTS storages (
    id INTEGER PRIMARY KEY,
    used_bytes INTEGER NOT NULL DEFAULT 0,
    file_count INTEGER NOT NULL DEFAULT 0,
    last_sync TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS files (
    storage_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    parent_path TEXT NOT NULL,
    size INTEGER NOT NULL DEFAULT 0,
    is_dir INTEGER NOT NULL DEFAULT 0,
    mod_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_probed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (storage_id, path),
    FOREIGN KEY (storage_id) REFERENCES storages(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);
CREATE INDEX IF NOT EXISTS idx_files_parent ON files(parent_path);
CREATE INDEX IF NOT EXISTS idx_files_storage_parent ON files(storage_id, parent_path);

CREATE TRIGGER IF NOT EXISTS trg_files_insert
AFTER INSERT ON files
FOR EACH ROW
WHEN NEW.is_dir = 0
BEGIN
    UPDATE storages
    SET used_bytes = used_bytes + NEW.size,
        file_count = file_count + 1,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = NEW.storage_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_files_delete
AFTER DELETE ON files
FOR EACH ROW
WHEN OLD.is_dir = 0
BEGIN
    UPDATE storages
    SET used_bytes = CASE WHEN used_bytes - OLD.size < 0 THEN 0 ELSE used_bytes - OLD.size END,
        file_count = CASE WHEN file_count - 1 < 0 THEN 0 ELSE file_count - 1 END,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = OLD.storage_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_files_update_same
AFTER UPDATE ON files
FOR EACH ROW
WHEN OLD.storage_id = NEW.storage_id AND (OLD.size != NEW.size OR OLD.is_dir != NEW.is_dir)
BEGIN
    UPDATE storages
    SET used_bytes = CASE WHEN used_bytes + (
            CASE WHEN NEW.is_dir = 0 THEN NEW.size ELSE 0 END -
            CASE WHEN OLD.is_dir = 0 THEN OLD.size ELSE 0 END
        ) < 0 THEN 0 ELSE used_bytes + (
            CASE WHEN NEW.is_dir = 0 THEN NEW.size ELSE 0 END -
            CASE WHEN OLD.is_dir = 0 THEN OLD.size ELSE 0 END
        ) END,
        file_count = CASE WHEN file_count + (
            CASE WHEN NEW.is_dir = 0 THEN 1 ELSE 0 END -
            CASE WHEN OLD.is_dir = 0 THEN 1 ELSE 0 END
        ) < 0 THEN 0 ELSE file_count + (
            CASE WHEN NEW.is_dir = 0 THEN 1 ELSE 0 END -
            CASE WHEN OLD.is_dir = 0 THEN 1 ELSE 0 END
        ) END,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = NEW.storage_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_files_update_diff
AFTER UPDATE ON files
FOR EACH ROW
WHEN OLD.storage_id != NEW.storage_id
BEGIN
    UPDATE storages
    SET used_bytes = CASE WHEN used_bytes - CASE WHEN OLD.is_dir = 0 THEN OLD.size ELSE 0 END < 0 THEN 0 ELSE used_bytes - CASE WHEN OLD.is_dir = 0 THEN OLD.size ELSE 0 END END,
        file_count = CASE WHEN file_count - CASE WHEN OLD.is_dir = 0 THEN 1 ELSE 0 END < 0 THEN 0 ELSE file_count - CASE WHEN OLD.is_dir = 0 THEN 1 ELSE 0 END END,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = OLD.storage_id;

    UPDATE storages
    SET used_bytes = used_bytes + CASE WHEN NEW.is_dir = 0 THEN NEW.size ELSE 0 END,
        file_count = file_count + CASE WHEN NEW.is_dir = 0 THEN 1 ELSE 0 END,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = NEW.storage_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_files_delete_dir
AFTER DELETE ON files
FOR EACH ROW
WHEN OLD.is_dir = 1 AND OLD.path != '/'
BEGIN
    DELETE FROM files
    WHERE storage_id = OLD.storage_id
      AND path >= OLD.path || '/' AND path < OLD.path || '0';
END;

CREATE TRIGGER IF NOT EXISTS trg_files_delete_root_dir
AFTER DELETE ON files
FOR EACH ROW
WHEN OLD.is_dir = 1 AND OLD.path = '/'
BEGIN
    DELETE FROM files
    WHERE storage_id = OLD.storage_id
      AND path > '/';
END;
`

// NormalizePath normalizes a file or directory path:
// - Resolves relative segments (path.Clean)
// - Replaces Windows backslashes with slashes
// - Ensures a leading slash
// - Removes trailing slash (unless root path "/")
// - Root path is strictly "/"
func NormalizePath(p string) string {
	if p == "" || p == "/" || p == "\\" || p == "." {
		return "/"
	}
	p = strings.ReplaceAll(p, "\\", "/")
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	cleaned := path.Clean(p)
	if cleaned == "." || cleaned == "" {
		return "/"
	}
	if len(cleaned) > 1 && strings.HasSuffix(cleaned, "/") {
		cleaned = strings.TrimSuffix(cleaned, "/")
	}
	return cleaned
}

// SplitPath normalizes path and splits into (parent_path, name).
// For root "/", parent_path is "" and name is "/".
// For "/a", parent_path is "/" and name is "a".
// For "/a/b", parent_path is "/a" and name is "b".
func SplitPath(p string) (parent string, name string) {
	norm := NormalizePath(p)
	if norm == "/" {
		return "", "/"
	}
	dir := path.Dir(norm)
	base := path.Base(norm)
	return NormalizePath(dir), base
}

// NewManager creates a Manager instance connected to the SQLite database.
func NewManager(filePath string) (*Manager, error) {
	if filePath == "" {
		filePath = config.DefaultDataFile
	}

	var legacyData *legacyStoreData
	if filePath != ":memory:" {
		// Check for legacy YAML file migration
		if dataBytes, err := os.ReadFile(filePath); err == nil && len(dataBytes) > 0 {
			if len(dataBytes) < 16 || string(dataBytes[:15]) != "SQLite format 3" {
				var legacy legacyStoreData
				if err := yaml.Unmarshal(dataBytes, &legacy); err == nil && len(legacy.Upstreams) > 0 {
					legacyData = &legacy
					_ = os.WriteFile(filePath+".bak", dataBytes, 0644)
					_ = os.Remove(filePath)
				}
			}
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("failed to read %s: %w", filePath, err)
		}

		dir := filepath.Dir(filePath)
		if dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
			}
		}
	}

	dsn := filePath
	if filePath == ":memory:" {
		dsn = fmt.Sprintf("file:mem_%d?mode=memory&cache=shared&_busy_timeout=5000&_foreign_keys=1&_synchronous=NORMAL&_pragma=recursive_triggers(ON)&_txlock=immediate", time.Now().UnixNano())
	} else {
		sep := "?"
		if strings.Contains(filePath, "?") {
			sep = "&"
		}
		dsn = fmt.Sprintf("%s%s_busy_timeout=5000&_foreign_keys=1&_journal_mode=WAL&_synchronous=NORMAL&_pragma=recursive_triggers(ON)&_txlock=immediate", filePath, sep)
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database %s: %w", filePath, err)
	}

	if filePath == ":memory:" {
		db.SetMaxOpenConns(1)
	} else {
		db.SetMaxOpenConns(25)
	}

	// Verify recursive_triggers is effective across multiple connections in the pool
	numVerify := 5
	if filePath == ":memory:" {
		numVerify = 1
	}
	conns := make([]*sql.Conn, 0, numVerify)
	for range numVerify {
		c, err := db.Conn(context.Background())
		if err != nil {
			for _, opened := range conns {
				_ = opened.Close()
			}
			_ = db.Close()
			return nil, fmt.Errorf("acquire connection for trigger verification: %w", err)
		}
		conns = append(conns, c)
	}
	for _, c := range conns {
		var rt int
		err := c.QueryRowContext(context.Background(), "PRAGMA recursive_triggers;").Scan(&rt)
		_ = c.Close()
		if err != nil || rt != 1 {
			_ = db.Close()
			return nil, fmt.Errorf("recursive_triggers verification failed (value=%d): %w", rt, err)
		}
	}

	// Explicitly execute required PRAGMAs
	if filePath != ":memory:" {
		if _, err := db.Exec("PRAGMA journal_mode = WAL;"); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("pragma journal_mode=WAL: %w", err)
		}
	}

	pragmas := []string{
		"PRAGMA synchronous = NORMAL;",
		"PRAGMA busy_timeout = 5000;",
		"PRAGMA foreign_keys = ON;",
		"PRAGMA recursive_triggers = ON;",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("execute %s: %w", p, err)
		}
	}

	_, _ = db.Exec("DROP TRIGGER IF EXISTS trg_files_delete_dir;")
	_, _ = db.Exec("DROP TRIGGER IF EXISTS trg_files_delete_root_dir;")

	if _, err := db.Exec(schemaDDL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize database schema: %w", err)
	}

	// Smooth schema migration for existing databases:
	// SQLite ALTER TABLE does not allow non-constant DEFAULT expressions (e.g. CURRENT_TIMESTAMP).
	// We add column with constant default or NULL, then backfill CURRENT_TIMESTAMP.
	if _, err := db.Exec("ALTER TABLE files ADD COLUMN last_probed_at TIMESTAMP DEFAULT '1970-01-01 00:00:00';"); err == nil {
		_, _ = db.Exec("UPDATE files SET last_probed_at = CURRENT_TIMESTAMP WHERE last_probed_at = '1970-01-01 00:00:00';")
	}

	// Migrate legacy YAML stats into storages if applicable
	if legacyData != nil && len(legacyData.Upstreams) > 0 {
		for id, node := range legacyData.Upstreams {
			if node != nil {
				_, _ = db.Exec(`
					INSERT INTO storages (id, used_bytes, file_count, last_sync, updated_at)
					VALUES (?, ?, ?, NULL, ?)
					ON CONFLICT(id) DO NOTHING;
				`, id, node.Used, node.Files, node.UpdatedAt)
			}
		}
	}

	m := &Manager{
		filePath: filePath,
		db:       db,
	}

	return m, nil
}

// FilePath returns the persisted file path.
func (m *Manager) FilePath() string {
	return m.filePath
}

// DB returns the underlying sql.DB instance.
func (m *Manager) DB() *sql.DB {
	return m.db
}

// WithTx executes fn within an IMMEDIATE SQLite transaction.
// If fn returns nil, the transaction is committed; if fn returns an error or panics, it is rolled back.
func (m *Manager) WithTx(ctx context.Context, fn func(tx *sql.Tx) error) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		} else if err != nil {
			_ = tx.Rollback()
		} else {
			if commitErr := tx.Commit(); commitErr != nil {
				err = fmt.Errorf("commit tx: %w", commitErr)
			}
		}
	}()

	err = fn(tx)
	return err
}

// EnsureStorage ensures a storage node entry exists in storages table.
func (m *Manager) EnsureStorage(id int) error {
	_, err := m.db.Exec(`
		INSERT INTO storages (id, used_bytes, file_count, updated_at)
		VALUES (?, 0, 0, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO NOTHING;
	`, id)
	return err
}

// Get retrieves StorageRecord for the given upstream id (compatibility wrapper around GetStorage).
func (m *Manager) Get(id int) (*StorageRecord, bool) {
	rec, err := m.GetStorage(id)
	if err != nil {
		return nil, false
	}
	return rec, true
}

// Set updates or inserts the quota statistics for an upstream node.
func (m *Manager) Set(id int, used int64, files int64, isSync bool) {
	if used < 0 {
		used = 0
	}
	if files < 0 {
		files = 0
	}

	now := time.Now()
	var lastSyncVal interface{}
	if isSync {
		lastSyncVal = now
	}

	_, _ = m.db.Exec(`
		INSERT INTO storages (id, used_bytes, file_count, last_sync, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			used_bytes = excluded.used_bytes,
			file_count = excluded.file_count,
			last_sync = CASE WHEN ? THEN excluded.last_sync ELSE storages.last_sync END,
			updated_at = excluded.updated_at;
	`, id, used, files, lastSyncVal, now, isSync)
}

// RecordWrite increments used capacity and optionally files count.
func (m *Manager) RecordWrite(id int, size int64, isNewFile bool) {
	if size <= 0 && !isNewFile {
		return
	}

	deltaFiles := int64(0)
	if isNewFile {
		deltaFiles = 1
	}
	deltaSize := size
	if deltaSize < 0 {
		deltaSize = 0
	}

	_, _ = m.db.Exec(`
		INSERT INTO storages (id, used_bytes, file_count, updated_at)
		VALUES (?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO UPDATE SET
			used_bytes = storages.used_bytes + ?,
			file_count = storages.file_count + ?,
			updated_at = CURRENT_TIMESTAMP;
	`, id, deltaSize, deltaFiles, deltaSize, deltaFiles)
}

// RecordOverwrite updates used capacity after an in-place overwrite.
func (m *Manager) RecordOverwrite(id int, deltaSize int64) {
	if deltaSize == 0 {
		return
	}

	_, _ = m.db.Exec(`
		INSERT INTO storages (id, used_bytes, file_count, updated_at)
		VALUES (?, CASE WHEN ? < 0 THEN 0 ELSE ? END, 0, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO UPDATE SET
			used_bytes = CASE WHEN storages.used_bytes + ? < 0 THEN 0 ELSE storages.used_bytes + ? END,
			updated_at = CURRENT_TIMESTAMP;
	`, id, deltaSize, deltaSize, deltaSize, deltaSize)
}

// RecordDelete decrements used capacity and files count.
func (m *Manager) RecordDelete(id int, size int64) {
	delSize := size
	if delSize < 0 {
		delSize = 0
	}

	_, _ = m.db.Exec(`
		UPDATE storages
		SET used_bytes = CASE WHEN used_bytes - ? < 0 THEN 0 ELSE used_bytes - ? END,
		    file_count = CASE WHEN file_count - 1 < 0 THEN 0 ELSE file_count - 1 END,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = ?;
	`, delSize, delSize, id)
}

// GetStorage retrieves StorageRecord for the given storage id.
func (m *Manager) GetStorage(id int) (*StorageRecord, error) {
	var r StorageRecord
	var lastSync sql.NullTime
	err := m.db.QueryRow(`
		SELECT id, used_bytes, file_count, last_sync, updated_at
		FROM storages
		WHERE id = ?;
	`, id).Scan(&r.ID, &r.UsedBytes, &r.FileCount, &lastSync, &r.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get storage %d: %w", id, err)
	}
	r.HumanUsed = config.FormatCapacity(uint64(r.UsedBytes))
	if lastSync.Valid {
		r.LastSync = &lastSync.Time
	}
	return &r, nil
}

// GetAllStorages returns all storage records ordered by id.
func (m *Manager) GetAllStorages() ([]StorageRecord, error) {
	rows, err := m.db.Query(`
		SELECT id, used_bytes, file_count, last_sync, updated_at
		FROM storages
		ORDER BY id ASC;
	`)
	if err != nil {
		return nil, fmt.Errorf("get all storages: %w", err)
	}
	defer rows.Close()

	var list []StorageRecord
	for rows.Next() {
		var r StorageRecord
		var lastSync sql.NullTime
		if err := rows.Scan(&r.ID, &r.UsedBytes, &r.FileCount, &lastSync, &r.UpdatedAt); err != nil {
			return nil, err
		}
		r.HumanUsed = config.FormatCapacity(uint64(r.UsedBytes))
		if lastSync.Valid {
			r.LastSync = &lastSync.Time
		}
		list = append(list, r)
	}
	return list, rows.Err()
}

// GetAllStorageIDs returns all storage IDs present in either storages or files table.
func (m *Manager) GetAllStorageIDs() ([]int, error) {
	rows, err := m.db.Query("SELECT id FROM storages UNION SELECT storage_id FROM files ORDER BY 1 ASC;")
	if err != nil {
		return nil, fmt.Errorf("get all storage ids: %w", err)
	}
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

// DeleteStorageTx deletes a storage and all its files within the provided transaction.
func (m *Manager) DeleteStorageTx(tx *sql.Tx, id int) error {
	if _, err := tx.Exec("DELETE FROM files WHERE storage_id = ?;", id); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM storages WHERE id = ?;", id); err != nil {
		return err
	}
	return nil
}

// DeleteStorage deletes a storage and cascades deletion of all its files.
func (m *Manager) DeleteStorage(id int) error {
	return m.WithTx(context.Background(), func(tx *sql.Tx) error {
		return m.DeleteStorageTx(tx, id)
	})
}

// RouteFile queries which storage has the specified file path.
// If multiple storages hold the path, returns the entry with lowest storage_id.
func (m *Manager) RouteFile(filePath string) (*FileRecord, error) {
	norm := NormalizePath(filePath)

	var r FileRecord
	var isDirInt int
	var modTime time.Time
	var lastProbedAt time.Time

	err := m.db.QueryRow(`
		SELECT storage_id, path, parent_path, size, is_dir, mod_time, last_probed_at
		FROM files
		WHERE path = ?
		ORDER BY storage_id ASC
		LIMIT 1;
	`, norm).Scan(&r.StorageID, &r.Path, &r.ParentPath, &r.Size, &isDirInt, &modTime, &lastProbedAt)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("route file %s: %w", norm, err)
	}

	r.IsDir = isDirInt == 1
	r.ModTime = modTime
	r.LastProbedAt = lastProbedAt
	return &r, nil
}

// GetFile retrieves a specific file record for a storage and path.
func (m *Manager) GetFile(storageID int, filePath string) (*FileRecord, error) {
	norm := NormalizePath(filePath)

	var r FileRecord
	var isDirInt int
	var modTime time.Time
	var lastProbedAt time.Time

	err := m.db.QueryRow(`
		SELECT storage_id, path, parent_path, size, is_dir, mod_time, last_probed_at
		FROM files
		WHERE storage_id = ? AND path = ?;
	`, storageID, norm).Scan(&r.StorageID, &r.Path, &r.ParentPath, &r.Size, &isDirInt, &modTime, &lastProbedAt)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get file %d:%s: %w", storageID, norm, err)
	}

	r.IsDir = isDirInt == 1
	r.ModTime = modTime
	r.LastProbedAt = lastProbedAt
	return &r, nil
}

// GetFilesByPath retrieves all file records for the given path across all storages.
func (m *Manager) GetFilesByPath(filePath string) ([]FileRecord, error) {
	norm := NormalizePath(filePath)

	rows, err := m.db.Query(`
		SELECT storage_id, path, parent_path, size, is_dir, mod_time, last_probed_at
		FROM files
		WHERE path = ?
		ORDER BY storage_id ASC;
	`, norm)
	if err != nil {
		return nil, fmt.Errorf("get files by path %s: %w", norm, err)
	}
	defer rows.Close()

	return scanFileRecords(rows)
}

// ListChildren lists direct children in parentPath across all storages (PROPFIND Depth:1).
func (m *Manager) ListChildren(parentPath string) ([]FileRecord, error) {
	norm := NormalizePath(parentPath)

	rows, err := m.db.Query(`
		SELECT storage_id, path, parent_path, size, is_dir, mod_time, last_probed_at
		FROM files
		WHERE parent_path = ?
		ORDER BY is_dir DESC, path ASC, storage_id ASC;
	`, norm)
	if err != nil {
		return nil, fmt.Errorf("list children of %s: %w", norm, err)
	}
	defer rows.Close()

	return scanFileRecords(rows)
}

// ListChildrenByStorage lists direct children in parentPath for a specific storage (PROPFIND Depth:1).
func (m *Manager) ListChildrenByStorage(storageID int, parentPath string) ([]FileRecord, error) {
	norm := NormalizePath(parentPath)

	rows, err := m.db.Query(`
		SELECT storage_id, path, parent_path, size, is_dir, mod_time, last_probed_at
		FROM files
		WHERE storage_id = ? AND parent_path = ?
		ORDER BY is_dir DESC, path ASC;
	`, storageID, norm)
	if err != nil {
		return nil, fmt.Errorf("list children of %d:%s: %w", storageID, norm, err)
	}
	defer rows.Close()

	return scanFileRecords(rows)
}

// ListSubtree queries all files and directories in rootPath and its descendants.
func (m *Manager) ListSubtree(rootPath string) ([]FileRecord, error) {
	norm := NormalizePath(rootPath)

	var rows *sql.Rows
	var err error
	if norm == "/" {
		rows, err = m.db.Query(`
			SELECT storage_id, path, parent_path, size, is_dir, mod_time, last_probed_at
			FROM files
			ORDER BY path ASC, storage_id ASC;
		`)
	} else {
		rows, err = m.db.Query(`
			SELECT storage_id, path, parent_path, size, is_dir, mod_time, last_probed_at
			FROM files
			WHERE path = ? OR (path >= ? || '/' AND path < ? || '0')
			ORDER BY path ASC, storage_id ASC;
		`, norm, norm, norm)
	}
	if err != nil {
		return nil, fmt.Errorf("list subtree %s: %w", norm, err)
	}
	defer rows.Close()

	return scanFileRecords(rows)
}

// ListSubtreeByStorage queries all files and directories in rootPath and its descendants for a specific storage.
func (m *Manager) ListSubtreeByStorage(storageID int, rootPath string) ([]FileRecord, error) {
	norm := NormalizePath(rootPath)

	var rows *sql.Rows
	var err error
	if norm == "/" {
		rows, err = m.db.Query(`
			SELECT storage_id, path, parent_path, size, is_dir, mod_time, last_probed_at
			FROM files
			WHERE storage_id = ?
			ORDER BY path ASC;
		`, storageID)
	} else {
		rows, err = m.db.Query(`
			SELECT storage_id, path, parent_path, size, is_dir, mod_time, last_probed_at
			FROM files
			WHERE (storage_id = ? AND path = ?) OR (storage_id = ? AND path >= ? || '/' AND path < ? || '0')
			ORDER BY path ASC;
		`, storageID, norm, storageID, norm, norm)
	}
	if err != nil {
		return nil, fmt.Errorf("list subtree %d:%s: %w", storageID, norm, err)
	}
	defer rows.Close()

	return scanFileRecords(rows)
}

// DeleteSubtree deletes rootPath and all its descendant files and directories across all storages.
func (m *Manager) DeleteSubtree(rootPath string) error {
	norm := NormalizePath(rootPath)

	return m.WithTx(context.Background(), func(tx *sql.Tx) error {
		if norm == "/" {
			_, err := tx.Exec("DELETE FROM files;")
			return err
		}

		_, err := tx.Exec(`
			DELETE FROM files
			WHERE path = ? OR (path >= ? || '/' AND path < ? || '0');
		`, norm, norm, norm)
		return err
	})
}

// DeleteSubtreeByStorageTx deletes rootPath and all its descendants for a specific storage within the provided transaction.
func (m *Manager) DeleteSubtreeByStorageTx(tx *sql.Tx, storageID int, rootPath string) error {
	norm := NormalizePath(rootPath)

	if norm == "/" {
		_, err := tx.Exec("DELETE FROM files WHERE storage_id = ?;", storageID)
		return err
	}

	_, err := tx.Exec(`
		DELETE FROM files
		WHERE (storage_id = ? AND path = ?) OR (storage_id = ? AND path >= ? || '/' AND path < ? || '0');
	`, storageID, norm, storageID, norm, norm)
	return err
}

// DeleteSubtreeByStorage deletes rootPath and all its descendants for a specific storage.
func (m *Manager) DeleteSubtreeByStorage(storageID int, rootPath string) error {
	return m.WithTx(context.Background(), func(tx *sql.Tx) error {
		return m.DeleteSubtreeByStorageTx(tx, storageID, rootPath)
	})
}

// DeleteFileTx deletes a single file in a specific storage within the provided transaction.
func (m *Manager) DeleteFileTx(tx *sql.Tx, storageID int, filePath string) error {
	norm := NormalizePath(filePath)

	_, err := tx.Exec("DELETE FROM files WHERE storage_id = ? AND path = ?;", storageID, norm)
	return err
}

// DeleteFile deletes a single file in a specific storage.
func (m *Manager) DeleteFile(storageID int, filePath string) error {
	return m.WithTx(context.Background(), func(tx *sql.Tx) error {
		return m.DeleteFileTx(tx, storageID, filePath)
	})
}

// PutFileTx inserts or updates a file record within the provided transaction.
// Triggers automatically update storages usage.
func (m *Manager) PutFileTx(tx *sql.Tx, record FileRecord) error {
	normPath := NormalizePath(record.Path)
	if record.ParentPath == "" && normPath != "/" {
		record.ParentPath, _ = SplitPath(normPath)
	} else if record.ParentPath != "" {
		record.ParentPath = NormalizePath(record.ParentPath)
	}
	if record.ModTime.IsZero() {
		record.ModTime = time.Now()
	}
	if record.LastProbedAt.IsZero() {
		record.LastProbedAt = time.Now()
	}
	if record.IsDir {
		record.Size = 0
	}
	isDirInt := 0
	if record.IsDir {
		isDirInt = 1
	}

	// Ensure storage row exists
	_, err := tx.Exec(`
		INSERT INTO storages (id, used_bytes, file_count, updated_at)
		VALUES (?, 0, 0, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO NOTHING;
	`, record.StorageID)
	if err != nil {
		return fmt.Errorf("ensure storage %d: %w", record.StorageID, err)
	}

	// Ensure all ancestor directories exist in files table
	ancestorPath := normPath
	for {
		p, _ := SplitPath(ancestorPath)
		if p == "" || p == "/" {
			break
		}
		grandParent, _ := SplitPath(p)
		if _, err := tx.Exec(`
			INSERT INTO files (storage_id, path, parent_path, size, is_dir, mod_time, last_probed_at)
			VALUES (?, ?, ?, 0, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			ON CONFLICT(storage_id, path) DO NOTHING;
		`, record.StorageID, p, grandParent); err != nil {
			return fmt.Errorf("ensure ancestor %s: %w", p, err)
		}
		ancestorPath = p
	}
	if !record.IsDir {
		if _, err := tx.Exec(`
			DELETE FROM files WHERE path = ? AND storage_id != ? AND is_dir = 0;
		`, normPath, record.StorageID); err != nil {
			return fmt.Errorf("cleanup duplicate files for %s: %w", normPath, err)
		}
	}
	_, err = tx.Exec(`
		INSERT INTO files (storage_id, path, parent_path, size, is_dir, mod_time, last_probed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(storage_id, path) DO UPDATE SET
			parent_path = excluded.parent_path,
			size = excluded.size,
			is_dir = excluded.is_dir,
			mod_time = excluded.mod_time,
			last_probed_at = excluded.last_probed_at;
	`, record.StorageID, normPath, record.ParentPath, record.Size, isDirInt, record.ModTime, record.LastProbedAt)

	if err != nil {
		return fmt.Errorf("put file %d:%s: %w", record.StorageID, normPath, err)
	}
	return nil
}

// PutFile inserts or updates a file record. Triggers automatically update storages usage.
func (m *Manager) PutFile(record FileRecord) error {
	return m.WithTx(context.Background(), func(tx *sql.Tx) error {
		return m.PutFileTx(tx, record)
	})
}

// UpdateLastProbedTx updates the last_probed_at timestamp of a file in the specified storage within the provided transaction.
func (m *Manager) UpdateLastProbedTx(tx *sql.Tx, storageID int, filePath string, t time.Time) error {
	norm := NormalizePath(filePath)
	if t.IsZero() {
		t = time.Now()
	}
	res, err := tx.Exec(`
		UPDATE files
		SET last_probed_at = ?
		WHERE storage_id = ? AND path = ?;
	`, t, storageID, norm)
	if err != nil {
		return fmt.Errorf("update last_probed_at %d:%s: %w", storageID, norm, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update last_probed_at rows affected %d:%s: %w", storageID, norm, err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateLastProbed updates the last_probed_at timestamp of a file in the specified storage.
func (m *Manager) UpdateLastProbed(storageID int, filePath string, t time.Time) error {
	return m.WithTx(context.Background(), func(tx *sql.Tx) error {
		return m.UpdateLastProbedTx(tx, storageID, filePath, t)
	})
}

// BatchInsertFiles performs batch insertion of files for a specific storage within a single transaction.
func (m *Manager) BatchInsertFiles(storageID int, files []FileRecord) error {
	tx, err := m.db.Begin()
	if err != nil {
		return fmt.Errorf("begin batch insert tx: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO storages (id, used_bytes, file_count, updated_at)
		VALUES (?, 0, 0, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO NOTHING;
	`, storageID)
	if err != nil {
		return fmt.Errorf("ensure storage %d: %w", storageID, err)
	}

	if len(files) > 0 {
		// Extract unique ancestor directories
		type dirKey struct {
			storageID int
			path      string
		}
		ancestors := make(map[dirKey]struct{})
		for _, f := range files {
			sID := storageID
			if f.StorageID != 0 {
				sID = f.StorageID
			}
			normPath := NormalizePath(f.Path)
			curr := normPath
			for {
				p, _ := SplitPath(curr)
				if p == "" || p == "/" {
					break
				}
				ancestors[dirKey{storageID: sID, path: p}] = struct{}{}
				curr = p
			}
		}

		if len(ancestors) > 0 {
			dirStmt, err := tx.Prepare(`
				INSERT INTO files (storage_id, path, parent_path, size, is_dir, mod_time, last_probed_at)
				VALUES (?, ?, ?, 0, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
				ON CONFLICT(storage_id, path) DO NOTHING;
			`)
			if err != nil {
				return fmt.Errorf("prepare insert ancestor directory: %w", err)
			}
			defer dirStmt.Close()

			for k := range ancestors {
				pParent, _ := SplitPath(k.path)
				if _, err := dirStmt.Exec(k.storageID, k.path, pParent); err != nil {
					return fmt.Errorf("insert ancestor directory %d:%s: %w", k.storageID, k.path, err)
				}
			}
		}

		stmt, err := tx.Prepare(`
			INSERT INTO files (storage_id, path, parent_path, size, is_dir, mod_time, last_probed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(storage_id, path) DO UPDATE SET
				parent_path = excluded.parent_path,
				size = excluded.size,
				is_dir = excluded.is_dir,
				mod_time = excluded.mod_time,
				last_probed_at = excluded.last_probed_at;
		`)
		if err != nil {
			return fmt.Errorf("prepare insert statement: %w", err)
		}
		defer stmt.Close()

		for _, f := range files {
			normPath := NormalizePath(f.Path)
			if f.ParentPath == "" && normPath != "/" {
				f.ParentPath, _ = SplitPath(normPath)
			} else if f.ParentPath != "" {
				f.ParentPath = NormalizePath(f.ParentPath)
			}
			if f.ModTime.IsZero() {
				f.ModTime = time.Now()
			}
			if f.LastProbedAt.IsZero() {
				f.LastProbedAt = time.Now()
			}
			if f.IsDir {
				f.Size = 0
			}
			isDirInt := 0
			if f.IsDir {
				isDirInt = 1
			}

			sID := storageID
			if f.StorageID != 0 {
				sID = f.StorageID
			}

			_, err = stmt.Exec(sID, normPath, f.ParentPath, f.Size, isDirInt, f.ModTime, f.LastProbedAt)
			if err != nil {
				return fmt.Errorf("batch insert file %s: %w", normPath, err)
			}
		}
	}

	_, err = tx.Exec("UPDATE storages SET last_sync = CURRENT_TIMESTAMP WHERE id = ?;", storageID)
	if err != nil {
		return fmt.Errorf("update last_sync for storage %d: %w", storageID, err)
	}

	return tx.Commit()
}

// BatchInsert performs batch insertion of file records across any storages in a single transaction.
func (m *Manager) BatchInsert(files []FileRecord) error {
	if len(files) == 0 {
		return nil
	}

	tx, err := m.db.Begin()
	if err != nil {
		return fmt.Errorf("begin batch insert tx: %w", err)
	}
	defer tx.Rollback()

	ensureStmt, err := tx.Prepare(`
		INSERT INTO storages (id, used_bytes, file_count, updated_at)
		VALUES (?, 0, 0, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO NOTHING;
	`)
	if err != nil {
		return fmt.Errorf("prepare ensure storage: %w", err)
	}
	defer ensureStmt.Close()

	ensured := make(map[int]bool)
	type dirKey struct {
		storageID int
		path      string
	}
	ancestors := make(map[dirKey]struct{})

	for _, f := range files {
		if !ensured[f.StorageID] {
			if _, err := ensureStmt.Exec(f.StorageID); err != nil {
				return fmt.Errorf("ensure storage %d: %w", f.StorageID, err)
			}
			ensured[f.StorageID] = true
		}

		normPath := NormalizePath(f.Path)
		curr := normPath
		for {
			p, _ := SplitPath(curr)
			if p == "" || p == "/" {
				break
			}
			ancestors[dirKey{storageID: f.StorageID, path: p}] = struct{}{}
			curr = p
		}
	}

	if len(ancestors) > 0 {
		dirStmt, err := tx.Prepare(`
			INSERT INTO files (storage_id, path, parent_path, size, is_dir, mod_time, last_probed_at)
			VALUES (?, ?, ?, 0, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			ON CONFLICT(storage_id, path) DO NOTHING;
		`)
		if err != nil {
			return fmt.Errorf("prepare insert ancestor directory: %w", err)
		}
		defer dirStmt.Close()

		for k := range ancestors {
			pParent, _ := SplitPath(k.path)
			if _, err := dirStmt.Exec(k.storageID, k.path, pParent); err != nil {
				return fmt.Errorf("insert ancestor directory %d:%s: %w", k.storageID, k.path, err)
			}
		}
	}

	stmt, err := tx.Prepare(`
		INSERT INTO files (storage_id, path, parent_path, size, is_dir, mod_time, last_probed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(storage_id, path) DO UPDATE SET
			parent_path = excluded.parent_path,
			size = excluded.size,
			is_dir = excluded.is_dir,
			mod_time = excluded.mod_time,
			last_probed_at = excluded.last_probed_at;
	`)
	if err != nil {
		return fmt.Errorf("prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, f := range files {
		normPath := NormalizePath(f.Path)
		if f.ParentPath == "" && normPath != "/" {
			f.ParentPath, _ = SplitPath(normPath)
		} else if f.ParentPath != "" {
			f.ParentPath = NormalizePath(f.ParentPath)
		}
		if f.ModTime.IsZero() {
			f.ModTime = time.Now()
		}
		if f.LastProbedAt.IsZero() {
			f.LastProbedAt = time.Now()
		}
		if f.IsDir {
			f.Size = 0
		}
		isDirInt := 0
		if f.IsDir {
			isDirInt = 1
		}

		_, err = stmt.Exec(f.StorageID, normPath, f.ParentPath, f.Size, isDirInt, f.ModTime, f.LastProbedAt)
		if err != nil {
			return fmt.Errorf("batch insert file %s: %w", normPath, err)
		}
	}

	return tx.Commit()
}

// MovePathTx moves/renames a file or directory in the specified storage within the provided transaction,
// cascading updates to child paths and parent paths if oldPath is a directory.
func (m *Manager) MovePathTx(tx *sql.Tx, storageID int, oldPath, newPath string) error {
	oldNorm := NormalizePath(oldPath)
	newNorm := NormalizePath(newPath)
	if oldNorm == newNorm {
		return nil
	}
	if oldNorm == "/" || newNorm == "/" {
		return fmt.Errorf("cannot move root path: %s -> %s", oldNorm, newNorm)
	}

	var isDir int
	err := tx.QueryRow(`SELECT is_dir FROM files WHERE storage_id = ? AND path = ?;`, storageID, oldNorm).Scan(&isDir)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("query file %d:%s: %w", storageID, oldNorm, err)
	}

	if isDir == 1 {
		// Prevent moving a directory into its own descendant
		if strings.HasPrefix(newNorm, oldNorm+"/") {
			return fmt.Errorf("cannot move directory %s into its own descendant %s", oldNorm, newNorm)
		}
	}

	newParent, _ := SplitPath(newNorm)
	res, err := tx.Exec(`
		UPDATE files
		SET path = ?, parent_path = ?, mod_time = CURRENT_TIMESTAMP
		WHERE storage_id = ? AND path = ?;
	`, newNorm, newParent, storageID, oldNorm)
	if err != nil {
		return fmt.Errorf("move item %s -> %s: %w", oldNorm, newNorm, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}

	if isDir == 1 {
		// Cascade update all descendant items under oldNorm:
		// Replace prefix oldNorm with newNorm for both path and parent_path.
		_, err = tx.Exec(`
			UPDATE files
			SET path = ? || SUBSTR(path, LENGTH(?) + 1),
			    parent_path = ? || SUBSTR(parent_path, LENGTH(?) + 1),
			    mod_time = CURRENT_TIMESTAMP
			WHERE storage_id = ? AND path >= ? || '/' AND path < ? || '0';
		`, newNorm, oldNorm, newNorm, oldNorm, storageID, oldNorm, oldNorm)
		if err != nil {
			return fmt.Errorf("move descendants of %s -> %s: %w", oldNorm, newNorm, err)
		}
	}

	return nil
}

// MovePath moves/renames a file or directory in the specified storage,
// cascading updates to child paths and parent paths if oldPath is a directory.
func (m *Manager) MovePath(storageID int, oldPath, newPath string) error {
	return m.WithTx(context.Background(), func(tx *sql.Tx) error {
		return m.MovePathTx(tx, storageID, oldPath, newPath)
	})
}

// CalibrateStorage recalculates used_bytes and file_count for a single storage based on files table.
func (m *Manager) CalibrateStorage(storageID int) error {
	_, err := m.db.Exec(`
		UPDATE storages
		SET used_bytes = COALESCE((SELECT SUM(size) FROM files WHERE storage_id = storages.id AND is_dir = 0), 0),
		    file_count = COALESCE((SELECT COUNT(*) FROM files WHERE storage_id = storages.id AND is_dir = 0), 0),
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = ?;
	`, storageID)
	return err
}

// CalibrateStorages recalculates used_bytes and file_count for specified storages (or all if none specified).
func (m *Manager) CalibrateStorages(storageIDs ...int) error {
	if len(storageIDs) == 0 {
		_, err := m.db.Exec(`
			UPDATE storages
			SET used_bytes = COALESCE((SELECT SUM(size) FROM files WHERE storage_id = storages.id AND is_dir = 0), 0),
			    file_count = COALESCE((SELECT COUNT(*) FROM files WHERE storage_id = storages.id AND is_dir = 0), 0),
			    updated_at = CURRENT_TIMESTAMP;
		`)
		return err
	}

	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		UPDATE storages
		SET used_bytes = COALESCE((SELECT SUM(size) FROM files WHERE storage_id = storages.id AND is_dir = 0), 0),
		    file_count = COALESCE((SELECT COUNT(*) FROM files WHERE storage_id = storages.id AND is_dir = 0), 0),
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = ?;
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, id := range storageIDs {
		if _, err := stmt.Exec(id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Save executes a WAL checkpoint with TRUNCATE to flush pending changes to main db and truncate wal file.
func (m *Manager) Save() error {
	if m.filePath == ":memory:" {
		return nil
	}
	_, err := m.db.Exec("PRAGMA wal_checkpoint(TRUNCATE);")
	return err
}

// Close checkpoints WAL with TRUNCATE and closes the database.
func (m *Manager) Close() error {
	var err error
	m.closeOnce.Do(func() {
		if m.filePath != ":memory:" {
			_, _ = m.db.Exec("PRAGMA wal_checkpoint(TRUNCATE);")
		}
		err = m.db.Close()
	})
	return err
}

func scanFileRecords(rows *sql.Rows) ([]FileRecord, error) {
	results := make([]FileRecord, 0)
	for rows.Next() {
		var r FileRecord
		var isDirInt int
		var modTime time.Time
		var lastProbedAt time.Time

		if err := rows.Scan(&r.StorageID, &r.Path, &r.ParentPath, &r.Size, &isDirInt, &modTime, &lastProbedAt); err != nil {
			return nil, err
		}
		r.IsDir = isDirInt == 1
		r.ModTime = modTime
		r.LastProbedAt = lastProbedAt
		results = append(results, r)
	}
	return results, rows.Err()
}
