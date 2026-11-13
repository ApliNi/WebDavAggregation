package client

import (
	"errors"
	"os"
	"time"
)

// Standard client errors for WebDAV operations.
var (
	// ErrNotFound indicates the requested resource does not exist.
	ErrNotFound = os.ErrNotExist

	// ErrAlreadyExists indicates the target resource or directory already exists.
	ErrAlreadyExists = os.ErrExist

	// ErrPreconditionFailed indicates a condition failed (e.g. Overwrite is false and destination exists).
	ErrPreconditionFailed = errors.New("precondition failed")

	// ErrQuotaNotSupported indicates the upstream server does not support RFC 4331 quota properties.
	ErrQuotaNotSupported = errors.New("quota not supported")

	// ErrRangeNotSatisfiable indicates the requested range does not overlap the resource (RFC 7233).
	ErrRangeNotSatisfiable = errors.New("range not satisfiable")
)

// FileInfo represents metadata of a WebDAV resource.
type FileInfo struct {
	Path        string    `json:"path"`
	Name        string    `json:"name"`
	IsDir       bool      `json:"is_dir"`
	Size        int64     `json:"size"`
	ModTime     time.Time `json:"mod_time"`
	ETag        string    `json:"etag"`
	ContentType string    `json:"content_type"`
}

// QuotaInfo represents disk space and quota information according to RFC 4331.
type QuotaInfo struct {
	AvailableBytes int64 `json:"available_bytes"`
	UsedBytes      int64 `json:"used_bytes"`
	TotalBytes     int64 `json:"total_bytes"`
}
