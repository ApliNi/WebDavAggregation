package server

import (
	"crypto/rand"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	// ErrLocked indicates a resource is already locked by another client.
	ErrLocked = errors.New("resource is locked")

	// ErrLockNotFound indicates the requested lock was not found or has expired.
	ErrLockNotFound = errors.New("lock not found")

	// ErrLockTokenMismatch indicates the provided lock token does not match the active lock.
	ErrLockTokenMismatch = errors.New("lock token mismatch")
)

// ActiveLock represents an active WebDAV lock.
type ActiveLock struct {
	Token     string
	Path      string
	Owner     string
	Depth     string
	Timeout   time.Duration
	ExpiresAt time.Time
	Exclusive bool
	CreatedAt time.Time
}

// LockManager provides thread-safe in-memory WebDAV locking.
type LockManager struct {
	mu    sync.RWMutex
	locks map[string]*ActiveLock
}

// NewLockManager creates a new in-memory LockManager.
func NewLockManager() *LockManager {
	return &LockManager{
		locks: make(map[string]*ActiveLock),
	}
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40 // Version 4
	b[8] = (b[8] & 0x3f) | 0x80 // Variant 10
	return fmt.Sprintf("urn:uuid:%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func cleanLockPath(p string) string {
	cleaned := path.Clean("/" + p)
	return cleaned
}

// Lock attempts to lock a resource.
func (lm *LockManager) Lock(davPath, owner, depth string, timeout time.Duration) (*ActiveLock, error) {
	p := cleanLockPath(davPath)
	if timeout <= 0 || timeout > 86400*time.Second {
		timeout = 300 * time.Second
	}
	if depth == "" {
		depth = "0"
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	now := time.Now()
	if existing, ok := lm.locks[p]; ok {
		if now.Before(existing.ExpiresAt) {
			return nil, ErrLocked
		}
		delete(lm.locks, p)
	}

	lock := &ActiveLock{
		Token:     newUUID(),
		Path:      p,
		Owner:     owner,
		Depth:     depth,
		Timeout:   timeout,
		ExpiresAt: now.Add(timeout),
		Exclusive: true,
		CreatedAt: now,
	}
	lm.locks[p] = lock
	return lock, nil
}

// Refresh refreshes an existing lock's expiration.
func (lm *LockManager) Refresh(davPath, token string, timeout time.Duration) (*ActiveLock, error) {
	p := cleanLockPath(davPath)
	if timeout <= 0 || timeout > 86400*time.Second {
		timeout = 300 * time.Second
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	now := time.Now()
	lock, ok := lm.locks[p]
	if !ok || now.After(lock.ExpiresAt) {
		delete(lm.locks, p)
		return nil, ErrLockNotFound
	}

	if token != "" && lock.Token != token {
		return nil, ErrLockTokenMismatch
	}

	lock.Timeout = timeout
	lock.ExpiresAt = now.Add(timeout)
	return lock, nil
}

// Unlock releases an existing lock.
func (lm *LockManager) Unlock(davPath, token string) error {
	p := cleanLockPath(davPath)

	lm.mu.Lock()
	defer lm.mu.Unlock()

	lock, ok := lm.locks[p]
	if !ok || time.Now().After(lock.ExpiresAt) {
		return ErrLockNotFound
	}

	if token != "" && lock.Token != token {
		return ErrLockTokenMismatch
	}

	delete(lm.locks, p)
	return nil
}

// Remove forcibly removes any lock associated with davPath (e.g. after DELETE).
func (lm *LockManager) Remove(davPath string) {
	p := cleanLockPath(davPath)
	lm.mu.Lock()
	defer lm.mu.Unlock()
	delete(lm.locks, p)
}

// GetLock retrieves an active, unexpired lock for davPath.
func (lm *LockManager) GetLock(davPath string) *ActiveLock {
	p := cleanLockPath(davPath)
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	lock, ok := lm.locks[p]
	if !ok || time.Now().After(lock.ExpiresAt) {
		return nil
	}
	return lock
}

// IsLocked checks whether davPath is locked by another client.
// It checks the exact path p, and walks up all parent/ancestor paths.
// If any ancestor has an active lock with Depth: "infinity" and the client did not provide its token,
// it is considered locked (returns true).
func (lm *LockManager) IsLocked(davPath string, ifHeader, lockTokenHeader string) bool {
	p := cleanLockPath(davPath)
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	now := time.Now()
	tokens := extractTokens(ifHeader, lockTokenHeader)
	hasToken := func(targetToken string) bool {
		for _, tok := range tokens {
			if tok == targetToken {
				return true
			}
		}
		return false
	}

	// 1. Check exact lock on current path
	if lock, ok := lm.locks[p]; ok && now.Before(lock.ExpiresAt) {
		if !hasToken(lock.Token) {
			return true
		}
	}

	// 2. Check all ancestor paths for active Depth: infinity locks
	curr := p
	for curr != "/" {
		curr = path.Dir(curr)
		if ancestorLock, ok := lm.locks[curr]; ok && now.Before(ancestorLock.ExpiresAt) {
			if strings.EqualFold(ancestorLock.Depth, "infinity") {
				if !hasToken(ancestorLock.Token) {
					return true
				}
			}
		}
	}

	return false
}

func parseTimeoutHeader(header string) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" || strings.EqualFold(header, "Infinite") {
		return 300 * time.Second
	}
	if strings.HasPrefix(strings.ToLower(header), "second-") {
		secStr := header[7:]
		if secs, err := strconv.Atoi(secStr); err == nil && secs > 0 {
			if secs > 86400 {
				secs = 86400
			}
			return time.Duration(secs) * time.Second
		}
	}
	return 300 * time.Second
}

func extractTokens(headers ...string) []string {
	var tokens []string
	for _, h := range headers {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}

		// Extract angle-bracket enclosed tokens: <token>
		remaining := h
		for {
			start := strings.Index(remaining, "<")
			if start == -1 {
				break
			}
			end := strings.Index(remaining[start:], ">")
			if end == -1 {
				break
			}
			end = start + end
			tok := strings.TrimSpace(remaining[start+1 : end])
			if tok != "" {
				tokens = append(tokens, tok)
			}
			remaining = remaining[end+1:]
		}

		// Also extract any raw urn:uuid: tokens not enclosed in angle brackets
		idx := 0
		for {
			pos := strings.Index(h[idx:], "urn:uuid:")
			if pos == -1 {
				break
			}
			start := idx + pos
			end := start + len("urn:uuid:")
			for end < len(h) && (h[end] == '-' || (h[end] >= '0' && h[end] <= '9') || (h[end] >= 'a' && h[end] <= 'f') || (h[end] >= 'A' && h[end] <= 'F')) {
				end++
			}
			rawTok := h[start:end]
			alreadyAdded := false
			for _, t := range tokens {
				if t == rawTok {
					alreadyAdded = true
					break
				}
			}
			if !alreadyAdded {
				tokens = append(tokens, rawTok)
			}
			idx = end
		}
	}
	return tokens
}
