package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"webdav-agg/src/cluster"
	"webdav-agg/src/config"
	"webdav-agg/src/data"
	"webdav-agg/src/logger"
)

// Server implements the WebDAV aggregation HTTP server.
type Server struct {
	cfg        *config.Config
	cluster    *cluster.Cluster
	lockMgr    *LockManager
	httpServer *http.Server
	listener   net.Listener
	mu         sync.Mutex
}

// NewServer initializes a new WebDAV aggregation Server.
// If clust is nil, it automatically initializes a new Cluster using cfg.
func NewServer(cfg *config.Config, clust *cluster.Cluster) (*Server, error) {
	if cfg == nil {
		return nil, errors.New("config cannot be nil")
	}

	if clust == nil {
		dataFile := cfg.GetDataFile()
		if cfg.DataFile == "" && cfg.FilePath() == "" {
			dataFile = ":memory:"
		}
		dm, err := data.NewManager(dataFile)
		if err != nil {
			return nil, fmt.Errorf("init data manager: %w", err)
		}
		clust, err = cluster.NewCluster(cfg, dm)
		if err != nil {
			return nil, fmt.Errorf("init cluster: %w", err)
		}
	}

	return &Server{
		cfg:     cfg,
		cluster: clust,
		lockMgr: NewLockManager(),
	}, nil
}

// Cluster returns the underlying Cluster manager.
func (s *Server) Cluster() *cluster.Cluster {
	return s.cluster
}

// InitData initializes the cluster metadata and quota from SQLite and remote nodes.
func (s *Server) InitData(ctx context.Context) error {
	if s.cluster != nil {
		return s.cluster.InitData(ctx)
	}
	return nil
}
// LockManager returns the WebDAV in-memory LockManager.
func (s *Server) LockManager() *LockManager {
	return s.lockMgr
}

// Config returns the server's configuration.
func (s *Server) Config() *config.Config {
	return s.cfg
}

// Addr returns the listening address of the server, or the configured address if not listening.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.cfg.Listen
}

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
	wroteHeader  bool
}

func (w *loggingResponseWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.statusCode = code
		w.wroteHeader = true
		w.ResponseWriter.WriteHeader(code)
	}
}

func (w *loggingResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.wroteHeader = true
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytesWritten += int64(n)
	return n, err
}

func (w *loggingResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *loggingResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

type countingReadCloser struct {
	reader    io.ReadCloser
	bytesRead atomic.Int64
}

func newCountingReadCloser(rc io.ReadCloser) *countingReadCloser {
	if rc == nil {
		rc = http.NoBody
	}
	return &countingReadCloser{reader: rc}
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	c.bytesRead.Add(int64(n))
	return n, err
}

func (c *countingReadCloser) Close() error {
	return c.reader.Close()
}

func (c *countingReadCloser) BytesRead() int64 {
	return c.bytesRead.Load()
}

func (c *countingReadCloser) Unwrap() io.Reader {
	return c.reader
}

var copyBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 32*1024)
		return &b
	},
}

func (w *loggingResponseWriter) ReadFrom(src io.Reader) (int64, error) {
	if !w.wroteHeader {
		w.wroteHeader = true
	}
	if rf, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		n, err := rf.ReadFrom(src)
		w.bytesWritten += n
		return n, err
	}
	bufPtr := copyBufPool.Get().(*[]byte)
	n, err := io.CopyBuffer(w.ResponseWriter, src, *bufPtr)
	copyBufPool.Put(bufPtr)
	w.bytesWritten += n
	return n, err
}

var byteUnits = []string{"B", "KB", "MB", "GB", "TB", "PB", "EB"}

func formatBytes(b int64) string {
	if b < 0 {
		b = 0
	}
	val := float64(b)
	i := 0
	for val >= 1024 && i < len(byteUnits)-1 {
		val /= 1024
		i++
	}
	return fmt.Sprintf("%.2f%s", val, byteUnits[i])
}

func formatDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	sec := d.Seconds()
	if sec < 60 {
		return fmt.Sprintf("%.2fs", sec)
	}
	if sec < 3600 {
		return fmt.Sprintf("%.2fm", sec/60)
	}
	return fmt.Sprintf("%.2fh", sec/3600)
}

func cleanIP(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return strings.Trim(host, "[]")
	}
	return strings.Trim(addr, "[]")
}

func getClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		for _, part := range strings.Split(xff, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				if ip := cleanIP(part); ip != "" {
					return ip
				}
			}
		}
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		if ip := cleanIP(xri); ip != "" {
			return ip
		}
	}
	return cleanIP(r.RemoteAddr)
}

// Handler returns an http.Handler that enforces Basic Auth (if configured)
// and handles WebDAV protocol requests with HTTP access logging.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lrw := &loggingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		bodyTracker := newCountingReadCloser(r.Body)
		r.Body = bodyTracker

		defer func() {
			duration := time.Since(start)
			bytesRead := bodyTracker.BytesRead()
			if bytesRead == 0 && r.ContentLength > 0 && (r.Method == "PUT" || r.Method == "POST" || r.Method == "PROPPATCH" || r.Method == "LOCK") && (lrw.statusCode >= 200 && lrw.statusCode < 300) {
				bytesRead = r.ContentLength
			}
			totalBytes := lrw.bytesWritten + bytesRead
			logger.Info("HTTP", "%s %d %q %s %s - %s", r.Method, lrw.statusCode, r.URL.Path, formatDuration(duration), formatBytes(totalBytes), getClientIP(r))
		}()

		// WebDAV service is mounted at /dav.
		// Accessing / or any path outside /dav returns 404 without response body.
		if r.URL.Path != "/dav" && !strings.HasPrefix(r.URL.Path, "/dav/") {
			lrw.WriteHeader(http.StatusNotFound)
			return
		}

		if s.cfg.Auth.Enabled() {
			user, pass, ok := r.BasicAuth()
			if !ok || user != s.cfg.Auth.User || pass != s.cfg.Auth.Pass {
				lrw.Header().Set("WWW-Authenticate", `Basic realm="webdav-agg"`)
				lrw.WriteHeader(http.StatusUnauthorized)
				return
			}
		}

		s.ServeHTTP(lrw, r)
	})
}

// ServeHTTP handles WebDAV protocol requests according to RFC 4918 and RFC 4331.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "OPTIONS":
		s.handleOptions(w, r)
	case "PROPFIND":
		s.handlePropfind(w, r)
	case "PROPPATCH":
		s.handleProppatch(w, r)
	case "GET":
		s.handleGet(w, r)
	case "HEAD":
		s.handleHead(w, r)
	case "PUT":
		s.handlePut(w, r)
	case "DELETE":
		s.handleDelete(w, r)
	case "MKCOL":
		s.handleMkcol(w, r)
	case "MOVE":
		s.handleMove(w, r)
	case "COPY":
		s.handleCopy(w, r)
	case "LOCK":
		s.handleLock(w, r)
	case "UNLOCK":
		s.handleUnlock(w, r)
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

type upstreamCheckResult struct {
	id  int
	url string
	err error
}

// CheckUpstreams concurrently verifies connectivity and quotas of all upstream nodes.
// If any upstream is unavailable, it logs error and returns an error.
func (s *Server) CheckUpstreams(ctx context.Context) error {
	nodes := s.cluster.AllNodes()
	if len(nodes) == 0 {
		return nil
	}

	results := make([]upstreamCheckResult, len(nodes))
	var wg sync.WaitGroup

	for i, n := range nodes {
		wg.Add(1)
		go func(idx int, node *cluster.Node) {
			defer wg.Done()
			res := upstreamCheckResult{
				id:  node.ID(),
				url: node.Config.URL,
			}

			if node.Client == nil {
				res.err = errors.New("client is nil")
				results[idx] = res
				return
			}
			checkCtx := ctx
			var cancel context.CancelFunc
			if _, hasDeadline := ctx.Deadline(); !hasDeadline {
				checkCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
				defer cancel()
			}

			if err := node.Client.Ping(checkCtx); err != nil {
				res.err = fmt.Errorf("ping failed: %w", err)
				results[idx] = res
				return
			}

			if err := node.UpdateQuota(checkCtx); err != nil {
				res.err = fmt.Errorf("quota check failed: %w", err)
				results[idx] = res
				return
			}
			results[idx] = res
		}(i, n)
	}

	wg.Wait()

	sort.Slice(results, func(i, j int) bool {
		return results[i].id < results[j].id
	})

	var failed []upstreamCheckResult
	for _, r := range results {
		if r.err != nil {
			failed = append(failed, r)
			logger.Error("Main", "Upstream[%d] %s check failed: %v", r.id, r.url, r.err)
		}
	}

	if len(failed) > 0 {
		logger.Error("Main", "Startup check failed: %d of %d upstream(s) unavailable", len(failed), len(nodes))
		return fmt.Errorf("startup check failed: %d upstream(s) unavailable", len(failed))
	}

	return nil
}

// Start begins listening and serving WebDAV requests on config.Listen.
func (s *Server) Start() error {
	return s.StartWithContext(context.Background())
}

// StartWithContext runs self-check, binds listener on config.Listen, and serves requests.
func (s *Server) StartWithContext(ctx context.Context) error {
	logger.Info("Main", "Starting WebDAV aggregation service...")

	if err := s.CheckUpstreams(ctx); err != nil {
		return err
	}

	if s.cluster != nil {
		if err := s.cluster.InitData(ctx); err != nil {
			return err
		}
	}

	s.mu.Lock()
	if s.listener != nil {
		s.mu.Unlock()
		return errors.New("server is already running")
	}

	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		s.mu.Unlock()
		logger.Error("Main", "Listen on %s failed: %v", s.cfg.Listen, err)
		return fmt.Errorf("listen on %s: %w", s.cfg.Listen, err)
	}
	s.listener = ln
	s.httpServer = &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	s.mu.Unlock()

	logger.Info("Main", "Listening on %s", s.cfg.Listen)

	err = s.httpServer.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	srv := s.httpServer
	s.mu.Unlock()

	if s.cluster != nil && s.cluster.DataManager() != nil {
		_ = s.cluster.DataManager().Close()
	}

	if srv == nil {
		return nil
	}
	return srv.Shutdown(ctx)
}

// Close immediately closes the server listener and connections.
func (s *Server) Close() error {
	s.mu.Lock()
	srv := s.httpServer
	s.mu.Unlock()

	if s.cluster != nil && s.cluster.DataManager() != nil {
		_ = s.cluster.DataManager().Close()
	}

	if srv == nil {
		return nil
	}
	return srv.Close()
}
