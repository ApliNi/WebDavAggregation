package client

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Client represents a WebDAV upstream client.
type Client struct {
	endpoint     *url.URL
	user         string
	pass         string
	proxyURL     string
	retry        int
	timeout      time.Duration
	httpClient   *http.Client
	baseBackoff  time.Duration
	ensureFlight dirFlight
}

// NewClient creates and initializes a new WebDAV Client.
func NewClient(rawURL, user, pass, proxyURL string, retry int, timeout time.Duration) (*Client, error) {
	if rawURL == "" {
		return nil, fmt.Errorf("url cannot be empty")
	}

	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid webdav url: %w", err)
	}
	if parsedURL.Scheme == "" {
		parsedURL.Scheme = "http"
	}
	if parsedURL.Path == "" {
		parsedURL.Path = "/"
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   50,
		MaxConnsPerHost:       100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	if proxyURL != "" {
		pURL, err := url.Parse(proxyURL)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy url: %w", err)
		}
		transport.Proxy = http.ProxyURL(pURL)
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}

	if retry < 0 {
		retry = 0
	}

	return &Client{
		endpoint:    parsedURL,
		user:        user,
		pass:        pass,
		proxyURL:    proxyURL,
		retry:       retry,
		timeout:     timeout,
		httpClient:  httpClient,
		baseBackoff: 100 * time.Millisecond,
	}, nil
}

// SetBaseBackoff sets the base backoff duration for retries.
func (c *Client) SetBaseBackoff(d time.Duration) {
	c.baseBackoff = d
}

// SetHTTPClient sets a custom http.Client (e.g. for testing).
func (c *Client) SetHTTPClient(client *http.Client) {
	c.httpClient = client
}

// Endpoint returns the parsed base URL of the client.
func (c *Client) Endpoint() *url.URL {
	u := *c.endpoint
	return &u
}

// resolveURL resolves a relative or root davPath against the client base URL.
func (c *Client) resolveURL(davPath string) string {
	base := *c.endpoint
	basePath := base.Path
	if basePath == "" {
		basePath = "/"
	}

	cleanDAV := davPath
	if !strings.HasPrefix(cleanDAV, "/") {
		cleanDAV = "/" + cleanDAV
	}

	hasTrailingSlash := strings.HasSuffix(davPath, "/") && len(davPath) > 1

	var joined string
	if basePath == "/" {
		joined = path.Clean(cleanDAV)
	} else {
		cleanBase := path.Clean(basePath)
		joined = path.Clean(path.Join(cleanBase, cleanDAV))
	}

	if (hasTrailingSlash || (davPath == "/" && strings.HasSuffix(basePath, "/"))) && !strings.HasSuffix(joined, "/") {
		joined += "/"
	}
	if joined == "" {
		joined = "/"
	}

	res := base
	res.Path = joined
	res.RawPath = ""
	res.RawQuery = ""
	res.Fragment = ""
	return res.String()
}

// parseHref extracts the relative davPath from a response href.
func (c *Client) parseHref(href string) string {
	u, err := url.Parse(href)
	var p string
	if err == nil && u.Path != "" {
		p = u.Path
	} else {
		p = href
	}

	if unescaped, err := url.PathUnescape(p); err == nil {
		p = unescaped
	}

	basePath := c.endpoint.Path
	if basePath == "" {
		basePath = "/"
	}
	cleanBasePath := path.Clean(basePath)

	if cleanBasePath != "/" && strings.HasPrefix(p, cleanBasePath) {
		p = strings.TrimPrefix(p, cleanBasePath)
	}

	cleanRel := path.Clean(p)
	if cleanRel == "." || cleanRel == "/" || cleanRel == "" {
		return "/"
	}
	if !strings.HasPrefix(cleanRel, "/") {
		cleanRel = "/" + cleanRel
	}
	return cleanRel
}

// newRequest constructs an http.Request with the resolved URL and basic auth.
func (c *Client) newRequest(ctx context.Context, method, davPath string, body io.Reader) (*http.Request, error) {
	reqURL := c.resolveURL(davPath)
	req, err := http.NewRequestWithContext(ctx, method, reqURL, body)
	if err != nil {
		return nil, fmt.Errorf("create request failed: %w", err)
	}

	if c.user != "" || c.pass != "" {
		req.SetBasicAuth(c.user, c.pass)
	}

	return req, nil
}

// doWithRetry executes an HTTP request operation with backoff retries.
func (c *Client) doWithRetry(ctx context.Context, send func() (*http.Response, error)) (*http.Response, error) {
	var resp *http.Response
	var err error

	for attempt := 0; attempt <= c.retry; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		resp, err = send()
		if err != nil {
			if resp != nil && resp.Body != nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if attempt < c.retry {
				delay := time.Duration(attempt+1) * c.baseBackoff
				if err := sleepWait(ctx, delay); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			if attempt < c.retry {
				delay := c.retryAfterDelay(resp, attempt)
				if resp.Body != nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}
				if err := sleepWait(ctx, delay); err != nil {
					return nil, err
				}
				continue
			}
			return resp, nil
		}

		if resp.StatusCode >= 500 && resp.StatusCode <= 599 {
			if attempt < c.retry {
				if resp.Body != nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}
				delay := time.Duration(attempt+1) * c.baseBackoff
				if err := sleepWait(ctx, delay); err != nil {
					return nil, err
				}
				continue
			}
			return resp, nil
		}

		return resp, nil
	}

	return resp, err
}

func (c *Client) retryAfterDelay(resp *http.Response, attempt int) time.Duration {
	if resp != nil {
		if retryAfter := strings.TrimSpace(resp.Header.Get("Retry-After")); retryAfter != "" {
			if sec, err := strconv.Atoi(retryAfter); err == nil && sec >= 0 {
				return time.Duration(sec) * time.Second
			}
			if t, err := http.ParseTime(retryAfter); err == nil {
				if d := time.Until(t); d >= 0 {
					return d
				}
			}
		}
	}
	shift := min(attempt, 20)
	jitter := time.Duration(rand.IntN(51)) * time.Millisecond
	return c.baseBackoff*(1<<shift) + jitter
}

func sleepWait(ctx context.Context, delay time.Duration) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Ping checks the connectivity to the WebDAV upstream.
func (c *Client) Ping(ctx context.Context) error {
	resp, err := c.doWithRetry(ctx, func() (*http.Response, error) {
		req, err := c.newRequest(ctx, "OPTIONS", "/", nil)
		if err != nil {
			return nil, err
		}
		return c.httpClient.Do(req)
	})
	if err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	// Fallback to PROPFIND depth 0 if OPTIONS is not supported
	if resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotImplemented {
		propResp, err := c.doWithRetry(ctx, func() (*http.Response, error) {
			req, err := c.newRequest(ctx, "PROPFIND", "/", nil)
			if err != nil {
				return nil, err
			}
			req.Header.Set("Depth", "0")
			return c.httpClient.Do(req)
		})
		if err != nil {
			return fmt.Errorf("ping fallback failed: %w", err)
		}
		defer propResp.Body.Close()
		_, _ = io.Copy(io.Discard, propResp.Body)

		if propResp.StatusCode == http.StatusMultiStatus || (propResp.StatusCode >= 200 && propResp.StatusCode < 300) {
			return nil
		}
		return fmt.Errorf("ping failed with status: %d", propResp.StatusCode)
	}

	return fmt.Errorf("ping failed with status: %d", resp.StatusCode)
}

type dirFlight struct {
	mu sync.Mutex
	m  map[string]*flightCall
}

type flightCall struct {
	done chan struct{}
	err  error
}

func (g *dirFlight) Do(ctx context.Context, key string, fn func(runCtx context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*flightCall)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return c.err
		}
	}

	c := &flightCall{done: make(chan struct{})}
	g.m[key] = c
	g.mu.Unlock()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				c.err = fmt.Errorf("ensure dir panic: %v", r)
			}
			close(c.done)

			g.mu.Lock()
			delete(g.m, key)
			g.mu.Unlock()
		}()

		runCtx := context.WithoutCancel(ctx)
		c.err = fn(runCtx)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.err
	}
}
