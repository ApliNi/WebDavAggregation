package client

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	propfindQuotaXML = `<?xml version="1.0" encoding="utf-8" ?>
<D:propfind xmlns:D="DAV:">
  <D:prop>
    <D:quota-available-bytes/>
    <D:quota-used-bytes/>
  </D:prop>
</D:propfind>`

	propfindListXML = `<?xml version="1.0" encoding="utf-8" ?>
<D:propfind xmlns:D="DAV:">
  <D:prop>
    <D:resourcetype/>
    <D:getcontentlength/>
    <D:getlastmodified/>
    <D:getetag/>
    <D:getcontenttype/>
    <D:displayname/>
  </D:prop>
</D:propfind>`
)

// XML structs for Quota PROPFIND
type quotaResponseXML struct {
	XMLName   xml.Name         `xml:"multistatus"`
	Responses []quotaMemberXML `xml:"response"`
}

type quotaMemberXML struct {
	PropStat []quotaPropStatXML `xml:"propstat"`
}

type quotaPropStatXML struct {
	Status string       `xml:"status"`
	Prop   quotaPropXML `xml:"prop"`
}

type quotaPropXML struct {
	AvailableBytes string `xml:"quota-available-bytes"`
	UsedBytes      string `xml:"quota-used-bytes"`
}

// XML structs for List PROPFIND
type propfindResponseXML struct {
	XMLName   xml.Name       `xml:"multistatus"`
	Responses []respEntryXML `xml:"response"`
}

type respEntryXML struct {
	Href     string            `xml:"href"`
	PropStat []respPropStatXML `xml:"propstat"`
}

type respPropStatXML struct {
	Status string      `xml:"status"`
	Prop   respPropXML `xml:"prop"`
}

type respPropXML struct {
	ResourceType  respResourceTypeXML `xml:"resourcetype"`
	ContentLength string              `xml:"getcontentlength"`
	LastModified  string              `xml:"getlastmodified"`
	ETag          string              `xml:"getetag"`
	ContentType   string              `xml:"getcontenttype"`
	DisplayName   string              `xml:"displayname"`
}

type respResourceTypeXML struct {
	Collection *struct{} `xml:"collection"`
}

// GetQuota queries RFC 4331 DAV:quota-available-bytes and DAV:quota-used-bytes.
func (c *Client) GetQuota(ctx context.Context) (*QuotaInfo, error) {
	resp, err := c.doWithRetry(ctx, func() (*http.Response, error) {
		req, err := c.newRequest(ctx, "PROPFIND", "/", bytes.NewReader([]byte(propfindQuotaXML)))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Depth", "0")
		req.Header.Set("Content-Type", "application/xml; charset=utf-8")
		return c.httpClient.Do(req)
	})
	if err != nil {
		return nil, fmt.Errorf("get quota failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMultiStatus && resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("get quota failed with status: %s", resp.Status)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read quota response failed: %w", err)
	}

	var ms quotaResponseXML
	if err := xml.Unmarshal(bodyBytes, &ms); err != nil {
		return nil, fmt.Errorf("unmarshal quota response failed: %w", err)
	}

	var avail int64 = -1
	var used int64 = -1
	found := false

	for _, r := range ms.Responses {
		for _, ps := range r.PropStat {
			if strings.Contains(ps.Status, "200") {
				if ps.Prop.AvailableBytes != "" {
					if v, err := strconv.ParseInt(strings.TrimSpace(ps.Prop.AvailableBytes), 10, 64); err == nil {
						avail = v
						found = true
					}
				}
				if ps.Prop.UsedBytes != "" {
					if v, err := strconv.ParseInt(strings.TrimSpace(ps.Prop.UsedBytes), 10, 64); err == nil {
						used = v
						found = true
					}
				}
			}
		}
	}

	if !found {
		return nil, ErrQuotaNotSupported
	}

	var total int64
	if avail >= 0 && used >= 0 {
		total = avail + used
	}

	return &QuotaInfo{
		AvailableBytes: avail,
		UsedBytes:      used,
		TotalBytes:     total,
	}, nil
}

// List sends a PROPFIND request and parses the multistatus XML response.
func (c *Client) List(ctx context.Context, davPath string, depth int) ([]FileInfo, error) {
	depthHeader := "0"
	if depth == 1 {
		depthHeader = "1"
	} else if depth > 1 || depth < 0 {
		depthHeader = "infinity"
	}

	resp, err := c.doWithRetry(ctx, func() (*http.Response, error) {
		req, err := c.newRequest(ctx, "PROPFIND", davPath, bytes.NewReader([]byte(propfindListXML)))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Depth", depthHeader)
		req.Header.Set("Content-Type", "application/xml; charset=utf-8")
		return c.httpClient.Do(req)
	})
	if err != nil {
		return nil, fmt.Errorf("list %s failed: %w", davPath, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("%w: %s", ErrNotFound, resp.Status)
	}

	if resp.StatusCode != http.StatusMultiStatus && resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("list %s failed with status: %s", davPath, resp.Status)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read list response failed: %w", err)
	}

	var ms propfindResponseXML
	if err := xml.Unmarshal(bodyBytes, &ms); err != nil {
		return nil, fmt.Errorf("unmarshal list response failed: %w", err)
	}

	var list []FileInfo
	for _, r := range ms.Responses {
		targetPath := c.parseHref(r.Href)

		info := FileInfo{
			Path: targetPath,
		}

		for _, ps := range r.PropStat {
			if strings.Contains(ps.Status, "200") {
				if ps.Prop.ResourceType.Collection != nil {
					info.IsDir = true
				}
				if ps.Prop.ContentLength != "" {
					if sz, err := strconv.ParseInt(strings.TrimSpace(ps.Prop.ContentLength), 10, 64); err == nil {
						info.Size = sz
					}
				}
				if ps.Prop.LastModified != "" {
					info.ModTime = parseWebDAVTime(strings.TrimSpace(ps.Prop.LastModified))
				}
				if ps.Prop.ETag != "" {
					info.ETag = strings.Trim(strings.TrimSpace(ps.Prop.ETag), "\"")
				}
				if ps.Prop.ContentType != "" {
					info.ContentType = strings.TrimSpace(ps.Prop.ContentType)
				}
				if ps.Prop.DisplayName != "" {
					info.Name = strings.TrimSpace(ps.Prop.DisplayName)
				}
			}
		}

		if !info.IsDir {
			if info.ContentType == "httpd/unix-directory" || (strings.HasSuffix(r.Href, "/") && info.Size == 0) {
				info.IsDir = true
			}
		}
		if info.IsDir {
			info.Size = 0
		}

		if info.Name == "" {
			if info.Path == "/" {
				info.Name = "/"
			} else {
				info.Name = path.Base(info.Path)
			}
		}

		list = append(list, info)
	}

	return list, nil
}
// Stat returns file information for the specified resource by executing a depth-0 PROPFIND.
func (c *Client) Stat(ctx context.Context, davPath string) (*FileInfo, error) {
	infos, err := c.List(ctx, davPath, 0)
	if err != nil {
		return nil, err
	}
	if len(infos) == 0 {
		return nil, ErrNotFound
	}
	return &infos[0], nil
}
// Walk recursively traverses all files and subdirectories rooted at davPath.
func (c *Client) Walk(ctx context.Context, davPath string, fn func(info FileInfo) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	cleanRoot := davPath
	if !strings.HasPrefix(cleanRoot, "/") {
		cleanRoot = "/" + cleanRoot
	}
	cleanRoot = path.Clean(cleanRoot)

	rootList, err := c.List(ctx, cleanRoot, 0)
	if err != nil {
		return err
	}
	if len(rootList) == 0 {
		return ErrNotFound
	}

	rootInfo := rootList[0]
	err = fn(rootInfo)
	if err != nil {
		if errors.Is(err, filepath.SkipDir) {
			return nil
		}
		return err
	}

	if !rootInfo.IsDir {
		return nil
	}

	return c.walkDir(ctx, cleanRoot, fn)
}

func (c *Client) walkDir(ctx context.Context, currentDir string, fn func(info FileInfo) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	entries, err := c.List(ctx, currentDir, 1)
	if err != nil {
		return err
	}

	cleanCurrent := path.Clean(currentDir)
	if !strings.HasPrefix(cleanCurrent, "/") {
		cleanCurrent = "/" + cleanCurrent
	}

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}

		cleanEntryPath := path.Clean(entry.Path)
		if cleanEntryPath == cleanCurrent {
			continue
		}

		err := fn(entry)
		if err != nil {
			if errors.Is(err, filepath.SkipDir) {
				if entry.IsDir {
					continue
				}
				return nil
			}
			return err
		}

		if entry.IsDir {
			if err := c.walkDir(ctx, entry.Path, fn); err != nil {
				return err
			}
		}
	}

	return nil
}

// Get streams a file from the WebDAV server and supports header passthrough (e.g. Range).
func (c *Client) Get(ctx context.Context, davPath string, header http.Header) (io.ReadCloser, http.Header, error) {
	resp, err := c.doWithRetry(ctx, func() (*http.Response, error) {
		req, err := c.newRequest(ctx, http.MethodGet, davPath, nil)
		if err != nil {
			return nil, err
		}
		if header != nil {
			for k, vv := range header {
				for _, v := range vv {
					req.Header.Add(k, v)
				}
			}
		}
		return c.httpClient.Do(req)
	})
	if err != nil {
		return nil, nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		_ = resp.Body.Close()
		return nil, nil, fmt.Errorf("%w: %s", ErrNotFound, resp.Status)
	}

	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		hdr := resp.Header
		if hdr == nil {
			hdr = make(http.Header)
		}
		hdr.Set("X-Upstream-Status", strconv.Itoa(resp.StatusCode))
		return nil, hdr, fmt.Errorf("%w: %s", ErrRangeNotSatisfiable, resp.Status)
	}

	if resp.StatusCode >= 400 {
		_ = resp.Body.Close()
		return nil, nil, fmt.Errorf("get %s failed: %s", davPath, resp.Status)
	}

	if resp.Header == nil {
		resp.Header = make(http.Header)
	}
	resp.Header.Set("X-Upstream-Status", strconv.Itoa(resp.StatusCode))

	return resp.Body, resp.Header, nil
}

// Put uploads a file via streaming and automatically creates missing parent directories using EnsureDir.
func (c *Client) Put(ctx context.Context, davPath string, body io.Reader, size int64, contentType string) error {
	return c.putInternal(ctx, davPath, body, size, contentType, true)
}

// PutRaw uploads a file via streaming without checking or creating parent directories.
func (c *Client) PutRaw(ctx context.Context, davPath string, body io.Reader, size int64, contentType string) error {
	return c.putInternal(ctx, davPath, body, size, contentType, false)
}

func (c *Client) putInternal(ctx context.Context, davPath string, body io.Reader, size int64, contentType string, ensureParent bool) error {
	if ensureParent {
		cleanPath := path.Clean(davPath)
		if !strings.HasPrefix(cleanPath, "/") {
			cleanPath = "/" + cleanPath
		}

		parentDir := path.Dir(cleanPath)
		if parentDir != "/" && parentDir != "." && parentDir != "" {
			if err := c.EnsureDir(ctx, parentDir); err != nil {
				return fmt.Errorf("ensure parent dir for %s: %w", davPath, err)
			}
		}
	}

	if body == nil {
		body = http.NoBody
		size = 0
	}

	isSeekable := false
	if _, ok := body.(io.ReadSeeker); ok {
		isSeekable = true
	} else if body == http.NoBody {
		isSeekable = true
	}

	send := func() (*http.Response, error) {
		if seeker, ok := body.(io.ReadSeeker); ok {
			if _, err := seeker.Seek(0, io.SeekStart); err != nil {
				return nil, fmt.Errorf("rewind body failed: %w", err)
			}
		}

		req, err := c.newRequest(ctx, http.MethodPut, davPath, body)
		if err != nil {
			return nil, err
		}

		if size >= 0 {
			req.ContentLength = size
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		} else {
			req.Header.Set("Content-Type", "application/octet-stream")
		}

		if rs, ok := body.(io.ReadSeeker); ok {
			req.GetBody = func() (io.ReadCloser, error) {
				if _, err := rs.Seek(0, io.SeekStart); err != nil {
					return nil, err
				}
				return io.NopCloser(rs), nil
			}
		}

		return c.httpClient.Do(req)
	}

	var resp *http.Response
	var err error
	if isSeekable {
		resp, err = c.doWithRetry(ctx, send)
	} else {
		resp, err = send()
	}
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusNoContent {
		return nil
	}

	return fmt.Errorf("put %s failed: %s", davPath, resp.Status)
}

// Mkcol creates a collection (directory) on the WebDAV server.
func (c *Client) Mkcol(ctx context.Context, davPath string) error {
	resp, err := c.doWithRetry(ctx, func() (*http.Response, error) {
		req, err := c.newRequest(ctx, "MKCOL", davPath, nil)
		if err != nil {
			return nil, err
		}
		return c.httpClient.Do(req)
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if resp.StatusCode == http.StatusMethodNotAllowed {
		return fmt.Errorf("%w: %s", ErrAlreadyExists, resp.Status)
	}
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusConflict {
		return fmt.Errorf("%w: %s", ErrNotFound, resp.Status)
	}

	return fmt.Errorf("mkcol %s failed: %s", davPath, resp.Status)
}

// EnsureDir recursively ensures that the directory and all its parent directories exist.
func (c *Client) EnsureDir(ctx context.Context, dirPath string) error {
	cleanDir := path.Clean(dirPath)
	if cleanDir == "." || cleanDir == "/" || cleanDir == "" {
		return nil
	}
	if !strings.HasPrefix(cleanDir, "/") {
		cleanDir = "/" + cleanDir
	}
	if cleanDir == "/" || cleanDir == "/." {
		return nil
	}

	return c.ensureFlight.Do(ctx, cleanDir, func(runCtx context.Context) error {
		timeout := c.timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		opCtx, cancel := context.WithTimeout(runCtx, timeout)
		defer cancel()

		// Fast path: check if dir already exists
		infos, err := c.List(opCtx, cleanDir, 0)
		if err == nil && len(infos) > 0 && infos[0].IsDir {
			return nil
		}

		parts := strings.Split(strings.Trim(cleanDir, "/"), "/")
		current := ""
		for _, part := range parts {
			if part == "" {
				continue
			}
			current += "/" + part
			mkErr := c.Mkcol(opCtx, current)
			if mkErr != nil {
				if errors.Is(mkErr, ErrAlreadyExists) {
					continue
				}
				return fmt.Errorf("ensure dir %s failed: %w", current, mkErr)
			}
		}

		return nil
	})
}
// Delete removes a file or directory from the WebDAV server.
func (c *Client) Delete(ctx context.Context, davPath string) error {
	resp, err := c.doWithRetry(ctx, func() (*http.Response, error) {
		req, err := c.newRequest(ctx, http.MethodDelete, davPath, nil)
		if err != nil {
			return nil, err
		}
		return c.httpClient.Do(req)
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: %s", ErrNotFound, resp.Status)
	}

	return fmt.Errorf("delete %s failed: %s", davPath, resp.Status)
}

// Copy copies a resource from srcPath to dstPath.
func (c *Client) Copy(ctx context.Context, srcPath, dstPath string, overwrite bool) error {
	return c.copyOrMove(ctx, "COPY", srcPath, dstPath, overwrite)
}

// Move moves a resource from srcPath to dstPath.
func (c *Client) Move(ctx context.Context, srcPath, dstPath string, overwrite bool) error {
	return c.copyOrMove(ctx, "MOVE", srcPath, dstPath, overwrite)
}

func (c *Client) copyOrMove(ctx context.Context, method, srcPath, dstPath string, overwrite bool) error {
	destURL := c.resolveURL(dstPath)
	overwriteVal := "T"
	if !overwrite {
		overwriteVal = "F"
	}

	resp, err := c.doWithRetry(ctx, func() (*http.Response, error) {
		req, err := c.newRequest(ctx, method, srcPath, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Destination", destURL)
		req.Header.Set("Overwrite", overwriteVal)
		return c.httpClient.Do(req)
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
		return nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: %s", ErrNotFound, resp.Status)
	}
	if resp.StatusCode == http.StatusPreconditionFailed {
		return fmt.Errorf("%w: %s", ErrPreconditionFailed, resp.Status)
	}

	return fmt.Errorf("%s %s -> %s failed: %s", strings.ToLower(method), srcPath, dstPath, resp.Status)
}

func parseWebDAVTime(s string) time.Time {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "\"")
	if s == "" {
		return time.Time{}
	}

	// 1. Standard HTTP-date formats (RFC 1123, RFC 850, ANSI C asctime)
	// http.ParseTime returns time in UTC.
	if t, err := http.ParseTime(s); err == nil {
		return t.UTC()
	}

	// 2. Formats with timezones (RFC 3339, ISO 8601, RFC 1123 variations)
	tzFormats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		time.RFC1123Z,
		time.RFC1123,
		time.RFC822Z,
		time.RFC822,
		time.RFC850,
		time.ANSIC,
		time.UnixDate,
		time.RubyDate,
		"2006-01-02 15:04:05 -0700",
		"2006-01-02 15:04:05 -07:00",
		"2006-01-02 15:04:05 MST",
		"2006-01-02 15:04:05.999999999 -0700",
		"2006-01-02 15:04:05.999999999 -07:00",
		"2006-01-02 15:04:05.999999999 MST",
	}
	for _, layout := range tzFormats {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}

	// 3. Formats without timezone (NAS local clock timestamps)
	// Parse directly in UTC to avoid local timezone offset skew.
	noTzFormats := []string{
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"Mon, 02 Jan 2006 15:04:05",
		"02 Jan 2006 15:04:05",
		"2006-01-02",
	}
	for _, layout := range noTzFormats {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t.UTC()
		}
	}

	return time.Time{}
}
