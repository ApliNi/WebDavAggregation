package server

import (
	"context"
	"database/sql"
	"encoding/xml"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"webdav-agg/src/client"
	"webdav-agg/src/cluster"
	"webdav-agg/src/data"
	"webdav-agg/src/logger"
)

func (s *Server) resolveDAVPath(p string) string {
	clean := path.Clean("/" + p)
	if clean == "/dav" {
		return "/"
	}
	if strings.HasPrefix(clean, "/dav/") {
		return path.Clean("/" + strings.TrimPrefix(clean, "/dav"))
	}
	return clean
}
// resolveDestinationPath normalizes the Destination header path against srcPath.
// If Destination was provided as an absolute URI via a reverse proxy (e.g. OpenList mounting at /dav/WebDavAggregation/)
// while the inner request path has that mount prefix stripped, this strips the mount prefix from destPath as well.
func (s *Server) resolveDestinationPath(srcPath, destURLPath string) string {
	destClean := s.resolveDAVPath(destURLPath)
	mountPrefixes := []string{"/WebDavAggregation"}
	for _, mp := range mountPrefixes {
		if strings.HasPrefix(destClean, mp+"/") && !strings.HasPrefix(srcPath, mp+"/") {
			candidate := strings.TrimPrefix(destClean, mp)
			if candidate == "" {
				candidate = "/"
			}
			destClean = path.Clean(candidate)
			break
		}
	}
	return destClean
}

func isValidContentRange(cr string) bool {
	cr = strings.TrimSpace(cr)
	if !strings.HasPrefix(cr, "bytes ") {
		return false
	}
	spec := strings.TrimPrefix(cr, "bytes ")
	parts := strings.Split(spec, "/")
	if len(parts) != 2 {
		return false
	}
	rangePart := parts[0]
	rangeVals := strings.Split(rangePart, "-")
	if len(rangeVals) != 2 {
		return false
	}
	start, err1 := strconv.ParseInt(rangeVals[0], 10, 64)
	end, err2 := strconv.ParseInt(rangeVals[1], 10, 64)
	if err1 != nil || err2 != nil || start < 0 || end < start {
		return false
	}
	return true
}

func isRangeUnsatisfiable(rangeHdr string, size int64) bool {
	rangeHdr = strings.TrimSpace(rangeHdr)
	if !strings.HasPrefix(rangeHdr, "bytes=") {
		return false
	}
	spec := strings.TrimPrefix(rangeHdr, "bytes=")
	if strings.Contains(spec, ",") {
		return false
	}
	if strings.HasPrefix(spec, "-") {
		return size <= 0
	}
	parts := strings.Split(spec, "-")
	if len(parts) < 1 {
		return false
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return false
	}
	return start >= size
}
func (s *Server) isExpired(rec data.FileRecord) bool {
	if s.cfg == nil {
		return false
	}
	d := s.cfg.CacheExpireDuration()
	if d < 0 {
		return false
	}
	if d == 0 {
		return true
	}
	if rec.LastProbedAt.IsZero() {
		return true
	}
	return time.Since(rec.LastProbedAt) > d
}

func cleanNodeDirRecursive(ctx context.Context, n *cluster.Node, dirPath string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if n == nil || n.Client == nil {
		return nil
	}
	infos, err := n.Client.List(ctx, dirPath, 1)
	if err != nil {
		if errors.Is(err, client.ErrNotFound) {
			return nil
		}
		return err
	}
	normRoot := data.NormalizePath(dirPath)
	for _, info := range infos {
		normItem := data.NormalizePath(info.Path)
		if normItem == normRoot {
			continue
		}
		if info.IsDir {
			_ = cleanNodeDirRecursive(ctx, n, info.Path)
			_ = n.Client.Delete(ctx, info.Path)
		} else {
			_ = n.Client.Delete(ctx, info.Path)
		}
	}
	return n.Client.Delete(ctx, dirPath)
}
func (s *Server) handleOptions(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("DAV", "1, 2")
	w.Header().Set("Allow", "OPTIONS, GET, HEAD, PUT, DELETE, MKCOL, MOVE, COPY, PROPFIND, PROPPATCH, LOCK, UNLOCK")
	w.Header().Set("MS-Author-Via", "DAV")
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleProppatch(w http.ResponseWriter, r *http.Request) {
	davPath := s.resolveDAVPath(r.URL.Path)

	if s.lockMgr.IsLocked(davPath, r.Header.Get("If"), r.Header.Get("Lock-Token")) {
		http.Error(w, "Locked", http.StatusLocked)
		return
	}

	dm := s.cluster.DataManager()
	if dm == nil {
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	records, err := dm.GetFilesByPath(davPath)
	if err != nil || len(records) == 0 {
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.WriteHeader(http.StatusMultiStatus)
		_, _ = w.Write(RenderProppatchMultiStatus(davPath, []ProppatchPropResult{{
			StatusCode: http.StatusNotFound,
		}}))
		return
	}

	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
	}

	if len(body) == 0 {
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.WriteHeader(http.StatusMultiStatus)
		_, _ = w.Write(renderProppatchResponse(davPath))
		return
	}

	req, err := ParseProppatchBytes(body)
	if err != nil {
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.WriteHeader(http.StatusMultiStatus)
		_, _ = w.Write(renderProppatchResponse(davPath))
		return
	}

	allNames := req.AllPropertyNames()
	if len(allNames) == 0 {
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.WriteHeader(http.StatusMultiStatus)
		_, _ = w.Write(renderProppatchResponse(davPath))
		return
	}

	var failedProp *xml.Name
	for _, name := range allNames {
		if IsProtectedLiveProperty(name) {
			failedProp = &name
			break
		}
	}

	var results []ProppatchPropResult
	if failedProp != nil {
		results = FailProppatchDependency(allNames, *failedProp, http.StatusForbidden, "cannot modify protected property")
	} else {
		for _, name := range allNames {
			results = append(results, ProppatchPropResult{
				Prop:       name,
				StatusCode: http.StatusOK,
			})
		}
	}

	respXML := RenderProppatchMultiStatus(davPath, results)
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	_, _ = w.Write(respXML)
}

func (s *Server) handlePropfind(w http.ResponseWriter, r *http.Request) {
	davPath := s.resolveDAVPath(r.URL.Path)
	depthStr := strings.TrimSpace(r.Header.Get("Depth"))
	depth := 1
	if depthStr == "0" {
		depth = 0
	} else if strings.EqualFold(depthStr, "infinity") {
		depth = 1
	}

	nodes := s.cluster.HealthyNodes()
	if len(nodes) == 0 {
		if davPath == "/" {
			w.Header().Set("Content-Type", "application/xml; charset=utf-8")
			w.WriteHeader(http.StatusMultiStatus)
			root := client.FileInfo{Path: "/", Name: "", IsDir: true}
			_, _ = w.Write(renderPropfindResponse(root, nil, 0, 0, s.lockMgr))
			return
		}
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	dm := s.cluster.DataManager()
	if dm == nil {
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	var selfEntry client.FileInfo

	if davPath == "/" {
		selfEntry = client.FileInfo{
			Path:  "/",
			Name:  "",
			IsDir: true,
		}
	} else {
		selfRecs, err := dm.GetFilesByPath(davPath)
		if err != nil || len(selfRecs) == 0 {
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}

		hasDir := false
		for _, r := range selfRecs {
			if r.IsDir {
				hasDir = true
				break
			}
		}

		if hasDir {
			selfEntry = client.FileInfo{
				Path:  davPath,
				Name:  path.Base(davPath),
				IsDir: true,
			}
			for _, r := range selfRecs {
				if r.IsDir && r.ModTime.After(selfEntry.ModTime) {
					selfEntry.ModTime = r.ModTime
				}
			}
		} else {
			if len(selfRecs) == 1 {
				rec := selfRecs[0]
				if s.isExpired(rec) {
					if n, ok := s.cluster.GetNode(rec.StorageID); ok && n != nil && n.Client != nil {
						info, probeErr := n.Client.Stat(r.Context(), davPath)
						if probeErr != nil {
							if errors.Is(probeErr, client.ErrNotFound) {
								_ = dm.DeleteFile(rec.StorageID, davPath)
								http.Error(w, "Not Found", http.StatusNotFound)
								return
							}
						} else if info != nil {
							_ = dm.UpdateLastProbed(rec.StorageID, davPath, time.Now())
							selfRecs[0].LastProbedAt = time.Now()
						}
					}
				}
				selfEntry = client.FileInfo{
					Path:        davPath,
					Name:        selfRecs[0].Name(),
					Size:        selfRecs[0].Size,
					IsDir:       false,
					ModTime:     selfRecs[0].ModTime,
					ETag:        selfRecs[0].ETag(),
					ContentType: selfRecs[0].ContentType(),
				}
			} else {
				instances := make([]cluster.FileInfoWithNode, 0, len(selfRecs))
				for _, sr := range selfRecs {
					n, _ := s.cluster.GetNode(sr.StorageID)
					instances = append(instances, cluster.FileInfoWithNode{
						FileInfo: client.FileInfo{
							Path:        sr.Path,
							Name:        sr.Name(),
							Size:        sr.Size,
							IsDir:       sr.IsDir,
							ModTime:     sr.ModTime,
							ETag:        sr.ETag(),
							ContentType: sr.ContentType(),
						},
						Node: n,
					})
				}
				newest, err := s.cluster.DeduplicateFiles(r.Context(), davPath, instances)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				for _, inst := range instances {
					if inst.Node != newest.Node {
						_ = dm.DeleteFile(inst.Node.ID(), davPath)
						st, err := dm.GetStorage(inst.Node.ID())
						if err == nil && st != nil {
							inst.Node.SetQuota(st.UsedBytes, st.FileCount)
						}
					}
				}
				selfEntry = newest.FileInfo
				selfEntry.Path = davPath
			}
		}
	}

	var children []client.FileInfo
	if depth > 0 && selfEntry.IsDir {
		childRecs, err := dm.ListChildren(davPath)
		if err == nil && len(childRecs) > 0 {
			childMap := make(map[string][]data.FileRecord)
			var childPaths []string
			for _, cr := range childRecs {
				if _, ok := childMap[cr.Path]; !ok {
					childPaths = append(childPaths, cr.Path)
				}
				childMap[cr.Path] = append(childMap[cr.Path], cr)
			}

			for _, cp := range childPaths {
				recs := childMap[cp]
				isDir := false
				for _, r := range recs {
					if r.IsDir {
						isDir = true
						break
					}
				}

				if isDir {
					dirInfo := client.FileInfo{
						Path:  cp,
						Name:  path.Base(cp),
						IsDir: true,
					}
					for _, r := range recs {
						if r.IsDir && r.ModTime.After(dirInfo.ModTime) {
							dirInfo.ModTime = r.ModTime
						}
					}
					children = append(children, dirInfo)
				} else {
					if len(recs) == 1 {
						children = append(children, client.FileInfo{
							Path:        cp,
							Name:        recs[0].Name(),
							Size:        recs[0].Size,
							IsDir:       false,
							ModTime:     recs[0].ModTime,
							ETag:        recs[0].ETag(),
							ContentType: recs[0].ContentType(),
						})
					} else {
						instances := make([]cluster.FileInfoWithNode, 0, len(recs))
						for _, r := range recs {
							n, _ := s.cluster.GetNode(r.StorageID)
							instances = append(instances, cluster.FileInfoWithNode{
								FileInfo: client.FileInfo{
									Path:        r.Path,
									Name:        r.Name(),
									Size:        r.Size,
									IsDir:       r.IsDir,
									ModTime:     r.ModTime,
									ETag:        r.ETag(),
									ContentType: r.ContentType(),
								},
								Node: n,
							})
						}
						newest, err := s.cluster.DeduplicateFiles(r.Context(), cp, instances)
						if err == nil && newest != nil {
							for _, inst := range instances {
								if inst.Node != newest.Node {
									_ = dm.DeleteFile(inst.Node.ID(), cp)
									st, err := dm.GetStorage(inst.Node.ID())
									if err == nil && st != nil {
										inst.Node.SetQuota(st.UsedBytes, st.FileCount)
									}
								}
							}
							fi := newest.FileInfo
							fi.Path = cp
							children = append(children, fi)
						}
					}
				}
			}

			sort.Slice(children, func(i, j int) bool {
				return children[i].Path < children[j].Path
			})
		}
	}

	availBytes, usedBytes, _ := s.cluster.TotalQuota()
	if availBytes == 0 && usedBytes == 0 {
		_ = s.cluster.RefreshAllQuotas(r.Context())
		availBytes, usedBytes, _ = s.cluster.TotalQuota()
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	respXML := renderPropfindResponse(selfEntry, children, availBytes, usedBytes, s.lockMgr)
	_, _ = w.Write(respXML)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	s.handleGetHead(w, r, false)
}

func (s *Server) handleHead(w http.ResponseWriter, r *http.Request) {
	s.handleGetHead(w, r, true)
}

func (s *Server) handleGetHead(w http.ResponseWriter, r *http.Request, isHead bool) {
	davPath := s.resolveDAVPath(r.URL.Path)

	nodes := s.cluster.HealthyNodes()
	if len(nodes) == 0 {
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	dm := s.cluster.DataManager()
	if dm == nil {
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	records, err := dm.GetFilesByPath(davPath)
	if err != nil || len(records) == 0 {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	hasDir := false
	for _, rec := range records {
		if rec.IsDir {
			hasDir = true
			break
		}
	}

	if hasDir {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		if !isHead {
			fmt.Fprintf(w, "<html><body><h1>Directory: %s</h1></body></html>", html.EscapeString(davPath))
		}
		return
	}

	var target data.FileRecord
	if len(records) == 1 {
		target = records[0]
	} else {
		instances := make([]cluster.FileInfoWithNode, 0, len(records))
		for _, rec := range records {
			n, _ := s.cluster.GetNode(rec.StorageID)
			instances = append(instances, cluster.FileInfoWithNode{
				FileInfo: client.FileInfo{
					Path:        rec.Path,
					Name:        rec.Name(),
					Size:        rec.Size,
					IsDir:       rec.IsDir,
					ModTime:     rec.ModTime,
					ETag:        rec.ETag(),
					ContentType: rec.ContentType(),
				},
				Node: n,
			})
		}
		newest, err := s.cluster.DeduplicateFiles(r.Context(), davPath, instances)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, inst := range instances {
			if inst.Node != newest.Node {
				_ = dm.DeleteFile(inst.Node.ID(), davPath)
				st, err := dm.GetStorage(inst.Node.ID())
				if err == nil && st != nil {
					inst.Node.SetQuota(st.UsedBytes, st.FileCount)
				}
			}
		}
		for _, rec := range records {
			if rec.StorageID == newest.Node.ID() {
				target = rec
				break
			}
		}
	}
	// Cache expiration probe check:
	// If the file cache is expired (or cache_expire == 0), probe upstream node to ensure file still exists physically.
	if !target.IsDir && s.isExpired(target) {
		if node, ok := s.cluster.GetNode(target.StorageID); ok && node != nil && node.Client != nil {
			info, probeErr := node.Client.Stat(r.Context(), davPath)
			if probeErr != nil {
				if errors.Is(probeErr, client.ErrNotFound) {
					_ = dm.DeleteFile(target.StorageID, davPath)
					http.Error(w, "Not Found", http.StatusNotFound)
					return
				}
				logger.Warn("DAV", "Upstream probe for %s on node %d encountered error: %v", davPath, target.StorageID, probeErr)
			} else if info != nil {
				_ = dm.UpdateLastProbed(target.StorageID, davPath, time.Now())
				target.LastProbedAt = time.Now()
			}
		}
	}
	// 1. Evaluate RFC 7232 Preconditions: If-Match & If-Unmodified-Since (Precondition Failed: 412)
	if im := r.Header.Get("If-Match"); im != "" {
		targetETag := strings.Trim(target.ETag(), `"`)
		matched := false
		if im == "*" {
			matched = true
		} else {
			for _, item := range strings.Split(im, ",") {
				item = strings.TrimSpace(item)
				if strings.HasPrefix(item, "W/") {
					continue // Strong comparison: weak tag cannot match If-Match
				}
				item = strings.Trim(item, `"`)
				if item == targetETag {
					matched = true
					break
				}
			}
		}
		if !matched {
			http.Error(w, "Precondition Failed", http.StatusPreconditionFailed)
			return
		}
	} else if ius := r.Header.Get("If-Unmodified-Since"); ius != "" {
		if t, err := http.ParseTime(ius); err == nil && !target.ModTime.IsZero() {
			if target.ModTime.Truncate(time.Second).After(t.Truncate(time.Second)) {
				http.Error(w, "Precondition Failed", http.StatusPreconditionFailed)
				return
			}
		}
	}

	// 2. Evaluate RFC 7232 Preconditions: If-None-Match & If-Modified-Since (Not Modified: 304)
	inm := r.Header.Get("If-None-Match")
	ims := r.Header.Get("If-Modified-Since")
	matched304 := false

	if inm != "" {
		if inm == "*" {
			matched304 = true
		} else {
			targetETag := strings.Trim(target.ETag(), `"`)
			for _, item := range strings.Split(inm, ",") {
				item = strings.TrimSpace(item)
				item = strings.TrimPrefix(item, "W/")
				item = strings.Trim(item, `"`)
				if item == targetETag {
					matched304 = true
					break
				}
			}
		}
	} else if ims != "" {
		if t, err := http.ParseTime(ims); err == nil && !target.ModTime.IsZero() {
			if !target.ModTime.Truncate(time.Second).After(t.Truncate(time.Second)) {
				matched304 = true
			}
		}
	}

	if matched304 {
		if etag := target.ETag(); etag != "" {
			w.Header().Set("ETag", fmt.Sprintf(`"%s"`, strings.Trim(etag, `"`)))
		}
		if !target.ModTime.IsZero() {
			w.Header().Set("Last-Modified", target.ModTime.UTC().Format(http.TimeFormat))
		}
		w.WriteHeader(http.StatusNotModified)
		return
	}

	if rangeHdr := r.Header.Get("Range"); rangeHdr != "" {
		if isRangeUnsatisfiable(rangeHdr, target.Size) {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", target.Size))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
	}

	if isHead {
		w.Header().Set("Content-Length", strconv.FormatInt(target.Size, 10))
		ct := target.ContentType()
		if ct == "" {
			ct = "application/octet-stream"
		}
		w.Header().Set("Content-Type", ct)
		if !target.ModTime.IsZero() {
			w.Header().Set("Last-Modified", target.ModTime.UTC().Format(http.TimeFormat))
		}
		if etag := target.ETag(); etag != "" {
			w.Header().Set("ETag", `"`+strings.Trim(etag, `"`)+`"`)
		}
		w.Header().Set("Accept-Ranges", "bytes")
		w.WriteHeader(http.StatusOK)
		return
	}

	targetNode, ok := s.cluster.GetNode(target.StorageID)
	if !ok || targetNode == nil || targetNode.Client == nil {
		http.Error(w, "Storage node not available", http.StatusBadGateway)
		return
	}

	fwdHeader := make(http.Header)
	for _, h := range []string{"Range", "If-Modified-Since", "If-None-Match", "If-Range"} {
		if val := r.Header.Get(h); val != "" {
			fwdHeader.Set(h, val)
		}
	}

	rc, upstreamHeader, err := targetNode.Client.Get(r.Context(), davPath, fwdHeader)
	if err != nil {
		if errors.Is(err, client.ErrNotFound) {
			_ = dm.DeleteFile(target.StorageID, davPath)
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}
		if errors.Is(err, client.ErrRangeNotSatisfiable) {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", target.Size))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer rc.Close()

	if target.LastProbedAt.IsZero() || time.Since(target.LastProbedAt) > time.Minute {
		_ = dm.UpdateLastProbed(target.StorageID, davPath, time.Now())
		target.LastProbedAt = time.Now()
	}
	for _, h := range []string{"Content-Length", "Content-Type", "Last-Modified", "Accept-Ranges"} {
		if val := upstreamHeader.Get(h); val != "" {
			w.Header().Set(h, val)
		}
	}
	// Enforce uniform canonical ETag across HEAD, GET, and PROPFIND
	if etag := target.ETag(); etag != "" {
		w.Header().Set("ETag", fmt.Sprintf(`"%s"`, strings.Trim(etag, `"`)))
	}
	if w.Header().Get("Last-Modified") == "" && !target.ModTime.IsZero() {
		w.Header().Set("Last-Modified", target.ModTime.UTC().Format(http.TimeFormat))
	}

	upstreamStatus := upstreamHeader.Get("X-Upstream-Status")
	if upstreamStatus == strconv.Itoa(http.StatusNotModified) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	if upstreamStatus == strconv.Itoa(http.StatusRequestedRangeNotSatisfiable) {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", target.Size))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	cr := upstreamHeader.Get("Content-Range")

	// Strict Range passthrough: only return 206 if upstream explicitly returned 206 AND a valid Content-Range header.
	// If upstream returned 200 (or Range is not satisfied/ignored), return 200 with complete Content-Length.
	isPartial := (upstreamStatus == strconv.Itoa(http.StatusPartialContent) || (upstreamStatus == "" && r.Header.Get("Range") != "")) && isValidContentRange(cr)

	if isPartial {
		w.Header().Set("Content-Range", cr)
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Del("Content-Range")
		if cl := upstreamHeader.Get("Content-Length"); cl != "" {
			w.Header().Set("Content-Length", cl)
		} else {
			w.Header().Set("Content-Length", strconv.FormatInt(target.Size, 10))
		}
		w.WriteHeader(http.StatusOK)
	}
	_, _ = io.Copy(w, rc)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	davPath := s.resolveDAVPath(r.URL.Path)

	if s.lockMgr.IsLocked(davPath, r.Header.Get("If"), r.Header.Get("Lock-Token")) {
		http.Error(w, "Locked", http.StatusLocked)
		return
	}

	if locker := s.cluster.PathLocker(); locker != nil {
		unlock, ok := locker.TryLock(davPath)
		if !ok {
			logger.Warn("DAV", "Conflict: another upload is currently in progress for path %s", davPath)
			http.Error(w, "Conflict: another upload is currently in progress for this path", http.StatusConflict)
			return
		}
		defer unlock()
	}
	nodes := s.cluster.HealthyNodes()
	if len(nodes) == 0 {
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	dm := s.cluster.DataManager()
	if dm == nil {
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	records, err := dm.GetFilesByPath(davPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Query metadata: %v", err), http.StatusInternalServerError)
		return
	}

	var (
		oldNodeID int
		oldSize   int64
		oldNode   *cluster.Node
		exists    bool
	)

	if len(records) > 0 {
		for _, rec := range records {
			if rec.IsDir {
				http.Error(w, "Method Not Allowed (cannot PUT on directory)", http.StatusMethodNotAllowed)
				return
			}
		}

		if r.Header.Get("If-None-Match") == "*" {
			http.Error(w, "Precondition Failed", http.StatusPreconditionFailed)
			return
		}

		exists = true
		if len(records) > 1 {
			instances := make([]cluster.FileInfoWithNode, 0, len(records))
			for _, rec := range records {
				n, _ := s.cluster.GetNode(rec.StorageID)
				instances = append(instances, cluster.FileInfoWithNode{
					FileInfo: client.FileInfo{
						Path:        rec.Path,
						Name:        rec.Name(),
						Size:        rec.Size,
						IsDir:       rec.IsDir,
						ModTime:     rec.ModTime,
						ETag:        rec.ETag(),
						ContentType: rec.ContentType(),
					},
					Node: n,
				})
			}
			newest, err := s.cluster.DeduplicateFilesLocked(r.Context(), davPath, instances)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			for _, inst := range instances {
				if inst.Node != nil && inst.Node != newest.Node {
					_ = dm.DeleteFile(inst.Node.ID(), davPath)
					st, err := dm.GetStorage(inst.Node.ID())
					if err == nil && st != nil {
						inst.Node.SetQuota(st.UsedBytes, st.FileCount)
					}
				}
			}
			oldNode = newest.Node
			if oldNode != nil {
				oldNodeID = oldNode.ID()
			}
			oldSize = newest.Size
		} else {
			oldNodeID = records[0].StorageID
			oldSize = records[0].Size
			oldNode, _ = s.cluster.GetNode(oldNodeID)
		}
	}

	var owCtx []cluster.OverwriteContext
	if exists {
		owCtx = []cluster.OverwriteContext{{
			OldNodeID: oldNodeID,
			OldSize:   oldSize,
		}}
	}

	targetNode, commit, rollback, err := s.cluster.ReserveWrite(r.ContentLength, owCtx...)
	if err != nil {
		if errors.Is(err, cluster.ErrInsufficientStorage) {
			http.Error(w, "Insufficient Storage", http.StatusInsufficientStorage)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer rollback()

	var cr *countingReadCloser
	if existing, ok := r.Body.(*countingReadCloser); ok {
		cr = existing
	} else {
		cr = newCountingReadCloser(r.Body)
		r.Body = cr
	}
	err = targetNode.Client.Put(r.Context(), davPath, cr, r.ContentLength, r.Header.Get("Content-Type"))
	if err != nil {
		rollback()
		if !exists || targetNode.ID() != oldNodeID {
			cleanCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = targetNode.Client.Delete(cleanCtx, davPath)
			cancel()
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	written := cr.BytesRead()
	if written == 0 && r.ContentLength > 0 {
		written = r.ContentLength
	}

	txErr := dm.WithTx(r.Context(), func(tx *sql.Tx) error {
		return dm.PutFileTx(tx, data.FileRecord{
			StorageID: targetNode.ID(),
			Path:      davPath,
			Size:      written,
			IsDir:     false,
			ModTime:   time.Now(),
		})
	})
	if txErr != nil {
		rollback()
		cleanCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = targetNode.Client.Delete(cleanCtx, davPath)
		cancel()
		http.Error(w, fmt.Sprintf("Failed to commit metadata: %v", txErr), http.StatusInternalServerError)
		return
	}

	commit(written)
	st, err := dm.GetStorage(targetNode.ID())
	if err == nil && st != nil {
		targetNode.SetQuota(st.UsedBytes, st.FileCount)
	}

	// If overwritten across different nodes, asynchronously delete old physical file on oldNode
	// and update oldNode's in-memory quota (already decremented in SQLite by trigger)
	if exists && targetNode.ID() != oldNodeID && oldNode != nil {
		go func(n *cluster.Node, p string, oid int) {
			if n.Client != nil {
				_ = n.Client.Delete(context.Background(), p)
			}
		}(oldNode, davPath, oldNodeID)

		if oldSt, err := dm.GetStorage(oldNodeID); err == nil && oldSt != nil {
			oldNode.SetQuota(oldSt.UsedBytes, oldSt.FileCount)
		}
	}

	if !exists {
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	davPath := s.resolveDAVPath(r.URL.Path)

	// Hard intercept root path deletion
	if davPath == "" || davPath == "/" {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	if s.lockMgr.IsLocked(davPath, r.Header.Get("If"), r.Header.Get("Lock-Token")) {
		http.Error(w, "Locked", http.StatusLocked)
		return
	}

	if locker := s.cluster.PathLocker(); locker != nil {
		unlock, ok := locker.TryLockSubtree(davPath)
		if !ok {
			logger.Warn("DAV", "Conflict: another operation is in progress for subtree %s", davPath)
			http.Error(w, "Conflict: another operation is in progress for this path", http.StatusConflict)
			return
		}
		defer unlock()
	}

	nodes := s.cluster.HealthyNodes()
	if len(nodes) == 0 {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	dm := s.cluster.DataManager()
	if dm == nil {
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	subtree, err := dm.ListSubtree(davPath)
	if err != nil || len(subtree) == 0 {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	storageIDs := make(map[int]bool)
	isDir := false
	for _, rec := range subtree {
		storageIDs[rec.StorageID] = true
		if rec.IsDir && rec.Path == davPath {
			isDir = true
		}
	}
	if isDir {
		for _, n := range s.cluster.HealthyNodes() {
			storageIDs[n.ID()] = true
		}
	}
	// Single regular file fast path
	if !isDir && len(subtree) == 1 {
		rec := subtree[0]
		node, ok := s.cluster.GetNode(rec.StorageID)
		if !ok || node == nil || node.Client == nil {
			http.Error(w, "Storage node unavailable", http.StatusBadGateway)
			return
		}
		err := node.Client.Delete(r.Context(), davPath)
		if err != nil && !errors.Is(err, client.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		txErr := dm.WithTx(r.Context(), func(tx *sql.Tx) error {
			return dm.DeleteSubtreeByStorageTx(tx, rec.StorageID, davPath)
		})
		if txErr != nil {
			http.Error(w, fmt.Sprintf("Failed to delete metadata: %v", txErr), http.StatusInternalServerError)
			return
		}
		st, err := dm.GetStorage(rec.StorageID)
		if err == nil && st != nil {
			node.SetQuota(st.UsedBytes, st.FileCount)
		}
		s.lockMgr.Remove(davPath)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Directory cascading bottom-up deletion across relevant upstream nodes:
	// Cloud drive drivers (such as 189) fail or hang on non-empty directory deletion.
	// We safely clean leaf files first, then subdirectories bottom-up, and finally the target directory.
	nodeFiles := make(map[int][]data.FileRecord)
	nodeDirs := make(map[int][]data.FileRecord)
	for _, rec := range subtree {
		if rec.Path == davPath {
			continue
		}
		if rec.IsDir {
			nodeDirs[rec.StorageID] = append(nodeDirs[rec.StorageID], rec)
		} else {
			nodeFiles[rec.StorageID] = append(nodeFiles[rec.StorageID], rec)
		}
	}

	var (
		delWG         sync.WaitGroup
		mu            sync.Mutex
		successfulIDs = make(map[int]bool)
		failures      []DeleteFailure
		lastErr       error
	)

	for sid := range storageIDs {
		node, ok := s.cluster.GetNode(sid)
		if !ok || node == nil || node.Client == nil {
			failures = append(failures, DeleteFailure{
				Path:       davPath,
				StatusCode: http.StatusBadGateway,
				StatusText: fmt.Sprintf("storage node %d unavailable", sid),
			})
			continue
		}
		delWG.Add(1)
		go func(n *cluster.Node, id int) {
			defer delWG.Done()
			ctx := r.Context()

			// Step 1: Delete all leaf files on this node under davPath
			files := nodeFiles[id]
			for _, f := range files {
				if ctx.Err() != nil {
					break
				}
				if err := n.Client.Delete(ctx, f.Path); err != nil && !errors.Is(err, client.ErrNotFound) {
					logger.Warn("DAV", "Delete leaf file %s on node %d failed: %v", f.Path, id, err)
				}
			}

			// Step 2: Delete subdirectories in bottom-up order (deepest depth first)
			dirs := nodeDirs[id]
			sort.Slice(dirs, func(i, j int) bool {
				di := strings.Count(dirs[i].Path, "/")
				dj := strings.Count(dirs[j].Path, "/")
				if di != dj {
					return di > dj
				}
				return len(dirs[i].Path) > len(dirs[j].Path)
			})
			for _, d := range dirs {
				if ctx.Err() != nil {
					break
				}
				if err := n.Client.Delete(ctx, d.Path); err != nil && !errors.Is(err, client.ErrNotFound) {
					if cleanErr := cleanNodeDirRecursive(ctx, n, d.Path); cleanErr != nil {
						logger.Warn("DAV", "Delete subdir %s on node %d failed: %v", d.Path, id, cleanErr)
					}
				}
			}

			// Step 3: Delete root target collection itself on this node.
			// Cloud drivers (such as 189) fail or hang on non-empty directories.
			// If deleting the directory directly fails, fall back to recursive bottom-up physical cleanup.
			err := n.Client.Delete(ctx, davPath)
			if err != nil && !errors.Is(err, client.ErrNotFound) {
				cleanErr := cleanNodeDirRecursive(ctx, n, davPath)
				if cleanErr == nil || errors.Is(cleanErr, client.ErrNotFound) {
					err = nil
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if err == nil || errors.Is(err, client.ErrNotFound) {
				successfulIDs[id] = true
			} else {
				lastErr = err
				failures = append(failures, DeleteFailure{
					Path:       davPath,
					StatusCode: http.StatusBadGateway,
					StatusText: err.Error(),
				})
			}
		}(node, sid)
	}
	delWG.Wait()

	if len(successfulIDs) == 0 {
		if lastErr != nil {
			http.Error(w, lastErr.Error(), http.StatusInternalServerError)
			return
		}
		http.Error(w, "Delete failed", http.StatusInternalServerError)
		return
	}

	txErr := dm.WithTx(r.Context(), func(tx *sql.Tx) error {
		for sid := range successfulIDs {
			if err := dm.DeleteSubtreeByStorageTx(tx, sid, davPath); err != nil {
				return err
			}
		}
		return nil
	})
	if txErr != nil {
		http.Error(w, fmt.Sprintf("Failed to delete metadata: %v", txErr), http.StatusInternalServerError)
		return
	}

	for sid := range successfulIDs {
		st, err := dm.GetStorage(sid)
		if err == nil && st != nil {
			if n, ok := s.cluster.GetNode(sid); ok && n != nil {
				n.SetQuota(st.UsedBytes, st.FileCount)
			}
		}
	}

	s.lockMgr.Remove(davPath)

	// RFC 4918 Section 9.6.1: If partial deletion occurred, return 207 Multi-Status
	if len(failures) > 0 {
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.WriteHeader(http.StatusMultiStatus)
		_, _ = w.Write(renderDeleteMultiStatus(failures))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleMkcol(w http.ResponseWriter, r *http.Request) {
	davPath := s.resolveDAVPath(r.URL.Path)

	if s.lockMgr.IsLocked(davPath, r.Header.Get("If"), r.Header.Get("Lock-Token")) {
		http.Error(w, "Locked", http.StatusLocked)
		return
	}

	if locker := s.cluster.PathLocker(); locker != nil {
		unlock, ok := locker.TryLock(davPath)
		if !ok {
			http.Error(w, "Conflict: another operation is in progress for this path", http.StatusConflict)
			return
		}
		defer unlock()
	}
	nodes := s.cluster.HealthyNodes()
	if len(nodes) == 0 {
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	dm := s.cluster.DataManager()
	if dm == nil {
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	existing, err := dm.GetFilesByPath(davPath)
	if err == nil && len(existing) > 0 {
		http.Error(w, "Method Not Allowed (resource already exists)", http.StatusMethodNotAllowed)
		return
	}

	parent := path.Dir(davPath)
	if parent != "/" && parent != "." && parent != "" {
		parentRecs, err := dm.GetFilesByPath(parent)
		if err != nil || len(parentRecs) == 0 {
			http.Error(w, "Conflict (parent does not exist)", http.StatusConflict)
			return
		}
	}

	node, err := s.cluster.SelectWriteNode(0)
	if err != nil {
		http.Error(w, "Insufficient Storage", http.StatusInsufficientStorage)
		return
	}

	if err := node.Client.EnsureDir(r.Context(), davPath); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	txErr := dm.WithTx(r.Context(), func(tx *sql.Tx) error {
		return dm.PutFileTx(tx, data.FileRecord{
			StorageID: node.ID(),
			Path:      davPath,
			IsDir:     true,
			ModTime:   time.Now(),
		})
	})
	if txErr != nil {
		_ = node.Client.Delete(context.Background(), davPath)
		http.Error(w, fmt.Sprintf("Failed to save directory metadata: %v", txErr), http.StatusInternalServerError)
		return
	}

	st, err := dm.GetStorage(node.ID())
	if err == nil && st != nil {
		node.SetQuota(st.UsedBytes, st.FileCount)
	}

	// Broadcast directory pre-warming asynchronously to all other healthy nodes
	go func(dirPath string, excludeID int) {
		for _, n := range s.cluster.HealthyNodes() {
			if n.ID() != excludeID && n.Client != nil {
				if err := n.Client.EnsureDir(context.Background(), dirPath); err == nil {
					_ = dm.WithTx(context.Background(), func(tx *sql.Tx) error {
						return dm.PutFileTx(tx, data.FileRecord{
							StorageID: n.ID(),
							Path:      dirPath,
							IsDir:     true,
							ModTime:   time.Now(),
						})
					})
				}
			}
		}
	}(davPath, node.ID())

	w.WriteHeader(http.StatusCreated)
}

func (s *Server) handleMove(w http.ResponseWriter, r *http.Request) {
	s.handleMoveCopy(w, r, true)
}

func (s *Server) handleCopy(w http.ResponseWriter, r *http.Request) {
	s.handleMoveCopy(w, r, false)
}

func (s *Server) handleMoveCopy(w http.ResponseWriter, r *http.Request, isMove bool) {
	destHeader := r.Header.Get("Destination")
	if destHeader == "" {
		http.Error(w, "Missing Destination header", http.StatusBadRequest)
		return
	}

	destURL, err := url.Parse(destHeader)
	if err != nil {
		http.Error(w, "Invalid Destination header", http.StatusBadRequest)
		return
	}

	srcPath := s.resolveDAVPath(r.URL.Path)
	destPath := s.resolveDestinationPath(srcPath, destURL.Path)

	if srcPath == destPath {
		http.Error(w, "Source and destination cannot be identical", http.StatusForbidden)
		return
	}

	if s.lockMgr.IsLocked(srcPath, r.Header.Get("If"), r.Header.Get("Lock-Token")) ||
		s.lockMgr.IsLocked(destPath, r.Header.Get("If"), r.Header.Get("Lock-Token")) {
		http.Error(w, "Locked", http.StatusLocked)
		return
	}

	nodes := s.cluster.HealthyNodes()
	if len(nodes) == 0 {
		http.Error(w, "Source not found", http.StatusNotFound)
		return
	}

	dm := s.cluster.DataManager()
	if dm == nil {
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	isSrcDir := false
	if preCheckRecs, err := dm.GetFilesByPath(srcPath); err == nil {
		for _, rec := range preCheckRecs {
			if rec.IsDir {
				isSrcDir = true
				break
			}
		}
	}

	if locker := s.cluster.PathLocker(); locker != nil {
		srcSubtree := isMove || isSrcDir
		destSubtree := true

		first, second := srcPath, destPath
		firstSubtree, secondSubtree := srcSubtree, destSubtree
		if first > second {
			first, second = destPath, srcPath
			firstSubtree, secondSubtree = destSubtree, srcSubtree
		}

		var (
			unlock1, unlock2 func()
			ok1, ok2         bool
		)

		if firstSubtree {
			unlock1, ok1 = locker.TryLockSubtree(first)
		} else {
			unlock1, ok1 = locker.TryLock(first)
		}
		if !ok1 {
			http.Error(w, "Conflict: another operation is in progress for path", http.StatusConflict)
			return
		}
		defer unlock1()

		if secondSubtree {
			unlock2, ok2 = locker.TryLockSubtree(second)
		} else {
			unlock2, ok2 = locker.TryLock(second)
		}
		if !ok2 {
			http.Error(w, "Conflict: another operation is in progress for destination path", http.StatusConflict)
			return
		}
		defer unlock2()
	}

	overwrite := r.Header.Get("Overwrite") != "F"

	// RFC 4918 Section 9.8.5: If destination parent collection does not exist, return 409 Conflict
	destParent := path.Dir(destPath)
	if destParent != "/" && destParent != "." && destParent != "" {
		parentRecs, err := dm.GetFilesByPath(destParent)
		if err != nil || len(parentRecs) == 0 {
			http.Error(w, "Conflict (destination parent does not exist)", http.StatusConflict)
			return
		}
	}

	srcRecords, err := dm.GetFilesByPath(srcPath)
	if err != nil || len(srcRecords) == 0 {
		http.Error(w, "Source not found", http.StatusNotFound)
		return
	}

	var (
		srcNode   *cluster.Node
		srcRecord data.FileRecord
	)
	hasDir := false
	for _, rec := range srcRecords {
		if rec.IsDir {
			hasDir = true
			srcRecord = rec
			break
		}
	}

	if hasDir {
		srcNode, _ = s.cluster.GetNode(srcRecord.StorageID)
	} else if len(srcRecords) > 1 {
		instances := make([]cluster.FileInfoWithNode, 0, len(srcRecords))
		for _, rec := range srcRecords {
			n, _ := s.cluster.GetNode(rec.StorageID)
			instances = append(instances, cluster.FileInfoWithNode{
				FileInfo: client.FileInfo{
					Path:        rec.Path,
					Name:        rec.Name(),
					Size:        rec.Size,
					IsDir:       rec.IsDir,
					ModTime:     rec.ModTime,
					ETag:        rec.ETag(),
					ContentType: rec.ContentType(),
				},
				Node: n,
			})
		}
		newest, err := s.cluster.DeduplicateFilesLocked(r.Context(), srcPath, instances)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, inst := range instances {
			if inst.Node != newest.Node {
				_ = dm.DeleteFile(inst.Node.ID(), srcPath)
				st, err := dm.GetStorage(inst.Node.ID())
				if err == nil && st != nil {
					inst.Node.SetQuota(st.UsedBytes, st.FileCount)
				}
			}
		}
		srcNode = newest.Node
		for _, rec := range srcRecords {
			if rec.StorageID == newest.Node.ID() {
				srcRecord = rec
				break
			}
		}
	} else {
		srcRecord = srcRecords[0]
		srcNode, _ = s.cluster.GetNode(srcRecord.StorageID)
	}

	if srcNode == nil || srcNode.Client == nil {
		http.Error(w, "Source storage node unavailable", http.StatusBadGateway)
		return
	}

	destRecords, err := dm.GetFilesByPath(destPath)
	destExists := err == nil && len(destRecords) > 0

	if destExists && !overwrite {
		http.Error(w, "Precondition Failed (destination exists)", http.StatusPreconditionFailed)
		return
	}

	canSameUpstream := false
	if !destExists {
		canSameUpstream = true
	} else {
		sameOnly := true
		for _, dr := range destRecords {
			if dr.StorageID != srcNode.ID() {
				sameOnly = false
				break
			}
		}
		if sameOnly {
			canSameUpstream = true
		}
	}

	if canSameUpstream {
		var opErr error
		if isMove {
			opErr = srcNode.Client.Move(r.Context(), srcPath, destPath, overwrite)
			if opErr != nil {
				if !srcRecord.IsDir {
					logger.Warn("DAV", "Native MOVE failed on node %d for %s -> %s: %v, falling back to copy+delete", srcNode.ID(), srcPath, destPath, opErr)
					rc, _, getErr := srcNode.Client.Get(r.Context(), srcPath, nil)
					if getErr != nil {
						http.Error(w, fmt.Sprintf("Move fallback GET failed: %v", getErr), http.StatusInternalServerError)
						return
					}
					putErr := srcNode.Client.Put(r.Context(), destPath, rc, srcRecord.Size, srcRecord.ContentType())
					_ = rc.Close()
					if putErr != nil {
						_ = srcNode.Client.Delete(context.Background(), destPath)
						http.Error(w, fmt.Sprintf("Move fallback PUT failed: %v", putErr), http.StatusInternalServerError)
						return
					}
					txErr := dm.WithTx(r.Context(), func(tx *sql.Tx) error {
						if destExists {
							if err := dm.DeleteSubtreeByStorageTx(tx, srcNode.ID(), destPath); err != nil {
								return err
							}
						}
						return dm.MovePathTx(tx, srcNode.ID(), srcPath, destPath)
					})
					if txErr != nil {
						_ = srcNode.Client.Delete(context.Background(), destPath)
						http.Error(w, fmt.Sprintf("Move fallback metadata failed: %v", txErr), http.StatusInternalServerError)
						return
					}
					_ = srcNode.Client.Delete(context.Background(), srcPath)
					st, err := dm.GetStorage(srcNode.ID())
					if err == nil && st != nil {
						srcNode.SetQuota(st.UsedBytes, st.FileCount)
					}
					if destExists {
						w.WriteHeader(http.StatusNoContent)
					} else {
						w.WriteHeader(http.StatusCreated)
					}
					return
				}
				// For directory, proceed to the two-phase pipeline fallback below
				logger.Warn("DAV", "Native directory MOVE failed on node %d for %s -> %s: %v, falling back to two-phase pipeline", srcNode.ID(), srcPath, destPath, opErr)
				goto pipelineFallback
			} else {
				txErr := dm.WithTx(r.Context(), func(tx *sql.Tx) error {
					if destExists {
						if err := dm.DeleteSubtreeByStorageTx(tx, srcNode.ID(), destPath); err != nil {
							return err
						}
					}
					return dm.MovePathTx(tx, srcNode.ID(), srcPath, destPath)
				})
				if txErr != nil {
					logger.Error("Main", "FATAL: MovePath transaction failed for storage %d from %s to %s: %v", srcNode.ID(), srcPath, destPath, txErr)
					_ = srcNode.Client.Move(context.Background(), destPath, srcPath, overwrite)
					http.Error(w, fmt.Sprintf("Failed to update metadata: %v", txErr), http.StatusInternalServerError)
					return
				}

				st, err := dm.GetStorage(srcNode.ID())
				if err == nil && st != nil {
					srcNode.SetQuota(st.UsedBytes, st.FileCount)
				}

				if destExists {
					w.WriteHeader(http.StatusNoContent)
				} else {
					w.WriteHeader(http.StatusCreated)
				}
				return
			}
		} else {
			opErr = srcNode.Client.Copy(r.Context(), srcPath, destPath, overwrite)
			if opErr != nil {
				if !srcRecord.IsDir {
					logger.Warn("DAV", "Native COPY failed on node %d for %s -> %s: %v, falling back to stream copy", srcNode.ID(), srcPath, destPath, opErr)
					rc, _, getErr := srcNode.Client.Get(r.Context(), srcPath, nil)
					if getErr != nil {
						http.Error(w, fmt.Sprintf("Copy fallback GET failed: %v", getErr), http.StatusInternalServerError)
						return
					}
					putErr := srcNode.Client.Put(r.Context(), destPath, rc, srcRecord.Size, srcRecord.ContentType())
					_ = rc.Close()
					if putErr != nil {
						_ = srcNode.Client.Delete(context.Background(), destPath)
						http.Error(w, fmt.Sprintf("Copy fallback PUT failed: %v", putErr), http.StatusInternalServerError)
						return
					}
					txErr := dm.WithTx(r.Context(), func(tx *sql.Tx) error {
						if destExists {
							if err := dm.DeleteSubtreeByStorageTx(tx, srcNode.ID(), destPath); err != nil {
								return err
							}
						}
						return dm.PutFileTx(tx, data.FileRecord{
							StorageID: srcNode.ID(),
							Path:      destPath,
							Size:      srcRecord.Size,
							IsDir:     false,
							ModTime:   time.Now(),
						})
					})
					if txErr != nil {
						_ = srcNode.Client.Delete(context.Background(), destPath)
						http.Error(w, fmt.Sprintf("Copy fallback metadata failed: %v", txErr), http.StatusInternalServerError)
						return
					}
					st, err := dm.GetStorage(srcNode.ID())
					if err == nil && st != nil {
						srcNode.SetQuota(st.UsedBytes, st.FileCount)
					}
					if destExists {
						w.WriteHeader(http.StatusNoContent)
					} else {
						w.WriteHeader(http.StatusCreated)
					}
					return
				}
				logger.Warn("DAV", "Native directory COPY failed on node %d for %s -> %s: %v, falling back to two-phase pipeline", srcNode.ID(), srcPath, destPath, opErr)
				goto pipelineFallback
			}

			txErr := dm.WithTx(r.Context(), func(tx *sql.Tx) error {
				if destExists {
					if err := dm.DeleteSubtreeByStorageTx(tx, srcNode.ID(), destPath); err != nil {
						return err
					}
				}
				if srcRecord.IsDir {
					subRecs, err := dm.ListSubtreeByStorage(srcNode.ID(), srcPath)
					if err != nil {
						return err
					}
					normSrc := data.NormalizePath(srcPath)
					normDest := data.NormalizePath(destPath)
					for _, sr := range subRecs {
						rel := strings.TrimPrefix(data.NormalizePath(sr.Path), normSrc)
						childDest := normDest + rel
						if err := dm.PutFileTx(tx, data.FileRecord{
							StorageID: srcNode.ID(),
							Path:      childDest,
							Size:      sr.Size,
							IsDir:     sr.IsDir,
							ModTime:   time.Now(),
						}); err != nil {
							return err
						}
					}
				} else {
					if err := dm.PutFileTx(tx, data.FileRecord{
						StorageID: srcNode.ID(),
						Path:      destPath,
						Size:      srcRecord.Size,
						IsDir:     false,
						ModTime:   time.Now(),
					}); err != nil {
						return err
					}
				}
				return nil
			})
			if txErr != nil {
				_ = srcNode.Client.Delete(context.Background(), destPath)
				http.Error(w, fmt.Sprintf("Failed to update metadata: %v", txErr), http.StatusInternalServerError)
				return
			}
		}

		st, err := dm.GetStorage(srcNode.ID())
		if err == nil && st != nil {
			srcNode.SetQuota(st.UsedBytes, st.FileCount)
		}

		if destExists {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusCreated)
		}
		return
	}

pipelineFallback:
	// Cross-upstream transfer or native MOVE fallback
	if srcRecord.IsDir {
		// Cross-node directory MOVE / COPY
		subRecs, err := dm.ListSubtreeByStorage(srcNode.ID(), srcPath)
		if err != nil {
			http.Error(w, fmt.Sprintf("List subtree: %v", err), http.StatusInternalServerError)
			return
		}

		var totalDirSize int64
		for _, sr := range subRecs {
			if !sr.IsDir {
				totalDirSize += sr.Size
			}
		}

		var (
			targetNode *cluster.Node
			commit     func(written int64)
			rollback   func()
		)
		if canSameUpstream {
			targetNode = srcNode
			var rErr error
			commit, rollback, rErr = s.cluster.ReserveWriteOnNode(targetNode, totalDirSize, 0)
			if rErr != nil {
				http.Error(w, "Insufficient Storage", http.StatusInsufficientStorage)
				return
			}
		} else if destExists {
			targetNode, _ = s.cluster.GetNode(destRecords[0].StorageID)
			if targetNode != nil {
				var rErr error
				commit, rollback, rErr = s.cluster.ReserveWriteOnNode(targetNode, totalDirSize, 0)
				if rErr != nil {
					http.Error(w, "Insufficient Storage", http.StatusInsufficientStorage)
					return
				}
			}
		} else {
			var rErr error
			targetNode, commit, rollback, rErr = s.cluster.ReserveWrite(totalDirSize)
			if rErr != nil {
				http.Error(w, "Insufficient Storage", http.StatusInsufficientStorage)
				return
			}
		}
		if rollback != nil {
			defer rollback()
		}

		if targetNode == nil || targetNode.Client == nil {
			http.Error(w, "Destination storage node unavailable", http.StatusBadGateway)
			return
		}

		normSrc := data.NormalizePath(srcPath)
		normDest := data.NormalizePath(destPath)

		// Separate into directories and files
		var dirRecs []data.FileRecord
		var fileRecs []data.FileRecord
		for _, sr := range subRecs {
			if sr.IsDir {
				dirRecs = append(dirRecs, sr)
			} else {
				fileRecs = append(fileRecs, sr)
			}
		}

		// Phase 1: Topological directory hierarchy creation (depth-ascending)
		sort.Slice(dirRecs, func(i, j int) bool {
			return len(dirRecs[i].Path) < len(dirRecs[j].Path)
		})

		var createdDestDirs []string
		var migrationErr error
		var createdFiles []string
		var createdFilesMu sync.Mutex

		migrationSuccess := false
		defer func() {
			if !migrationSuccess {
				logger.Warn("DAV", "Directory migration did not complete successfully, executing rollback cleanup on target...")
				createdFilesMu.Lock()
				filesToClean := append([]string(nil), createdFiles...)
				createdFilesMu.Unlock()
				for _, p := range filesToClean {
					cleanCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					_ = targetNode.Client.Delete(cleanCtx, p)
					cancel()
				}
				for i := len(createdDestDirs) - 1; i >= 0; i-- {
					cleanCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					_ = targetNode.Client.Delete(cleanCtx, createdDestDirs[i])
					cancel()
				}
				if rollback != nil {
					rollback()
				}
			}
		}()

		for _, dr := range dirRecs {
			rel := strings.TrimPrefix(data.NormalizePath(dr.Path), normSrc)
			childDest := normDest + rel
			if err := targetNode.Client.EnsureDir(r.Context(), childDest); err != nil {
				migrationErr = fmt.Errorf("create dir %s: %w", childDest, err)
				break
			}
			createdDestDirs = append(createdDestDirs, childDest)
		}
		// Phase 2: Concurrent file transfer pipeline via Worker Pool
		if migrationErr == nil && len(fileRecs) > 0 {
			numWorkers := min(8, len(fileRecs))
			fileCh := make(chan data.FileRecord, len(fileRecs))
			for _, fr := range fileRecs {
				fileCh <- fr
			}
			close(fileCh)

			workerCtx, workerCancel := context.WithCancel(r.Context())
			defer workerCancel()

			var fileWG sync.WaitGroup
			var fileOnce sync.Once

			for range numWorkers {
				fileWG.Add(1)
				go func() {
					defer fileWG.Done()
					for fr := range fileCh {
						if workerCtx.Err() != nil {
							return
						}
						rel := strings.TrimPrefix(data.NormalizePath(fr.Path), normSrc)
						childDest := normDest + rel

						rc, _, getErr := srcNode.Client.Get(workerCtx, fr.Path, nil)
						if getErr != nil {
							fileOnce.Do(func() {
								migrationErr = fmt.Errorf("get %s from source: %w", fr.Path, getErr)
								workerCancel()
							})
							return
						}

						putErr := targetNode.Client.PutRaw(workerCtx, childDest, rc, fr.Size, fr.ContentType())
						_ = rc.Close()
						if putErr != nil {
							_ = targetNode.Client.Delete(context.Background(), childDest)
							fileOnce.Do(func() {
								migrationErr = fmt.Errorf("put %s to dest: %w", childDest, putErr)
								workerCancel()
							})
							return
						}

						createdFilesMu.Lock()
						createdFiles = append(createdFiles, childDest)
						createdFilesMu.Unlock()
					}
				}()
			}
			fileWG.Wait()
		}

		if migrationErr == nil && r.Context().Err() != nil {
			migrationErr = r.Context().Err()
		}
		if migrationErr != nil {
			logger.Error("DAV", "Cross-node directory migration failed: %v, rolling back target...", migrationErr)
			http.Error(w, migrationErr.Error(), http.StatusInternalServerError)
			return
		}

		txErr := dm.WithTx(r.Context(), func(tx *sql.Tx) error {
			if destExists {
				for _, dr := range destRecords {
					if err := dm.DeleteSubtreeByStorageTx(tx, dr.StorageID, destPath); err != nil {
						return err
					}
				}
			}
			for _, sr := range subRecs {
				rel := strings.TrimPrefix(data.NormalizePath(sr.Path), normSrc)
				childDest := normDest + rel
				if err := dm.PutFileTx(tx, data.FileRecord{
					StorageID: targetNode.ID(),
					Path:      childDest,
					Size:      sr.Size,
					IsDir:     sr.IsDir,
					ModTime:   time.Now(),
				}); err != nil {
					return err
				}
			}
			if isMove {
				if err := dm.DeleteSubtreeByStorageTx(tx, srcNode.ID(), srcPath); err != nil {
					return err
				}
			}
			return nil
		})
		if txErr != nil {
			logger.Error("DAV", "Cross-node directory metadata transaction failed: %v, rolling back target...", txErr)
			http.Error(w, fmt.Sprintf("Failed to update metadata: %v", txErr), http.StatusInternalServerError)
			return
		}

		if commit != nil {
			commit(totalDirSize)
		}
		stTarget, err := dm.GetStorage(targetNode.ID())
		if err == nil && stTarget != nil {
			targetNode.SetQuota(stTarget.UsedBytes, stTarget.FileCount)
		}

		if isMove {
			_ = srcNode.Client.Delete(r.Context(), srcPath)
			stSrc, err := dm.GetStorage(srcNode.ID())
			if err == nil && stSrc != nil {
				srcNode.SetQuota(stSrc.UsedBytes, stSrc.FileCount)
			}
		}

		migrationSuccess = true
		if destExists {
			for _, dr := range destRecords {
				if dr.StorageID != targetNode.ID() {
					if dn, ok := s.cluster.GetNode(dr.StorageID); ok && dn != nil {
						if dn.Client != nil {
							_ = dn.Client.Delete(r.Context(), destPath)
						}
						stOld, err := dm.GetStorage(dr.StorageID)
						if err == nil && stOld != nil {
							dn.SetQuota(stOld.UsedBytes, stOld.FileCount)
						}
					}
				}
			}
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusCreated)
		}
		return
	}

	// Single file cross-upstream transfer
	var (
		targetNode *cluster.Node
		commit     func(written int64)
		rollback   func()
	)
	if destExists {
		targetNode, _ = s.cluster.GetNode(destRecords[0].StorageID)
		if targetNode != nil {
			var err error
			commit, rollback, err = s.cluster.ReserveWriteOnNode(targetNode, srcRecord.Size, destRecords[0].Size)
			if err != nil {
				http.Error(w, "Insufficient Storage", http.StatusInsufficientStorage)
				return
			}
		}
	} else {
		var err error
		targetNode, commit, rollback, err = s.cluster.ReserveWrite(srcRecord.Size)
		if err != nil {
			http.Error(w, "Insufficient Storage", http.StatusInsufficientStorage)
			return
		}
	}
	if rollback != nil {
		defer rollback()
	}

	if targetNode == nil || targetNode.Client == nil {
		http.Error(w, "Destination storage node unavailable", http.StatusBadGateway)
		return
	}
	rc, _, err := srcNode.Client.Get(r.Context(), srcPath, nil)
	if err != nil {
		if rollback != nil {
			rollback()
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rc.Close()

	if err := targetNode.Client.Put(r.Context(), destPath, rc, srcRecord.Size, srcRecord.ContentType()); err != nil {
		_ = targetNode.Client.Delete(context.Background(), destPath)
		if rollback != nil {
			rollback()
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	txErr := dm.WithTx(r.Context(), func(tx *sql.Tx) error {
		if destExists {
			for _, dr := range destRecords {
				if err := dm.DeleteSubtreeByStorageTx(tx, dr.StorageID, destPath); err != nil {
					return err
				}
			}
		}
		if err := dm.PutFileTx(tx, data.FileRecord{
			StorageID: targetNode.ID(),
			Path:      destPath,
			Size:      srcRecord.Size,
			IsDir:     false,
			ModTime:   time.Now(),
		}); err != nil {
			return err
		}
		if isMove {
			if err := dm.DeleteFileTx(tx, srcNode.ID(), srcPath); err != nil {
				return err
			}
		}
		return nil
	})
	if txErr != nil {
		_ = targetNode.Client.Delete(context.Background(), destPath)
		if rollback != nil {
			rollback()
		}
		http.Error(w, fmt.Sprintf("Failed to update metadata: %v", txErr), http.StatusInternalServerError)
		return
	}

	if commit != nil {
		commit(srcRecord.Size)
	}
	stTarget, err := dm.GetStorage(targetNode.ID())
	if err == nil && stTarget != nil {
		targetNode.SetQuota(stTarget.UsedBytes, stTarget.FileCount)
	}

	if isMove {
		_ = srcNode.Client.Delete(r.Context(), srcPath)
		stSrc, err := dm.GetStorage(srcNode.ID())
		if err == nil && stSrc != nil {
			srcNode.SetQuota(stSrc.UsedBytes, stSrc.FileCount)
		}
	}

	if destExists {
		for _, dr := range destRecords {
			if dr.StorageID != targetNode.ID() {
				if dn, ok := s.cluster.GetNode(dr.StorageID); ok && dn != nil {
					if dn.Client != nil {
						_ = dn.Client.Delete(r.Context(), destPath)
					}
					stOld, err := dm.GetStorage(dr.StorageID)
					if err == nil && stOld != nil {
						dn.SetQuota(stOld.UsedBytes, stOld.FileCount)
					}
				}
			}
		}
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.WriteHeader(http.StatusCreated)
	}
}
func (s *Server) handleLock(w http.ResponseWriter, r *http.Request) {
	davPath := s.resolveDAVPath(r.URL.Path)
	depth := r.Header.Get("Depth")
	if depth == "" {
		depth = "0"
	}
	timeout := parseTimeoutHeader(r.Header.Get("Timeout"))

	bodyBytes, _ := io.ReadAll(r.Body)

	// Lock Refresh
	if len(bodyBytes) == 0 {
		tokens := extractTokens(r.Header.Get("If"), r.Header.Get("Lock-Token"))
		if len(tokens) == 0 {
			http.Error(w, "Missing lock token for refresh", http.StatusBadRequest)
			return
		}

		lock, err := s.lockMgr.Refresh(davPath, tokens[0], timeout)
		if err != nil {
			http.Error(w, err.Error(), http.StatusPreconditionFailed)
			return
		}

		w.Header().Set("Lock-Token", "<"+lock.Token+">")
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, renderLockResponse(lock))
		return
	}

	// New Lock
	var li lockInfoXML
	_ = xml.Unmarshal(bodyBytes, &li)
	owner := li.Owner.Href
	if owner == "" {
		owner = strings.TrimSpace(li.Owner.Inner)
	}
	if owner == "" {
		owner = "unknown"
	}

	lock, err := s.lockMgr.Lock(davPath, owner, depth, timeout)
	if err != nil {
		http.Error(w, "Locked", http.StatusLocked)
		return
	}

	exists := false
	if dm := s.cluster.DataManager(); dm != nil {
		recs, err := dm.GetFilesByPath(davPath)
		if err == nil && len(recs) > 0 {
			exists = true
		}
	}

	statusCode := http.StatusOK
	if !exists {
		statusCode = http.StatusCreated
	}

	w.Header().Set("Lock-Token", "<"+lock.Token+">")
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(statusCode)
	_, _ = io.WriteString(w, renderLockResponse(lock))
}

func (s *Server) handleUnlock(w http.ResponseWriter, r *http.Request) {
	davPath := s.resolveDAVPath(r.URL.Path)
	tokens := extractTokens(r.Header.Get("Lock-Token"), r.Header.Get("If"))
	if len(tokens) == 0 {
		http.Error(w, "Missing Lock-Token header", http.StatusBadRequest)
		return
	}

	if err := s.lockMgr.Unlock(davPath, tokens[0]); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
