package server

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"webdav-agg/src/client"
)

type lockInfoXML struct {
	XMLName   xml.Name     `xml:"lockinfo"`
	LockScope lockScopeXML `xml:"lockscope"`
	LockType  lockTypeXML  `xml:"locktype"`
	Owner     lockOwnerXML `xml:"owner"`
}

type lockScopeXML struct {
	Exclusive *struct{} `xml:"exclusive"`
	Shared    *struct{} `xml:"shared"`
}

type lockTypeXML struct {
	Write *struct{} `xml:"write"`
}

type lockOwnerXML struct {
	Href  string `xml:"href"`
	Inner string `xml:",innerxml"`
}

func escapeXML(s string) string {
	var buf bytes.Buffer
	_ = xml.EscapeText(&buf, []byte(s))
	return buf.String()
}

func formatHref(davPath string, isDir bool) string {
	clean := path.Clean("/" + davPath)
	fullPath := "/dav" + clean
	if clean == "/" {
		fullPath = "/dav/"
	}
	escaped := (&url.URL{Path: fullPath}).EscapedPath()
	if isDir && !strings.HasSuffix(escaped, "/") {
		escaped += "/"
	}
	return escaped
}

func renderLockDiscovery(lock *ActiveLock) string {
	if lock == nil {
		return "<D:lockdiscovery/>"
	}
	timeoutSec := int64(lock.Timeout.Seconds())
	if timeoutSec <= 0 {
		timeoutSec = 300
	}
	return fmt.Sprintf(`<D:lockdiscovery>
  <D:activelock>
    <D:locktype><D:write/></D:locktype>
    <D:lockscope><D:exclusive/></D:lockscope>
    <D:depth>%s</D:depth>
    <D:owner><D:href>%s</D:href></D:owner>
    <D:timeout>Second-%d</D:timeout>
    <D:locktoken>
      <D:href>%s</D:href>
    </D:locktoken>
    <D:lockroot>
      <D:href>%s</D:href>
    </D:lockroot>
  </D:activelock>
</D:lockdiscovery>`,
		escapeXML(lock.Depth),
		escapeXML(lock.Owner),
		timeoutSec,
		escapeXML(lock.Token),
		escapeXML(formatHref(lock.Path, false)),
	)
}

func renderLockResponse(lock *ActiveLock) string {
	timeoutSec := int64(lock.Timeout.Seconds())
	if timeoutSec <= 0 {
		timeoutSec = 300
	}
	return fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<D:prop xmlns:D="DAV:">
  <D:lockdiscovery>
    <D:activelock>
      <D:locktype><D:write/></D:locktype>
      <D:lockscope><D:exclusive/></D:lockscope>
      <D:depth>%s</D:depth>
      <D:owner><D:href>%s</D:href></D:owner>
      <D:timeout>Second-%d</D:timeout>
      <D:locktoken>
        <D:href>%s</D:href>
      </D:locktoken>
      <D:lockroot>
        <D:href>%s</D:href>
      </D:lockroot>
    </D:activelock>
  </D:lockdiscovery>
</D:prop>`,
		escapeXML(lock.Depth),
		escapeXML(lock.Owner),
		timeoutSec,
		escapeXML(lock.Token),
		escapeXML(formatHref(lock.Path, false)),
	)
}

func renderPropfindResponse(self client.FileInfo, children []client.FileInfo, availBytes, usedBytes int64, lm *LockManager) []byte {
	var buf bytes.Buffer
	buf.WriteString("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n")
	buf.WriteString("<D:multistatus xmlns:D=\"DAV:\">\n")

	// Render self
	renderPropfindEntry(&buf, self, availBytes, usedBytes, lm)

	// Render children
	for _, child := range children {
		renderPropfindEntry(&buf, child, availBytes, usedBytes, lm)
	}

	buf.WriteString("</D:multistatus>\n")
	return buf.Bytes()
}

func renderPropfindEntry(buf *bytes.Buffer, item client.FileInfo, availBytes, usedBytes int64, lm *LockManager) {
	href := formatHref(item.Path, item.IsDir)
	displayName := item.Name
	if displayName == "" {
		if item.Path == "/" {
			displayName = ""
		} else {
			displayName = path.Base(item.Path)
		}
	}

	modTime := item.ModTime
	if modTime.IsZero() {
		modTime = time.Now()
	}
	modTimeStr := modTime.UTC().Format(http.TimeFormat)

	buf.WriteString("  <D:response>\n")
	buf.WriteString(fmt.Sprintf("    <D:href>%s</D:href>\n", escapeXML(href)))
	buf.WriteString("    <D:propstat>\n")
	buf.WriteString("      <D:prop>\n")
	if displayName != "" {
		buf.WriteString(fmt.Sprintf("        <D:displayname>%s</D:displayname>\n", escapeXML(displayName)))
	}
	buf.WriteString(fmt.Sprintf("        <D:getlastmodified>%s</D:getlastmodified>\n", modTimeStr))

	if item.IsDir {
		buf.WriteString("        <D:resourcetype><D:collection/></D:resourcetype>\n")
		// RFC 4331 Quota properties on collections
		if availBytes < 0 {
			availBytes = 0
		}
		if usedBytes < 0 {
			usedBytes = 0
		}
		buf.WriteString(fmt.Sprintf("        <D:quota-available-bytes>%d</D:quota-available-bytes>\n", availBytes))
		buf.WriteString(fmt.Sprintf("        <D:quota-used-bytes>%d</D:quota-used-bytes>\n", usedBytes))
	} else {
		buf.WriteString("        <D:resourcetype/>\n")
		buf.WriteString(fmt.Sprintf("        <D:getcontentlength>%d</D:getcontentlength>\n", item.Size))
		ct := item.ContentType
		if ct == "" {
			ct = "application/octet-stream"
		}
		buf.WriteString(fmt.Sprintf("        <D:getcontenttype>%s</D:getcontenttype>\n", escapeXML(ct)))
		etag := item.ETag
		if etag == "" {
			etag = fmt.Sprintf(`%x-%x`, modTime.UnixNano(), item.Size)
		}
		cleanETag := strings.Trim(etag, `"`)
		buf.WriteString(fmt.Sprintf("        <D:getetag>\"%s\"</D:getetag>\n", escapeXML(cleanETag)))
	}

	if lm != nil {
		if lock := lm.GetLock(item.Path); lock != nil {
			buf.WriteString("        " + renderLockDiscovery(lock) + "\n")
		}
	}

	buf.WriteString("      </D:prop>\n")
	buf.WriteString("      <D:status>HTTP/1.1 200 OK</D:status>\n")
	buf.WriteString("    </D:propstat>\n")
	buf.WriteString("  </D:response>\n")
}
// ProppatchOperationType specifies whether a property should be set or removed.
type ProppatchOperationType int

const (
	ProppatchSet ProppatchOperationType = iota
	ProppatchRemove
)

func (t ProppatchOperationType) String() string {
	if t == ProppatchSet {
		return "set"
	}
	return "remove"
}

// ProppatchProperty represents a single property parsed from a PROPPATCH request.
type ProppatchProperty struct {
	XMLName xml.Name
	Attrs   []xml.Attr
	Value   string // inner XML or textual content
}

// ProppatchOperation represents one update action (<set> or <remove>) containing properties.
type ProppatchOperation struct {
	Type       ProppatchOperationType
	Properties []ProppatchProperty
}

// ProppatchRequest represents a parsed RFC 4918 PROPPATCH request body (<propertyupdate>).
type ProppatchRequest struct {
	Operations []ProppatchOperation
}

// AllProperties returns all properties targeted across all operations in this request.
func (r *ProppatchRequest) AllProperties() []ProppatchProperty {
	var props []ProppatchProperty
	for _, op := range r.Operations {
		props = append(props, op.Properties...)
	}
	return props
}

// AllPropertyNames returns the xml.Name of each property targeted in this request.
func (r *ProppatchRequest) AllPropertyNames() []xml.Name {
	var names []xml.Name
	for _, op := range r.Operations {
		for _, p := range op.Properties {
			names = append(names, p.XMLName)
		}
	}
	return names
}

// HasProperty checks whether a property matching the given namespace and local name exists in the request.
func (r *ProppatchRequest) HasProperty(space, local string) bool {
	for _, op := range r.Operations {
		for _, p := range op.Properties {
			if (space == "" || p.XMLName.Space == space) && p.XMLName.Local == local {
				return true
			}
		}
	}
	return false
}

type rawPropertyElement struct {
	XMLName  xml.Name
	InnerXML string `xml:",innerxml"`
}

// ParseProppatchRequest parses an RFC 4918 compliant PROPPATCH XML request body from an io.Reader.
func ParseProppatchRequest(r io.Reader) (*ProppatchRequest, error) {
	if r == nil {
		return nil, fmt.Errorf("nil request reader")
	}

	decoder := xml.NewDecoder(r)

	// Locate root element
	var rootStart xml.StartElement
	for {
		tok, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("failed to read XML root: %w", err)
		}
		if start, ok := tok.(xml.StartElement); ok {
			rootStart = start
			break
		}
	}

	if rootStart.Name.Local != "propertyupdate" || (rootStart.Name.Space != "" && rootStart.Name.Space != "DAV:") {
		return nil, fmt.Errorf("unexpected root element <%s> in namespace '%s', expected <propertyupdate> in DAV: namespace",
			rootStart.Name.Local, rootStart.Name.Space)
	}

	req := &ProppatchRequest{}

	for {
		tok, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("XML syntax error: %w", err)
		}

		switch t := tok.(type) {
		case xml.StartElement:
			if t.Name.Space != "" && t.Name.Space != "DAV:" {
				return nil, fmt.Errorf("unexpected element <%s> in namespace '%s', expected DAV: namespace", t.Name.Local, t.Name.Space)
			}
			var opType ProppatchOperationType
			switch t.Name.Local {
			case "set":
				opType = ProppatchSet
			case "remove":
				opType = ProppatchRemove
			default:
				return nil, fmt.Errorf("unexpected element <%s> inside <propertyupdate>, expected <set> or <remove>", t.Name.Local)
			}

			props, err := parsePropContainer(decoder, t)
			if err != nil {
				return nil, err
			}
			req.Operations = append(req.Operations, ProppatchOperation{
				Type:       opType,
				Properties: props,
			})

		case xml.EndElement:
			if t.Name == rootStart.Name {
				return req, nil
			}
		}
	}
	return req, nil
}

// ParseProppatchBytes parses an RFC 4918 compliant PROPPATCH XML request body from byte slice.
func ParseProppatchBytes(body []byte) (*ProppatchRequest, error) {
	return ParseProppatchRequest(bytes.NewReader(body))
}

func parsePropContainer(decoder *xml.Decoder, opStart xml.StartElement) ([]ProppatchProperty, error) {
	var props []ProppatchProperty
	for {
		tok, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("malformed XML inside <%s>: %w", opStart.Name.Local, err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			if t.Name.Local != "prop" || (t.Name.Space != "" && t.Name.Space != "DAV:") {
				return nil, fmt.Errorf("unexpected element <%s> inside <%s>, expected <prop>", t.Name.Local, opStart.Name.Local)
			}
			p, err := parseProps(decoder, t)
			if err != nil {
				return nil, err
			}
			props = append(props, p...)
		case xml.CharData:
			if len(bytes.TrimSpace(t)) > 0 {
				return nil, fmt.Errorf("unexpected text content inside <%s>", opStart.Name.Local)
			}
		case xml.EndElement:
			if t.Name == opStart.Name {
				return props, nil
			}
		}
	}
}

func parseProps(decoder *xml.Decoder, propStart xml.StartElement) ([]ProppatchProperty, error) {
	var props []ProppatchProperty
	for {
		tok, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("malformed XML inside <prop>: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			var item rawPropertyElement
			if err := decoder.DecodeElement(&item, &t); err != nil {
				return nil, fmt.Errorf("failed to decode property <%s>: %w", t.Name.Local, err)
			}
			props = append(props, ProppatchProperty{
				XMLName: item.XMLName,
				Attrs:   t.Attr,
				Value:   item.InnerXML,
			})
		case xml.EndElement:
			if t.Name == propStart.Name {
				return props, nil
			}
		}
	}
}

// ProppatchPropResult represents the execution result for an individual property in a PROPPATCH operation.
type ProppatchPropResult struct {
	Prop        xml.Name
	StatusCode  int
	StatusText  string // optional custom status text; defaults to http.StatusText(StatusCode)
	Description string // optional D:responsedescription
}

type propstatGroup struct {
	statusCode  int
	statusText  string
	description string
	props       []xml.Name
}

// RenderProppatchMultiStatus generates a RFC 4918 compliant 207 Multi-Status XML response
// grouping properties by status code into <D:propstat> elements.
func RenderProppatchMultiStatus(davPath string, results []ProppatchPropResult) []byte {
	isDir := strings.HasSuffix(davPath, "/")
	href := formatHref(davPath, isDir)
	var buf bytes.Buffer
	buf.WriteString("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n")
	buf.WriteString("<D:multistatus xmlns:D=\"DAV:\">\n")
	buf.WriteString("  <D:response>\n")
	buf.WriteString(fmt.Sprintf("    <D:href>%s</D:href>\n", escapeXML(href)))

	if len(results) == 0 {
		buf.WriteString("    <D:propstat>\n")
		buf.WriteString("      <D:prop/>\n")
		buf.WriteString("      <D:status>HTTP/1.1 200 OK</D:status>\n")
		buf.WriteString("    </D:propstat>\n")
	} else {
		var groups []*propstatGroup
		groupMap := make(map[string]*propstatGroup)

		for _, res := range results {
			statusText := res.StatusText
			if statusText == "" {
				statusText = http.StatusText(res.StatusCode)
			}
			key := fmt.Sprintf("%d|%s|%s", res.StatusCode, statusText, res.Description)
			g, ok := groupMap[key]
			if !ok {
				g = &propstatGroup{
					statusCode:  res.StatusCode,
					statusText:  statusText,
					description: res.Description,
				}
				groups = append(groups, g)
				groupMap[key] = g
			}
			if res.Prop.Local != "" {
				g.props = append(g.props, res.Prop)
			}
		}

		for _, g := range groups {
			buf.WriteString("    <D:propstat>\n")
			if len(g.props) == 0 {
				buf.WriteString("      <D:prop/>\n")
			} else {
				buf.WriteString("      <D:prop>\n")
				for _, p := range g.props {
					local := p.Local
					if idx := strings.Index(local, ":"); idx != -1 {
						if local[:idx] == "D" && (p.Space == "" || p.Space == "DAV:") {
							local = local[idx+1:]
						}
					}
					if p.Space == "" || p.Space == "DAV:" {
						buf.WriteString(fmt.Sprintf("        <D:%s/>\n", escapeXML(local)))
					} else {
						buf.WriteString(fmt.Sprintf("        <%s xmlns=\"%s\"/>\n", escapeXML(local), escapeXML(p.Space)))
					}
				}
				buf.WriteString("      </D:prop>\n")
			}
			buf.WriteString(fmt.Sprintf("      <D:status>HTTP/1.1 %d %s</D:status>\n", g.statusCode, escapeXML(g.statusText)))
			if g.description != "" {
				buf.WriteString(fmt.Sprintf("      <D:responsedescription>%s</D:responsedescription>\n", escapeXML(g.description)))
			}
			buf.WriteString("    </D:propstat>\n")
		}
	}

	buf.WriteString("  </D:response>\n")
	buf.WriteString("</D:multistatus>")
	return buf.Bytes()
}

// RenderProppatchSuccess generates a 207 Multi-Status response where all properties succeeded with 200 OK.
func RenderProppatchSuccess(davPath string, props []ProppatchProperty) []byte {
	results := make([]ProppatchPropResult, len(props))
	for i, p := range props {
		results[i] = ProppatchPropResult{
			Prop:       p.XMLName,
			StatusCode: http.StatusOK,
		}
	}
	return RenderProppatchMultiStatus(davPath, results)
}

// RenderProppatchSuccessNames generates a 207 Multi-Status response where all given property names succeeded with 200 OK.
func RenderProppatchSuccessNames(davPath string, names []xml.Name) []byte {
	results := make([]ProppatchPropResult, len(names))
	for i, name := range names {
		results[i] = ProppatchPropResult{
			Prop:       name,
			StatusCode: http.StatusOK,
		}
	}
	return RenderProppatchMultiStatus(davPath, results)
}

// FailProppatchDependency creates a slice of ProppatchPropResult for an atomic failure:
// failedProp receives failedStatus, and all other properties receive 424 Failed Dependency per RFC 4918 §9.2.
func FailProppatchDependency(allProps []xml.Name, failedProp xml.Name, failedStatus int, description string) []ProppatchPropResult {
	results := make([]ProppatchPropResult, len(allProps))
	for i, p := range allProps {
		if p.Local == failedProp.Local && (failedProp.Space == "" || p.Space == failedProp.Space) {
			results[i] = ProppatchPropResult{
				Prop:        p,
				StatusCode:  failedStatus,
				Description: description,
			}
		} else {
			results[i] = ProppatchPropResult{
				Prop:       p,
				StatusCode: http.StatusFailedDependency, // 424
			}
		}
	}
	return results
}

// IsProtectedLiveProperty reports whether the given property is a RFC 4918 protected live property
// that cannot be modified or removed by PROPPATCH (e.g. getcontentlength, resourcetype, getetag, etc.).
func IsProtectedLiveProperty(name xml.Name) bool {
	if name.Space != "" && name.Space != "DAV:" {
		return false
	}
	switch name.Local {
	case "creationdate", "getcontentlength", "getetag", "getlastmodified",
		"lockdiscovery", "resourcetype", "supportedlock",
		"quota-available-bytes", "quota-used-bytes":
		return true
	default:
		return false
	}
}

func renderProppatchResponse(davPath string) []byte {
	return RenderProppatchMultiStatus(davPath, nil)
}

// DeleteFailure records a failed deletion attempt on a specific path/node.
type DeleteFailure struct {
	Path       string
	StatusCode int
	StatusText string
}

// renderDeleteMultiStatus generates a RFC 4918 207 Multi-Status XML response for partial delete failures.
func renderDeleteMultiStatus(failures []DeleteFailure) []byte {
	var buf bytes.Buffer
	buf.WriteString("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n")
	buf.WriteString("<D:multistatus xmlns:D=\"DAV:\">\n")
	for _, f := range failures {
		href := formatHref(f.Path, false)
		statusText := f.StatusText
		if statusText == "" {
			statusText = http.StatusText(f.StatusCode)
		}
		buf.WriteString("  <D:response>\n")
		buf.WriteString(fmt.Sprintf("    <D:href>%s</D:href>\n", escapeXML(href)))
		buf.WriteString(fmt.Sprintf("    <D:status>HTTP/1.1 %d %s</D:status>\n", f.StatusCode, escapeXML(statusText)))
		buf.WriteString("  </D:response>\n")
	}
	buf.WriteString("</D:multistatus>")
	return buf.Bytes()
}
