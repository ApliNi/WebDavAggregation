package server

import (
	"encoding/xml"
	"net/http"
	"strings"
	"testing"
)

func TestParseProppatch_RFC4918Example(t *testing.T) {
	// RFC 4918 Section 9.2.2 Example
	reqXML := `<?xml version="1.0" encoding="utf-8" ?>
<D:propertyupdate xmlns:D="DAV:"
xmlns:Z="http://ns.example.com/standards/z39.50/">
  <D:set>
    <D:prop>
      <Z:authors>
        <Z:Author>Jim Whitehead</Z:Author>
        <Z:Author>Roy Fielding</Z:Author>
      </Z:authors>
    </D:prop>
  </D:set>
  <D:remove>
    <D:prop>
      <Z:Copyright-Owner/>
    </D:prop>
  </D:remove>
</D:propertyupdate>`

	req, err := ParseProppatchBytes([]byte(reqXML))
	if err != nil {
		t.Fatalf("ParseProppatchBytes failed: %v", err)
	}

	if len(req.Operations) != 2 {
		t.Fatalf("expected 2 operations, got %d", len(req.Operations))
	}

	// First operation: SET
	op0 := req.Operations[0]
	if op0.Type != ProppatchSet {
		t.Errorf("expected op0 to be ProppatchSet, got %v", op0.Type)
	}
	if len(op0.Properties) != 1 {
		t.Fatalf("expected 1 property in op0, got %d", len(op0.Properties))
	}
	prop0 := op0.Properties[0]
	if prop0.XMLName.Local != "authors" || prop0.XMLName.Space != "http://ns.example.com/standards/z39.50/" {
		t.Errorf("unexpected XMLName for op0 property: %+v", prop0.XMLName)
	}
	if !strings.Contains(prop0.Value, "Jim Whitehead") || !strings.Contains(prop0.Value, "Roy Fielding") {
		t.Errorf("unexpected property value: %q", prop0.Value)
	}

	// Second operation: REMOVE
	op1 := req.Operations[1]
	if op1.Type != ProppatchRemove {
		t.Errorf("expected op1 to be ProppatchRemove, got %v", op1.Type)
	}
	if len(op1.Properties) != 1 {
		t.Fatalf("expected 1 property in op1, got %d", len(op1.Properties))
	}
	prop1 := op1.Properties[0]
	if prop1.XMLName.Local != "Copyright-Owner" || prop1.XMLName.Space != "http://ns.example.com/standards/z39.50/" {
		t.Errorf("unexpected XMLName for op1 property: %+v", prop1.XMLName)
	}

	// Test helper methods
	allProps := req.AllProperties()
	if len(allProps) != 2 {
		t.Errorf("expected 2 properties in AllProperties(), got %d", len(allProps))
	}
	names := req.AllPropertyNames()
	if len(names) != 2 {
		t.Errorf("expected 2 names in AllPropertyNames(), got %d", len(names))
	}
	if !req.HasProperty("http://ns.example.com/standards/z39.50/", "authors") {
		t.Errorf("expected HasProperty for authors to be true")
	}
	if !req.HasProperty("http://ns.example.com/standards/z39.50/", "Copyright-Owner") {
		t.Errorf("expected HasProperty for Copyright-Owner to be true")
	}
	if req.HasProperty("DAV:", "displayname") {
		t.Errorf("expected HasProperty for displayname to be false")
	}
}

func TestParseProppatch_MultiplePropsInSinglePropContainer(t *testing.T) {
	reqXML := `<?xml version="1.0" encoding="utf-8" ?>
<D:propertyupdate xmlns:D="DAV:" xmlns:custom="http://example.com/ns">
  <D:set>
    <D:prop>
      <D:displayname>New Display Name</D:displayname>
      <custom:tag attr="val">custom-value</custom:tag>
    </D:prop>
  </D:set>
</D:propertyupdate>`

	req, err := ParseProppatchRequest(strings.NewReader(reqXML))
	if err != nil {
		t.Fatalf("ParseProppatchRequest failed: %v", err)
	}

	if len(req.Operations) != 1 {
		t.Fatalf("expected 1 operation, got %d", len(req.Operations))
	}
	op := req.Operations[0]
	if op.Type != ProppatchSet {
		t.Errorf("expected ProppatchSet, got %v", op.Type)
	}
	if len(op.Properties) != 2 {
		t.Fatalf("expected 2 properties, got %d", len(op.Properties))
	}

	p1 := op.Properties[0]
	if p1.XMLName.Local != "displayname" || p1.XMLName.Space != "DAV:" {
		t.Errorf("unexpected p1: %+v", p1.XMLName)
	}
	if p1.Value != "New Display Name" {
		t.Errorf("unexpected p1 value: %q", p1.Value)
	}

	p2 := op.Properties[1]
	if p2.XMLName.Local != "tag" || p2.XMLName.Space != "http://example.com/ns" {
		t.Errorf("unexpected p2: %+v", p2.XMLName)
	}
	if p2.Value != "custom-value" {
		t.Errorf("unexpected p2 value: %q", p2.Value)
	}
	if len(p2.Attrs) == 0 {
		t.Errorf("expected p2 attributes to be preserved")
	}
}

func TestParseProppatch_ValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		xml     string
		wantErr string
	}{
		{
			name:    "empty body",
			xml:     "",
			wantErr: "failed to read XML root",
		},
		{
			name:    "invalid root element",
			xml:     `<D:propfind xmlns:D="DAV:"><D:allprop/></D:propfind>`,
			wantErr: "unexpected root element <propfind>",
		},
		{
			name:    "invalid root namespace",
			xml:     `<propertyupdate xmlns="http://wrong.namespace/"/>`,
			wantErr: "DAV: namespace",
		},
		{
			name: "unknown element inside propertyupdate",
			xml: `<D:propertyupdate xmlns:D="DAV:">
  <D:unknown/>
</D:propertyupdate>`,
			wantErr: "unexpected element <unknown>",
		},
		{
			name: "unknown element inside set",
			xml: `<D:propertyupdate xmlns:D="DAV:">
  <D:set>
    <D:foo/>
  </D:set>
</D:propertyupdate>`,
			wantErr: "unexpected element <foo>",
		},
		{
			name: "unexpected non-whitespace text inside set",
			xml: `<D:propertyupdate xmlns:D="DAV:">
  <D:set>
    some random text
    <D:prop><D:displayname>A</D:displayname></D:prop>
  </D:set>
</D:propertyupdate>`,
			wantErr: "unexpected text content",
		},
		{
			name: "malformed XML",
			xml: `<D:propertyupdate xmlns:D="DAV:">
  <D:set>
    <D:prop><D:displayname>unclosed
</D:propertyupdate>`,
			wantErr: "syntax error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseProppatchBytes([]byte(tt.xml))
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}

	// Nil reader test
	if _, err := ParseProppatchRequest(nil); err == nil {
		t.Errorf("expected error for nil reader")
	}
}

func TestRenderProppatchMultiStatus_Empty(t *testing.T) {
	out := RenderProppatchMultiStatus("/folder/file.txt", nil)
	outStr := string(out)

	if !strings.Contains(outStr, "<D:multistatus xmlns:D=\"DAV:\">") {
		t.Errorf("missing multistatus header: %s", outStr)
	}
	if !strings.Contains(outStr, "<D:href>/dav/folder/file.txt</D:href>") {
		t.Errorf("unexpected href in empty response: %s", outStr)
	}
	if !strings.Contains(outStr, "<D:prop/>") {
		t.Errorf("expected <D:prop/> in empty response: %s", outStr)
	}
	if !strings.Contains(outStr, "<D:status>HTTP/1.1 200 OK</D:status>") {
		t.Errorf("expected 200 OK status: %s", outStr)
	}

	// Verify it parses as valid XML
	var parsed struct {
		XMLName xml.Name `xml:"multistatus"`
	}
	if err := xml.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("failed to unmarshal generated XML: %v", err)
	}
}

func TestRenderProppatchMultiStatus_SuccessAndFailure(t *testing.T) {
	results := []ProppatchPropResult{
		{
			Prop:       xml.Name{Space: "DAV:", Local: "displayname"},
			StatusCode: http.StatusOK,
		},
		{
			Prop:       xml.Name{Space: "http://example.com/ns", Local: "custom-prop"},
			StatusCode: http.StatusOK,
		},
		{
			Prop:        xml.Name{Space: "DAV:", Local: "getcontentlength"},
			StatusCode:  http.StatusForbidden,
			Description: "Cannot modify protected property",
		},
		{
			Prop:       xml.Name{Space: "http://example.com/ns", Local: "another-prop"},
			StatusCode: http.StatusFailedDependency,
		},
	}

	out := RenderProppatchMultiStatus("/test/dir/", results)
	outStr := string(out)

	// Directory trailing slash preserved
	if !strings.Contains(outStr, "<D:href>/dav/test/dir/</D:href>") {
		t.Errorf("missing directory href: %s", outStr)
	}

	// Check grouping into propstats
	if !strings.Contains(outStr, "<D:displayname/>") {
		t.Errorf("missing D:displayname: %s", outStr)
	}
	if !strings.Contains(outStr, `<custom-prop xmlns="http://example.com/ns"/>`) {
		t.Errorf("missing custom-prop namespace: %s", outStr)
	}
	if !strings.Contains(outStr, "<D:status>HTTP/1.1 200 OK</D:status>") {
		t.Errorf("missing 200 OK status: %s", outStr)
	}
	if !strings.Contains(outStr, "<D:status>HTTP/1.1 403 Forbidden</D:status>") {
		t.Errorf("missing 403 Forbidden status: %s", outStr)
	}
	if !strings.Contains(outStr, "<D:responsedescription>Cannot modify protected property</D:responsedescription>") {
		t.Errorf("missing responsedescription: %s", outStr)
	}
	if !strings.Contains(outStr, "<D:status>HTTP/1.1 424 Failed Dependency</D:status>") {
		t.Errorf("missing 424 Failed Dependency status: %s", outStr)
	}

	// Verify RFC 4918 structure: in each propstat, <prop> MUST precede <status>
	propIdx := strings.Index(outStr, "<D:prop>")
	statusIdx := strings.Index(outStr, "<D:status>")
	if propIdx == -1 || statusIdx == -1 || propIdx >= statusIdx {
		t.Errorf("<D:prop> must appear before <D:status> in propstat: prop=%d, status=%d", propIdx, statusIdx)
	}

	// Validate valid XML structure
	var parsed struct {
		XMLName  xml.Name `xml:"multistatus"`
		Response struct {
			Href     string `xml:"href"`
			Propstat []struct {
				Status string `xml:"status"`
			} `xml:"propstat"`
		} `xml:"response"`
	}
	if err := xml.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("failed to unmarshal generated XML: %v", err)
	}
	if len(parsed.Response.Propstat) != 3 {
		t.Errorf("expected 3 propstats grouped by status, got %d", len(parsed.Response.Propstat))
	}
}

func TestRenderProppatchSuccess(t *testing.T) {
	props := []ProppatchProperty{
		{XMLName: xml.Name{Space: "DAV:", Local: "displayname"}, Value: "hello"},
		{XMLName: xml.Name{Space: "http://example.com/ns", Local: "tag"}, Value: "world"},
	}

	out := RenderProppatchSuccess("/doc.pdf", props)
	outStr := string(out)

	if !strings.Contains(outStr, "<D:href>/dav/doc.pdf</D:href>") {
		t.Errorf("unexpected href: %s", outStr)
	}
	if !strings.Contains(outStr, "<D:displayname/>") {
		t.Errorf("missing D:displayname: %s", outStr)
	}
	if !strings.Contains(outStr, `<tag xmlns="http://example.com/ns"/>`) {
		t.Errorf("missing tag with namespace: %s", outStr)
	}
	if !strings.Contains(outStr, "<D:status>HTTP/1.1 200 OK</D:status>") {
		t.Errorf("missing 200 OK status: %s", outStr)
	}
}

func TestRenderProppatchSuccessNames(t *testing.T) {
	names := []xml.Name{
		{Space: "DAV:", Local: "displayname"},
		{Space: "DAV:", Local: "D:custom"}, // test stripping redundant D: prefix
	}

	out := RenderProppatchSuccessNames("/file.txt", names)
	outStr := string(out)

	if !strings.Contains(outStr, "<D:displayname/>") {
		t.Errorf("missing D:displayname: %s", outStr)
	}
	if !strings.Contains(outStr, "<D:custom/>") {
		t.Errorf("expected D: prefix to be stripped: %s", outStr)
	}
}

func TestFailProppatchDependency(t *testing.T) {
	allProps := []xml.Name{
		{Space: "DAV:", Local: "displayname"},
		{Space: "DAV:", Local: "getcontentlength"},
		{Space: "http://example.com/ns", Local: "tag"},
	}

	failedProp := xml.Name{Space: "DAV:", Local: "getcontentlength"}
	results := FailProppatchDependency(allProps, failedProp, http.StatusForbidden, "read-only property")

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// displayname -> 424
	if results[0].StatusCode != http.StatusFailedDependency {
		t.Errorf("expected 424 for displayname, got %d", results[0].StatusCode)
	}

	// getcontentlength -> 403
	if results[1].StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 for getcontentlength, got %d", results[1].StatusCode)
	}
	if results[1].Description != "read-only property" {
		t.Errorf("expected description 'read-only property', got %q", results[1].Description)
	}

	// tag -> 424
	if results[2].StatusCode != http.StatusFailedDependency {
		t.Errorf("expected 424 for tag, got %d", results[2].StatusCode)
	}
}

func TestIsProtectedLiveProperty(t *testing.T) {
	protected := []string{
		"creationdate",
		"getcontentlength",
		"getetag",
		"getlastmodified",
		"lockdiscovery",
		"resourcetype",
		"supportedlock",
		"quota-available-bytes",
		"quota-used-bytes",
	}

	for _, p := range protected {
		if !IsProtectedLiveProperty(xml.Name{Space: "DAV:", Local: p}) {
			t.Errorf("expected %s to be protected in DAV:", p)
		}
		if !IsProtectedLiveProperty(xml.Name{Space: "", Local: p}) {
			t.Errorf("expected %s to be protected with empty space", p)
		}
		// In custom namespace, should not be protected
		if IsProtectedLiveProperty(xml.Name{Space: "http://custom.com", Local: p}) {
			t.Errorf("expected %s in custom namespace NOT to be protected", p)
		}
	}

	unprotected := []string{
		"displayname",
		"getcontenttype",
		"customproperty",
	}

	for _, p := range unprotected {
		if IsProtectedLiveProperty(xml.Name{Space: "DAV:", Local: p}) {
			t.Errorf("expected %s NOT to be protected", p)
		}
	}
}
