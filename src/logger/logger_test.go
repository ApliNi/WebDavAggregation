package logger

import (
	"bytes"
	"regexp"
	"strings"
	"sync"
	"testing"
)

const standardRegexPattern = `^\[\d{2}:\d{2}:\d{2} (INFO|WARN|ERROR|DEBUG)\]: \[\w+\] .*`

func TestRegexFormatValidation(t *testing.T) {
	re := regexp.MustCompile(standardRegexPattern)

	cases := []struct {
		level Level
		typ   string
		msg   string
	}{
		{DEBUG, "Main", "debug message"},
		{INFO, "Main", "starting server"},
		{WARN, "Main", "warning message"},
		{ERROR, "Main", "error message"},
		{INFO, "HTTP", "GET /webdav 200 OK"},
		{DEBUG, "Cluster", "node healthy"},
		{WARN, "Lock", "lock contention detected"},
		{ERROR, "Server", "bind failed: port in use"},
	}

	for _, tc := range cases {
		var buf bytes.Buffer
		l := New(&buf, DEBUG)
		l.Log(tc.level, tc.typ, "%s", tc.msg)

		output := buf.String()
		lines := strings.Split(strings.TrimRight(output, "\r\n"), "\n")
		if len(lines) == 0 {
			t.Fatalf("expected output for %v %s", tc.level, tc.typ)
		}

		firstLine := lines[0]
		if !re.MatchString(firstLine) {
			t.Errorf("line %q does not match standard regex %q", firstLine, standardRegexPattern)
		}
	}
}

func TestLogLevelFiltering(t *testing.T) {
	tests := []struct {
		name          string
		level         Level
		expectedCount int
	}{
		{"DEBUG level allows all", DEBUG, 4},
		{"INFO level filters DEBUG", INFO, 3},
		{"WARN level filters DEBUG and INFO", WARN, 2},
		{"ERROR level only allows ERROR", ERROR, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			l := New(&buf, tt.level)

			l.Debug("HTTP", "debug msg")
			l.Info("HTTP", "info msg")
			l.Warn("HTTP", "warn msg")
			l.Error("HTTP", "error msg")

			output := strings.TrimSpace(buf.String())
			var count int
			if len(output) > 0 {
				count = len(strings.Split(output, "\n"))
			}

			if count != tt.expectedCount {
				t.Fatalf("expected %d lines for level %v, got %d:\n%s", tt.expectedCount, tt.level, count, output)
			}
		})
	}
}

func TestDynamicSetLevelAndSetOutput(t *testing.T) {
	var buf1 bytes.Buffer
	l := New(&buf1, WARN)

	if l.GetLevel() != WARN {
		t.Fatalf("expected initial level WARN, got %v", l.GetLevel())
	}
	if l.GetOutput() != &buf1 {
		t.Fatalf("expected initial output &buf1")
	}

	l.Info("Main", "should not appear")
	if buf1.Len() != 0 {
		t.Fatalf("expected empty buffer at WARN level, got %q", buf1.String())
	}

	l.SetLevel(INFO)
	if l.GetLevel() != INFO {
		t.Fatalf("expected updated level INFO, got %v", l.GetLevel())
	}
	l.Info("Main", "should appear in buf1")
	if !strings.Contains(buf1.String(), "should appear in buf1") {
		t.Fatalf("expected message in buf1, got %q", buf1.String())
	}

	var buf2 bytes.Buffer
	l.SetOutput(&buf2)
	if l.GetOutput() != &buf2 {
		t.Fatalf("expected output updated to &buf2")
	}

	l.Info("Main", "should appear in buf2")
	if !strings.Contains(buf2.String(), "should appear in buf2") {
		t.Fatalf("expected message in buf2, got %q", buf2.String())
	}
	if strings.Contains(buf1.String(), "should appear in buf2") {
		t.Fatalf("buf1 should not receive new logs after output change")
	}
}

func TestMultilineMessageFormat(t *testing.T) {
	re := regexp.MustCompile(standardRegexPattern)

	var buf bytes.Buffer
	l := New(&buf, INFO)

	msg := "Upstream[1] http://127.0.0.1:5243:\n  - capacity: 1.98T"
	l.Info("Main", "%s", msg)

	output := strings.ReplaceAll(buf.String(), "\r\n", "\n")
	expectedPattern := `^\[\d{2}:\d{2}:\d{2} INFO\]: \[Main\] Upstream\[1\] http://127\.0\.0\.1:5243:\n  - capacity: 1\.98T\n$`
	matched, err := regexp.MatchString(expectedPattern, output)
	if err != nil {
		t.Fatalf("regex error: %v", err)
	}
	if !matched {
		t.Fatalf("multiline log did not match expected pattern, got:\n%s", output)
	}

	lines := strings.Split(strings.TrimRight(output, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	if !re.MatchString(lines[0]) {
		t.Errorf("first line does not match standard regex: %s", lines[0])
	}
	if lines[1] != "  - capacity: 1.98T" {
		t.Errorf("second line expected '  - capacity: 1.98T', got %q", lines[1])
	}
}

func TestBoundLogger(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, DEBUG)

	mainLog := l.WithType("Main")
	if mainLog.Type() != "Main" || mainLog.Logger() != l {
		t.Fatalf("bound logger metadata mismatch")
	}

	mainLog.Debug("bound debug: %d", 1)
	mainLog.Info("bound info: %d", 2)
	mainLog.Warn("bound warn: %d", 3)
	mainLog.Error("bound error: %d", 4)

	httpLog := l.WithModule("HTTP")
	httpLog.Debugf("bound debugf: %d", 5)
	httpLog.Infof("bound infof: %d", 6)
	httpLog.Warnf("bound warnf: %d", 7)
	httpLog.Errorf("bound errorf: %d", 8)

	out := buf.String()
	for _, expected := range []string{
		"DEBUG]: [Main] bound debug: 1",
		"INFO]: [Main] bound info: 2",
		"WARN]: [Main] bound warn: 3",
		"ERROR]: [Main] bound error: 4",
		"DEBUG]: [HTTP] bound debugf: 5",
		"INFO]: [HTTP] bound infof: 6",
		"WARN]: [HTTP] bound warnf: 7",
		"ERROR]: [HTTP] bound errorf: 8",
	} {
		if !strings.Contains(out, expected) {
			t.Errorf("missing expected entry %q in output:\n%s", expected, out)
		}
	}
}

func TestDefaultLogger(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(DEBUG)

	if GetLevel() != DEBUG {
		t.Fatalf("expected default logger level DEBUG, got %v", GetLevel())
	}
	if GetOutput() != &buf {
		t.Fatalf("expected default logger output to match buffer")
	}

	Debug("HTTP", "test %s", "debug")
	Debugf("HTTP", "testf %s", "debug")
	Info("Main", "test %s", "info")
	Infof("Main", "testf %s", "info")
	Warn("Cluster", "test %s", "warn")
	Warnf("Cluster", "testf %s", "warn")
	Error("Server", "test %s", "error")
	Errorf("Server", "testf %s", "error")
	Log(INFO, "Direct", "test %s", "direct")
	Logf(WARN, "Direct", "testf %s", "directf")

	bound := WithType("Bound")
	bound.Info("bound default info")
	boundMod := WithModule("BoundMod")
	boundMod.Warn("bound mod warn")

	out := buf.String()
	for _, expected := range []string{
		"DEBUG]: [HTTP] test debug",
		"DEBUG]: [HTTP] testf debug",
		"INFO]: [Main] test info",
		"INFO]: [Main] testf info",
		"WARN]: [Cluster] test warn",
		"WARN]: [Cluster] testf warn",
		"ERROR]: [Server] test error",
		"ERROR]: [Server] testf error",
		"INFO]: [Direct] test direct",
		"WARN]: [Direct] testf directf",
		"INFO]: [Bound] bound default info",
		"WARN]: [BoundMod] bound mod warn",
	} {
		if !strings.Contains(out, expected) {
			t.Errorf("missing expected log entry %q in:\n%s", expected, out)
		}
	}
}

func TestParseLevel(t *testing.T) {
	cases := []struct {
		input    string
		expected Level
		err      bool
	}{
		{"DEBUG", DEBUG, false},
		{"debug", DEBUG, false},
		{"INFO", INFO, false},
		{"info", INFO, false},
		{"WARN", WARN, false},
		{"warning", WARN, false},
		{"WARNING", WARN, false},
		{"ERROR", ERROR, false},
		{"error", ERROR, false},
		{"invalid", INFO, true},
		{"", INFO, true},
	}

	for _, tc := range cases {
		lvl, err := ParseLevel(tc.input)
		if tc.err && err == nil {
			t.Errorf("expected error for input %q, got nil", tc.input)
		}
		if !tc.err && err != nil {
			t.Errorf("unexpected error for input %q: %v", tc.input, err)
		}
		if !tc.err && lvl != tc.expected {
			t.Errorf("input %q: expected level %v, got %v", tc.input, tc.expected, lvl)
		}
	}
}

func TestLevelString(t *testing.T) {
	if DEBUG.String() != "DEBUG" {
		t.Errorf("expected DEBUG, got %s", DEBUG.String())
	}
	if INFO.String() != "INFO" {
		t.Errorf("expected INFO, got %s", INFO.String())
	}
	if WARN.String() != "WARN" {
		t.Errorf("expected WARN, got %s", WARN.String())
	}
	if ERROR.String() != "ERROR" {
		t.Errorf("expected ERROR, got %s", ERROR.String())
	}
	if Level(99).String() != "UNKNOWN" {
		t.Errorf("expected UNKNOWN, got %s", Level(99).String())
	}
}

func TestIsLevelEnabled(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, WARN)

	if l.IsLevelEnabled(DEBUG) {
		t.Errorf("DEBUG should not be enabled when level is WARN")
	}
	if l.IsLevelEnabled(INFO) {
		t.Errorf("INFO should not be enabled when level is WARN")
	}
	if !l.IsLevelEnabled(WARN) {
		t.Errorf("WARN should be enabled when level is WARN")
	}
	if !l.IsLevelEnabled(ERROR) {
		t.Errorf("ERROR should be enabled when level is WARN")
	}
}

func TestNilWriterHandling(t *testing.T) {
	l := New(nil, INFO)
	l.Info("Main", "this should not panic")
	l.SetOutput(nil)
	l.Error("Main", "this should also not panic")
}

func TestEmptyTypeFallback(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, INFO)
	l.Info("", "message with empty type")

	re := regexp.MustCompile(standardRegexPattern)
	line := strings.TrimSpace(buf.String())
	if !re.MatchString(line) {
		t.Errorf("line with empty type %q should fallback and match regex %q", line, standardRegexPattern)
	}
	if !strings.Contains(line, "[App]") {
		t.Errorf("expected default type [App], got %q", line)
	}
}

func TestConcurrentLoggingAndDynamicReconfiguration(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, INFO)
	re := regexp.MustCompile(standardRegexPattern)

	var wg sync.WaitGroup
	workers := 20
	iterations := 100

	wg.Add(workers)
	for i := range workers {
		go func(workerID int) {
			defer wg.Done()
			for j := range iterations {
				switch j % 4 {
				case 0:
					l.Debugf("Worker", "debug w=%d iter=%d", workerID, j)
				case 1:
					l.Infof("Worker", "info w=%d iter=%d", workerID, j)
				case 2:
					l.Warnf("Worker", "warn w=%d iter=%d", workerID, j)
				case 3:
					l.Errorf("Worker", "error w=%d iter=%d", workerID, j)
				}
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		levels := []Level{DEBUG, INFO, WARN, ERROR}
		for i := range iterations {
			l.SetLevel(levels[i%len(levels)])
			_ = l.GetLevel()
			_ = l.GetOutput()
		}
	}()

	wg.Wait()

	output := strings.ReplaceAll(buf.String(), "\r\n", "\n")
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) == 0 {
		t.Fatal("expected logged lines during concurrent test")
	}

	for idx, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if !re.MatchString(line) {
			t.Fatalf("line %d %q violates regex %s", idx, line, standardRegexPattern)
		}
	}
}
