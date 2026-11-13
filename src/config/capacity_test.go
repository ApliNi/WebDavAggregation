package config

import (
	"testing"
)

func TestParseCapacity(t *testing.T) {
	tests := []struct {
		input         string
		expectedMode  CapacityMode
		expectedBytes uint64
		expectedRatio float64
		hasErr        bool
	}{
		{"1024B", CapacityModeLimit, 1024, 0, false},
		{"1K", CapacityModeLimit, 1024, 0, false},
		{"1KB", CapacityModeLimit, 1024, 0, false},
		{"1KiB", CapacityModeLimit, 1024, 0, false},
		{"100M", CapacityModeLimit, 100 * 1024 * 1024, 0, false},
		{"100MB", CapacityModeLimit, 100 * 1024 * 1024, 0, false},
		{"500G", CapacityModeLimit, 500 * 1024 * 1024 * 1024, 0, false},
		{"500GB", CapacityModeLimit, 500 * 1024 * 1024 * 1024, 0, false},
		{"2T", CapacityModeLimit, 2 * 1024 * 1024 * 1024 * 1024, 0, false},
		{"2TB", CapacityModeLimit, 2 * 1024 * 1024 * 1024 * 1024, 0, false},
		{"2TiB", CapacityModeLimit, 2 * 1024 * 1024 * 1024 * 1024, 0, false},
		{"1.5G", CapacityModeLimit, uint64(1.5 * 1024 * 1024 * 1024), 0, false},
		{" 2T ", CapacityModeLimit, 2 * 1024 * 1024 * 1024 * 1024, 0, false},
		{"2 t", CapacityModeLimit, 2 * 1024 * 1024 * 1024 * 1024, 0, false},
		{"1P", CapacityModeLimit, 1024 * 1024 * 1024 * 1024 * 1024, 0, false},
		// Ratio mode tests
		{"2.00", CapacityModeRatio, 0, 2.00, false},
		{"1.00", CapacityModeRatio, 0, 1.00, false},
		{"2", CapacityModeRatio, 0, 2.0, false},
		{"0.5", CapacityModeRatio, 0, 0.5, false},
		{" 1.5 ", CapacityModeRatio, 0, 1.5, false},
		// Error cases
		{"", CapacityModeLimit, 0, 0, true},
		{"0", CapacityModeLimit, 0, 0, true},
		{"0B", CapacityModeLimit, 0, 0, true},
		{"-2T", CapacityModeLimit, 0, 0, true},
		{"-1.00", CapacityModeLimit, 0, 0, true},
		{"invalid", CapacityModeLimit, 0, 0, true},
		{"100XYZ", CapacityModeLimit, 0, 0, true},
	}

	for _, tt := range tests {
		pc, err := ParseCapacity(tt.input)
		if (err != nil) != tt.hasErr {
			t.Errorf("ParseCapacity(%q) error = %v, wantErr %v", tt.input, err, tt.hasErr)
			continue
		}
		if !tt.hasErr {
			if pc.Mode != tt.expectedMode {
				t.Errorf("ParseCapacity(%q) mode = %v, want %v", tt.input, pc.Mode, tt.expectedMode)
			}
			if tt.expectedMode == CapacityModeLimit && pc.Bytes != tt.expectedBytes {
				t.Errorf("ParseCapacity(%q) bytes = %d, want %d", tt.input, pc.Bytes, tt.expectedBytes)
			}
			if tt.expectedMode == CapacityModeRatio && pc.Ratio != tt.expectedRatio {
				t.Errorf("ParseCapacity(%q) ratio = %f, want %f", tt.input, pc.Ratio, tt.expectedRatio)
			}
		}
	}
}

func TestFormatCapacity(t *testing.T) {
	tests := []struct {
		input    uint64
		expected string
	}{
		{0, "0B"},
		{500, "500B"},
		{1024, "1K"},
		{1536, "1.5K"},
		{100 * 1024 * 1024, "100M"},
		{500 * 1024 * 1024 * 1024, "500G"},
		{2 * 1024 * 1024 * 1024 * 1024, "2T"},
	}

	for _, tt := range tests {
		got := FormatCapacity(tt.input)
		if got != tt.expected {
			t.Errorf("FormatCapacity(%d) = %s, want %s", tt.input, got, tt.expected)
		}
	}
}

func TestParseAndFormatRoundTrip(t *testing.T) {
	inputs := []string{"1024B", "1K", "100M", "500G", "2T"}
	for _, in := range inputs {
		pc, err := ParseCapacity(in)
		if err != nil {
			t.Fatalf("ParseCapacity(%q) failed: %v", in, err)
		}
		formatted := FormatCapacity(pc.Bytes)
		parsedBack, err := ParseCapacity(formatted)
		if err != nil {
			t.Fatalf("ParseCapacity(%q) failed: %v", formatted, err)
		}
		if parsedBack.Bytes != pc.Bytes {
			t.Errorf("Roundtrip mismatch for %q: original bytes %d, roundtrip bytes %d", in, pc.Bytes, parsedBack.Bytes)
		}
	}
}
