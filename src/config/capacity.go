package config

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	_         = iota
	kb uint64 = 1 << (10 * iota)
	mb
	gb
	tb
	pb
	eb
)

// CapacityMode indicates whether a capacity represents an absolute limit or a proportion ratio.
type CapacityMode int

const (
	// CapacityModeLimit represents an absolute capacity with an explicit unit (e.g. 200G, 2T, 1024B).
	CapacityModeLimit CapacityMode = iota
	// CapacityModeRatio represents a unit-less capacity ratio (e.g. 2.00, 1.00) without hard limit.
	CapacityModeRatio
)

// ParsedCapacity stores the parsed representation of an upstream capacity configuration.
type ParsedCapacity struct {
	Raw   string
	Mode  CapacityMode
	Bytes uint64  // Set when Mode == CapacityModeLimit
	Ratio float64 // Set when Mode == CapacityModeRatio
}

// String returns a human-readable representation of the capacity.
func (pc ParsedCapacity) String() string {
	if pc.Mode == CapacityModeRatio {
		return fmt.Sprintf("%.2f (ratio)", pc.Ratio)
	}
	return FormatCapacity(pc.Bytes)
}

// ParseCapacity converts a capacity string into a ParsedCapacity.
// Supported formats:
// 1. Ratio mode (unit-less numbers > 0, e.g. "2.00", "1.00", "1.5", "2").
// 2. Absolute limit mode (numbers with units, e.g. "200G", "2T", "100M", "1024B").
// Supported units (case-insensitive): B, K/KB/KiB, M/MB/MiB, G/GB/GiB, T/TB/TiB, P/PB/PiB, E/EB/EiB.
func ParseCapacity(s string) (ParsedCapacity, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return ParsedCapacity{}, errors.New("capacity string cannot be empty")
	}

	runes := []rune(trimmed)
	i := 0
	if runes[0] == '+' {
		i = 1
	} else if runes[0] == '-' {
		return ParsedCapacity{}, fmt.Errorf("capacity cannot be negative: %s", trimmed)
	}

	var numPart strings.Builder
	for ; i < len(runes); i++ {
		r := runes[i]
		if (r >= '0' && r <= '9') || r == '.' {
			numPart.WriteRune(r)
		} else {
			break
		}
	}

	numStr := numPart.String()
	if numStr == "" {
		return ParsedCapacity{}, fmt.Errorf("invalid capacity format: %q", trimmed)
	}

	val, err := strconv.ParseFloat(numStr, 64)
	if err != nil || val <= 0 || math.IsNaN(val) || math.IsInf(val, 0) {
		return ParsedCapacity{}, fmt.Errorf("invalid positive numeric value %q in capacity %q", numStr, trimmed)
	}

	unitStr := strings.ToUpper(strings.TrimSpace(string(runes[i:])))
	if unitStr == "" {
		// Pure number without unit represents capacity ratio mode
		return ParsedCapacity{
			Raw:   trimmed,
			Mode:  CapacityModeRatio,
			Ratio: val,
		}, nil
	}

	var multiplier uint64
	switch unitStr {
	case "B":
		multiplier = 1
	case "K", "KB", "KIB":
		multiplier = kb
	case "M", "MB", "MIB":
		multiplier = mb
	case "G", "GB", "GIB":
		multiplier = gb
	case "T", "TB", "TIB":
		multiplier = tb
	case "P", "PB", "PIB":
		multiplier = pb
	case "E", "EB", "EIB":
		multiplier = eb
	default:
		return ParsedCapacity{}, fmt.Errorf("unknown capacity unit %q in %q", unitStr, trimmed)
	}

	total := val * float64(multiplier)
	if total > float64(math.MaxUint64) {
		return ParsedCapacity{}, fmt.Errorf("capacity overflow: %s", trimmed)
	}

	rounded := uint64(math.Round(total))
	if rounded == 0 {
		return ParsedCapacity{}, fmt.Errorf("capacity cannot be zero: %s", trimmed)
	}

	return ParsedCapacity{
		Raw:   trimmed,
		Mode:  CapacityModeLimit,
		Bytes: rounded,
	}, nil
}

// FormatCapacity formats a byte count into a human-readable capacity string (e.g. 2T, 500G, 100M, 1024B).
func FormatCapacity(bytes uint64) string {
	if bytes == 0 {
		return "0B"
	}

	type unitDef struct {
		unit string
		val  uint64
	}

	units := []unitDef{
		{"E", eb},
		{"P", pb},
		{"T", tb},
		{"G", gb},
		{"M", mb},
		{"K", kb},
	}

	for _, u := range units {
		if bytes >= u.val {
			val := float64(bytes) / float64(u.val)
			if math.Mod(val, 1.0) == 0 {
				return fmt.Sprintf("%.0f%s", val, u.unit)
			}
			s := fmt.Sprintf("%.2f", val)
			s = strings.TrimRight(s, "0")
			s = strings.TrimRight(s, ".")
			return s + u.unit
		}
	}

	return fmt.Sprintf("%dB", bytes)
}
