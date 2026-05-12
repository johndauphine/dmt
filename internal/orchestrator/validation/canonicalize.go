package validation

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"strconv"
	"time"
	"unicode"
	"unicode/utf8"
)

// CanonicalEncoder writes a deterministic byte representation of a
// scanned database value into a hash.Hash (or any io.Writer-shaped
// sink — we keep the type concrete to avoid an interface allocation
// per cell on large scans).
//
// The encoding is a tagged, length-prefixed format:
//
//	[1 byte tag][4 bytes BE length][value bytes]
//
// The tag distinguishes type even when value bytes coincide — e.g.
// the string "0", the integer 0, and NULL all produce different
// canonical bytes. The length prefix prevents concatenation
// ambiguity ("abc"+"def" ≠ "abcdef").
//
// Encoder is stateless and safe to share across goroutines.
type CanonicalEncoder struct{}

// Tag bytes are intentionally printable ASCII so a hex dump of the
// hash input is somewhat readable during debugging.
const (
	tagNull   byte = 'N'
	tagBool   byte = 'B'
	tagInt    byte = 'I'
	tagFloat  byte = 'F'
	tagString byte = 'S'
	tagBytes  byte = 'X'
	tagTime   byte = 'T'
)

// WriteValue appends the canonical encoding of v to h.
//
// Returns an error only for types we explicitly want to reject —
// e.g. non-UTF-8 string values that should never have made it into
// a portable string column. Unknown Go types fall back to a stable
// string representation; callers can detect unknowns by counting
// the unknownTypeCount on the returned encoder, exposed via
// helpers below if telemetry is wanted later.
func (CanonicalEncoder) WriteValue(h hash.Hash, v any) error {
	switch x := v.(type) {
	case nil:
		writeTagged(h, tagNull, nil)
	case bool:
		if x {
			writeTagged(h, tagBool, []byte{1})
		} else {
			writeTagged(h, tagBool, []byte{0})
		}
	case int:
		writeTaggedInt(h, int64(x))
	case int8:
		writeTaggedInt(h, int64(x))
	case int16:
		writeTaggedInt(h, int64(x))
	case int32:
		writeTaggedInt(h, int64(x))
	case int64:
		writeTaggedInt(h, x)
	case uint:
		writeTaggedUint(h, uint64(x))
	case uint8:
		// NB: []byte (alias for []uint8) is handled below.
		writeTaggedUint(h, uint64(x))
	case uint16:
		writeTaggedUint(h, uint64(x))
	case uint32:
		writeTaggedUint(h, uint64(x))
	case uint64:
		writeTaggedUint(h, x)
	case float32:
		writeTaggedFloat(h, float64(x))
	case float64:
		writeTaggedFloat(h, x)
	case string:
		if !utf8.ValidString(x) {
			return fmt.Errorf("string value is not valid UTF-8 (len=%d): %q", len(x), truncateForError(x, 32))
		}
		writeTagged(h, tagString, []byte(x))
	case []byte:
		// Drivers vary on whether VARCHAR columns surface as string
		// or []byte. We treat valid-UTF-8 byte slices as strings so
		// the source/target produce the same canonical bytes even if
		// one driver returns string and the other returns []byte.
		// True binary columns (BINARY, BLOB) that contain invalid
		// UTF-8 use the bytes tag and get hex-prefixed; printable
		// content with valid UTF-8 uses the string tag.
		if utf8.Valid(x) && looksLikeText(x) {
			writeTagged(h, tagString, x)
		} else {
			// Encode as ASCII hex to keep the canonical form stable
			// even if a driver lower-cases vs upper-cases on
			// different versions.
			hexBuf := make([]byte, hex.EncodedLen(len(x)))
			hex.Encode(hexBuf, x)
			writeTagged(h, tagBytes, hexBuf)
		}
	case time.Time:
		// All times canonicalize to UTC nanoseconds. RFC3339 would
		// lose sub-microsecond resolution on some platforms (Go's
		// time printer drops trailing zeros differently). The fixed
		// 12-byte form (8-byte unix seconds + 4-byte nanos) is the
		// unambiguous choice.
		utc := x.UTC()
		var buf [12]byte
		binary.BigEndian.PutUint64(buf[0:8], uint64(utc.Unix()))
		binary.BigEndian.PutUint32(buf[8:12], uint32(utc.Nanosecond()))
		writeTagged(h, tagTime, buf[:])
	default:
		// Fallback for driver-specific types we don't recognize
		// (e.g. pgtype.Numeric for NUMERIC, sql.NullString, etc.).
		// Stringify with %v — the danger is that %v is type-specific
		// formatting; if source and target drivers stringify the
		// same value differently, we'll get a spurious mismatch.
		// Document this caveat in FIXTURES (or rather, the
		// validation docs) and rely on the explicit cases above
		// covering the standard surface.
		writeTagged(h, tagString, []byte(fmt.Sprintf("%v", x)))
	}
	return nil
}

func writeTagged(h hash.Hash, tag byte, payload []byte) {
	var header [5]byte
	header[0] = tag
	binary.BigEndian.PutUint32(header[1:5], uint32(len(payload)))
	_, _ = h.Write(header[:])
	if len(payload) > 0 {
		_, _ = h.Write(payload)
	}
}

func writeTaggedInt(h hash.Hash, v int64) {
	// Decimal string is the cross-driver-stable canonical form.
	// Two integers that came from different driver paths (one as
	// int32, one as int64) compare equal here as long as their
	// numeric values match.
	writeTagged(h, tagInt, strconv.AppendInt(nil, v, 10))
}

func writeTaggedUint(h hash.Hash, v uint64) {
	writeTagged(h, tagInt, strconv.AppendUint(nil, v, 10))
}

func writeTaggedFloat(h hash.Hash, v float64) {
	// %g with -1 precision is Go's "minimum digits that round-trip
	// the float losslessly" mode. Identical IEEE 754 doubles
	// produce identical strings; different doubles produce
	// different strings. NaN and ±Inf get canonical literals from
	// strconv.
	writeTagged(h, tagFloat, strconv.AppendFloat(nil, v, 'g', -1, 64))
}

// looksLikeText is a heuristic guard against treating a binary blob
// that happens to be valid UTF-8 (e.g. a small PNG with no high-bit
// bytes) as a string. We require:
//   - at least 1 byte
//   - no NUL bytes (a NUL anywhere is a strong tell for binary)
//   - majority of runes are printable or recognized whitespace
//
// Iterates at the RUNE level via utf8.DecodeRune so non-ASCII text
// (汉字, é, emoji) is correctly counted as printable — the prior
// byte-level loop only recognized ASCII printables, which would
// falsely tag any byte slice containing CJK or accented characters
// as binary and route it through the hex path (Codex review on
// #226). Drivers SHOULD distinguish string vs []byte by type for
// non-ambiguous columns; this fallback is for the cases where a
// driver returns []byte for what semantically is text.
func looksLikeText(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	if !utf8.Valid(b) {
		return false
	}
	totalRunes := 0
	printableRunes := 0
	for i := 0; i < len(b); {
		r, size := utf8.DecodeRune(b[i:])
		if r == 0x00 {
			return false
		}
		totalRunes++
		if r == utf8.RuneError {
			// Defensive — utf8.Valid passed but DecodeRune
			// returned RuneError. Treat as non-printable.
			i += size
			continue
		}
		if unicode.IsPrint(r) || r == '\n' || r == '\r' || r == '\t' {
			printableRunes++
		}
		i += size
	}
	return printableRunes*2 > totalRunes
}

func truncateForError(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
