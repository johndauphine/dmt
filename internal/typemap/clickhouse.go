// ClickHouse dialect mapper (#507).
//
// ClickHouse is columnar with a strict-but-distinct type system:
// nullability is a type wrapper (Nullable(T)) rather than a column
// constraint, LowCardinality(T) is a storage optimization wrapper that
// is transparent for mapping purposes, and there is no VARCHAR length —
// String is unbounded (FixedString(N) is the only sized string and is
// padding-semantics CHAR, not VARCHAR).
package typemap

import (
	"fmt"
	"strings"
)

// clickhouseToCanonical normalizes a ClickHouse column to canonical
// form. Wrappers are stripped first: Nullable(...) (nullability is
// carried on ColumnInfo.IsNullable by the introspection layer, which
// derives it from the same wrapper) and LowCardinality(...) (storage
// detail only).
func clickhouseToCanonical(col ColumnInfo) CanonicalType {
	udt := strings.TrimSpace(col.UDTName)
	// Wrappers nest in either order (LowCardinality(Nullable(String))
	// and Nullable(LowCardinality(String)) are both valid) — strip
	// until a fixed point (codex).
	for {
		next := unwrapCHType(unwrapCHType(udt, "Nullable"), "LowCardinality")
		if next == udt {
			break
		}
		udt = next
	}
	upper := strings.ToUpper(udt)

	// Parameterized families first.
	switch {
	case strings.HasPrefix(upper, "FIXEDSTRING"):
		if n := chTypeParam(udt); n != nil {
			return CanonicalType{Kind: KindChar, Length: n}
		}
		return CanonicalType{Kind: KindChar}
	case strings.HasPrefix(upper, "DECIMAL"):
		// Decimal(P, S); Decimal32/64/128/256(S) imply the precision.
		p, s := chDecimalParams(udt)
		return CanonicalType{Kind: KindDecimal, Precision: p, Scale: s}
	case strings.HasPrefix(upper, "DATETIME64"), strings.HasPrefix(upper, "DATETIME"):
		return CanonicalType{Kind: KindTimestamp}
	case strings.HasPrefix(upper, "ENUM8"), strings.HasPrefix(upper, "ENUM16"):
		return CanonicalType{Kind: KindEnum}
	case strings.HasPrefix(upper, "ARRAY"):
		return CanonicalType{Kind: KindArray}
	}

	switch upper {
	case "BOOL", "BOOLEAN":
		return CanonicalType{Kind: KindBoolean}
	case "INT8", "INT16":
		return CanonicalType{Kind: KindSmallInt}
	case "UINT8", "UINT16", "INT32":
		// UInt16 max (65535) exceeds SmallInt; promote to Integer.
		return CanonicalType{Kind: KindInteger}
	case "UINT32", "INT64":
		// UInt32 max exceeds Integer; promote to BigInt.
		return CanonicalType{Kind: KindBigInt}
	case "UINT64", "INT128", "INT256", "UINT128", "UINT256":
		// No canonical kind holds these without loss — keep the native
		// name (UNWRAPPED: nullability is re-applied from IsNullable at
		// DDL emission, so returning the wrapped name would double-wrap)
		// so the AI fallback or a same-engine target decides.
		return CanonicalType{Kind: KindRaw, TypeName: udt}
	case "FLOAT32":
		return CanonicalType{Kind: KindFloat}
	case "FLOAT64":
		return CanonicalType{Kind: KindDouble}
	case "STRING":
		return CanonicalType{Kind: KindText}
	case "DATE", "DATE32":
		return CanonicalType{Kind: KindDate}
	case "UUID":
		return CanonicalType{Kind: KindUUID}
	case "JSON", "OBJECT('JSON')":
		return CanonicalType{Kind: KindJSON}
	case "IPV4", "IPV6":
		return CanonicalType{Kind: KindText}
	default:
		return CanonicalType{Kind: KindRaw, TypeName: udt}
	}
}

// unwrapCHType strips one "Wrapper(...)" layer, case-insensitively.
func unwrapCHType(t, wrapper string) string {
	if len(t) > len(wrapper)+2 &&
		strings.EqualFold(t[:len(wrapper)], wrapper) &&
		t[len(wrapper)] == '(' && strings.HasSuffix(t, ")") {
		return strings.TrimSpace(t[len(wrapper)+1 : len(t)-1])
	}
	return t
}

// chTypeParam parses the single integer parameter of "Type(N)".
func chTypeParam(t string) *int {
	open := strings.Index(t, "(")
	closeIdx := strings.LastIndex(t, ")")
	if open < 0 || closeIdx <= open {
		return nil
	}
	var n int
	if _, err := fmt.Sscanf(strings.TrimSpace(t[open+1:closeIdx]), "%d", &n); err != nil {
		return nil
	}
	return &n
}

// chDecimalParams parses Decimal(P, S) and the sized aliases
// Decimal32(S)/Decimal64(S)/Decimal128(S)/Decimal256(S), whose
// precisions are fixed at 9/18/38/76.
func chDecimalParams(t string) (precision, scale *int) {
	upper := strings.ToUpper(t)
	implied := map[string]int{"DECIMAL32": 9, "DECIMAL64": 18, "DECIMAL128": 38, "DECIMAL256": 76}
	for name, p := range implied {
		if strings.HasPrefix(upper, name+"(") {
			pCopy := p
			return &pCopy, chTypeParam(t)
		}
	}
	open := strings.Index(t, "(")
	closeIdx := strings.LastIndex(t, ")")
	if open < 0 || closeIdx <= open {
		return nil, nil
	}
	parts := strings.Split(t[open+1:closeIdx], ",")
	if len(parts) >= 1 {
		var p int
		if _, err := fmt.Sscanf(strings.TrimSpace(parts[0]), "%d", &p); err == nil {
			precision = &p
		}
	}
	if len(parts) >= 2 {
		var s int
		if _, err := fmt.Sscanf(strings.TrimSpace(parts[1]), "%d", &s); err == nil {
			scale = &s
		}
	}
	return precision, scale
}

// clickhouseFromCanonical emits a canonical type as ClickHouse DDL.
// Nullability is NOT emitted here — the DDL generator wraps the
// fragment in Nullable(...) for nullable columns (ClickHouse expresses
// nullability as a type wrapper, not a column constraint).
func clickhouseFromCanonical(ct CanonicalType) DdlType {
	switch ct.Kind {
	case KindBoolean:
		return exactDDL("Bool")
	case KindSmallInt:
		return exactDDL("Int16")
	case KindInteger:
		return exactDDL("Int32")
	case KindBigInt:
		return exactDDL("Int64")
	case KindFloat:
		return exactDDL("Float32")
	case KindDouble:
		return exactDDL("Float64")
	case KindDecimal:
		switch {
		case ct.Precision != nil && ct.Scale != nil:
			return exactDDL(fmt.Sprintf("Decimal(%d, %d)", *ct.Precision, *ct.Scale))
		case ct.Precision != nil:
			return exactDDL(fmt.Sprintf("Decimal(%d, 0)", *ct.Precision))
		default:
			// ClickHouse requires explicit precision; 38,9 is a safe
			// general-purpose default (Decimal128 range).
			return approxDDL("Decimal(38, 9)", "source decimal had no declared precision")
		}
	case KindVarchar, KindText:
		// No length-bounded VARCHAR in ClickHouse — String is unbounded.
		return exactDDL("String")
	case KindChar:
		if ct.Length != nil {
			return exactDDL(fmt.Sprintf("FixedString(%d)", *ct.Length))
		}
		return exactDDL("FixedString(1)")
	case KindBytes:
		return exactDDL("String")
	case KindDate:
		// Date32 covers 1900–2299; plain Date starts at 1970.
		return exactDDL("Date32")
	case KindTime:
		return approxDDL("String", "No TIME type in ClickHouse; stored as String")
	case KindTimestamp:
		return exactDDL("DateTime64(3)")
	case KindInterval:
		return approxDDL("String", "INTERVAL is not a storable column type in ClickHouse")
	case KindUUID:
		return exactDDL("UUID")
	case KindJSON, KindJSONB:
		// The native JSON column type is still maturing; String is the
		// portable choice and queryable via JSON functions.
		return approxDDL("String", "JSON stored as String (native JSON type not assumed)")
	case KindEnum:
		return approxDDL("String", "source ENUM stored as String (value set not preserved)")
	case KindArray:
		return approxDDL("String", "source array stored as String")
	case KindRaw:
		return exactDDL(ct.TypeName)
	default:
		return exactDDL("String")
	}
}
