package tuning

import (
	"math"
	"math/big"
	"strings"
)

// TableMemoryStat is the runtime-only schema evidence used by the
// cardinality-aware working-set model. It is intentionally independent of
// persisted tuning history: historical regression features retain their
// existing scalar widths.
type TableMemoryStat struct {
	Name        string
	RowCount    int64
	AvgRowBytes int64
}

// MemoryProfile is an immutable, runtime-only collection of every in-scope
// table's cardinality and average width. Its fields remain private so callers
// cannot accidentally truncate a validated profile after construction.
//
// A profile is complete only when it contains at least one table and every
// table has a name, positive row count, and positive average width. Catalog
// zeroes mean "unknown", so an empty table cannot be distinguished from an
// unavailable estimate and conservatively falls back to the scalar model.
type MemoryProfile struct {
	tables   []TableMemoryStat
	complete bool
}

// NewMemoryProfile copies tables into an immutable runtime profile.
func NewMemoryProfile(tables []TableMemoryStat) MemoryProfile {
	return NewMemoryProfileForTableCount(tables, len(tables))
}

// NewMemoryProfileForTableCount additionally verifies that the schema reader
// returned one record for every table the caller expected. This prevents a
// partially returned catalog result from becoming growth-authorizing evidence.
func NewMemoryProfileForTableCount(tables []TableMemoryStat, expectedTableCount int) MemoryProfile {
	profile := MemoryProfile{
		tables:   append([]TableMemoryStat(nil), tables...),
		complete: len(tables) > 0 && expectedTableCount == len(tables),
	}
	seenNames := make(map[string]struct{}, len(profile.tables))
	for _, table := range profile.tables {
		if strings.TrimSpace(table.Name) == "" || table.RowCount <= 0 || table.AvgRowBytes <= 0 {
			profile.complete = false
			break
		}
		// Schema filtering is case-insensitive, so completeness must use the
		// same identity. Otherwise case variants could satisfy the expected
		// count while a different in-scope table is missing.
		nameIdentity := strings.ToLower(table.Name)
		if _, exists := seenNames[nameIdentity]; exists {
			profile.complete = false
			break
		}
		seenNames[nameIdentity] = struct{}{}
	}
	return profile
}

// Complete reports whether every in-scope table supplied usable cardinality
// and width evidence.
func (p MemoryProfile) Complete() bool {
	return p.complete && len(p.tables) > 0
}

// Len reports how many in-scope table records the profile carries, including
// records whose unknown values make the profile incomplete.
func (p MemoryProfile) Len() int {
	return len(p.tables)
}

// Tables returns a defensive copy for diagnostics and tests.
func (p MemoryProfile) Tables() []TableMemoryStat {
	return append([]TableMemoryStat(nil), p.tables...)
}

// MemoryModel evaluates one immutable table profile, falling back to the
// legacy widest-width scalar model whenever the profile is incomplete.
type MemoryModel struct {
	profile          MemoryProfile
	fallbackRowBytes int64
}

// NewMemoryModel builds the shared pre-run/runtime memory model.
func NewMemoryModel(profile MemoryProfile, fallbackRowBytes int64) MemoryModel {
	return MemoryModel{profile: profile, fallbackRowBytes: fallbackRowBytes}
}

// UsesTableProfile reports whether cardinality-aware math is active.
func (m MemoryModel) UsesTableProfile() bool {
	return m.profile.Complete()
}

// TableCount reports the number of records carried by the runtime profile.
func (m MemoryModel) TableCount() int {
	return m.profile.Len()
}

// EstimatedMemMB returns the rounded-up modeled working set. A complete
// profile evaluates:
//
//	workers * (readAhead + writeAhead) *
//	max_table(avgWidth * min(chunkSize, rowCount))
//
// Incomplete profiles retain the existing scalar widest-width behavior.
func (m MemoryModel) EstimatedMemMB(workers, readAheadBuffers, writeAheadWriters, chunkSize int) int64 {
	bytes, valid, _ := m.modeledMemoryBytes(workers, readAheadBuffers, writeAheadWriters, int64(chunkSize))
	if !valid {
		return 0
	}
	mb := bytes / bytesPerMiB
	if bytes%bytesPerMiB != 0 {
		mb++
	}
	return mb
}

// ExceedsBudget compares the exact modeled product with an MiB budget.
func (m MemoryModel) ExceedsBudget(budgetMB int64, workers, readAheadBuffers, writeAheadWriters, chunkSize int) bool {
	if !m.UsesTableProfile() {
		return MemoryEstimateExceedsBudget(
			budgetMB,
			workers,
			readAheadBuffers,
			writeAheadWriters,
			chunkSize,
			m.fallbackRowBytes,
		)
	}
	if budgetMB <= 0 || workers <= 0 || readAheadBuffers < 0 || writeAheadWriters < 0 || chunkSize <= 0 {
		return false
	}
	buffers := new(big.Int).SetInt64(int64(readAheadBuffers))
	buffers.Add(buffers, big.NewInt(int64(writeAheadWriters)))
	if buffers.Sign() <= 0 {
		return false
	}

	maxTableBytes := new(big.Int)
	for _, table := range m.profile.tables {
		rows := int64(chunkSize)
		if table.RowCount < rows {
			rows = table.RowCount
		}
		tableBytes := new(big.Int).Mul(big.NewInt(table.AvgRowBytes), big.NewInt(rows))
		if tableBytes.Cmp(maxTableBytes) > 0 {
			maxTableBytes.Set(tableBytes)
		}
	}

	estimate := new(big.Int).SetInt64(int64(workers))
	estimate.Mul(estimate, buffers)
	estimate.Mul(estimate, maxTableBytes)
	budget := new(big.Int).SetInt64(budgetMB)
	budget.Lsh(budget, 20)
	return estimate.Cmp(budget) > 0
}

// SafeChunkSize returns the largest row chunk that fits the model. It uses a
// monotonic overflow-safe binary search for complete profiles. As with the
// scalar API, malformed sizing tuples return zero, while an over-budget
// one-row minimum returns one so the migration can still make progress.
func (m MemoryModel) SafeChunkSize(budgetMB int64, workers, readAheadBuffers, writeAheadWriters int) int64 {
	rows, _ := m.safeChunkSizeDetail(budgetMB, workers, readAheadBuffers, writeAheadWriters)
	return rows
}

func (m MemoryModel) safeChunkSizeDetail(budgetMB int64, workers, readAheadBuffers, writeAheadWriters int) (rows int64, minimumExceedsBudget bool) {
	if !m.UsesTableProfile() {
		return safeChunkSizeDetail(
			budgetMB,
			workers,
			readAheadBuffers,
			writeAheadWriters,
			m.fallbackRowBytes,
		)
	}
	if budgetMB <= 0 || workers <= 0 || readAheadBuffers < 0 || writeAheadWriters < 0 {
		return 0, false
	}
	buffers, valid, _ := bufferCount(readAheadBuffers, writeAheadWriters)
	if !valid || buffers <= 0 {
		return 0, false
	}

	if m.ExceedsBudget(budgetMB, workers, readAheadBuffers, writeAheadWriters, 1) {
		return 1, true
	}

	var maxRows int64
	for _, table := range m.profile.tables {
		if table.RowCount > maxRows {
			maxRows = table.RowCount
		}
	}
	if maxRows <= 0 {
		return safeChunkSizeDetail(
			budgetMB,
			workers,
			readAheadBuffers,
			writeAheadWriters,
			m.fallbackRowBytes,
		)
	}

	// Once chunkSize reaches the largest table cardinality, every table's
	// contribution is saturated. If that full-dataset footprint fits, memory
	// places no useful row-count ceiling on the chunk.
	if !m.exceedsBudgetAtChunk64(budgetMB, workers, readAheadBuffers, writeAheadWriters, maxRows) {
		return math.MaxInt64, false
	}

	low, high := int64(1), maxRows
	for low < high {
		mid := low + (high-low+1)/2
		if m.exceedsBudgetAtChunk64(budgetMB, workers, readAheadBuffers, writeAheadWriters, mid) {
			high = mid - 1
		} else {
			low = mid
		}
	}
	return low, false
}

func (m MemoryModel) exceedsBudgetAtChunk64(budgetMB int64, workers, readAheadBuffers, writeAheadWriters int, chunkSize int64) bool {
	if chunkSize <= 0 {
		return false
	}
	buffers := new(big.Int).SetInt64(int64(readAheadBuffers))
	buffers.Add(buffers, big.NewInt(int64(writeAheadWriters)))
	if budgetMB <= 0 || workers <= 0 || buffers.Sign() <= 0 {
		return false
	}

	maxTableBytes := new(big.Int)
	for _, table := range m.profile.tables {
		rows := chunkSize
		if table.RowCount < rows {
			rows = table.RowCount
		}
		tableBytes := new(big.Int).Mul(big.NewInt(table.AvgRowBytes), big.NewInt(rows))
		if tableBytes.Cmp(maxTableBytes) > 0 {
			maxTableBytes.Set(tableBytes)
		}
	}
	estimate := new(big.Int).SetInt64(int64(workers))
	estimate.Mul(estimate, buffers)
	estimate.Mul(estimate, maxTableBytes)
	budget := new(big.Int).SetInt64(budgetMB)
	budget.Lsh(budget, 20)
	return estimate.Cmp(budget) > 0
}

func (m MemoryModel) modeledMemoryBytes(workers, readAheadBuffers, writeAheadWriters int, chunkSize int64) (bytes int64, valid, overflow bool) {
	if !m.UsesTableProfile() {
		maxInt := int64(int(^uint(0) >> 1))
		if chunkSize <= 0 || chunkSize > maxInt {
			return 0, false, false
		}
		return modeledMemoryBytes(workers, readAheadBuffers, writeAheadWriters, int(chunkSize), m.fallbackRowBytes)
	}
	if workers <= 0 || chunkSize <= 0 {
		return 0, false, false
	}
	buffers, valid, overflow := bufferCount(readAheadBuffers, writeAheadWriters)
	if !valid {
		return 0, false, overflow
	}

	var maxTableBytes int64
	for _, table := range m.profile.tables {
		rows := chunkSize
		if table.RowCount < rows {
			rows = table.RowCount
		}
		tableBytes, tableOverflow := saturatingMulPositive(table.AvgRowBytes, rows)
		overflow = overflow || tableOverflow
		if tableBytes > maxTableBytes {
			maxTableBytes = tableBytes
		}
	}
	if maxTableBytes <= 0 {
		return 0, false, overflow
	}

	product, stepOverflow := saturatingMulPositive(int64(workers), buffers)
	overflow = overflow || stepOverflow
	if overflow {
		return math.MaxInt64, true, true
	}
	product, stepOverflow = saturatingMulPositive(product, maxTableBytes)
	overflow = overflow || stepOverflow
	if overflow {
		return math.MaxInt64, true, true
	}
	return product, true, false
}
