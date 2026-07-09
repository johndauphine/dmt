package transfer

import (
	"math"
	"reflect"
	"testing"
)

func TestSplitPKRangeNormalRange(t *testing.T) {
	ranges := splitPKRange(int64(1), int64(100), 4, true)
	want := [][2]int64{
		{1, 25},
		{25, 49},
		{49, 73},
		{73, 100},
	}

	assertRangePairs(t, ranges, want)
	assertMonotonicCoverage(t, ranges, 1, 100)
}

func TestSplitPKRangeNearMaxInt64(t *testing.T) {
	minPK := int64(math.MaxInt64 - 9)
	maxPK := int64(math.MaxInt64)

	ranges := splitPKRange(minPK, maxPK, 4, true)
	want := [][2]int64{
		{math.MaxInt64 - 9, math.MaxInt64 - 7},
		{math.MaxInt64 - 7, math.MaxInt64 - 5},
		{math.MaxInt64 - 5, math.MaxInt64 - 3},
		{math.MaxInt64 - 3, math.MaxInt64},
	}

	assertRangePairs(t, ranges, want)
	assertMonotonicCoverage(t, ranges, minPK, maxPK)
}

func TestSplitPKRangeNegativeToPositiveOverflowSafe(t *testing.T) {
	minPK := int64(-math.MaxInt64)
	maxPK := int64(math.MaxInt64)

	ranges := splitPKRange(minPK, maxPK, 4, true)
	want := [][2]int64{
		{-math.MaxInt64, -4611686018427387904},
		{-4611686018427387904, -1},
		{-1, 4611686018427387902},
		{4611686018427387902, math.MaxInt64},
	}

	assertRangePairs(t, ranges, want)
	assertMonotonicCoverage(t, ranges, minPK, maxPK)
}

func TestSplitPKRangeTinyRangeReducesReaders(t *testing.T) {
	ranges := splitPKRange(int64(10), int64(12), 10, true)
	want := [][2]int64{
		{10, 11},
		{11, 12},
	}

	assertRangePairs(t, ranges, want)
	assertMonotonicCoverage(t, ranges, 10, 12)
}

func TestSplitPKRangeSingleRowKeepsInclusiveLowerBound(t *testing.T) {
	ranges := splitPKRange(int64(42), int64(42), 4, true)
	want := [][2]int64{{42, 42}}

	assertRangePairs(t, ranges, want)
	assertMonotonicCoverage(t, ranges, 42, 42)
}

func TestSplitPKRangeResumeLowerBoundIsExclusive(t *testing.T) {
	ranges := splitPKRange(int64(10), int64(20), 2, false)
	for i, r := range ranges {
		if r.minInclusive {
			t.Fatalf("range %d lower bound is inclusive on resume: %+v", i, r)
		}
	}
}

func TestSplitPKRangeUnsignedBoundsIncludeMinimum(t *testing.T) {
	minPK := uint64(0)
	maxPK := ^uint64(0)
	ranges := splitPKRange(minPK, maxPK, 4, true)
	if len(ranges) != 1 {
		t.Fatalf("unsigned range count = %d, want 1", len(ranges))
	}
	if ranges[0].minPK != minPK || ranges[0].maxPK != maxPK || !ranges[0].minInclusive {
		t.Fatalf("unsigned range = %+v, want inclusive [%d,%d]", ranges[0], minPK, maxPK)
	}
}

func assertRangePairs(t *testing.T, ranges []pkRange, want [][2]int64) {
	t.Helper()

	got := make([][2]int64, 0, len(ranges))
	for _, r := range ranges {
		minPK, ok := r.minPK.(int64)
		if !ok {
			t.Fatalf("range minPK type = %T, want int64", r.minPK)
		}
		maxPK, ok := r.maxPK.(int64)
		if !ok {
			t.Fatalf("range maxPK type = %T, want int64", r.maxPK)
		}
		got = append(got, [2]int64{minPK, maxPK})
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ranges = %v, want %v", got, want)
	}
}

func assertMonotonicCoverage(t *testing.T, ranges []pkRange, minPK, maxPK int64) {
	t.Helper()

	if len(ranges) == 0 {
		t.Fatal("expected at least one range")
	}

	var previousMax int64
	for i, r := range ranges {
		rangeMin, ok := r.minPK.(int64)
		if !ok {
			t.Fatalf("range %d minPK type = %T, want int64", i, r.minPK)
		}
		rangeMax, ok := r.maxPK.(int64)
		if !ok {
			t.Fatalf("range %d maxPK type = %T, want int64", i, r.maxPK)
		}

		if rangeMax < rangeMin || (rangeMax == rangeMin && !r.minInclusive) {
			t.Fatalf("range %d wraps or does not progress: min=%d max=%d", i, rangeMin, rangeMax)
		}

		if i == 0 {
			if rangeMin != minPK {
				t.Fatalf("first range min = %d, want %d", rangeMin, minPK)
			}
			if !r.minInclusive {
				t.Fatal("first range lower bound is exclusive, want inclusive")
			}
		} else {
			if rangeMin != previousMax {
				t.Fatalf("range %d starts at %d, want previous max %d", i, rangeMin, previousMax)
			}
			if r.minInclusive {
				t.Fatalf("range %d lower bound is inclusive; adjacent ranges would overlap", i)
			}
		}

		previousMax = rangeMax
	}

	if previousMax != maxPK {
		t.Fatalf("last range max = %d, want %d", previousMax, maxPK)
	}
}
