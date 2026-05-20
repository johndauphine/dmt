package transfer

import (
	"math"
	"reflect"
	"testing"
)

func TestSplitPKRangeNormalRange(t *testing.T) {
	ranges := splitPKRange(int64(1), int64(100), 4)
	want := [][2]int64{
		{0, 25},
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

	ranges := splitPKRange(minPK, maxPK, 4)
	want := [][2]int64{
		{math.MaxInt64 - 10, math.MaxInt64 - 7},
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

	ranges := splitPKRange(minPK, maxPK, 4)
	want := [][2]int64{
		{math.MinInt64, -4611686018427387904},
		{-4611686018427387904, -1},
		{-1, 4611686018427387902},
		{4611686018427387902, math.MaxInt64},
	}

	assertRangePairs(t, ranges, want)
	assertMonotonicCoverage(t, ranges, minPK, maxPK)
}

func TestSplitPKRangeTinyRangeReducesReaders(t *testing.T) {
	ranges := splitPKRange(int64(10), int64(12), 10)
	want := [][2]int64{
		{9, 11},
		{11, 12},
	}

	assertRangePairs(t, ranges, want)
	assertMonotonicCoverage(t, ranges, 10, 12)
}

func TestSplitPKRangeSingleRowKeepsExclusiveLowerBound(t *testing.T) {
	ranges := splitPKRange(int64(42), int64(42), 4)
	want := [][2]int64{{41, 42}}

	assertRangePairs(t, ranges, want)
	assertMonotonicCoverage(t, ranges, 42, 42)
}

func TestDecrementPKDoesNotWrapAtIntegerMinimum(t *testing.T) {
	if got := decrementPK(int64(math.MinInt64)); got != int64(math.MinInt64) {
		t.Fatalf("decrementPK(MinInt64) = %v, want %v", got, int64(math.MinInt64))
	}

	if got := decrementPK(int32(math.MinInt32)); got != int32(math.MinInt32) {
		t.Fatalf("decrementPK(MinInt32) = %v, want %v", got, int32(math.MinInt32))
	}

	minIntValue := minInt()
	if got := decrementPK(minIntValue); got != minIntValue {
		t.Fatalf("decrementPK(min int) = %v, want %v", got, minIntValue)
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

		if rangeMax <= rangeMin {
			t.Fatalf("range %d wraps or does not progress: min=%d max=%d", i, rangeMin, rangeMax)
		}

		if i == 0 {
			wantFirstMin := minPK - 1
			if rangeMin != wantFirstMin {
				t.Fatalf("first range min = %d, want %d", rangeMin, wantFirstMin)
			}
		} else if rangeMin != previousMax {
			t.Fatalf("range %d starts at %d, want previous max %d", i, rangeMin, previousMax)
		}

		previousMax = rangeMax
	}

	if previousMax != maxPK {
		t.Fatalf("last range max = %d, want %d", previousMax, maxPK)
	}
}
