package dbtuning

import (
	"testing"
)

func TestParseBytes(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"0", 0},
		{"1024", 1024},
		{"1k", 1024},
		{"1KB", 1024},
		{"1 kB", 1024},
		{"1 MB", 1024 * 1024},
		{"2GB", 2 * 1024 * 1024 * 1024},
		{"1.5 GB", int64(1.5 * 1024 * 1024 * 1024)},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := ParseBytes(tc.input)
			if err != nil {
				t.Fatalf("ParseBytes(%q) errored: %v", tc.input, err)
			}
			if got != tc.want {
				t.Errorf("ParseBytes(%q) = %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}

func TestParseBytes_Invalid(t *testing.T) {
	bad := []string{"", "garbage", "5XB", "MB", "10 yottabytes"}
	for _, s := range bad {
		t.Run(s, func(t *testing.T) {
			if _, err := ParseBytes(s); err == nil {
				t.Errorf("ParseBytes(%q) should error", s)
			}
		})
	}
}

func TestFormatMB(t *testing.T) {
	tests := []struct {
		mb   int64
		want string
	}{
		{128, "128MB"},
		{1024, "1GB"},
		{1023, "1023MB"}, // not a clean GB
		{8192, "8GB"},
		{1024 * 1024, "1TB"},
		{2048, "2GB"},
		{0, "0MB"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if got := FormatMB(tc.mb); got != tc.want {
				t.Errorf("FormatMB(%d) = %q, want %q", tc.mb, got, tc.want)
			}
		})
	}
}

func TestSummarizeRecommendations(t *testing.T) {
	tests := []struct {
		name          string
		recs          []TuningRecommendation
		wantPotential string
	}{
		{"empty", nil, "none"},
		{
			"three critical → high",
			[]TuningRecommendation{
				{Parameter: "a", Priority: 1},
				{Parameter: "b", Priority: 1},
				{Parameter: "c", Priority: 1},
			},
			"high",
		},
		{
			"one critical → medium",
			[]TuningRecommendation{{Parameter: "a", Priority: 1}},
			"medium",
		},
		{
			"only important → low",
			[]TuningRecommendation{{Parameter: "a", Priority: 2}},
			"low",
		},
		{
			"only optional → low",
			[]TuningRecommendation{{Parameter: "a", Priority: 3}},
			"low",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pot, impact := summarizeRecommendations(tc.recs)
			if pot != tc.wantPotential {
				t.Errorf("potential = %q, want %q", pot, tc.wantPotential)
			}
			if impact == "" {
				t.Error("impact summary should never be empty")
			}
		})
	}
}

func TestRecommend_PopulatesAllFields(t *testing.T) {
	r := Recommend(RecOpts{
		Parameter:        "p",
		CurrentValue:     "c",
		RecommendedValue: "r",
		Impact:           "high",
		Reason:           "reason",
		Priority:         1,
		CanApplyRuntime:  true,
		SQLCommand:       "SET p = r;",
		RequiresRestart:  false,
		ConfigFile:       "p = r",
	})
	if r.Parameter != "p" || r.Impact != "high" || r.Priority != 1 {
		t.Errorf("Recommend dropped fields: %+v", r)
	}
	if !r.CanApplyRuntime || r.SQLCommand == "" {
		t.Errorf("runtime-application fields not preserved: %+v", r)
	}
}

func TestDetectSystemInfo_NonZero(t *testing.T) {
	// gopsutil should always return positive values on any platform
	// dmt supports. This is a smoke test that the import wiring is
	// correct, not a value check.
	sys := DetectSystemInfo()
	if sys.HostMemoryMB <= 0 {
		t.Errorf("expected non-zero host memory; got %d", sys.HostMemoryMB)
	}
	if sys.CPUCores <= 0 {
		t.Errorf("expected non-zero CPU cores; got %d", sys.CPUCores)
	}
}
