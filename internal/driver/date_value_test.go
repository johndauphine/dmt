package driver

import (
	"testing"
	"time"
)

func TestParseDateValue(t *testing.T) {
	want := time.Date(2026, 5, 19, 8, 47, 11, 520123000, time.UTC)

	tests := []struct {
		name  string
		value any
		want  *time.Time
	}{
		{
			name:  "native time",
			value: want,
			want:  &want,
		},
		{
			name:  "rfc3339 nano string",
			value: "2026-05-19T08:47:11.520123Z",
			want:  &want,
		},
		{
			name:  "space separated string",
			value: "2026-05-19 08:47:11.520123",
			want:  &want,
		},
		{
			name:  "byte slice",
			value: []byte("2026-05-19T08:47:11.520123Z"),
			want:  &want,
		},
		{
			name:  "nil",
			value: nil,
			want:  nil,
		},
		{
			name:  "empty string",
			value: " ",
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDateValue(tt.value)
			if err != nil {
				t.Fatalf("ParseDateValue() error = %v", err)
			}
			if tt.want == nil {
				if got != nil {
					t.Fatalf("ParseDateValue() = %v, want nil", got)
				}
				return
			}
			if got == nil || !got.Equal(*tt.want) {
				t.Fatalf("ParseDateValue() = %v, want %v", got, *tt.want)
			}
		})
	}
}

func TestParseDateValueRejectsUnsupportedType(t *testing.T) {
	if _, err := ParseDateValue(123); err == nil {
		t.Fatal("ParseDateValue() error = nil, want unsupported type error")
	}
}
