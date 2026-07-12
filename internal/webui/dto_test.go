package webui

import (
	"testing"

	"github.com/johndauphine/dmt/internal/driver"
)

func TestNewAnalyzeDTOCarriesRowWidthProvenance(t *testing.T) {
	dto := newAnalyzeDTO(&driver.SmartConfigSuggestions{
		AvgRowSizeBytes:          2_000,
		RepresentativeRowBytes:   240,
		SafetyRowBytes:           8_192,
		SafetyRowBytesKnown:      true,
		MemoryEstimateOverBudget: true,
	})

	if dto.AvgRowSizeBytes != 2_000 || dto.RepresentativeRowBytes != 240 ||
		dto.SafetyRowBytes != 8_192 || !dto.SafetyRowBytesKnown || !dto.MemoryEstimateOverBudget {
		t.Fatalf("row-width DTO mapping = legacy %d representative %d safety %d known %v",
			dto.AvgRowSizeBytes, dto.RepresentativeRowBytes, dto.SafetyRowBytes, dto.SafetyRowBytesKnown)
	}
}
