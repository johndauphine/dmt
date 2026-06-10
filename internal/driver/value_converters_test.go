package driver

import (
	"testing"
	"time"
)

// TestDefaultValueConverters pins the #477 normalization table: same
// cases, same nil handling as the table that lived in transfer/scan.go.
func TestDefaultValueConverters(t *testing.T) {
	convs := DefaultValueConverters([]string{"int", "uniqueidentifier", "bit", "datetime", "varbinary"})
	if convs[0] != nil {
		t.Error("pass-through type got a converter")
	}
	guid := []byte{0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0x77, 0x66, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}
	if got := convs[1](guid); got != "00112233-4455-6677-8899-aabbccddeeff" {
		t.Errorf("GUID conversion = %v", got)
	}
	if got := convs[2](int64(1)); got != true {
		t.Errorf("bit(1) = %v, want true", got)
	}
	sentinel := time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(-1, 0, 0)
	if got := convs[3](sentinel); got != nil {
		t.Errorf("pre-year-1 datetime = %v, want nil", got)
	}
	if got := convs[4]([]byte{}); got != nil {
		t.Errorf("empty varbinary = %v, want nil", got)
	}
}
