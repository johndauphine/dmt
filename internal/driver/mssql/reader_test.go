package mssql

import (
	"strings"
	"testing"
)

// TestBuildExactRowCountQuery_NOLOCK is the regression guard for
// #253's third sub-fix: pre-#253 the exact-row-count query always
// included `WITH (NOLOCK)`, silently overriding any operator who
// set migration.strict_consistency: true. The hint must drop out
// when strictConsistency is true and stay when it's false.
func TestBuildExactRowCountQuery_NOLOCK(t *testing.T) {
	t.Run("default behavior includes NOLOCK", func(t *testing.T) {
		got := buildExactRowCountQuery("[dbo].[Users]", false)
		if !strings.Contains(got, "WITH (NOLOCK)") {
			t.Errorf("strict_consistency=false should keep NOLOCK; got %q", got)
		}
	})

	t.Run("strict_consistency drops NOLOCK", func(t *testing.T) {
		got := buildExactRowCountQuery("[dbo].[Users]", true)
		if strings.Contains(got, "NOLOCK") {
			t.Errorf("strict_consistency=true must drop NOLOCK; got %q", got)
		}
	})

	t.Run("table identifier preserved", func(t *testing.T) {
		got := buildExactRowCountQuery("[my schema].[Orders]", true)
		if !strings.Contains(got, "[my schema].[Orders]") {
			t.Errorf("qualified table name missing in %q", got)
		}
	})
}
