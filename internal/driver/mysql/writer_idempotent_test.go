package mysql

import (
	"strings"
	"testing"
)

// TestBuildInsertSQL_IdempotentOnDupSuffix pins the resume-safe shape added
// for #227: when the writer's WriteBatch is called with IdempotentOnDup, the
// generated INSERT must end with
//
//	ON DUPLICATE KEY UPDATE <pk> = <pk>
//
// — a no-op self-assignment that converts duplicate-key conflicts into silent
// no-ops without touching non-PK columns. We deliberately do NOT use
// INSERT IGNORE because that also masks data-conversion errors, which would
// turn legitimate type-mismatch failures into silent data drops on resume.
func TestBuildInsertSQL_IdempotentOnDupSuffix(t *testing.T) {
	tests := []struct {
		name         string
		dupSuffix    string
		mustContain  []string
		mustNotMatch []string
	}{
		{
			name:      "plain INSERT (no resume)",
			dupSuffix: "",
			mustContain: []string{
				"INSERT INTO `db`.`posts` (`id`, `title`) VALUES (?, ?), (?, ?)",
			},
			mustNotMatch: []string{
				"ON DUPLICATE KEY",
				"INSERT IGNORE",
			},
		},
		{
			name:      "idempotent INSERT (resume): no-op PK self-assignment",
			dupSuffix: " ON DUPLICATE KEY UPDATE `id` = `id`",
			mustContain: []string{
				"INSERT INTO `db`.`posts` (`id`, `title`) VALUES (?, ?), (?, ?)",
				"ON DUPLICATE KEY UPDATE `id` = `id`",
			},
			mustNotMatch: []string{
				"INSERT IGNORE", // never — masks data-conversion errors
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := buildInsertSQL("`db`.`posts`", "`id`, `title`", 2, 2, tt.dupSuffix)
			for _, sub := range tt.mustContain {
				if !strings.Contains(sql, sub) {
					t.Errorf("missing required substring %q in:\n%s", sub, sql)
				}
			}
			for _, sub := range tt.mustNotMatch {
				if strings.Contains(sql, sub) {
					t.Errorf("forbidden substring %q present in:\n%s", sub, sql)
				}
			}
		})
	}
}
