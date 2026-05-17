package postgres

import (
	"context"
	"database/sql"
	"regexp"
	"strconv"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

// gatherDatabaseContext collects PostgreSQL database metadata for AI context.
func (w *Writer) gatherDatabaseContext() *driver.DatabaseContext {
	ctx := context.Background()

	dbCtx := &driver.DatabaseContext{
		DatabaseName:             w.config.Database,
		ServerName:               w.config.Host,
		IdentifierCase:           "lower",
		CaseSensitiveIdentifiers: true, // PostgreSQL preserves case in quotes
		CaseSensitiveData:        true, // Default is case-sensitive
		MaxIdentifierLength:      63,
		VarcharSemantics:         "char", // PostgreSQL VARCHAR is always characters
		BytesPerChar:             4,      // UTF-8 max
		MaxVarcharLength:         10485760,
	}

	// Query server version
	var version string
	if w.pool.QueryRow(ctx, "SELECT version()").Scan(&version) == nil {
		dbCtx.Version = version
		// Parse major version using regex to handle any version format
		// Matches patterns like "PostgreSQL 16.1", "PostgreSQL 17", etc.
		versionRegex := regexp.MustCompile(`PostgreSQL\s+(\d+)`)
		if matches := versionRegex.FindStringSubmatch(version); len(matches) > 1 {
			if majorVer, err := strconv.Atoi(matches[1]); err == nil {
				dbCtx.MajorVersion = majorVer
			}
		}
	}

	// Query encoding
	var encoding string
	if w.pool.QueryRow(ctx, "SHOW server_encoding").Scan(&encoding) == nil {
		dbCtx.Charset = encoding
		dbCtx.Encoding = encoding
		switch encoding {
		case "UTF8":
			dbCtx.BytesPerChar = 4
		case "LATIN1", "SQL_ASCII":
			dbCtx.BytesPerChar = 1
		}
	}

	// Query collation
	var collation sql.NullString
	if w.pool.QueryRow(ctx, `
		SELECT datcollate FROM pg_database WHERE datname = current_database()
	`).Scan(&collation) == nil && collation.Valid {
		dbCtx.Collation = collation.String
	}

	// Query LC_CTYPE for character classification
	var lcCtype sql.NullString
	if w.pool.QueryRow(ctx, `
		SELECT datctype FROM pg_database WHERE datname = current_database()
	`).Scan(&lcCtype) == nil && lcCtype.Valid {
		if dbCtx.Notes != "" {
			dbCtx.Notes += "; "
		}
		dbCtx.Notes += "LC_CTYPE=" + lcCtype.String
	}

	// Standard PostgreSQL features
	dbCtx.Features = []string{"TEXT", "JSON", "JSONB", "ARRAY", "HSTORE", "UUID", "BYTEA", "NUMERIC"}

	// Version-specific features
	if dbCtx.MajorVersion >= 14 {
		dbCtx.Features = append(dbCtx.Features, "MULTIRANGE")
	}
	if dbCtx.MajorVersion >= 15 {
		dbCtx.Features = append(dbCtx.Features, "JSON_TABLE")
	}

	logging.Debug("PostgreSQL context: encoding=%s, collation=%s, version=%d",
		dbCtx.Encoding, dbCtx.Collation, dbCtx.MajorVersion)

	return dbCtx
}
