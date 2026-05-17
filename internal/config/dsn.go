package config

import (
	"fmt"
	"net/url"
)

// SourceDSN returns the source database connection string.
// Uses driver registry to determine the correct DSN builder.
func (c *Config) SourceDSN() string {
	// Use driver registry to get canonical name (e.g., "pg" -> "postgres")
	driverName := canonicalDriverName(c.Source.Type)
	switch driverName {
	case "postgres":
		return c.buildPostgresDSN(c.Source.Host, c.Source.Port, c.Source.Database,
			c.Source.User, c.Source.Password, c.Source.SSLMode,
			c.Source.Auth, c.Source.GSSEncMode)
	case "mssql":
		encrypt := c.Source.Encrypt != nil && *c.Source.Encrypt
		return c.buildMSSQLDSN(c.Source.Host, c.Source.Port, c.Source.Database,
			c.Source.User, c.Source.Password, encrypt, c.Source.TrustServerCert,
			c.Source.PacketSize, c.Source.Auth, c.Source.Krb5Conf, c.Source.Keytab, c.Source.Realm, c.Source.SPN)
	default:
		// Unknown driver type - should have been caught in validation
		// Return empty string to trigger connection error
		return ""
	}
}

// TargetDSN returns the target database connection string.
// Uses driver registry to determine the correct DSN builder.
func (c *Config) TargetDSN() string {
	// Use driver registry to get canonical name (e.g., "sqlserver" -> "mssql")
	driverName := canonicalDriverName(c.Target.Type)
	switch driverName {
	case "mssql":
		encrypt := c.Target.Encrypt != nil && *c.Target.Encrypt
		return c.buildMSSQLDSN(c.Target.Host, c.Target.Port, c.Target.Database,
			c.Target.User, c.Target.Password, encrypt, c.Target.TrustServerCert,
			c.Target.PacketSize, c.Target.Auth, c.Target.Krb5Conf, c.Target.Keytab, c.Target.Realm, c.Target.SPN)
	case "postgres":
		return c.buildPostgresDSN(c.Target.Host, c.Target.Port, c.Target.Database,
			c.Target.User, c.Target.Password, c.Target.SSLMode,
			c.Target.Auth, c.Target.GSSEncMode)
	default:
		// Unknown driver type - should have been caught in validation
		// Return empty string to trigger connection error
		return ""
	}
}

// buildMSSQLDSN builds an MSSQL connection string with optional Kerberos auth
func (c *Config) buildMSSQLDSN(host string, port int, database, user, password string, encrypt bool,
	trustServerCert bool, packetSize int, auth, krb5Conf, keytab, realm, spn string) string {

	encryptStr := "disable"
	if encrypt {
		encryptStr = "true"
	}
	trustCert := "false"
	if trustServerCert {
		trustCert = "true"
	}

	// URL-encode values that may contain special characters
	// Use QueryEscape for user/password to encode @ and : which are reserved in userinfo
	encodedDB := url.QueryEscape(database)
	encodedUser := url.QueryEscape(user)
	encodedPass := url.QueryEscape(password)

	// Kerberos authentication
	if auth == "kerberos" {
		dsn := fmt.Sprintf("sqlserver://%s:%d?database=%s&encrypt=%s&TrustServerCertificate=%s&authenticator=krb5",
			host, port, encodedDB, encryptStr, trustCert)

		// Add packet size for better throughput (default 4KB is too small)
		if packetSize > 0 {
			dsn += fmt.Sprintf("&packet+size=%d", packetSize)
		}

		// Optional Kerberos parameters
		if krb5Conf != "" {
			dsn += "&krb5-configfile=" + url.QueryEscape(krb5Conf)
		}
		if keytab != "" {
			dsn += "&krb5-keytabfile=" + url.QueryEscape(keytab)
		}
		if realm != "" {
			dsn += "&krb5-realm=" + url.QueryEscape(realm)
		}
		if spn != "" {
			dsn += "&ServerSPN=" + url.QueryEscape(spn)
		}
		// If user specified, use it as the principal
		if user != "" {
			dsn += "&krb5-username=" + url.QueryEscape(user)
		}
		return dsn
	}

	// Password authentication (default)
	dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=%s&TrustServerCertificate=%s",
		encodedUser, encodedPass, host, port, encodedDB, encryptStr, trustCert)

	// Add packet size for better throughput (default 4KB is too small)
	if packetSize > 0 {
		dsn += fmt.Sprintf("&packet+size=%d", packetSize)
	}

	return dsn
}

// buildPostgresDSN builds a PostgreSQL connection string with optional Kerberos auth
func (c *Config) buildPostgresDSN(host string, port int, database, user, password, sslMode,
	auth, gssEncMode string) string {

	// URL-encode values that may contain special characters
	// Use QueryEscape for user/password to encode @ and : which are reserved in userinfo
	// Use PathEscape for database since it's in the URL path
	encodedDB := url.PathEscape(database)
	encodedUser := url.QueryEscape(user)
	encodedPass := url.QueryEscape(password)

	// Kerberos/GSSAPI authentication
	if auth == "kerberos" {
		gssEnc := "prefer"
		if gssEncMode != "" {
			gssEnc = gssEncMode
		}
		// For Kerberos, we don't include password in the DSN
		if user != "" {
			return fmt.Sprintf("postgres://%s@%s:%d/%s?sslmode=%s&gssencmode=%s",
				encodedUser, host, port, encodedDB, sslMode, gssEnc)
		}
		return fmt.Sprintf("postgres://%s:%d/%s?sslmode=%s&gssencmode=%s",
			host, port, encodedDB, sslMode, gssEnc)
	}

	// Password authentication (default)
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		encodedUser, encodedPass, host, port, encodedDB, sslMode)
}
