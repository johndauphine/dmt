// Package dbconfig provides database configuration types used by both
// the config and driver packages. This package exists to break the
// circular import between config and driver packages.
package dbconfig

// SourceConfig holds source database connection settings.
// This is the configuration needed to connect to a source database for reading.
type SourceConfig struct {
	Type            string `yaml:"type"` // "mssql" or "postgres" (default: mssql)
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	Database        string `yaml:"database"`
	User            string `yaml:"user"`
	Password        string `yaml:"password"`
	Schema          string `yaml:"schema"`
	SSLMode         string `yaml:"ssl_mode"`          // PostgreSQL: disable, require, verify-ca, verify-full (default: require)
	TrustServerCert bool   `yaml:"trust_server_cert"` // MSSQL: trust server certificate (default: false)
	Encrypt         *bool  `yaml:"encrypt"`           // MSSQL: enable TLS encryption (default: true)
	PacketSize      int    `yaml:"packet_size"`       // MSSQL: TDS packet size in bytes (default: 32767, max: 32767)
	// Kerberos authentication (alternative to user/password)
	Auth       string `yaml:"auth"`       // "password" (default) or "kerberos"
	Krb5Conf   string `yaml:"krb5_conf"`  // Path to krb5.conf (optional, uses system default)
	Keytab     string `yaml:"keytab"`     // Path to keytab file (optional, uses credential cache)
	Realm      string `yaml:"realm"`      // Kerberos realm (optional, auto-detected)
	SPN        string `yaml:"spn"`        // Service Principal Name for MSSQL (optional)
	GSSEncMode string `yaml:"gssencmode"` // PostgreSQL GSSAPI encryption: disable, prefer, require (default: prefer)
}

// TargetConfig holds target database connection settings.
// This is the configuration needed to connect to a target database for writing.
type TargetConfig struct {
	Type            string `yaml:"type"` // "postgres" or "mssql" (default: postgres)
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	Database        string `yaml:"database"`
	User            string `yaml:"user"`
	Password        string `yaml:"password"`
	Schema          string `yaml:"schema"`
	SSLMode         string `yaml:"ssl_mode"`          // PostgreSQL: disable, require, verify-ca, verify-full (default: require)
	TrustServerCert bool   `yaml:"trust_server_cert"` // MSSQL: trust server certificate (default: false)
	Encrypt         *bool  `yaml:"encrypt"`           // MSSQL: enable TLS encryption (default: true)
	PacketSize      int    `yaml:"packet_size"`       // MSSQL: TDS packet size in bytes (default: 32767, max: 32767)
	// Kerberos authentication (alternative to user/password)
	Auth       string `yaml:"auth"`       // "password" (default) or "kerberos"
	Krb5Conf   string `yaml:"krb5_conf"`  // Path to krb5.conf (optional, uses system default)
	Keytab     string `yaml:"keytab"`     // Path to keytab file (optional, uses credential cache)
	Realm      string `yaml:"realm"`      // Kerberos realm (optional, auto-detected)
	SPN        string `yaml:"spn"`        // Service Principal Name for MSSQL (optional)
	GSSEncMode string `yaml:"gssencmode"` // PostgreSQL GSSAPI encryption: disable, prefer, require (default: prefer)
}

// DSNOptions returns a map of options for building a DSN.
// This consolidates the DSN option handling for source configs.
func (c *SourceConfig) DSNOptions() map[string]any {
	opts := make(map[string]any)
	if c.SSLMode != "" {
		opts["sslmode"] = c.SSLMode
	}
	if c.Encrypt != nil {
		opts["encrypt"] = *c.Encrypt
	}
	if c.TrustServerCert {
		opts["trustServerCertificate"] = true
	}
	if c.PacketSize > 0 {
		opts["packetSize"] = c.PacketSize
	}
	return opts
}

// DSNOptions returns a map of options for building a DSN.
// This consolidates the DSN option handling for target configs.
func (c *TargetConfig) DSNOptions() map[string]any {
	opts := make(map[string]any)
	if c.SSLMode != "" {
		opts["sslmode"] = c.SSLMode
	}
	if c.Encrypt != nil {
		opts["encrypt"] = *c.Encrypt
	}
	if c.TrustServerCert {
		opts["trustServerCertificate"] = true
	}
	if c.PacketSize > 0 {
		opts["packetSize"] = c.PacketSize
	}
	return opts
}
