package version

// Version is the current version of dmt.
// Can be overridden at build time with -ldflags "-X ...version.Version=..."
var Version = "3.53.0"

// Name is the application name.
const Name = "dmt"

// Description is a short description of the application.
const Description = "High-performance database migration tool"
