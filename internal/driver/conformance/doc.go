// Package conformance provides reusable test helpers for database driver
// implementations.
//
// Required behavior checked by this package:
//   - the driver is registered under its canonical name and aliases;
//   - Driver.Name, Aliases, Defaults, Dialect, ProbeTarget, and PreFlight are
//     callable without a live external database;
//   - dialect identity, identifier quoting, table qualification, placeholders,
//     column lists, row-count query shape, and date-type metadata are stable;
//   - nil database handles do not panic during target probing or preflight.
//
// Optional behavior is expressed by expectation fields on DriverCase. Engines
// opt in only to behavior that is meaningful for that dialect, such as ignoring
// schemas during table qualification, including schema qualification, or
// exposing AI prompt augmentation. Dialects can also opt into exact pagination
// SQL/argument ordering assertions for keyset and ROW_NUMBER reads with date
// filters. The harness intentionally does not assert schema introspection
// details; those remain engine-specific and often require live databases.
package conformance
