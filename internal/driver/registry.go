package driver

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// registry holds all registered drivers.
var (
	registryMu sync.RWMutex
	drivers    = make(map[string]Driver)
)

// Register adds a driver to the global registry.
// This is typically called from a driver package's init() function.
//
// Example:
//
//	func init() {
//	    driver.Register(&PostgresDriver{})
//	}
//
// Panics if a driver with the same name is already registered.
func Register(d Driver) {
	registryMu.Lock()
	defer registryMu.Unlock()

	name := d.Name()
	if _, exists := drivers[name]; exists {
		panic(fmt.Sprintf("driver %q already registered", name))
	}

	// Register primary name
	drivers[name] = d

	// Register aliases
	for _, alias := range d.Aliases() {
		if _, exists := drivers[alias]; exists {
			panic(fmt.Sprintf("driver alias %q already registered", alias))
		}
		drivers[alias] = d
	}
}

// Get retrieves a driver by name or alias (case-insensitive).
// Returns an error if no driver is registered with that name.
func Get(nameOrAlias string) (Driver, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()

	// Lookup is case-insensitive since drivers are registered lowercase
	d, exists := drivers[strings.ToLower(nameOrAlias)]
	if !exists {
		return nil, fmt.Errorf("unknown database driver: %q (available: %v)", nameOrAlias, Available())
	}
	return d, nil
}

// Available returns a sorted list of registered driver names.
// This includes only primary names, not aliases.
func Available() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()

	// Collect unique primary names
	seen := make(map[string]bool)
	for _, d := range drivers {
		seen[d.Name()] = true
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// IsRegistered returns true if a driver with the given name or alias exists (case-insensitive).
func IsRegistered(nameOrAlias string) bool {
	registryMu.RLock()
	defer registryMu.RUnlock()
	_, exists := drivers[strings.ToLower(nameOrAlias)]
	return exists
}
