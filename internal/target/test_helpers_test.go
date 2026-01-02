package target

// intPtr returns a pointer to an int value.
// This helper is useful for test cases that need optional partition IDs.
func intPtr(i int) *int {
	return &i
}
