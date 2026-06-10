package config

import (
	"runtime/debug"
	"testing"
)

// TestApplyRuntimeMemoryLimit_SetsGOMEMLIMIT verifies the #462 wiring: the
// effective budget becomes the runtime's soft memory limit so GC pacing and
// pipeline buffer sizing work from the same number.
func TestApplyRuntimeMemoryLimit_SetsGOMEMLIMIT(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "") // isolate from an inherited operator env var
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	c := &Config{}
	c.autoConfig.EffectiveMaxMemoryMB = 512
	c.ApplyRuntimeMemoryLimit()

	if got := debug.SetMemoryLimit(-1); got != int64(512)<<20 {
		t.Fatalf("memory limit = %d, want %d", got, int64(512)<<20)
	}
}

func TestApplyRuntimeMemoryLimit_NoBudgetIsNoOp(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "")
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	c := &Config{}
	c.ApplyRuntimeMemoryLimit()

	if got := debug.SetMemoryLimit(-1); got != prev {
		t.Fatalf("memory limit changed to %d with no budget configured; want unchanged %d", got, prev)
	}
}

// TestApplyRuntimeMemoryLimit_EnvWins verifies an operator-supplied
// GOMEMLIMIT is left untouched — the runtime already honors it and the
// derived budget must not override an explicit choice.
func TestApplyRuntimeMemoryLimit_EnvWins(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "1GiB")
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	c := &Config{}
	c.autoConfig.EffectiveMaxMemoryMB = 512
	c.ApplyRuntimeMemoryLimit()

	if got := debug.SetMemoryLimit(-1); got != prev {
		t.Fatalf("memory limit = %d, want untouched %d when GOMEMLIMIT env is set", got, prev)
	}
}
