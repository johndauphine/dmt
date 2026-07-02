package tui

import (
	"os/exec"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

// GitInfo holds git repository information
type GitInfo struct {
	Branch string
	Status string // "Clean" or "Dirty"
}

// gitInfoMsg carries a GetGitInfo result back to Update.
type gitInfoMsg GitInfo

// gitInfoCmd runs GetGitInfo off the Bubble Tea event loop. GetGitInfo execs
// blocking `git` subprocesses (`git status` routinely takes seconds on large
// repos or NFS/WSL2 mounts); calling it inline in Update() froze the whole UI —
// keystrokes, output rendering, and migration cancellation — every tick (#559).
func gitInfoCmd() tea.Cmd {
	return func() tea.Msg {
		return gitInfoMsg(GetGitInfo())
	}
}

// GetGitInfo retrieves the current git branch and status
func GetGitInfo() GitInfo {
	info := GitInfo{
		Branch: "no-git",
		Status: "",
	}

	// Check branch
	cmd := exec.Command("git", "rev-parse", "--abbrev-ref", "HEAD")
	out, err := cmd.Output()
	if err == nil {
		info.Branch = strings.TrimSpace(string(out))
	} else {
		// Not a git repo or git not found
		return info
	}

	// Check status
	cmd = exec.Command("git", "status", "--porcelain")
	out, err = cmd.Output()
	if err == nil {
		if len(strings.TrimSpace(string(out))) > 0 {
			info.Status = "Dirty"
		} else {
			info.Status = "Clean"
		}
	}

	return info
}
