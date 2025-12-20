package tui

import (
	"os/exec"
	"strings"
)

// GitInfo holds git repository information
type GitInfo struct {
	Branch string
	Status string // "Clean" or "Dirty"
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
