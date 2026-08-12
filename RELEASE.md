# Releasing dmt

This document is the canonical process for cutting a release. The matching policy — what bump level a change requires, what's stable, the deprecation cycle — lives in [VERSIONING.md](./VERSIONING.md).

## Cutting a release

The release is gated on green CI. Don't tag a commit whose `ci.yml` or `integration.yml` is red.

1. **Pick the version** per [VERSIONING.md](./VERSIONING.md) (MAJOR / MINOR / PATCH).
   The tag major, `go.mod` module suffix, and `internal/version.Version` major
   must stay synchronized. For v5, use `github.com/johndauphine/dmt/v5` and
   `v5.*.*` tags; a future major requires updating the module suffix and all
   intra-project imports before its first release.
2. **Update `CHANGELOG.md`**:
   - Move every entry under `## [Unreleased]` to a new versioned section: `## [1.2.3] - YYYY-MM-DD`
   - Leave `## [Unreleased]` empty for future PRs to populate
   - The version header MUST match the tag you're about to push (the release workflow validates this)
3. **Open a release PR** with the CHANGELOG edit. Merge it green.
4. **Tag** on the merge commit:
   ```bash
   git checkout main && git pull --ff-only
   git tag -a v1.2.3 -m "v1.2.3"
   git push origin v1.2.3
   ```
5. **The `release.yml` workflow takes over**:
   - Validates the tag matches a CHANGELOG section header
   - Cross-compiles binaries for `darwin-amd64`, `darwin-arm64`, `linux-amd64`, `linux-arm64`, `windows-amd64`
   - Generates SHA-256 checksums
   - Publishes a GitHub Release with the binaries + checksums attached
6. **Verify the release** — download a binary, run `./dmt --version`, confirm it reports the tagged version.

## Release cadence

- **Patch** — as needed for bugfixes. No fixed schedule; cut when a real fix needs to ship.
- **Minor** — monthly target on the first Monday, batching the `[Unreleased]` accumulation.
- **Major** — announced at least 30 days in advance via a `[major-release]` issue on GitHub. The 30 days give operators on previous-version pins time to plan the upgrade.

The `[Unreleased]` section IS the queue. If it's been empty for a month, no minor release happens.

## Who can release

Currently: **@johndauphine only**. Practically, this is enforced by branch protection (only the repo owner can push tags matching `v*.*.*` to `main`).

Future: when the project gains co-maintainers, this section becomes the formal list. Adding/removing a releaser is itself a release-process change and requires a PR to this file.

## Hotfix path

For a critical bug requiring a same-day patch on an older release line (`v1.2.3` shipped, `main` is at `v1.4.0`, customer hits the bug on `v1.2.x` and can't upgrade):

1. `git checkout -b hotfix/v1.2.4 v1.2.3` (branch from the old tag, not main)
2. Cherry-pick or write the minimal fix; PR it; merge into the hotfix branch
3. Tag `v1.2.4` on the hotfix branch
4. Push tag — release workflow handles the rest
5. **Forward-port the fix to main** if it isn't already there

Don't merge the hotfix branch INTO main — main is ahead. Forward-porting is the responsibility of the releaser.

## Pre-release / RC

For releases that need a soak period (especially the eventual `v1.0.0`):

- Tag as `v1.0.0-rc.1`, `v1.0.0-rc.2`, etc.
- Release workflow publishes these as **pre-releases** (the GitHub UI marks them; they're not surfaced as "latest")
- After ~1 week of soak with no blocking issues, tag the final `v1.0.0`

## Release notes

GitHub Release notes are generated FROM the `CHANGELOG.md` section by the workflow. Anything that should appear in the release announcement must be in the CHANGELOG entry. Don't write release notes separately.

## What goes IN a release

- The `dmt` binary cross-compiled for each supported OS/arch
- `SHA-256` checksums for every binary
- A `CHANGELOG.md` excerpt for the released version
- (Optional, post-1.0) Docker image at `ghcr.io/johndauphine/dmt:v1.2.3`
- (Optional, post-1.0) Homebrew tap and Scoop bucket updates

## What does NOT go in a release

- No source archive — GitHub auto-generates `.zip`/`.tar.gz` from the tag, which is enough
- No npm/PyPI packages — dmt is a Go binary, not a library
- No `latest` floating tag — operators should pin a specific version. Helps when the next release breaks their workflow.
