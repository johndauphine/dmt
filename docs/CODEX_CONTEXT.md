# Context Update

- Feature scope: Added encrypted profile storage in SQLite, TUI/CLI profile management, and run-history origin tracking. Profiles are encrypted at rest with AES-GCM using DMT_MASTER_KEY (base64 32-byte key). YAML remains primary for CI/CD.

- Profile storage:
  - New table profiles with name, description, config_enc, timestamps.
  - Encryption logic in internal/checkpoint/profiles.go (SaveProfile(name, description, config) / GetProfile / ListProfiles / DeleteProfile).
  - profile.name and profile.description added to config schema (internal/config/config.go).
  - Profile name inference: if --name omitted, uses profile.name or filename stem (CLI + TUI).

- Run history:
  - runs table now stores profile_name and config_path.
  - History output includes Origin column (profile or config file).
  - Run detail shows Origin: profile:<name> or config:<path>.

- TUI:
  - Added /resume command and help text.
  - /profile commands in TUI now list descriptions.
  - Wizard loading suppresses permission warnings to avoid breaking prompts.
  - Styling updated to match Gemini look: darker palette, purple accents, gold input border, and transparent background.

- Status behavior:
  - /status no longer reports running when there are 0 running tasks; shows “No active migration (last incomplete run …)” and suggests /resume.

- Docs:
  - README documents encrypted profiles, DMT_MASTER_KEY, profile.name/description, /resume, and migration.data_dir.

- Key files:
  - internal/checkpoint/profiles.go (encryption + profile CRUD)
  - internal/checkpoint/state.go (schema migration, run fields)
  - internal/config/config.go (profile metadata, LoadWithOptions, default data dir auto-create)
  - internal/tui/model.go + internal/tui/styles.go (TUI commands + styling)
  - cmd/migrate/main.go (CLI profile commands + --profile)

- Commits on main:
  - 6ecc69a Add encrypted SQLite profiles and record run origin
  - 1122042 Add profile metadata and description display
  - 21147b2 Improve TUI UX and profile metadata handling

- Tests run:
  - validate against config.yaml (success).
  - Full run --profile + history cycle (success).

- Notes:
  - Sandbox required HOME override for profile tests, and explicit key from .bashrc.
  - Screenshot file was deleted.
