package secrets

import "fmt"

// SecretsNotFoundError is returned when the secrets file doesn't exist
type SecretsNotFoundError struct {
	Path string
}

func (e *SecretsNotFoundError) Error() string {
	return fmt.Sprintf(`secrets file not found: %s

To create a secrets file, run:
  dmt init-secrets

Or create %s manually with:

ai:
  default_provider: anthropic
  providers:
    anthropic:
      api_key: "your-api-key"

encryption:
  master_key: "your-master-key"
`, e.Path, e.Path)
}

// GenerateTemplate returns a template secrets file content with no AI
// section. This is the default for `dmt init-secrets` — dmt runs
// migrations deterministically (no AI required) since #167. AI remains
// available as an optional enhancement; users who want it can pass
// `init-secrets --with-ai` to seed the AI provider section, or paste
// the snippet returned by AISection() into their secrets file by hand.
func GenerateTemplate() string {
	return generateTemplate(false)
}

// GenerateTemplateWithAI returns a template secrets file content with
// the AI provider section seeded. Used by `dmt init-secrets --with-ai`
// (#174) so users who want AI features can opt in at template-creation
// time without editing the file.
func GenerateTemplateWithAI() string {
	return generateTemplate(true)
}

// AISection returns the AI portion of the secrets template as a
// standalone snippet, suitable for appending to a previously
// AI-free file or for inclusion in documentation.
func AISection() string {
	return aiSection
}

const aiSection = `ai:
  default_provider: anthropic  # Which provider to use by default

  providers:
    # Cloud providers (require API key)
    anthropic:
      api_key: ""  # Get from https://console.anthropic.com/
      model: "claude-sonnet-5"  # optional
      # max_requests: 100  # optional, max uncached AI calls per dmt process/run

    openai:
      api_key: ""  # Get from https://platform.openai.com/
      model: "gpt-5.5"  # optional
      # max_requests: 100  # optional, max uncached AI calls per dmt process/run

    gemini:
      api_key: ""  # Get from https://makersuite.google.com/
      model: "gemini-2.0-flash"  # optional
      # max_requests: 100  # optional, max uncached AI calls per dmt process/run

    # Local providers (no API key needed)
    ollama:
      base_url: "http://localhost:11434"
      model: "llama3"
      # context_window: 8192  # optional, defaults to 8192 (conservative)
      # max_tokens: 16000     # optional, max output tokens (default: 16000 for local, 4000 for cloud)
      # max_requests: 100     # optional, max uncached AI calls per dmt process/run
      # Common context_window values:
      # - llama3:8b, llama3.2: 8192
      # - llama3:70b, llama3.1: 131072 (128K)
      # - qwen2.5, deepseek: 32768 (32K)
      # - mistral, mixtral: 8192-32768 (varies)

    lmstudio:
      base_url: "http://localhost:1234"
      model: "local-model"
      # context_window: 8192  # optional, configure based on your model
      # max_tokens: 16000     # optional, increase for reasoning models (e.g., Qwen3, GPT-OSS)
      # max_requests: 100     # optional, max uncached AI calls per dmt process/run
`

func generateTemplate(withAI bool) string {
	const headerNoAI = `# DMT Secrets Configuration
# This file contains sensitive configuration that should not be committed to version control.
# Permissions should be restricted: chmod 600 ~/.secrets/dmt-config.yaml

# AI features are OPTIONAL. dmt runs migrations deterministically without
# an AI provider (type mapping, error diagnosis, DB tuning, and runtime
# parameter adjustment all use built-in rule catalogs since #167).
#
# To enable AI later WITHOUT losing the values below, append an ai:
# section to this file. See the AI provider snippet at:
#   https://github.com/johndauphine/dmt#optional-ai-enhancements
#
# (Do NOT run "dmt init-secrets --force --with-ai" — that overwrites
# this file and would erase your master_key, slack webhook, and any
# other values you've set, which can render encrypted profiles
# unusable.)

`
	const headerWithAI = `# DMT Secrets Configuration
# This file contains sensitive configuration that should not be committed to version control.
# Permissions should be restricted: chmod 600 ~/.secrets/dmt-config.yaml

`
	header := headerNoAI
	aiBlock := ""
	if withAI {
		header = headerWithAI
		aiBlock = aiSection + "\n"
	}

	body := `encryption:
  master_key: ""  # Used for encrypting profiles, generate with: openssl rand -base64 32

notifications:
  slack:
    webhook_url: ""  # Slack webhook URL for migration notifications

# Global migration defaults (can be overridden per-migration)
migration_defaults:
  # Performance settings (auto-tuned if not set)
  # workers: 4                    # Parallel workers (default: based on CPU cores)
  # max_memory_mb: 0              # Max memory in MB (default: 70% of available)
  # read_ahead_buffers: 8         # Chunks to buffer ahead
  # write_ahead_writers: 2        # Parallel writers per job
  # parallel_readers: 2           # Parallel readers per job

  # Schema creation defaults
  create_indexes: true            # Create non-PK indexes
  create_foreign_keys: true       # Create FK constraints
  create_check_constraints: false # Create CHECK constraints

  # Consistency and validation
  strict_consistency: false       # Pin each table to one stable source transaction (#640)
  sample_validation: false        # Validate sample data after migration
  sample_size: 100                # Rows to sample for validation

  # Checkpoint and recovery
  checkpoint_frequency: 10        # Save progress every N chunks
  max_retries: 3                  # Retry failed tables N times
  history_retention_days: 30      # Keep run history for N days

  # Runtime tuning (deterministic rule-based controller since PR #195; the
  # field is named ai_adjust for backward compatibility and is being
  # renamed to runtime_tuning in #211).
  ai_adjust: true                 # Enable mid-migration parameter adjustment
  ai_adjust_interval: "30s"       # How often the controller evaluates metrics
`
	return header + aiBlock + body
}
