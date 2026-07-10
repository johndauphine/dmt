package checkpoint

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const transferTaskKeyPrefix = "transfer:v2:"

var ErrLegacyTaskIdentity = errors.New("legacy delimiter-encoded transfer task identity")

func legacyTaskIdentityError(runID string) error {
	return fmt.Errorf("%w in run %q; its quoted identifiers cannot be migrated unambiguously—finish or abandon that checkpoint and start a fresh run", ErrLegacyTaskIdentity, runID)
}

// TransferTaskIdentity is the collision-free correctness identity for one
// table transfer task. Database identifiers are opaque strings; PartitionID
// is nil for the table-level task and non-nil for a partition task.
type TransferTaskIdentity struct {
	Schema      string `json:"schema"`
	Table       string `json:"table"`
	PartitionID *int   `json:"partition_id,omitempty"`
}

// TaskKey is the display/backward-compatibility key stored alongside the
// structured columns. Correctness queries must use the structured fields.
func (id TransferTaskIdentity) TaskKey() string {
	b, _ := json.Marshal(id)
	return transferTaskKeyPrefix + base64.RawURLEncoding.EncodeToString(b)
}

func (id TransferTaskIdentity) LegacyTaskKey() string {
	key := fmt.Sprintf("transfer:%s.%s", id.Schema, id.Table)
	if id.PartitionID != nil {
		key += fmt.Sprintf(":p%d", *id.PartitionID)
	}
	return key
}

// ParseTransferTaskKey decodes only the canonical v2 form. Legacy delimiter
// keys are deliberately not parsed because quoted identifiers make them
// ambiguous (#646).
func ParseTransferTaskKey(key string) (TransferTaskIdentity, bool) {
	if len(key) <= len(transferTaskKeyPrefix) || key[:len(transferTaskKeyPrefix)] != transferTaskKeyPrefix {
		return TransferTaskIdentity{}, false
	}
	b, err := base64.RawURLEncoding.DecodeString(key[len(transferTaskKeyPrefix):])
	if err != nil {
		return TransferTaskIdentity{}, false
	}
	var id TransferTaskIdentity
	if err := json.Unmarshal(b, &id); err != nil || id.Table == "" {
		return TransferTaskIdentity{}, false
	}
	// Accept exactly one representation for each identity. Without this
	// round-trip check, reordered JSON fields or an explicit null partition
	// could produce a second FileState map key for the same structured task.
	if id.TaskKey() != key {
		return TransferTaskIdentity{}, false
	}
	return id, true
}

func TransferTaskDisplayName(key string) string {
	if identity, ok := ParseTransferTaskKey(key); ok {
		return identity.Schema + "." + identity.Table
	}
	return ""
}

// TransferTaskDisplayLabel renders a canonical task key for operators without
// using that human-readable form as a correctness identity.
func TransferTaskDisplayLabel(key string) string {
	identity, ok := ParseTransferTaskKey(key)
	if !ok {
		return ""
	}
	label := identity.Schema + "." + identity.Table
	if identity.PartitionID != nil {
		label += fmt.Sprintf(" (partition %d)", *identity.PartitionID)
	}
	return label
}

// StructuredTaskBackend is implemented by production backends. Keeping it
// separate from StateBackend lets narrow test fakes retain their small API;
// production paths fail over to legacy methods only for those fakes.
type StructuredTaskBackend interface {
	CreateTransferTask(runID string, identity TransferTaskIdentity) (int64, error)
	CountTransferPartitionTasks(runID, schema, table string) (int, error)
	ClearTransferPartitionProgress(runID, schema, table string) error
	GetTransferPartitionProgressSummary(runID, schema, table string) (PartitionProgressSummary, error)
	MarkTransferTaskComplete(runID string, identity TransferTaskIdentity) error
}

// AtomicTransferCompletionBackend commits the aggregate table completion and
// optional incremental watermark as one backend transaction/atomic file save.
type AtomicTransferCompletionBackend interface {
	CompleteTransferTask(runID string, identity TransferTaskIdentity, targetSchema string, watermark *time.Time) error
}

func TransferTaskKeyForBackend(backend StateBackend, identity TransferTaskIdentity) string {
	if _, ok := backend.(StructuredTaskBackend); ok {
		return identity.TaskKey()
	}
	return identity.LegacyTaskKey()
}

func CreateTransferTask(backend StateBackend, runID string, identity TransferTaskIdentity) (int64, string, error) {
	if structured, ok := backend.(StructuredTaskBackend); ok {
		id, err := structured.CreateTransferTask(runID, identity)
		return id, identity.TaskKey(), err
	}
	key := identity.LegacyTaskKey()
	id, err := backend.CreateTask(runID, "transfer", key)
	return id, key, err
}

func CountTransferPartitionTasks(backend StateBackend, runID, schema, table string) (int, error) {
	if structured, ok := backend.(StructuredTaskBackend); ok {
		return structured.CountTransferPartitionTasks(runID, schema, table)
	}
	return backend.CountPartitionTasks(runID, TransferTaskIdentity{Schema: schema, Table: table}.LegacyTaskKey())
}

func ClearTransferPartitionProgress(backend StateBackend, runID, schema, table string) error {
	if structured, ok := backend.(StructuredTaskBackend); ok {
		return structured.ClearTransferPartitionProgress(runID, schema, table)
	}
	return backend.ClearPartitionTransferProgress(runID, TransferTaskIdentity{Schema: schema, Table: table}.LegacyTaskKey())
}

func GetTransferPartitionProgressSummary(backend StateBackend, runID, schema, table string) (PartitionProgressSummary, error) {
	if structured, ok := backend.(StructuredTaskBackend); ok {
		return structured.GetTransferPartitionProgressSummary(runID, schema, table)
	}
	return backend.GetPartitionTransferProgressSummary(runID, TransferTaskIdentity{Schema: schema, Table: table}.LegacyTaskKey())
}

func MarkTransferTaskComplete(backend StateBackend, runID string, identity TransferTaskIdentity) error {
	if structured, ok := backend.(StructuredTaskBackend); ok {
		return structured.MarkTransferTaskComplete(runID, identity)
	}
	return backend.MarkTaskComplete(runID, identity.LegacyTaskKey())
}

func CompleteTransferTask(backend StateBackend, runID string, identity TransferTaskIdentity, targetSchema string, watermark *time.Time) error {
	if atomic, ok := backend.(AtomicTransferCompletionBackend); ok {
		return atomic.CompleteTransferTask(runID, identity, targetSchema, watermark)
	}
	// Compatibility path for narrow test fakes. Production backends implement
	// AtomicTransferCompletionBackend and never split these writes.
	if watermark != nil {
		if err := backend.UpdateSyncTimestamp(identity.Schema, identity.Table, targetSchema, *watermark); err != nil {
			return err
		}
	}
	return MarkTransferTaskComplete(backend, runID, identity)
}
