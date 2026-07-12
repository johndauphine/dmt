package aicopilot

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/johndauphine/dmt/internal/logging"
)

var performanceNumberPattern = regexp.MustCompile(`[-+]?\d[\d,]*(?:\.\d+)?`)
var performanceWordPattern = regexp.MustCompile(`[a-z0-9]+`)
var performanceANSIPattern = regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]`)
var performanceC1ANSIPattern = regexp.MustCompile(`\x{009b}[0-?]*[ -/]*[@-~]`)

func ParsePerformanceExplanation(raw string, payload PerformancePayload) (*PerformanceExplanation, error) {
	body := extractJSONObject(strings.TrimSpace(raw))
	var explanation PerformanceExplanation
	if err := json.Unmarshal([]byte(body), &explanation); err != nil {
		return nil, fmt.Errorf("parsing AI performance explanation JSON: %w", err)
	}

	allowedTargets := allowedFindingTargetsFromPayload(payload)
	generalNumbers := numbersForPerformanceGeneralText(payload)
	explanation.Summary = sanitizePerformanceGeneralText(explanation.Summary, 800, generalNumbers, payload)
	if explanation.Summary == "" {
		explanation.Summary = deterministicPerformanceSummary(payload)
	}
	explanation.Error = sanitizePerformanceGeneralText(explanation.Error, 800, generalNumbers, payload)

	findings := explanation.Findings[:0]
	for _, f := range explanation.Findings {
		f.Knob = strings.ToLower(strings.TrimSpace(f.Knob))
		if !allowedTargets[f.Knob] {
			continue
		}
		targetEvidenceNumbers := numbersForPerformanceTarget(payload, f.Knob, true)
		targetActionNumbers := numbersForPerformanceTarget(payload, f.Knob, false)
		dbExactValues := dbTuningExactValuesForTarget(payload, f.Knob)
		dbParameter := dbTuningParameterFromTarget(f.Knob)
		f.Category = normalizePerformanceCategory(f.Category)
		f.Rationale = sanitizePerformanceFindingText(f.Rationale, 600, targetEvidenceNumbers, dbExactValues, dbParameter, payload, true)
		f.NextAction = sanitizePerformanceFindingText(f.NextAction, 600, targetActionNumbers, dbExactValues, dbParameter, payload, false)
		f.Source = "ai_advisory"
		if f.Rationale == "" && f.NextAction == "" {
			continue
		}
		evidence := f.Evidence[:0]
		for _, item := range f.Evidence {
			item = sanitizePerformanceFindingText(item, 300, targetEvidenceNumbers, dbExactValues, dbParameter, payload, true)
			if item != "" {
				evidence = append(evidence, item)
			}
		}
		if len(evidence) > 5 {
			evidence = evidence[:5]
		}
		f.Evidence = evidence
		if len(f.Evidence) == 0 {
			continue
		}
		findings = append(findings, f)
		if len(findings) == 5 {
			break
		}
	}
	explanation.Findings = findings

	notes := explanation.Notes[:0]
	for _, note := range explanation.Notes {
		note = sanitizePerformanceGeneralText(note, 400, generalNumbers, payload)
		if note != "" {
			notes = append(notes, note)
		}
	}
	explanation.Notes = notes
	return &explanation, nil
}

func sanitizePerformanceText(s string, max int, allowedNumbers map[string]bool) string {
	s = limitText(sanitizePerformanceDisplayText(logging.Scrub(s)), max)
	if s == "" || hasUnsupportedPerformanceNumber(s, allowedNumbers) {
		return ""
	}
	return s
}

func sanitizePerformanceDisplayText(s string) string {
	s = performanceC1ANSIPattern.ReplaceAllString(s, " ")
	s = performanceANSIPattern.ReplaceAllString(s, " ")
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f) {
			return ' '
		}
		return r
	}, s)
	return strings.Join(strings.Fields(s), " ")
}

func sanitizePerformanceGeneralText(s string, max int, allowedNumbers map[string]bool, payload PerformancePayload) string {
	s = sanitizePerformanceText(s, max, allowedNumbers)
	if s == "" {
		return s
	}
	if mentionsUnapprovedDBTuningValueToken(s, payload) {
		return ""
	}
	if mentionsDBTuningValueSynonym(s) {
		return ""
	}
	if mentionsPerformanceKnobWithUnsupportedNumber(s, payload, false) {
		return ""
	}
	if invalidConfigAssignmentInText(s, aiConfigChangeContext{}) != "" {
		return ""
	}
	params := dbTuningParametersMentioned(s, payload)
	if len(params) == 0 && mentionsDBTuningExactValue(s, payload) {
		return ""
	}
	for _, param := range params {
		values := dbTuningExactValuesForParam(payload, param)
		if len(values) == 0 || !usesOnlyExactDBTuningValues(s, values, param) {
			return ""
		}
	}
	return s
}

func sanitizePerformanceFindingText(s string, max int, allowedNumbers map[string]bool, exactValues []string, dbParameter string, payload PerformancePayload, includeRuntimeMetrics bool) string {
	s = sanitizePerformanceText(s, max, allowedNumbers)
	if s == "" {
		return s
	}
	if len(exactValues) > 0 {
		if !usesOnlyExactDBTuningValues(s, exactValues, dbParameter) {
			return ""
		}
		return s
	}
	if containsDBTuningText(s, payload) || mentionsDBTuningValueSynonym(s) {
		return ""
	}
	if mentionsPerformanceKnobWithUnsupportedNumber(s, payload, includeRuntimeMetrics) {
		return ""
	}
	if invalidConfigAssignmentInText(s, aiConfigChangeContext{}) != "" {
		return ""
	}
	return s
}

func normalizePerformanceCategory(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "baseline", "history", "regression", "exploration", "runtime", "memory", "connections", "db_tuning", "other":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return ""
	}
}

func usesOnlyExactDBTuningValues(s string, exactValues []string, dbParameter string) bool {
	remaining := strings.ToLower(s)
	matched := false
	values := append([]string(nil), exactValues...)
	sort.SliceStable(values, func(i, j int) bool {
		return len(values[i]) > len(values[j])
	})
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		pattern := exactDBTuningValuePattern(value)
		if pattern.MatchString(remaining) {
			matched = true
			remaining = pattern.ReplaceAllString(remaining, "${1} ${2}")
		}
	}
	return matched &&
		len(performanceNumberPattern.FindAllString(remaining, -1)) == 0 &&
		!hasUnapprovedDBTuningValueCue(remaining, dbParameter) &&
		!hasUnapprovedDBTuningToken(remaining, dbParameter) &&
		!hasUnapprovedDBTuningResidualWord(remaining, dbParameter)
}

func exactDBTuningValuePattern(value string) *regexp.Regexp {
	quoted := regexp.QuoteMeta(value)
	if len(value) <= 2 && isAlphaString(value) {
		return regexp.MustCompile(`(^|[^[:alnum:]_])(?:(?:current|recommend|recommends|recommended|from|to|set)\s+` + quoted + `|` + quoted + `\s+(?:to|from)|=\s*` + quoted + `)([^[:alnum:]_]|$)`)
	}
	return regexp.MustCompile(`(^|[^[:alnum:]_])` + quoted + `([^[:alnum:]_]|$)`)
}

func isAlphaString(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < 'a' || r > 'z' {
			return false
		}
	}
	return true
}

func hasUnapprovedDBTuningValueCue(s, dbParameter string) bool {
	tokens := dbTuningTokens(s)
	for i, token := range tokens {
		if !isDBTuningValueCue(token) {
			continue
		}
		for j := i + 1; j < len(tokens); j++ {
			candidate := tokens[j]
			if isDBTuningValueCue(candidate) || isAllowedDBTuningResidualToken(candidate, dbParameter) {
				continue
			}
			return true
		}
	}
	return false
}

func hasUnapprovedDBTuningToken(s, dbParameter string) bool {
	for _, token := range dbTuningTokens(s) {
		if strings.Contains(token, "_") && !isAllowedDBTuningResidualToken(token, dbParameter) {
			return true
		}
	}
	return false
}

func hasUnapprovedDBTuningResidualWord(s, dbParameter string) bool {
	for _, token := range dbTuningTokens(s) {
		if !isAllowedDBTuningResidualToken(token, dbParameter) {
			return true
		}
	}
	return false
}

func dbTuningTokens(s string) []string {
	return regexp.MustCompile(`[a-z][a-z0-9_]*`).FindAllString(strings.ToLower(s), -1)
}

func isDBTuningValueCue(token string) bool {
	switch token {
	case "change", "changed", "changing", "current", "recommend", "recommends", "recommended", "set", "should", "try", "use", "uses", "using", "from", "to":
		return true
	default:
		return false
	}
}

func mentionsDBTuningDirective(s string) bool {
	lower := strings.ToLower(s)
	return strings.Contains(lower, "db_tuning") ||
		strings.Contains(lower, "database tuning") ||
		strings.Contains(lower, "set ") ||
		strings.Contains(lower, "recommend") ||
		containsDBTuningValueCue(lower)
}

func containsDBTuningValueCue(s string) bool {
	for _, token := range dbTuningTokens(s) {
		if isDBTuningValueCue(token) {
			return true
		}
	}
	return false
}

func containsDBTuningText(s string, payload PerformancePayload) bool {
	return len(dbTuningParametersMentioned(s, payload)) > 0 ||
		mentionsDBTuningExactValue(s, payload) ||
		mentionsUnapprovedDBTuningValueToken(s, payload)
}

func mentionsDBTuningExactValue(s string, payload PerformancePayload) bool {
	phrase := canonicalPerformanceIdentifierText(s)
	for _, tuning := range payload.DatabaseTuning {
		for _, rec := range tuning.Recommendations {
			for _, value := range []string{rec.CurrentValue, rec.RecommendedValue} {
				value = strings.ToLower(strings.TrimSpace(value))
				if value != "" && (exactDBTuningValuePattern(value).MatchString(strings.ToLower(s)) ||
					strings.Contains(phrase, canonicalPerformanceIdentifierText(value)) ||
					strings.Contains(phrase, canonicalPerformanceIdentifierText(spacedDBTuningUnitValue(value)))) {
					return true
				}
			}
		}
	}
	return false
}

func spacedDBTuningUnitValue(value string) string {
	var b strings.Builder
	b.Grow(len(value) + 2)
	var prev rune
	for _, r := range value {
		if prev >= '0' && prev <= '9' && ((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')) {
			b.WriteByte(' ')
		}
		b.WriteRune(r)
		prev = r
	}
	return b.String()
}

func mentionsUnapprovedDBTuningValueToken(s string, payload PerformancePayload) bool {
	known := map[string]bool{}
	for _, tuning := range payload.DatabaseTuning {
		for _, rec := range tuning.Recommendations {
			for _, value := range []string{rec.CurrentValue, rec.RecommendedValue} {
				value = strings.ToLower(strings.TrimSpace(value))
				if value != "" {
					known[value] = true
				}
			}
		}
	}
	for _, token := range dbTuningTokens(s) {
		if dbTuningTokenLooksEnumValue(token) && !known[token] {
			return true
		}
		switch token {
		case "local", "logical", "minimal", "off", "on", "remote_apply", "remote_write", "replica":
			if !known[token] {
				return true
			}
		}
	}
	return false
}

func dbTuningTokenLooksEnumValue(token string) bool {
	switch token {
	case "bulk_logged", "remote_apply", "remote_write", "single_user", "multi_user", "read_committed_snapshot":
		return true
	default:
		return false
	}
}

func mentionsDBTuningValueSynonym(s string) bool {
	phrase := normalizedPerformancePhraseText(s)
	return strings.Contains(phrase, " wal mode ") ||
		strings.Contains(phrase, " bulk logged ") ||
		strings.Contains(phrase, " commit mode ") ||
		strings.Contains(phrase, " logical ") ||
		strings.Contains(phrase, " multi user ") ||
		strings.Contains(phrase, " read committed snapshot ") ||
		strings.Contains(phrase, " remote apply ") ||
		strings.Contains(phrase, " remote write ") ||
		strings.Contains(phrase, " replica ") ||
		strings.Contains(phrase, " single user ") ||
		strings.Contains(phrase, " minimal ")
}

func dbTuningParametersMentioned(s string, payload PerformancePayload) []string {
	canonical := canonicalPerformanceIdentifierText(s)
	var out []string
	seen := map[string]bool{}
	for _, tuning := range payload.DatabaseTuning {
		for _, rec := range tuning.Recommendations {
			param := strings.ToLower(strings.TrimSpace(rec.Parameter))
			if param == "" || seen[param] || !strings.Contains(canonical, canonicalPerformanceIdentifierText(param)) {
				continue
			}
			seen[param] = true
			out = append(out, param)
		}
	}
	return out
}

func isAllowedDBTuningResidualToken(token, dbParameter string) bool {
	if token == "" {
		return true
	}
	if token == strings.ToLower(strings.TrimSpace(dbParameter)) {
		return true
	}
	switch token {
	case "a", "an", "and", "advisory", "database", "database_tuning", "db", "db_tuning",
		"current", "deterministic", "evidence", "for", "from", "in", "includes", "is", "it", "of",
		"or", "parameter", "payload", "role", "source", "supports", "target", "that",
		"the", "this", "to", "tuning", "value", "values", "was", "with",
		"recommend", "recommends", "recommended", "set", "use", "uses", "using":
		return true
	default:
		return false
	}
}

func hasUnsupportedPerformanceNumber(s string, allowed map[string]bool) bool {
	for _, match := range performanceNumberPattern.FindAllString(s, -1) {
		if !allowed[normalizePerformanceNumber(match)] {
			return true
		}
	}
	return false
}

func numbersFromPerformancePayload(payload PerformancePayload) map[string]bool {
	out := map[string]bool{}
	data, err := json.Marshal(payload)
	if err != nil {
		return out
	}
	for _, match := range performanceNumberPattern.FindAllString(string(data), -1) {
		out[normalizePerformanceNumber(match)] = true
	}
	return out
}

func numbersForPerformanceGeneralText(payload PerformancePayload) map[string]bool {
	out := map[string]bool{}
	add := func(v any) {
		for _, match := range performanceNumberPattern.FindAllString(fmt.Sprint(v), -1) {
			out[normalizePerformanceNumber(match)] = true
		}
	}
	addNonZero := func(v any) {
		if fmt.Sprint(v) == "0" || fmt.Sprint(v) == "0.0" {
			return
		}
		add(v)
	}
	addNonZero(payload.Workload.CPUCores)
	addNonZero(payload.Workload.MemoryGB)
	addNonZero(payload.Workload.AvailableMemoryMB)
	addNonZero(payload.Workload.MaxMemoryMB)
	addNonZero(payload.Workload.TotalTables)
	addNonZero(payload.Workload.TotalRows)
	addNonZero(payload.Workload.AvgRowBytes)
	addNonZero(payload.Workload.UncappedAvgRowBytes)
	addNonZero(payload.Workload.RepresentativeRowBytes)
	addNonZero(payload.Workload.SafetyRowBytes)
	addNonZero(payload.Workload.LargestTableBytes)
	addNonZero(payload.Workload.EstimatedMemoryMB)
	addPerformanceKnobNumbers(payload.DeterministicKnobs, addNonZero)
	for _, adjustment := range payload.RuntimeAdjustments {
		for _, value := range adjustment.Adjustments {
			addNonZero(value)
		}
		addRuntimeMetricNumber(adjustment.ThroughputBefore, add)
		addRuntimeMetricNumber(adjustment.ThroughputAfter, add)
		addRuntimeMetricNumber(adjustment.EffectPercent, add)
		addRuntimeMetricNumber(adjustment.CPUBefore, add)
		addRuntimeMetricNumber(adjustment.CPUAfter, add)
		addRuntimeMetricNumber(adjustment.MemoryBefore, add)
		addRuntimeMetricNumber(adjustment.MemoryAfter, add)
	}
	for _, tuning := range payload.DatabaseTuning {
		addNonZero(tuning.RecommendationCt)
		for _, rec := range tuning.Recommendations {
			addNonZero(rec.CurrentValue)
			addNonZero(rec.RecommendedValue)
			addNonZero(rec.Priority)
		}
	}
	addNonZero(payload.ObservedMetrics.MaxSourceConnections)
	addNonZero(payload.ObservedMetrics.MaxTargetConnections)
	addNonZero(payload.ObservedMetrics.RecentSuccessfulRuns)
	addNonZero(payload.ObservedMetrics.RunsWithRetries)
	addNonZero(payload.ObservedMetrics.TotalChunkRetries)
	addRuntimeMetricNumber(payload.ObservedMetrics.ChunkRetryRate, add)
	addRuntimeMetricNumber(payload.ObservedMetrics.AverageThroughputRows, add)
	addRuntimeMetricNumber(payload.ObservedMetrics.BestThroughputRows, add)
	addNonZero(payload.ObservedMetrics.RuntimeAdjustmentCount)
	return out
}

func mentionsPerformanceKnobWithUnsupportedNumber(s string, payload PerformancePayload, includeRuntimeMetrics bool) bool {
	lower := strings.ToLower(s)
	phrase := normalizedPerformancePhraseText(s)
	for _, knob := range allowedPerformanceKnobs() {
		if !strings.Contains(lower, knob) && !mentionsPerformanceKnobPhrase(phrase, knob) {
			continue
		}
		if containsNumberWord(phrase) {
			return true
		}
		allowed := numbersForPerformanceTarget(payload, knob, includeRuntimeMetrics)
		for _, match := range performanceNumberPattern.FindAllString(s, -1) {
			if !allowed[normalizePerformanceNumber(match)] {
				return true
			}
		}
	}
	return false
}

func containsNumberWord(phrase string) bool {
	for _, word := range []string{
		" couple ", " few ", " several ",
		" zero ", " one ", " two ", " three ", " four ", " five ", " six ", " seven ", " eight ", " nine ", " ten ",
		" eleven ", " twelve ", " thirteen ", " fourteen ", " fifteen ", " sixteen ", " seventeen ", " eighteen ", " nineteen ", " twenty ",
		" thirty ", " forty ", " fifty ", " sixty ", " seventy ", " eighty ", " ninety ",
		" dozen ", " dozens ", " score ", " scores ",
		" hundred ", " hundreds ", " thousand ", " thousands ", " million ", " millions ", " billion ", " billions ",
	} {
		if strings.Contains(phrase, word) {
			return true
		}
	}
	return false
}

func mentionsPerformanceKnobPhrase(phrase, knob string) bool {
	switch knob {
	case "workers":
		return strings.Contains(phrase, " worker count ") ||
			strings.Contains(phrase, " worker counts ") ||
			strings.Contains(phrase, " concurrency ") ||
			strings.Contains(phrase, " parallelism ")
	case "parallel_readers":
		return strings.Contains(phrase, " parallel readers ") || strings.Contains(phrase, " parallel reader ")
	case "read_ahead_buffers":
		return strings.Contains(phrase, " read ahead buffers ") || strings.Contains(phrase, " read ahead buffer ")
	case "write_ahead_writers":
		return strings.Contains(phrase, " write ahead writers ") || strings.Contains(phrase, " write ahead writer ")
	case "chunk_size":
		return strings.Contains(phrase, " chunk size ")
	case "max_partitions":
		return strings.Contains(phrase, " max partitions ") ||
			strings.Contains(phrase, " partition count ") ||
			strings.Contains(phrase, " partitions ")
	case "large_table_threshold":
		return strings.Contains(phrase, " large table threshold ") ||
			strings.Contains(phrase, " large table cutoff ")
	case "max_source_connections":
		return strings.Contains(phrase, " max source connections ") ||
			strings.Contains(phrase, " source connections ") ||
			strings.Contains(phrase, " source connection limit ")
	case "max_target_connections":
		return strings.Contains(phrase, " max target connections ") ||
			strings.Contains(phrase, " target connections ") ||
			strings.Contains(phrase, " target connection limit ")
	case "upsert_merge_chunk_size":
		return strings.Contains(phrase, " upsert merge chunk size ") ||
			strings.Contains(phrase, " merge chunk size ") ||
			strings.Contains(phrase, " upsert chunk size ")
	case "checkpoint_frequency":
		return strings.Contains(phrase, " checkpoint frequency ") ||
			strings.Contains(phrase, " checkpoint interval ")
	case "max_retries":
		return strings.Contains(phrase, " max retries ") ||
			strings.Contains(phrase, " retry limit ") ||
			strings.Contains(phrase, " retry limits ") ||
			strings.Contains(phrase, " retry count ") ||
			strings.Contains(phrase, " retries ")
	default:
		return false
	}
}

func canonicalPerformanceIdentifierText(s string) string {
	words := performanceWordPattern.FindAllString(strings.ToLower(s), -1)
	if len(words) == 0 {
		return "_"
	}
	return "_" + strings.Join(words, "_") + "_"
}

func normalizedPerformancePhraseText(s string) string {
	words := performanceWordPattern.FindAllString(strings.ToLower(s), -1)
	if len(words) == 0 {
		return " "
	}
	return " " + strings.Join(words, " ") + " "
}

func addPerformanceKnobNumbers(knobs PerformanceKnobs, add func(any)) {
	add(knobs.Workers)
	add(knobs.ChunkSize)
	add(knobs.ReadAheadBuffers)
	add(knobs.WriteAheadWriters)
	add(knobs.ParallelReaders)
	add(knobs.MaxPartitions)
	add(knobs.LargeTableThreshold)
	add(knobs.MaxSourceConnections)
	add(knobs.MaxTargetConnections)
	add(knobs.UpsertMergeChunkSize)
	add(knobs.CheckpointFrequency)
	add(knobs.MaxRetries)
}

func numbersForPerformanceTarget(payload PerformancePayload, target string, includeRuntimeMetrics bool) map[string]bool {
	out := map[string]bool{}
	add := func(v any) {
		for _, match := range performanceNumberPattern.FindAllString(fmt.Sprint(v), -1) {
			out[normalizePerformanceNumber(match)] = true
		}
	}
	addNonZero := func(v any) {
		if fmt.Sprint(v) == "0" || fmt.Sprint(v) == "0.0" {
			return
		}
		add(v)
	}
	addMemoryWorkload := func() {
		addNonZero(payload.Workload.RepresentativeRowBytes)
		addNonZero(payload.Workload.SafetyRowBytes)
		addNonZero(payload.Workload.EstimatedMemoryMB)
		addNonZero(payload.Workload.MaxMemoryMB)
	}
	switch strings.ToLower(strings.TrimSpace(target)) {
	case "workers":
		addNonZero(payload.DeterministicKnobs.Workers)
		addMemoryWorkload()
		if includeRuntimeMetrics {
			addNonZero(payload.Workload.CPUCores)
		}
		addRuntimeAdjustmentNumbers(payload, target, includeRuntimeMetrics, add)
		addRecentRunNumbers(payload, target, includeRuntimeMetrics, add)
	case "chunk_size":
		addNonZero(payload.DeterministicKnobs.ChunkSize)
		addMemoryWorkload()
		addRuntimeAdjustmentNumbers(payload, target, includeRuntimeMetrics, add)
		addRecentRunNumbers(payload, target, includeRuntimeMetrics, add)
	case "read_ahead_buffers":
		addNonZero(payload.DeterministicKnobs.ReadAheadBuffers)
		addMemoryWorkload()
		addRuntimeAdjustmentNumbers(payload, target, includeRuntimeMetrics, add)
		addRecentRunNumbers(payload, target, includeRuntimeMetrics, add)
	case "write_ahead_writers":
		addNonZero(payload.DeterministicKnobs.WriteAheadWriters)
		addMemoryWorkload()
		addRuntimeAdjustmentNumbers(payload, target, includeRuntimeMetrics, add)
		addRecentRunNumbers(payload, target, includeRuntimeMetrics, add)
	case "parallel_readers":
		addNonZero(payload.DeterministicKnobs.ParallelReaders)
		addRuntimeAdjustmentNumbers(payload, target, includeRuntimeMetrics, add)
		addRecentRunNumbers(payload, target, includeRuntimeMetrics, add)
	case "max_partitions":
		addNonZero(payload.DeterministicKnobs.MaxPartitions)
		addRuntimeAdjustmentNumbers(payload, target, includeRuntimeMetrics, add)
		addRecentRunNumbers(payload, target, includeRuntimeMetrics, add)
	case "large_table_threshold":
		addNonZero(payload.DeterministicKnobs.LargeTableThreshold)
		addRuntimeAdjustmentNumbers(payload, target, includeRuntimeMetrics, add)
		addRecentRunNumbers(payload, target, includeRuntimeMetrics, add)
	case "max_source_connections":
		addNonZero(payload.DeterministicKnobs.MaxSourceConnections)
		addRuntimeAdjustmentNumbers(payload, target, includeRuntimeMetrics, add)
		addRecentRunNumbers(payload, target, includeRuntimeMetrics, add)
	case "max_target_connections":
		addNonZero(payload.DeterministicKnobs.MaxTargetConnections)
		addRuntimeAdjustmentNumbers(payload, target, includeRuntimeMetrics, add)
		addRecentRunNumbers(payload, target, includeRuntimeMetrics, add)
	case "upsert_merge_chunk_size":
		addNonZero(payload.DeterministicKnobs.UpsertMergeChunkSize)
		addRuntimeAdjustmentNumbers(payload, target, includeRuntimeMetrics, add)
		addRecentRunNumbers(payload, target, includeRuntimeMetrics, add)
	case "checkpoint_frequency":
		addNonZero(payload.DeterministicKnobs.CheckpointFrequency)
		addRuntimeAdjustmentNumbers(payload, target, includeRuntimeMetrics, add)
		addRecentRunNumbers(payload, target, includeRuntimeMetrics, add)
	case "max_retries":
		addNonZero(payload.DeterministicKnobs.MaxRetries)
		addRuntimeAdjustmentNumbers(payload, target, includeRuntimeMetrics, add)
		addRecentRunNumbers(payload, target, includeRuntimeMetrics, add)
	default:
		const prefix = "db_tuning."
		target = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(target)), prefix)
		if target == "" {
			return out
		}
		for _, tuning := range payload.DatabaseTuning {
			for _, rec := range tuning.Recommendations {
				if strings.ToLower(strings.TrimSpace(rec.Parameter)) != target {
					continue
				}
				add(rec.CurrentValue)
				add(rec.RecommendedValue)
			}
		}
	}
	return out
}

func addRuntimeAdjustmentNumbers(payload PerformancePayload, target string, includeMetrics bool, add func(any)) {
	target = strings.ToLower(strings.TrimSpace(target))
	for _, adjustment := range payload.RuntimeAdjustments {
		matchesTarget := strings.EqualFold(strings.TrimSpace(adjustment.Action), target)
		if len(adjustment.Adjustments) > 0 {
			value, ok := adjustment.Adjustments[target]
			if ok {
				add(value)
				matchesTarget = true
			}
		}
		if matchesTarget && includeMetrics {
			addRuntimeMetricNumber(adjustment.ThroughputBefore, add)
			addRuntimeMetricNumber(adjustment.ThroughputAfter, add)
			addRuntimeMetricNumber(adjustment.EffectPercent, add)
			addRuntimeMetricNumber(adjustment.CPUBefore, add)
			addRuntimeMetricNumber(adjustment.CPUAfter, add)
			addRuntimeMetricNumber(adjustment.MemoryBefore, add)
			addRuntimeMetricNumber(adjustment.MemoryAfter, add)
		}
	}
}

func addRecentRunNumbers(payload PerformancePayload, target string, includeMetrics bool, add func(any)) {
	if !includeMetrics {
		return
	}
	target = strings.ToLower(strings.TrimSpace(target))
	addNonZero := func(v any) {
		if fmt.Sprint(v) == "0" || fmt.Sprint(v) == "0.0" {
			return
		}
		add(v)
	}
	for _, run := range payload.RecentRuns {
		switch target {
		case "workers":
			addNonZero(run.Knobs.Workers)
		case "chunk_size":
			addNonZero(run.Knobs.ChunkSize)
		case "read_ahead_buffers":
			addNonZero(run.Knobs.ReadAheadBuffers)
		case "write_ahead_writers":
			addNonZero(run.Knobs.WriteAheadWriters)
		case "parallel_readers":
			addNonZero(run.Knobs.ParallelReaders)
		case "max_partitions":
			addNonZero(run.Knobs.MaxPartitions)
		case "large_table_threshold":
			addNonZero(run.Knobs.LargeTableThreshold)
		case "max_source_connections":
			addNonZero(run.Knobs.MaxSourceConnections)
		case "max_target_connections":
			addNonZero(run.Knobs.MaxTargetConnections)
		case "upsert_merge_chunk_size":
			addNonZero(run.Knobs.UpsertMergeChunkSize)
		case "checkpoint_frequency":
			addNonZero(run.Knobs.CheckpointFrequency)
		case "max_retries":
			addNonZero(run.Knobs.MaxRetries)
		}
		if includeMetrics {
			addRuntimeMetricNumber(run.FinalThroughput, add)
			addRuntimeMetricNumber(run.FinalDurationSeconds, add)
			if run.ChunkRetryCount > 0 {
				add(run.ChunkRetryCount)
			}
		}
	}
}

func dbTuningExactValuesForTarget(payload PerformancePayload, target string) []string {
	target = dbTuningParameterFromTarget(target)
	if target == "" {
		return nil
	}
	return dbTuningExactValuesForParam(payload, target)
}

func dbTuningExactValuesForParam(payload PerformancePayload, param string) []string {
	param = strings.ToLower(strings.TrimSpace(param))
	if param == "" {
		return nil
	}
	var values []string
	for _, tuning := range payload.DatabaseTuning {
		for _, rec := range tuning.Recommendations {
			if strings.ToLower(strings.TrimSpace(rec.Parameter)) != param {
				continue
			}
			for _, value := range []string{rec.CurrentValue, rec.RecommendedValue} {
				value = strings.TrimSpace(value)
				if value != "" {
					values = append(values, value)
				}
			}
		}
	}
	return values
}

func dbTuningParameterFromTarget(target string) string {
	const prefix = "db_tuning."
	target = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(target)), prefix)
	if target == "" {
		return ""
	}
	return target
}

func addRuntimeMetricNumber(v float64, add func(any)) {
	if v == 0 {
		return
	}
	add(v)
}

func normalizePerformanceNumber(s string) string {
	s = strings.TrimPrefix(strings.ReplaceAll(strings.TrimSpace(s), ",", ""), "+")
	negative := strings.HasPrefix(s, "-")
	if negative {
		s = strings.TrimPrefix(s, "-")
	}
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		intPart := strings.TrimLeft(s[:dot], "0")
		fracPart := strings.TrimRight(s[dot+1:], "0")
		if intPart == "" {
			intPart = "0"
		}
		if fracPart == "" {
			s = intPart
		} else {
			s = intPart + "." + fracPart
		}
	} else {
		s = strings.TrimLeft(s, "0")
		if s == "" {
			s = "0"
		}
	}
	if negative && s != "0" {
		return "-" + s
	}
	return s
}

func allowedKnobsFromPayload(payload PerformancePayload) map[string]bool {
	if len(payload.AllowedKnobs) == 0 {
		return allowedPerformanceKnobSet()
	}
	out := make(map[string]bool, len(payload.AllowedKnobs))
	base := allowedPerformanceKnobSet()
	for _, knob := range payload.AllowedKnobs {
		knob = strings.ToLower(strings.TrimSpace(knob))
		if base[knob] {
			out[knob] = true
		}
	}
	return out
}

func allowedFindingTargetsFromPayload(payload PerformancePayload) map[string]bool {
	out := allowedKnobsFromPayload(payload)
	dbTargets := dbTuningTargetsFromPayload(payload)
	for _, target := range payload.AllowedFindingTargets {
		target = strings.ToLower(strings.TrimSpace(target))
		if target == "" {
			continue
		}
		if dbTargets[target] {
			out[target] = true
		}
	}
	return out
}

func dbTuningTargetsFromPayload(payload PerformancePayload) map[string]bool {
	out := map[string]bool{}
	for _, tuning := range payload.DatabaseTuning {
		for _, rec := range tuning.Recommendations {
			parameter := strings.ToLower(strings.TrimSpace(rec.Parameter))
			if parameter != "" {
				out["db_tuning."+parameter] = true
			}
		}
	}
	return out
}
