# Testing AI-Driven Real-Time Parameter Adjustment

This guide explains how to test the new AI-driven real-time parameter adjustment feature.

## Prerequisites

1. **Build the binary**:
   ```bash
   /usr/local/go/bin/go build -o dmt ./cmd/migrate
   ```

2. **Configure environment variables**:
   ```bash
   # AI Configuration
   export DMT_AI_PROVIDER=claude
   export DMT_AI_API_KEY=sk-ant-...  # Your Anthropic API key

   # Database Configuration
   export SOURCE_HOST=your-mssql-host
   export SOURCE_DB=your-database
   export SOURCE_USER=your-user
   export SOURCE_PASS=your-password

   export TARGET_HOST=your-postgres-host
   export TARGET_DB=your-database
   export TARGET_USER=your-user
   export TARGET_PASS=your-password
   ```

3. **Create a config file** (use `test-ai-adjust.yaml` as template):
   ```yaml
   migration:
     ai_adjust: true
     ai_adjust_interval: 30s  # Evaluation interval
   ```

## Running with AI Adjustment

```bash
# Run with debug logging to see AI decisions
./dmt run -c test-ai-adjust.yaml --verbosity debug
```

## What to Look For in Logs

### 1. Initial AI Monitoring Setup
Look for:
```
[INFO] AI-driven parameter adjustment enabled (interval: 30s)
[INFO] AI monitoring started (evaluation interval: 30s)
```

### 2. Metrics Collection (every 30 seconds)
```
[DEBUG] Metrics snapshot: 125000 rows/sec, memory=512MB (45.2%), CPU=65.3%, throughput_trend=12.5%
```

Watch for:
- **Rows/sec**: Actual throughput
- **Memory**: Current usage and percentage
- **CPU**: Processor utilization
- **Throughput trend**: % change from previous sample (positive = improving)

### 3. AI Analysis and Decisions
Look for decision logs:
```
[INFO] AI adjustment applied: scale_up - Throughput declining and resources available - increasing workers (confidence: medium)
[DEBUG] Scaled writers from 4 to 6
```

Or:
```
[INFO] AI adjustment applied: reduce_chunk - Memory saturated - reducing chunk size to free memory (confidence: high)
[DEBUG] Config updated: chunk_size=5000, readers=2, writers=4, buffers=8
```

Or:
```
[INFO] Using cached AI decision (age 45.2s)
```
(This means decision was cached to reduce API calls)

### 4. Fallback Behavior (if AI unavailable)
If AI is not configured or fails:
```
[DEBUG] AI adjustment requested but AI not configured
[WARN] AI adjustment failed: context deadline exceeded, using fallback rules
[INFO] AI adjustment applied: continue - Performance stable (fallback rules) (confidence: low)
```

### 5. Circuit Breaker (if AI fails repeatedly)
After 3 consecutive failures:
```
[WARN] AI adjustment circuit breaker OPEN after 3 failures - will retry in 5m0s
```

After timeout:
```
[INFO] AI adjustment circuit breaker CLOSED - resuming
```

## Expected Behavior

### Phase 1: Initial Adjustment (first 2-3 samples, 60-90 seconds)
- System collects baseline metrics
- AI analyzes initial performance
- May recommend initial scaling based on observed patterns

### Phase 2: Continuous Optimization (ongoing)
- AI re-evaluates every 30 seconds (or configured interval)
- Decisions cached for 60 seconds to reduce API calls
- Adjustments applied at chunk boundaries
- Worker count can change between chunks
- Chunk size changes apply to next table

### Phase 3: Stable State
- Throughput stabilizes
- AI returns "continue" (no changes needed)
- Minimal API calls due to decision caching
- System runs efficiently with auto-tuned parameters

## Performance Expectations

### API Costs
- **Without caching**: ~$0.012/hour (120 evaluations at $0.0001 each)
- **With caching**: ~$0.006/hour (60 actual API calls)
- **Total for 8-hour migration**: ~$0.05

### Throughput Impact
- **Best case**: 15-30% throughput improvement through optimization
- **Typical case**: 5-15% improvement or stable performance
- **Conservative**: No degradation (AI recommends "continue" if uncertain)

## Testing Checklist

- [ ] Binary builds without errors
- [ ] Config file with `ai_adjust: true` loads successfully
- [ ] AI provider configured (Claude by default)
- [ ] Logs show "AI monitoring started"
- [ ] Metrics snapshots appear every 30 seconds
- [ ] AI analysis appears in logs
- [ ] Parameter adjustments are logged (or "continue" decisions)
- [ ] Migration completes successfully
- [ ] Final performance metrics show adjustment history

## Troubleshooting

### AI monitoring not starting
```
[DEBUG] AI adjustment requested but AI not configured
```
**Fix**: Set `DMT_AI_API_KEY` and `DMT_AI_PROVIDER` environment variables

### API call timeouts
```
[WARN] AI adjustment failed: context deadline exceeded
```
**Fix**: Increase `timeout_seconds` in config (default: 30)

### Too many API calls
```
[INFO] Throughput increasing: 100K rows/sec -> 120K rows/sec
```
**Note**: This is expected during optimization phase. Caching limits to ~2 calls/minute after warmup.

### Circuit breaker triggered
```
[WARN] AI adjustment circuit breaker OPEN
```
**Note**: Automatic recovery after 5 minutes. Check AI provider status.

## Advanced Testing

### Test with different intervals
```yaml
migration:
  ai_adjust: true
  ai_adjust_interval: 15s  # More frequent evaluation
```

### Test with specific model
```yaml
ai:
  model: claude-haiku-4-5-20251001  # Faster/cheaper than default
```

### Monitor memory usage
Enable in logs to see memory pressure detection:
```
[DEBUG] Memory increasing: true
[DEBUG] Memory saturated: true
```

## Performance Profiling

To measure the impact of AI adjustment:

1. **Run without AI adjustment**:
   ```yaml
   migration:
     ai_adjust: false
   ```
   Record: time, throughput, final worker count

2. **Run with AI adjustment**:
   ```yaml
   migration:
     ai_adjust: true
   ```
   Record: time, throughput, worker count changes, API costs

3. **Compare results**:
   - Throughput improvement %
   - Time saved
   - API costs incurred
   - ROI of optimization

## Next Steps

- [ ] Test with real databases
- [ ] Measure throughput improvement
- [ ] Validate memory management
- [ ] Monitor API costs
- [ ] Collect feedback on parameter adjustments
- [ ] Consider integration with monitoring/alerting systems
