# Trade Execution Logger & Audit System — Rules

## Immutability & Audit Trail

1. **Write-once trades**: A `trades` record, once inserted, is never mutated directly. Its state evolves only via appended events in the `events` JSON array.

2. **Corrections as new records**: If a trade needs correction (e.g., "we filled 500 more shares than recorded"), create a new audit log entry with reason and previous/new state. Never UPDATE an existing trade row.

3. **State history in JSON**: Each trade's `events` array is append-only. Example:
   ```json
   {
     "events": [
       {"submitted_at": "2025-01-15T09:00:00Z", "submitted_by": "user123"},
       {"filled_at": "2025-01-15T09:05:00Z", "filled_quantity": 1000, "filled_by": "broker_system"},
       {"corrected_at": "2025-01-15T14:00:00Z", "correction_reason": "partial fill missed", "corrected_by": "user456"}
     ]
   }
   ```

4. **Compliance queries must include audit log**: When compliance pulls a trade, return both the `trades` record and any `audit_log` entries referencing it.

## Exactly-Once Semantics

1. **Idempotency key**: Each Kafka message includes a unique `trade_id` (UUID). Before inserting, check if this `trade_id` already exists in `trades`. If yes, skip (it's a duplicate).

2. **Atomic inserts**: Wrap Kafka consumer logic in a transaction. Either the trade and its initial event are inserted, or neither are. No halfway states.

3. **Failure recovery**: If Spring Boot crashes mid-write, Kafka offset is not committed. On restart, the consumer replays from the last committed offset and re-attempts the insert. The idempotency check prevents duplicates.

4. **Kafka configuration**:
   - Producer: `enable.idempotence = true`
   - Consumer: `isolation.level = read_committed`, manual offset commit after successful DB write

## Access Control

1. **Trader**: Can only query trades where `trader_id = authenticated_user_id`.
   - REST endpoint `/trades` filters all results by `WHERE trader_id = current_user_id`

2. **Risk Manager**: Can only query trades where `desk_id = authenticated_user_desk_id`.
   - REST endpoint `/trades` filters all results by `WHERE desk_id = current_user_desk_id`

3. **Compliance Officer**: Can query all trades without restriction.
   - REST endpoint `/trades` has no row-level filter

4. **User action tracking**: Every operation (submit, fill, cancel, correct) logs the `user_id` who performed it in `events` or `audit_log`. Compliance uses this for audit trails.

## Validation Rules

1. **Trade validation on ingest**:
   - `trader_id`, `symbol`, `quantity`, `price`, `side` are non-null
   - `quantity > 0`, `price > 0`
   - `side` is one of: BUY, SELL
   - `symbol` is 1-10 characters (alphanumeric)

2. **State transition rules**:
   - Initial state: SUBMITTED
   - Valid transitions: SUBMITTED → FILLED or SUBMITTED → CANCELLED
   - No transitions from FILLED or CANCELLED (they are terminal)

3. **Event ordering**: All events in the `events` array must be in chronological order. If Kafka delivers out-of-order messages, the consumer reorders them by timestamp before appending.

## Query Latency & Caching

1. **Few-second target**: Most queries should return in < 5 seconds for a 1-year dataset with 100K trades/day (~36M records).

2. **Index strategy**:
   - `idx_trader_id`: Fast lookup by trader
   - `idx_symbol`: Fast lookup by stock symbol
   - `idx_state`: Fast lookup by trade state (submitted/filled/cancelled)
   - `idx_created_at`: Fast lookup by date range

3. **Redis caching (Phase 3+)**: Cache results of frequent queries (e.g., "my fills today") with 5-minute TTL. Key format: `trader:{trader_id}:fills:{date}`

## LLM Query Layer (Phase 4)

1. **Simple queries only**: Queries should map to standard SQL patterns (WHERE, GROUP BY, ORDER BY, LIMIT).

2. **SQL generation**: Ollama takes a question (e.g., "losing tech trades") and outputs SQL. No free-form aggregations or complex JOINs.

3. **Injection safety**: Validate the generated SQL before execution. Whitelist allowed tables, columns, and operators.

4. **Fallback**: If Ollama times out or generates invalid SQL, return an error and suggest manual query.

## Enforcement: Verify-Then-Proceed Loops

Every phase uses verification before moving forward:

1. **Phase 1 (Ingestion)**: Ingest one batch of trades, verify all inserted with correct events, verify role-based filtering works, verify no duplicates after replay. Only then move to Phase 2.

2. **Phase 2 (State changes)**: Submit a trade, fill it partially, verify state machine transitions are correct, verify events are in order. Only then move to Phase 3.

3. **Phase 3 (Audit corrections)**: Insert a trade, correct it, verify both the trade and audit log are queryable together, verify compliance can see the correction. Only then move to Phase 4.

4. **Phase 4 (LLM queries)**: Run a natural language query, verify SQL is generated safely, verify results match expected rows. Only then move to Phase 5.

5. **Phase 5 (WebSocket)**: Emit a trade, verify WebSocket subscriber receives update in < 1 second. Only then move to Phase 6.

6. **Phase 6 (RBAC)**: Run same query as trader, risk manager, compliance; verify each sees only their allowed rows. Only then consider MVP complete.
