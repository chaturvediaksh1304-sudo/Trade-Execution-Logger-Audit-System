# Trade Execution Logger & Audit System — Phases

## Phase 1: Kafka Ingestion + Postgres Storage + Basic REST API (MVP)

**Goal**: Ingest trades from Kafka, store immutably in Postgres, expose REST endpoints to query.

**Deliverables**:
1. Kafka consumer (Spring Cloud Stream or KafkaListener) that reads from `trades-raw` topic
2. Trade ingestion service with idempotency checks (deduplicate on `trade_id`)
3. Postgres schema: `trades` table with nested JSON `events` array, `audit_log` table
4. REST endpoints:
   - `GET /trades` (returns all trades, unauthenticated for now)
   - `GET /trades/{id}` (returns single trade with full state history)
   - `GET /trades/symbol/{symbol}` (returns trades for a symbol)
5. Docker setup (Postgres + Kafka + Spring Boot containers)
6. Basic error handling (log failures, emit dead-letter topic)

**Done Criteria**:
- Ingest 100 mock trades from Kafka without loss or duplication
- All trades appear in Postgres with correct state history
- Query endpoints return results in < 1 second
- Replay from Kafka (stop consumer, restart) produces no duplicates

---

## Phase 2: Trade State Tracking (SUBMITTED → FILLED → CANCELLED)

**Goal**: Model trade lifecycle as state transitions, track every change in the events array.

**Deliverables**:
1. State machine service (validates transitions: SUBMITTED → FILLED or CANCELLED only)
2. Kafka consumer enhancement: when a fill message arrives, update the same trade (append to events, update state)
3. Event structure includes: `submitted_at`, `filled_at`, `filled_quantity`, `cancelled_at`, `cancelled_by`, etc.
4. REST endpoints for state transitions:
   - `POST /trades/{id}/fill` (mark as filled)
   - `POST /trades/{id}/cancel` (mark as cancelled)
5. Validation: no state regressions, all timestamps in chronological order

**Done Criteria**:
- Submit a trade (SUBMITTED state), fill it partially, fill it fully, verify state machine prevents invalid transitions
- Query a filled trade, see complete event history (submitted_at → filled_at)
- Replay state changes from Kafka; verify final state matches the last event

---

## Phase 3: Immutable Audit Trail + Corrections

**Goal**: Implement correction flow: corrections create new audit log records, never mutate original trade.

**Deliverables**:
1. Audit log service: when a correction is needed, insert into `audit_log` table with reason, previous state, new state
2. REST endpoint:
   - `POST /trades/{id}/correct` (payload: `{reason, action, details}`)
3. Compliance query enhancement: when fetching a trade, include all `audit_log` entries that reference it
4. Immutability enforcement: add DB constraint (or application-level check) to prevent direct UPDATEs to `trades`
5. Query audit trail: `GET /trades/{id}/audit` returns all corrections for that trade

**Done Criteria**:
- Correct a trade (e.g., adjust filled quantity), verify original trade is unchanged
- Verify audit log entry is created with user ID, reason, timestamp
- Query trade + audit log together; compliance sees full trail
- Attempt to UPDATE the trade directly; verify it fails

---

## Phase 4: LLM Query Layer (Local Ollama)

**Goal**: Translate natural language queries to SQL via Ollama.

**Deliverables**:
1. Ollama integration (spawn local container or connect to running instance)
2. LLM query service: REST endpoint `POST /query` with JSON payload `{"question": "losing tech trades"}`
3. Prompt engineering: given the question + schema info, ask Ollama to generate SQL
4. SQL validation: whitelist allowed tables/columns, reject injections
5. Execution layer: execute SQL, return results (or error if unsafe SQL is generated)
6. Fallback: if Ollama fails, return error message suggesting manual query

**Done Criteria**:
- Query: "losing tech trades" → generates SQL like `SELECT * FROM trades WHERE side='SELL' AND symbol LIKE 'TECH%' AND price < purchase_price`
- Query: "trades submitted today" → generates SQL with date filtering
- Invalid query attempt (e.g., "DROP TABLE") → rejected by validation layer
- Ollama timeout → graceful error response

---

## Phase 5: WebSocket for Real-Time Updates

**Goal**: Push trade updates to subscribed clients (risk managers, traders) in real-time.

**Deliverables**:
1. Spring WebSocket config + STOMP broker
2. WebSocket endpoint: clients connect and subscribe to `/topic/trades` (or `/topic/trades/desk/{desk_id}`)
3. Trade ingestion service publishes to WebSocket broker when a new trade or state change occurs
4. Payload structure: trade ID, symbol, state change, new state
5. Client-side filter: based on user role, only receive trades they're allowed to see
6. Latency target: < 1 second from trade insert to WebSocket push

**Done Criteria**:
- Connect WebSocket client, submit a trade, receive update in < 1 second
- Two traders on different desks; verify each receives only their own desk's trades
- Risk manager; verify receives all trades on their desk in real-time

---

## Phase 6: Role-Based Access Control (RBAC)

**Goal**: Enforce access control for all query endpoints (REST + WebSocket).

**Deliverables**:
1. Authentication layer: Spring Security, JWT tokens or session-based auth
2. User context: extract user ID, role (trader/risk_manager/compliance), desk_id from token
3. Authorization middleware: intercept all endpoints, apply row-level filters:
   - Trader: `WHERE trader_id = current_user_id`
   - Risk manager: `WHERE desk_id = current_user_desk_id`
   - Compliance: no filter
4. Audit logging: log all queries with user ID, timestamp, filters applied
5. WebSocket filtering: ensure subscriptions respect RBAC
6. Update REST endpoints to apply role-based filtering (currently no filtering)

**Done Criteria**:
- Trader queries `/trades`; sees only their own (< 50 records out of 100K)
- Risk manager queries `/trades`; sees only their desk
- Compliance queries `/trades`; sees all 100K
- Same query run by different roles returns different result sets
- Attempt to bypass filter (e.g., modify JWT); verify rejected

---

## Phase 7: Production Hardening (Post-Portfolio)

**Goal**: Harden for real-world use (optional for portfolio project, but mentioned for completeness).

**Deliverables**:
1. Connection pooling (HikariCP), query optimization
2. Postgres backups, point-in-time recovery
3. Monitoring & alerting (logs, metrics, dashboards)
4. Performance profiling (slow query logs, index analysis)
5. Load testing (100K trades/day sustained throughput)
6. Documentation & runbooks

---

## Priority Mapping

- **Must complete for portfolio**: Phases 1-3 (ingestion, state tracking, audit trail)
- **Highly recommended**: Phase 4 (LLM queries, shows AI/ML integration)
- **Nice-to-have**: Phase 5 (WebSocket, real-time)
- **Can skip**: Phase 6 (RBAC requires auth setup; can simplify to no-auth for demo)
- **Skip entirely**: Phase 7
