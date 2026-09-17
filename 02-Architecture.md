# Trade Execution Logger & Audit System — Architecture

## Tech Stack

- **Language**: Java 17+
- **Framework**: Spring Boot 3.x
- **Database**: Postgres 14+
- **Message Queue**: Kafka (exactly-once semantics via idempotent writes + transactions)
- **Caching**: Redis (optional, Phase 3+)
- **LLM**: Ollama (local, Phase 4)
- **APIs**: REST (Spring MVC), WebSocket (Spring WebSocket, Phase 5)
- **Deployment**: Docker, CI/CD via GitHub Actions

## Core Architecture

```
Kafka Producer
    |
    v
Kafka Topic (trades-raw)
    |
    v
Spring Boot Consumer (KafkaListener)
    |
    v
Trade Ingestion Service (validates, idempotency checks)
    |
    v
Postgres (immutable audit trail)
    ^
    |
Rest Controller / WebSocket Handler
    |
    v
Risk Manager / Compliance Client
```

## Database Schema

### `trades` Table (immutable core)
```sql
CREATE TABLE trades (
  id UUID PRIMARY KEY,
  trader_id VARCHAR NOT NULL,
  desk_id VARCHAR NOT NULL,
  symbol VARCHAR NOT NULL,
  quantity BIGINT NOT NULL,
  price DECIMAL NOT NULL,
  side VARCHAR NOT NULL, -- BUY | SELL
  state VARCHAR NOT NULL, -- SUBMITTED | FILLED | CANCELLED
  events JSONB NOT NULL, -- [{submitted_at, filled_at, cancelled_at, filled_quantity, ...}]
  created_by UUID NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  INDEX idx_trader_id (trader_id),
  INDEX idx_symbol (symbol),
  INDEX idx_state (state),
  INDEX idx_created_at (created_at)
);
```

### `audit_log` Table (corrections + compliance)
```sql
CREATE TABLE audit_log (
  id UUID PRIMARY KEY,
  trade_id UUID REFERENCES trades(id),
  action VARCHAR NOT NULL, -- CORRECTION | UPDATE | CANCELLATION
  reason TEXT,
  previous_state JSONB,
  new_state JSONB,
  user_id UUID NOT NULL,
  created_at TIMESTAMP NOT NULL,
  INDEX idx_trade_id (trade_id),
  INDEX idx_user_id (user_id)
);
```

## Key Services

### 1. Trade Ingestion Service
- Consumes from Kafka topic
- Validates schema, enforces business rules
- Detects duplicates (exactly-once via idempotency key)
- Inserts into `trades` table with ACID guarantees
- Publishes to internal event stream for WebSocket subscribers

### 2. Trade Query Service
- REST endpoints for querying by trader, symbol, date range, state
- Applies role-based access control (row-level security)
- Supports drill-down into individual trade state history
- Aggregation queries (sum, average, count by symbol/desk)

### 3. LLM Query Service (Phase 4)
- REST endpoint: `POST /query` with `{"question": "losing tech trades"}`
- Translates natural language to SQL via Ollama
- Executes SQL against Postgres
- Returns results

### 4. Access Control Service
- Middleware to intercept all requests
- Maps user role (trader, risk_manager, compliance) to access policy
- Traders: `WHERE trader_id = current_user_id`
- Risk managers: `WHERE desk_id = current_user_desk_id`
- Compliance: no WHERE clause (see all)

## Kafka Configuration

- Topic: `trades-raw`
- Partitions: 3+ (scale with throughput)
- Replication factor: 2+
- Consumer group: `trade-logger-service`
- Idempotent producer: `enable.idempotence = true`
- Exactly-once: `isolation.level = read_committed`

## Data Flow

1. Kafka emits trade message: `{id, trader_id, symbol, qty, price, side, ts}`
2. Consumer deserializes and validates
3. Idempotency check: does this trade ID already exist?
4. Insert into `trades` table with `state = SUBMITTED`
5. Append event to `events` JSON array: `{submitted_at, submitted_by}`
6. If trade is executed mid-stream (partial/full fill):
   - Update `events` array: append `{filled_at, filled_quantity, filled_price}`
   - Update `state` to `FILLED`
7. If user corrects/cancels:
   - Insert into `audit_log` with reason
   - Append new event to `events`
   - Update `state` if needed

## Scaling Considerations

- Postgres: indexes on `(trader_id, created_at)`, `(symbol, state)` for fast queries
- Redis (Phase 3+): cache frequent queries (e.g., "my fills today") with 5-min TTL
- Kafka: scale partitions with expected throughput growth
- Spring Boot: horizontal scaling with load balancer

## Failure Handling

- Kafka consumer failure: replay from last committed offset (exactly-once preserved)
- Postgres write failure: log, emit dead-letter topic, manual intervention
- LLM query timeout (Phase 4): fallback to predefined simple queries or manual SQL
