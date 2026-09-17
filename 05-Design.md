# Trade Execution Logger & Audit System — Design

## Postgres Schema

### `trades` Table

```sql
CREATE TABLE trades (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  trader_id VARCHAR(255) NOT NULL,
  desk_id VARCHAR(255) NOT NULL,
  symbol VARCHAR(10) NOT NULL,
  quantity BIGINT NOT NULL,
  price DECIMAL(15, 8) NOT NULL,
  side VARCHAR(10) NOT NULL,  -- BUY or SELL
  state VARCHAR(20) NOT NULL DEFAULT 'SUBMITTED',  -- SUBMITTED, FILLED, CANCELLED
  events JSONB NOT NULL,  -- [{submitted_at, filled_at, cancelled_at, ...}]
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT valid_quantity CHECK (quantity > 0),
  CONSTRAINT valid_price CHECK (price > 0),
  CONSTRAINT valid_side CHECK (side IN ('BUY', 'SELL'))
);

-- Indexes
CREATE INDEX idx_trades_trader_id ON trades(trader_id);
CREATE INDEX idx_trades_symbol ON trades(symbol);
CREATE INDEX idx_trades_state ON trades(state);
CREATE INDEX idx_trades_created_at ON trades(created_at);
CREATE INDEX idx_trades_desk_id ON trades(desk_id);
```

### `events` JSONB Structure

```json
{
  "events": [
    {
      "type": "SUBMITTED",
      "timestamp": "2025-01-15T09:00:00Z",
      "user_id": "user123",
      "submitted_quantity": 1000
    },
    {
      "type": "FILLED",
      "timestamp": "2025-01-15T09:05:30Z",
      "user_id": "broker_system",
      "filled_quantity": 500,
      "filled_price": 150.25
    },
    {
      "type": "FILLED",
      "timestamp": "2025-01-15T09:15:00Z",
      "user_id": "broker_system",
      "filled_quantity": 500,
      "filled_price": 150.30
    },
    {
      "type": "CANCELLED",
      "timestamp": "2025-01-15T14:00:00Z",
      "user_id": "user123",
      "reason": "End of day cancel"
    }
  ]
}
```

### `audit_log` Table

```sql
CREATE TABLE audit_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  trade_id UUID NOT NULL REFERENCES trades(id),
  action VARCHAR(50) NOT NULL,  -- CORRECTION, UPDATE, etc.
  reason TEXT,
  previous_state JSONB,
  new_state JSONB,
  user_id VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_audit_trade_id (trade_id),
  INDEX idx_audit_user_id (user_id),
  INDEX idx_audit_created_at (created_at)
);
```

## Java Classes & Structure

```
src/main/java/com/tradelogger/
├── config/
│   ├── KafkaConfig.java (exactly-once consumer config)
│   ├── SecurityConfig.java (Spring Security, JWT)
│   └── WebSocketConfig.java (Phase 5)
├── model/
│   ├── Trade.java
│   ├── TradeEvent.java
│   ├── AuditLog.java
│   └── User.java
├── repository/
│   ├── TradeRepository.java
│   ├── AuditLogRepository.java
│   └── UserRepository.java
├── service/
│   ├── TradeIngestionService.java
│   ├── TradeStateService.java
│   ├── AuditService.java
│   ├── LLMQueryService.java (Phase 4)
│   ├── AccessControlService.java (Phase 6)
│   └── WebSocketPublisher.java (Phase 5)
├── controller/
│   ├── TradeController.java
│   ├── QueryController.java (Phase 4)
│   └── WebSocketHandler.java (Phase 5)
├── kafka/
│   ├── TradeKafkaListener.java
│   └── TradeDeserializer.java
├── util/
│   ├── IdempotencyChecker.java
│   └── SQLValidator.java (Phase 4)
└── TradeLoggerApplication.java
```

## REST API Endpoints

### Phase 1

- `GET /trades` - List all trades (paginated)
- `GET /trades/{id}` - Get single trade with events
- `GET /trades/symbol/{symbol}` - Filter by symbol
- `GET /trades/trader/{trader_id}` - Filter by trader

### Phase 2

- `POST /trades/{id}/fill` - Mark trade as filled (payload: filled_quantity, filled_price)
- `POST /trades/{id}/cancel` - Mark trade as cancelled

### Phase 3

- `POST /trades/{id}/correct` - Submit correction (payload: reason, action, details)
- `GET /trades/{id}/audit` - Get audit log for a trade

### Phase 4

- `POST /query` - Natural language query (payload: {question})

### Phase 6

- All endpoints above gain role-based filtering

## Kafka Configuration

Topic: `trades-raw`
Partitions: 3
Replication factor: 2
Retention: 7 days

Message Schema:
```json
{
  "trade_id": "uuid",
  "trader_id": "string",
  "desk_id": "string",
  "symbol": "string",
  "quantity": 1000,
  "price": 150.25,
  "side": "BUY",
  "timestamp": "2025-01-15T09:00:00Z"
}
```

Consumer Config:
```properties
spring.kafka.consumer.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=trade-logger-service
spring.kafka.consumer.enable-auto-commit=false
spring.kafka.consumer.isolation-level=read_committed
spring.kafka.producer.acks=all
spring.kafka.producer.enable-idempotence=true
```

## LLM Query Flow (Phase 4)

1. User calls `POST /query {"question": "losing tech trades"}`
2. LLMQueryService receives request
3. Build Ollama prompt:
   ```
   Given the trades schema with columns: id, trader_id, desk_id, symbol, quantity, price, side, state, events (JSONB with filled_price, filled_at)
   
   Generate a PostgreSQL query for: losing tech trades
   
   Only return the SQL query, nothing else.
   ```
4. Call Ollama API (local, port 11434)
5. Parse response SQL
6. Validate SQL (whitelist tables/columns)
7. Execute against Postgres
8. Return results JSON

## Error Handling

- Kafka consumer failure: log, emit to dead-letter topic, alert
- Duplicate trade detected: log as duplicate, skip insert
- SQL generation fails: return 500 with message "Could not generate query"
- Invalid SQL generated: return 400 with message "Invalid SQL generated"
- Role-based access violation: return 403 Forbidden

## Assumptions (Flag if Wrong)

1. Kafka is already running and `trades-raw` topic exists
2. Postgres is running with a database created (migrations handle table creation)
3. Authentication is optional for Phase 1-4; Phase 6 adds Spring Security
4. Ollama is running locally on port 11434 (or Docker container)
5. Trade IDs are UUIDs (globally unique)
6. "Losing trade" means sold at lower price than filled, or similar logic (business logic to be defined in Phase 2)
