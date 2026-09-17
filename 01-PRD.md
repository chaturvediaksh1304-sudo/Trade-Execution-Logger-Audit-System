# Trade Execution Logger & Audit System — PRD

## Problem

Financial institutions need an immutable audit trail of every trade execution for compliance, real-time risk visibility, and post-trade analysis. Today, traders, risk managers, and compliance teams access scattered systems (order management, execution venues, risk dashboards) and lack a unified source of truth for drill-down investigation.

## Solution

A Java Spring Boot backend that ingests trade executions in real-time via Kafka, stores them with full state history in Postgres, and exposes queryable APIs (REST + WebSocket) for three distinct audiences. The system maintains immutable audit trails, tracks every state change, and surfaces insights via natural language queries backed by local LLM.

## Users & Personas

### Trader
- Needs to review their own execution history: fills, prices, timestamps
- Queries: "Show me my fills from today," "What was my average execution price on AAPL?"
- Access: Own trades only

### Risk Manager
- Aggregates positions across traders on their desk/book
- Monitors real-time exposure, concentration
- Queries: "What's our net tech exposure? Show losing trades in semiconductors."
- Access: Desk/book-level trades

### Compliance Officer
- Audits trade lifecycle for regulatory violations
- Generates reports and investigates anomalies
- Needs immutable records with full state history (who did what, when)
- Access: All trades globally

## Must-Have Features (MVP)

1. Real-time trade ingestion via Kafka with exactly-once semantics
2. Immutable audit trail capturing every state change (submitted → filled → cancelled)
3. Postgres storage with nested JSON state history (`trade.events: [{submitted_at, filled_at, ...}]`)
4. REST API for querying trades with drill-down into individual records and aggregates
5. Role-based access control (traders see own, risk managers see desk/book, compliance sees all)
6. User action tracking (which user submitted, filled, cancelled each trade)

## Nice-to-Have Features (Post-MVP)

1. WebSocket for real-time trade updates
2. Local Ollama LLM query layer (translate "losing tech trades" to SQL)
3. Redis caching layer for frequent queries
4. Advanced indexing on Postgres for sub-second response times

## Performance Requirements

- Throughput: 100K trades/day
- Query latency: < few seconds for risk manager queries
- Historical data: 1+ year queryable without performance degradation
- Consistency: Exactly-once semantics, no lost or duplicate trades

## Out of Scope

- Market data integration
- Position aggregation or Greeks calculation
- Regulatory filing generation
- Mobile apps or complex UIs
