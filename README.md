# Trade Execution Logger & Audit System

A Java Spring Boot backend that ingests trade executions in real-time via Kafka, stores them with full state history in Postgres, and exposes queryable APIs (REST + WebSocket) for traders, risk managers, and compliance officers.

Trades are write-once: a record is never mutated, and every state change (submitted → filled → cancelled) is appended as an event, giving compliance an immutable audit trail with full attribution.

## Docs

| Doc | Contents |
| --- | --- |
| [01-PRD.md](01-PRD.md) | Problem, personas, MVP scope, performance requirements |
| [02-Architecture.md](02-Architecture.md) | Tech stack and system architecture |
| [03-Rules.md](03-Rules.md) | Immutability and audit-trail invariants |
| [04-Phases.md](04-Phases.md) | Delivery phases, starting with the Kafka → Postgres → REST MVP |
| [05-Design.md](05-Design.md) | Postgres schema and API design |

## Stack

Java 17+ · Spring Boot 3.x · Postgres 14+ · Kafka · Redis · Ollama (local LLM query layer)

## Status

Design phase — documentation only. No implementation committed yet.
