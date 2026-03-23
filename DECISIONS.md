# DECISIONS.md — bank-pipeline-api Implementation Contract

> Architecture source of truth: `../ARCHITECTURE.md`
> Frozen: 2026-03-23

---

## Technology Decisions

| Concern | Decision | Rationale |
|---|---|---|
| API framework | FastAPI 0.111+ | async, typed, OpenAPI auto-docs |
| ORM | SQLAlchemy 2.x (async) | clean model layer, migration support |
| Migrations | Alembic | deterministic schema creation |
| Database | PostgreSQL 16 | primary store for all runs, incidents, evidence |
| Ontology store | PostgreSQL tables (ontology_nodes, ontology_edges) | Neo4j optional; PG is sufficient for demo; can swap |
| Containerization | Docker Compose | local dev; single `docker compose up` |
| API port | 8080 (external) → 8000 (internal FastAPI) | standard local dev convention |
| Config | environment variables via `.env` | 12-factor |
| Seed data | deterministic Python scripts in `app/seed/` | re-runnable, idempotent |
| Control logic | pure Python functions, no AI | auditable, unit-testable |
| Evidence | JSONB in `incident_evidence.content_json` | structured, queryable |
| RCA agent | EXTERNAL ONLY — never inside this service | hard boundary |

---

## Scope Decisions

| Item | In Scope | Out of Scope |
|---|---|---|
| Pipeline simulation | YES | Real COBOL execution |
| Deterministic control evaluation | YES | AI/LLM for pass/fail |
| Incident creation | YES | Incident auto-resolution |
| Evidence collection | YES | Full log aggregation |
| RCA context API | YES | RCA reasoning logic |
| RCA result write-back | YES | RCA agent logic |
| Neo4j ontology | OPTIONAL | Required |
| Frontend UI | Phase 10 only | Earlier phases |
| Auth/auth | None for demo | JWT/OAuth |

---

## ORC Codes (from orc-mapping.csv)

| Code | Name | CFR | Status |
|---|---|---|---|
| SGL | Single Ownership | 12 CFR 330.6 | ACTIVE |
| JNT | Joint Ownership | 12 CFR 330.9 | ACTIVE |
| REV | Revocable Trust | 12 CFR 330.10 | ACTIVE |
| BUS | Business/Organization | 12 CFR 330.11 | ACTIVE |
| EBP | Employee Benefit Plan | 12 CFR 330.14 | ACTIVE |
| CRA | Certain Retirement Accounts | 12 CFR 330.14(c) | ACTIVE |
| GOV1 | Government - Federal | 12 CFR 330.15 | ACTIVE |
| GOV2 | Government - State | 12 CFR 330.15 | ACTIVE |
| GOV3 | Government - Municipal | 12 CFR 330.15 | ACTIVE |
| ANC | Annuity Contract | 12 CFR 330.8 | ACTIVE |
| IRR | Irrevocable Trust | 12 CFR 330.13 | **NOT_IMPLEMENTED** |

IRR is intentionally not implemented — this is the source of CTL-DEP-002 / CTL-TRUST-001.

---

## Pipeline Stages (in order)

1. `ingestion` — load raw records from seed tables
2. `transformation` — normalize, apply ORC codes, check hold amounts
3. `reconciliation` — cross-system balance checks, ACH return netting
4. `reporting` — produce insurance result rows, flag pending records

---

## SMDIA

- $250,000 per depositor per ORC (12 CFR Part 330)
- Hardcoded in legacy systems — this is itself a control failure (CTL-DEP-004)
- This platform stores it in the DB config table so it *can* be changed

---

## Phase 1 First Vertical Slice: ORC Misclassification (IRR→SGL)

Scenario SCN-002 is the first end-to-end demo target because:
- it has a clean code reference (ORC-ASSIGNMENT.cob KNOWN ISSUES #3)
- the trust seed data (TR-003, TR-008, TR-015) directly demonstrates it
- it fires controls CTL-DEP-002 and CTL-TRUST-001 simultaneously
- the ontology traversal path has 3 clean nodes

---

## Assumptions

1. Docker and docker-compose are available on the dev machine.
2. Python 3.12 is used inside the container.
3. PostgreSQL 16 is the minimum DB version.
4. No external network access is required for the demo.
5. Seeds are reset-safe; running `POST /seed/load` twice is idempotent via upsert.
6. All monetary amounts are stored as NUMERIC(15,2) in PostgreSQL.
7. Timestamps are UTC throughout.
