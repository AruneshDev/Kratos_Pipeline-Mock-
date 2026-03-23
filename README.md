# bank-pipeline-api

A containerized regulatory compliance pipeline for a fictional bank (MyBank). It simulates the nightly batch processing that banking systems run to classify deposit accounts, calculate FDIC insurance coverage, and screen wire transfers — then automatically detects control failures, raises incidents, and exposes those incidents for AI-agent-driven root cause analysis (RCA).

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Environment Variables](#environment-variables)
- [API Reference](#api-reference)
- [Operational Systems](#operational-systems)
- [Pipeline Stages](#pipeline-stages)
- [Regulatory Controls](#regulatory-controls)
- [Ontology (v2)](#ontology-v2)
- [Scenario SCN-002](#scenario-scn-002)
- [Database Schema](#database-schema)
- [Project Structure](#project-structure)

---

## Overview

The pipeline ingests data from three operational banking systems, runs 21 deterministic regulatory controls at each pipeline stage, creates incidents when controls fail, and exposes a structured RCA context endpoint for an external AI agent to consume.

**Key design principles:**

- **Ontology-first** — every runtime record (ControlResult, Incident, RCAResult) carries typed ontology entity references. Root causes resolve to specific node IDs (e.g., `SCR-ORC-ASSIGNMENT`) rather than vague strings.
- **Deterministic controls** — all 21 evaluators run pure SQL against the database. No mocking or probabilistic logic.
- **Agent-ready RCA** — `GET /rca/context/{incident_id}` returns a fully structured payload (regulatory chain, implicated entities, ontology traversal, instructions) that an AI agent can consume and act on without additional context.
- **Idempotent seeding** — `POST /seed/load` is safe to call multiple times.

---

## Architecture

```
Docker Stack
├── mybank_postgres   PostgreSQL 16-alpine  (port 5432)
└── mybank_api        FastAPI 0.111         (port 8080 → 8000 internal)
    ├── POST /runs              trigger pipeline execution
    ├── POST /seed/load         load ontology, controls, sample data
    ├── GET  /seed/status       row counts for all domain tables
    ├── GET  /incidents         list incidents (filterable)
    ├── GET  /incidents/{id}    single incident with evidence
    ├── GET  /rca/context/{id}  structured RCA context for AI agents
    ├── POST /rca/results/{id}  submit RCA result
    ├── GET  /rca/results/{id}  retrieve stored RCA result
    └── GET  /health            liveness + DB connectivity check
```

**Stack:** FastAPI · SQLAlchemy 2.0 async (asyncpg) · Alembic · PostgreSQL JSONB · Docker Compose

---

## Quick Start

**Prerequisites:** Docker Desktop

```bash
# 1. Clone and enter the project
cd bank-pipeline-api

# 2. Copy environment file
cp .env.example .env

# 3. Build and start containers
docker compose up -d

# 4. Wait for healthy status, then run the migration
docker exec mybank_api alembic upgrade head

# 5. Load seed data (ontology + controls + sample accounts/trusts/wires + SCN-002 data)
curl -X POST http://localhost:8080/seed/load

# 6. Check health
curl http://localhost:8080/health

# 7. Trigger a pipeline run
curl -X POST http://localhost:8080/runs \
  -H "Content-Type: application/json" \
  -d '{"run_name": "run-001", "scenario_id": "SCN-002"}'
```

API docs are available at **http://localhost:8080/docs** once the container is running.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_DB` | `mybank` | Database name |
| `POSTGRES_USER` | `mybank` | Database user |
| `POSTGRES_PASSWORD` | `mybank_dev` | Database password |
| `APP_ENV` | `development` | Application environment |
| `LOG_LEVEL` | `INFO` | Logging level |
| `SMDIA` | `250000.00` | Standard Maximum Deposit Insurance Amount (FDIC regulatory constant) |

All variables are set in `.env` (copy from `.env.example`). Docker Compose reads them automatically.

---

## API Reference

### Trigger a Pipeline Run

```
POST /runs
Content-Type: application/json

{ "run_name": "my-run", "scenario_id": "SCN-002" }
```

Returns a full run summary including per-stage control results and failure counts.

### Seed Data

```
POST /seed/load          # idempotent — loads v2 ontology, controls, sample data
GET  /seed/status        # row counts for all domain tables
```

### Incidents

```
GET /incidents?status=OPEN&severity=P1&limit=50
GET /incidents/{incident_id}
```

Incidents include all v2 ontology entity fields: `failed_control_objective_node`, `failed_rule_node`, `implicated_script_node`, `owner_node`, etc.

### RCA

```
GET  /rca/context/{incident_id}
```

Returns:
- `incident` — full incident record with all entity refs
- `control` — control metadata including v2 chain columns
- `control_chain` — 7-node regulatory chain (Regulation → ControlObjective → Rule → Transformation → Pipeline → Script → Owner), each with full node data
- `implicated_entities` — system, job, tables, columns, code_event, log_source
- `evidence` — all collected evidence artifacts
- `ontology_traversal` — BFS traversal from entry node (up to 5 hops)
- `agent_instructions` — structured instructions for the AI agent

```
POST /rca/results/{incident_id}
Content-Type: application/json

{
  "root_cause": "Description of root cause",
  "root_cause_entity_type": "Script",
  "root_cause_entity_id": "SCR-ORC-ASSIGNMENT",
  "confidence_score": 0.97,
  "recommendation": "Fix steps...",
  "evidence_summary": "Evidence supporting this conclusion...",
  "supporting_entities": [
    { "entity_type": "Transformation", "entity_id": "TRN-ORC-CLASSIFICATION", "rationale": "..." }
  ],
  "traversal_path": ["CO-ORC-COMPLETENESS", "RULE-IRR-ORC-BRANCH", "SCR-ORC-ASSIGNMENT"],
  "reasoning_summary": "BFS traversal reasoning...",
  "submitted_by": "my-rca-agent"
}
```

Updates incident status to `RCA_COMPLETE`.

---

## Operational Systems

| System | Key | Technology | Domain |
|---|---|---|---|
| Legacy Deposit | `legacy_deposit` | COBOL + Java + SQL | Deposit accounts, ORC classification, FDIC insurance calculation |
| Trust & Custody | `trust_custody` | COBOL + Java + SQL | Trust accounts (IRR/REV/EBP/CUS/UTMA), beneficiary tracking |
| Wire Transfer | `wire_transfer` | Python + Java + SQL | Wire transactions, OFAC screening, ACH settlement |

Source code for these systems lives in `../operational_systems/` (read-only reference — not executed by the API).

---

## Pipeline Stages

Stages execute in order across applicable systems:

| Stage | Systems |
|---|---|
| `ingestion` | legacy_deposit, trust_custody, wire_transfer |
| `orc_assign` | legacy_deposit, trust_custody, wire_transfer |
| `aggregation` | legacy_deposit, trust_custody |
| `insurance` | legacy_deposit, trust_custody, wire_transfer |
| `screening` | wire_transfer |
| `reconciliation` | wire_transfer |
| `reporting` | all |

Each stage run creates a `StageRun` record with a `pipeline_node_id` linking to the ontology.

---

## Regulatory Controls

### Deposit Controls

| ID | Name | Severity | Regulation |
|---|---|---|---|
| CTL-DEP-001 | Per-Depositor ORC Aggregation Failure | P1 | 12 CFR 330 |
| CTL-DEP-002 | IRR Trust ORC Falls Through to SGL | P1 | 12 CFR 330.13 |
| CTL-DEP-003 | Pending ORC Accounts Detected | P3 | 12 CFR 330 |
| CTL-DEP-004 | SMDIA Value Drift from Regulatory Constant | P1 | 12 CFR 330 |
| CTL-DEP-005 | JNT Account Missing Second Owner | P2 | 12 CFR 330.9(a) |
| CTL-DEP-006 | Revocable Trust with Deceased Grantor | P3 | 12 CFR 330.10 |
| CTL-DEP-007 | GOV Account Missing Entity Type | P2 | 12 CFR 330.15 |
| CTL-DEP-008 | Collaterally Pledged Balance in Insurance Pool | P2 | 12 CFR 330.7(b) |

### Trust Controls

| ID | Name | Severity | Regulation |
|---|---|---|---|
| CTL-TRUST-001 | **IRR Trust ORC Misclassification — Missing IRR Branch** | **P1** | 12 CFR 330.13 |
| CTL-TRUST-002 | Trust Has No Beneficiaries Recorded | P3 | 12 CFR 330 |
| CTL-TRUST-003 | EBP Per-Participant Coverage Exceeds SMDIA | P2 | 12 CFR 330 |
| CTL-TRUST-004 | Deceased Beneficiary in Active Trust | P3 | 12 CFR 330 |
| CTL-TRUST-005 | Beneficiary Allocation Sum ≠ 100% | P2 | 12 CFR 330 |
| CTL-TRUST-006 | Custodial/UTMA Trust Missing Trustee | P3 | 12 CFR 330 |

### Wire Controls

| ID | Name | Severity | Regulation |
|---|---|---|---|
| CTL-WIRE-001 | Settled Wire Processed Without OFAC Screening | P1 | 31 CFR 1010.410 |
| CTL-WIRE-002 | Large-Value Wire Not OFAC Screened | P1 | 31 CFR 1010.410 |
| CTL-WIRE-003 | SWIFT Wire Missing Settlement Channel | P2 | 31 CFR 1010 |
| CTL-WIRE-004 | Stale ACH Wire Pending Beyond Settlement Window | P1 | 31 CFR 1010 |
| CTL-WIRE-005 | Wire Transaction Missing ORC Assignment | P3 | 12 CFR 330 |
| CTL-WIRE-006 | Large Wire Without Insurance Calculation | P2 | 12 CFR 330 |
| CTL-WIRE-007 | Wire Transaction in HELD Status | P3 | 31 CFR 1010 |

---

## Ontology (v2)

The v2 ontology uses **16 canonical node types** and **21 relationship types** to model the complete regulatory-to-code traceability chain.

### Node Types

```
System · Job · Pipeline · Script · Transformation · CodeEvent
DataSource · Dataset · Table · Column · LogSource
Regulation · ControlObjective · Rule · Owner
```

### Relationship Types

```
RUNS_JOB · DEPENDS_ON · EXECUTES · USES_SCRIPT · HAS_TRANSFORMATION
TYPICALLY_IMPLEMENTS · READS · WRITES · CONTAINS · HAS_COLUMN
DERIVED_FROM · SOURCED_FROM · CHANGED_BY · LOGGED_IN
MANDATES · IMPLEMENTED_BY · ENFORCED_BY
OWNS_PIPELINE · OWNS_JOB · OWNS_SYSTEM · OWNS_CONTROL
```

### 62 seeded nodes — example chain for CTL-TRUST-001

```
REG-12-CFR-330           (Regulation)
  └─MANDATES──► CO-ORC-COMPLETENESS      (ControlObjective)
                  └─IMPLEMENTED_BY──► RULE-IRR-ORC-BRANCH   (Rule)
                                         └─ENFORCED_BY──► TRN-ORC-CLASSIFICATION  (Transformation)
                                                              └─USES_SCRIPT──► SCR-ORC-ASSIGNMENT    (Script)

SYS-TRUST-CUSTODY  (System)
  └─RUNS_JOB──► JOB-DAILY-TRUST  (Job)
                  └─EXECUTES──► PL-TRUST-ORC-ASSIGN  (Pipeline)
                                  └─USES_SCRIPT──► SCR-ORC-ASSIGNMENT  (Script)
                                                     └─HAS_TRANSFORMATION──► TRN-ORC-CLASSIFICATION
                                                     └─CHANGED_BY──► EVT-ORC-ASSIGN-LAST-CHANGE  (CodeEvent)

OWN-TRUST-OPERATIONS  (Owner)  ──OWNS_PIPELINE──► PL-TRUST-ORC-ASSIGN
```

---

## Scenario SCN-002

**Title:** IRR Trust ORC Misclassification

**Bug:** `ORC-ASSIGNMENT.cob` contains no `WHEN 'IRR'` branch in its `EVALUATE ORC-TYPE` statement. All IRR (Irrevocable) trust accounts silently fall through to `SGL` (Single) classification, resulting in incorrect FDIC insurance coverage calculation — a violation of 12 CFR 330.13.

**Seeded data:** Three IRR trusts (`TR-003`, `TR-008`, `TR-015`) all have `orc_assigned = 'SGL'`.

**End-to-end flow:**

```
POST /runs  →  CTL-TRUST-001 fires (3 FAIL records)
                 ↓
             P1 Incident created
             • failed_control_objective_node = CO-ORC-COMPLETENESS
             • failed_rule_node              = RULE-IRR-ORC-BRANCH
             • failed_transformation_node   = TRN-ORC-CLASSIFICATION
             • implicated_pipeline_node     = PL-TRUST-ORC-ASSIGN
             • implicated_script_node       = SCR-ORC-ASSIGNMENT
             • implicated_system_node       = SYS-TRUST-CUSTODY
             • owner_node                   = OWN-TRUST-OPERATIONS
                 ↓
GET /rca/context/{id}  →  full control_chain + implicated_entities returned
                 ↓
POST /rca/results/{id}  →  agent submits:
             • root_cause_entity_type = "Script"
             • root_cause_entity_id   = "SCR-ORC-ASSIGNMENT"
             • confidence_score       = 0.97
                 ↓
             Incident status → RCA_COMPLETE
```

---

## Database Schema

### 17 tables across 2 migrations

| Category | Tables |
|---|---|
| Domain | `parties`, `accounts`, `trust_accounts`, `beneficiaries`, `wire_transactions` |
| Pipeline | `pipeline_runs`, `stage_runs` |
| Controls | `controls`, `control_results` |
| Incidents | `incidents`, `incident_evidence` |
| RCA | `rca_results` |
| Ontology | `ontology_nodes`, `ontology_edges` |
| Supporting | `insurance_results`, `audit_log` |

### Migrations

| Version | Description |
|---|---|
| `0001` | Full initial schema — all 17 tables |
| `0002` | Ontology-first redesign — ~40 new columns adding ontology entity references to `pipeline_runs`, `stage_runs`, `controls`, `control_results`, `incidents`, `rca_results`; widens node/relationship ID columns to `VARCHAR(80)`/`VARCHAR(30)`; adds `properties JSONB` to `ontology_nodes` |

---

## Project Structure

```
bank-pipeline-api/
├── docker-compose.yml
├── .env.example
├── seed_data/
│   ├── ontology_seed_v2.json       # 62 nodes, 101 edges (v2 canonical ontology)
│   ├── control_registry_v2.json    # 21 controls with full ontology chain columns
│   ├── sample_accounts.csv
│   ├── sample_trust_accounts.csv
│   └── sample_wire_transactions.csv
└── backend/
    ├── Dockerfile
    ├── requirements.txt
    ├── alembic.ini
    ├── alembic/
    │   └── versions/
    │       ├── 0001_initial.py
    │       └── 0002_ontology_v2.py
    └── app/
        ├── main.py
        ├── core/
        │   └── config.py           # Settings (SMDIA, DB URL, env)
        ├── db/
        │   ├── models.py           # All SQLAlchemy ORM models
        │   └── session.py          # Async session factory
        ├── api/
        │   ├── runs.py             # POST /runs
        │   ├── seed.py             # POST /seed/load, GET /seed/status
        │   └── rca.py              # /incidents + /rca/* endpoints
        └── services/
            ├── pipeline_runner.py  # Stage orchestration, STAGE_PIPELINE_MAP
            ├── control_engine.py   # 21 control evaluators, CONTROL_IMPLICATED_ENTITIES
            └── incident_service.py # Incident creation, _CONTROL_META v2 dict
```
