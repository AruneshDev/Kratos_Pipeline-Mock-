# TASKS.md — Implementation Task List

> Source: ARCHITECTURE.md + DECISIONS.md
> Last updated: 2026-03-23

---

## PHASE 0 — Contract Freeze
- [x] Create DECISIONS.md
- [x] Create TASKS.md
- [x] Create control_registry.json seed file
- [x] Create ontology_seed.json seed file

## PHASE 1 — Repo + Container Scaffold
- [x] Create `backend/` folder structure
- [x] Create `app/main.py` (FastAPI app factory)
- [x] Create `app/core/config.py` (env-based config)
- [x] Create `app/api/health.py` (GET /health)
- [x] Create `app/db/session.py` (postgres connection)
- [x] Create `Dockerfile`
- [x] Create `docker-compose.yml`
- [x] Create `.env.example`
- [x] Create `requirements.txt`
- [x] Verify: `docker compose up` starts cleanly
- [x] Verify: `GET /health` returns `{"status": "ok"}`
- [x] Verify: postgres accessible from API container

## PHASE 2 — Database Foundation
- [x] Create `app/db/models.py` (SQLAlchemy models)
- [x] Create `app/db/base.py` (declarative base)
- [x] Create Alembic migration for initial schema
- [x] Tables: pipeline_runs, stage_runs, accounts, trust_accounts,
           beneficiaries, wire_transactions, controls, control_results,
           incidents, incident_evidence, rca_results, audit_log,
           ontology_nodes, ontology_edges, parties
- [x] Verify: `alembic upgrade head` creates all tables
- [x] Verify: API can write + read a test row

## PHASE 3 — Seed + Sample Data Load
- [x] Create `app/seed/seed_accounts.py`
- [x] Create `app/seed/seed_trusts.py`
- [x] Create `app/seed/seed_wires.py`
- [x] Create `app/seed/seed_controls.py` (reads control_registry.json)
- [x] Create `app/seed/seed_ontology.py` (reads ontology_seed.json)
- [x] Create `app/seed/seed_parties.py`
- [x] Create `app/api/seed.py` — POST /seed/load, GET /seed/status
- [x] Verify: POST /seed/load populates all tables
- [x] Verify: GET /seed/status shows record counts
- [x] Verify: re-run is idempotent

## PHASE 4 — Pipeline Runner
- [x] Create `app/services/pipeline_runner.py`
- [x] Create pipeline stage modules in `app/services/stages/`
- [x] Create `app/api/runs.py`
- [x] Endpoints: POST /runs, GET /runs, GET /runs/{run_id}, GET /runs/{run_id}/incidents
- [x] Verify: run is created with a run_id
- [x] Verify: stages execute and persist to stage_runs
- [x] Verify: run status queryable

## PHASE 5 — Control Engine
- [x] Create `app/services/control_engine.py` (21 deterministic evaluators)
- [x] STAGE_CONTROL_MAP maps stage+system tuples to evaluator functions
- [x] evaluate_stage_controls() dispatches evaluators, saves ControlResult rows
- [x] Verify: ≥3 controls evaluate and produce FAIL results
- [x] Verify: evidence_json is populated (CTL-TRUST-001: 3 IRR violations)

## PHASE 6 — Incident Service
- [x] Create `app/services/incident_service.py`
- [x] _CONTROL_META maps control_id to (severity, ontology_entry_node)
- [x] create_incident_from_control() creates Incident + IncidentEvidence rows
- [x] Verify: failed controls create incidents (7 incidents from last run)
- [x] Verify: incidents have structured evidence (3 IRR trusts in evidence_json)

## PHASE 7 — RCA Handoff Contract
- [x] Create `app/api/rca.py`
- [x] Endpoints: GET /incidents, GET /incidents/{id}, GET /rca/context/{incident_id},
             POST /rca/results/{incident_id}, GET /rca/results/{incident_id}
- [x] BFS ontology traversal (up to 5 hops) from entry_node
- [x] Verify: GET /rca/context returns full payload with ontology traversal
- [x] Verify: RCA result persisted and incident updated to RCA_COMPLETE

## PHASE 8 — First End-to-End Vertical Slice (SCN-002: IRR Trust ORC Misclassification) ✅
- [x] SCN-002 seed data: TR-003, TR-008, TR-015 (trust_type=IRR, orc_assigned=SGL)
- [x] Pipeline run fires CTL-TRUST-001 → 3 failures, 7 total incidents created
- [x] P1 incident f024ea9d-b040-4293-a88a-4afc39827bae created with full evidence
- [x] GET /rca/context returns ontology traversal from ONT-SYM-ORC-MISCLASSIFICATION
- [x] POST /rca/results submitted by copilot-rca-agent (confidence=0.95, node=ONT-RC-MISSING-ORC-BRANCH)
- [x] Incident updated: status=RCA_COMPLETE, rca_completed=true, confidence_score=0.95

## PHASE 9 — Testing + Stabilization
- [ ] Unit tests for each control rule
- [ ] Integration test: full E2E flow
- [ ] Schema validation tests
- [ ] Error handling + structured logging

## PHASE 10 — Optional UI
- [ ] Simple HTML/JS or React frontend
- [ ] Run trigger, incident list, evidence view
