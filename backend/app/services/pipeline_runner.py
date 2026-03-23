"""
Pipeline runner service.

Creates a PipelineRun, iterates through all stage definitions,
invokes the control engine for each stage, and writes StageRun records.
"""
import uuid
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings

log = logging.getLogger(__name__)

UTC = timezone.utc

# Maps (stage_name, system) → pipeline ontology node ID
STAGE_PIPELINE_MAP: dict[tuple[str, str], str] = {
    ("orc_assign",    "legacy_deposit"): "PL-DEPOSIT-ORC-ASSIGN",
    ("orc_assign",    "trust_custody"):  "PL-TRUST-ORC-ASSIGN",
    ("orc_assign",    "wire_transfer"):  "PL-WIRE-SCREENING",
    ("aggregation",   "legacy_deposit"): "PL-DEPOSIT-AGGREGATION",
    ("aggregation",   "trust_custody"):  "PL-DEPOSIT-AGGREGATION",
    ("insurance",     "legacy_deposit"): "PL-DEPOSIT-INSURANCE-CALC",
    ("insurance",     "trust_custody"):  "PL-TRUST-INSURANCE-CALC",
    ("insurance",     "wire_transfer"):  "PL-WIRE-RECONCILIATION",
    ("screening",     "wire_transfer"):  "PL-WIRE-SCREENING",
    ("reconciliation","wire_transfer"):  "PL-WIRE-RECONCILIATION",
}

# Maps system name → system ontology node ID
SYSTEM_NODE_MAP: dict[str, str] = {
    "legacy_deposit": "SYS-LEGACY-DEPOSIT",
    "trust_custody":  "SYS-TRUST-CUSTODY",
    "wire_transfer":  "SYS-WIRE-TRANSFER",
    "all":            "SYS-PIPELINE-API",
}

# Stages executed in order. Each entry: (stage_name, system, applicable_systems)
PIPELINE_STAGES = [
    ("ingestion",    "legacy_deposit",  ["legacy_deposit"]),
    ("ingestion",    "trust_custody",   ["trust_custody"]),
    ("ingestion",    "wire_transfer",   ["wire_transfer"]),
    ("orc_assign",   "legacy_deposit",  ["legacy_deposit"]),
    ("orc_assign",   "trust_custody",   ["trust_custody"]),
    ("orc_assign",   "wire_transfer",   ["wire_transfer"]),
    ("aggregation",  "legacy_deposit",  ["legacy_deposit"]),
    ("aggregation",  "trust_custody",   ["trust_custody"]),
    ("insurance",    "legacy_deposit",  ["legacy_deposit"]),
    ("insurance",    "trust_custody",   ["trust_custody"]),
    ("insurance",    "wire_transfer",   ["wire_transfer"]),
    ("screening",    "wire_transfer",   ["wire_transfer"]),
    ("reconciliation","wire_transfer",  ["wire_transfer"]),
    ("reporting",    "all",             ["legacy_deposit", "trust_custody", "wire_transfer"]),
]


async def create_run(
    db: AsyncSession,
    run_name: str | None = None,
    scenario_id: str | None = None,
    triggered_by: str = "api",
) -> uuid.UUID:
    run_id = uuid.uuid4()
    await db.execute(
        text("""
            INSERT INTO pipeline_runs
                (run_id, run_name, scenario_id, triggered_by, status, started_at,
                 job_node_id, system_node_id)
            VALUES
                (:run_id, :run_name, :scenario_id, :triggered_by, 'RUNNING', now(),
                 'JOB-NIGHTLY-PIPELINE-RUN', 'SYS-PIPELINE-API')
        """),
        {"run_id": str(run_id), "run_name": run_name, "scenario_id": scenario_id, "triggered_by": triggered_by},
    )
    await db.flush()
    return run_id


async def complete_run(db: AsyncSession, run_id: uuid.UUID, error_count: int = 0) -> None:
    status = "FAILED" if error_count > 0 else "COMPLETED"
    await db.execute(
        text("""
            UPDATE pipeline_runs
            SET status = :status, completed_at = now(), error_count = :error_count
            WHERE run_id = :run_id
        """),
        {"run_id": str(run_id), "status": status, "error_count": error_count},
    )


async def create_stage_run(
    db: AsyncSession,
    run_id: uuid.UUID,
    stage_name: str,
    system: str,
) -> uuid.UUID:
    stage_run_id = uuid.uuid4()
    pipeline_node_id = STAGE_PIPELINE_MAP.get((stage_name, system))
    await db.execute(
        text("""
            INSERT INTO stage_runs
                (stage_run_id, run_id, stage_name, system, status, started_at, pipeline_node_id)
            VALUES (:stage_run_id, :run_id, :stage_name, :system, 'RUNNING', now(), :pipeline_node_id)
        """),
        {
            "stage_run_id": str(stage_run_id),
            "run_id": str(run_id),
            "stage_name": stage_name,
            "system": system,
            "pipeline_node_id": pipeline_node_id,
        },
    )
    await db.flush()
    return stage_run_id


async def complete_stage_run(
    db: AsyncSession,
    stage_run_id: uuid.UUID,
    records_in: int = 0,
    records_out: int = 0,
    records_errored: int = 0,
    status: str = "COMPLETED",
) -> None:
    await db.execute(
        text("""
            UPDATE stage_runs
            SET status = :status,
                completed_at = now(),
                records_in = :records_in,
                records_out = :records_out,
                records_errored = :records_errored
            WHERE stage_run_id = :stage_run_id
        """),
        {
            "stage_run_id": str(stage_run_id),
            "status": status,
            "records_in": records_in,
            "records_out": records_out,
            "records_errored": records_errored,
        },
    )


async def run_pipeline(
    db: AsyncSession,
    run_name: str | None = None,
    scenario_id: str | None = None,
    triggered_by: str = "api",
) -> dict[str, Any]:
    """
    Execute the full pipeline:
    1. Create PipelineRun
    2. For each stage/system: create StageRun, run controls, complete StageRun
    3. Complete PipelineRun
    Returns a summary dict.
    """
    from app.services.control_engine import evaluate_stage_controls  # local import to avoid circular deps

    run_id = await create_run(db, run_name=run_name, scenario_id=scenario_id, triggered_by=triggered_by)
    log.info("Pipeline run started | run_id=%s scenario=%s", run_id, scenario_id)

    stage_summaries = []
    total_errors = 0

    for stage_name, system, _ in PIPELINE_STAGES:
        stage_run_id = await create_stage_run(db, run_id, stage_name, system)
        try:
            result = await evaluate_stage_controls(db, run_id, stage_run_id, stage_name, system)
            await complete_stage_run(
                db,
                stage_run_id,
                records_in=result.get("records_evaluated", 0),
                records_out=result.get("records_passed", 0),
                records_errored=result.get("failures", 0),
                status="COMPLETED",
            )
            stage_summaries.append({
                "stage": stage_name,
                "system": system,
                "status": "COMPLETED",
                **result,
            })
            total_errors += result.get("failures", 0)
        except Exception as exc:
            log.exception("Stage %s/%s failed: %s", stage_name, system, exc)
            await complete_stage_run(db, stage_run_id, status="FAILED")
            stage_summaries.append({"stage": stage_name, "system": system, "status": "FAILED", "error": str(exc)})
            total_errors += 1

    await complete_run(db, run_id, error_count=total_errors)
    await db.commit()
    log.info("Pipeline run complete | run_id=%s errors=%d", run_id, total_errors)

    return {
        "run_id": str(run_id),
        "status": "COMPLETED" if total_errors == 0 else "COMPLETED_WITH_ERRORS",
        "total_stage_errors": total_errors,
        "stages": stage_summaries,
    }
