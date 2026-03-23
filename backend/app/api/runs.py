"""
Pipeline runs router — POST /runs, GET /runs/{run_id}
"""
import logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.pipeline_runner import run_pipeline

log = logging.getLogger(__name__)
router = APIRouter(prefix="/runs", tags=["runs"])


class RunRequest(BaseModel):
    run_name: str | None = None
    scenario_id: str | None = None
    triggered_by: str = "api"


@router.post("")
async def trigger_run(body: RunRequest, db: AsyncSession = Depends(get_db)):
    """Trigger a full pipeline run synchronously and return the summary."""
    result = await run_pipeline(
        db,
        run_name=body.run_name,
        scenario_id=body.scenario_id,
        triggered_by=body.triggered_by,
    )
    return result


@router.get("")
async def list_runs(limit: int = 20, db: AsyncSession = Depends(get_db)):
    rows = await db.execute(
        text("""
            SELECT run_id, run_name, scenario_id, triggered_by, status,
                   started_at, completed_at, total_records, error_count
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT :limit
        """),
        {"limit": limit},
    )
    runs = rows.mappings().fetchall()
    return [dict(r) for r in runs]


@router.get("/{run_id}")
async def get_run(run_id: str, db: AsyncSession = Depends(get_db)):
    row = await db.execute(
        text("""
            SELECT r.run_id, r.run_name, r.scenario_id, r.status,
                   r.started_at, r.completed_at, r.error_count,
                   (
                       SELECT json_agg(json_build_object(
                           'stage_run_id', sr.stage_run_id,
                           'stage_name', sr.stage_name,
                           'system', sr.system,
                           'status', sr.status,
                           'records_in', sr.records_in,
                           'records_out', sr.records_out,
                           'records_errored', sr.records_errored
                       ) ORDER BY sr.started_at)
                       FROM stage_runs sr WHERE sr.run_id = r.run_id
                   ) AS stages,
                   (
                       SELECT json_agg(json_build_object(
                           'control_id', cr.control_id,
                           'status', cr.status,
                           'fail_count', cr.fail_count,
                           'warn_count', cr.warn_count
                       ))
                       FROM control_results cr WHERE cr.run_id = r.run_id
                   ) AS control_results
            FROM pipeline_runs r
            WHERE r.run_id = :run_id
        """),
        {"run_id": run_id},
    )
    data = row.mappings().fetchone()
    if not data:
        raise HTTPException(status_code=404, detail="Run not found")
    return dict(data)


@router.get("/{run_id}/incidents")
async def get_run_incidents(run_id: str, db: AsyncSession = Depends(get_db)):
    rows = await db.execute(
        text("""
            SELECT incident_id, control_id, title, severity, status,
                   source_system, stage, ontology_entry_node, created_at,
                   rca_triggered, rca_completed
            FROM incidents
            WHERE run_id = :run_id
            ORDER BY severity, created_at
        """),
        {"run_id": run_id},
    )
    return [dict(r) for r in rows.mappings().fetchall()]
