"""
RCA (Root Cause Analysis) endpoints.

GET  /rca/context/{incident_id}  — returns the full evidence + ontology traversal context
POST /rca/results/{incident_id}  — accepts RCA result from an external agent
GET  /incidents                  — list all incidents
GET  /incidents/{incident_id}    — get a single incident with evidence
"""
import json
import logging
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db

log = logging.getLogger(__name__)
router = APIRouter(tags=["rca"])


# ─────────────────────────────────────────────────────────────────────
# Incidents
# ─────────────────────────────────────────────────────────────────────

@router.get("/incidents")
async def list_incidents(
    status: Optional[str] = None,
    severity: Optional[str] = None,
    limit: int = 50,
    db: AsyncSession = Depends(get_db),
):
    filters = "WHERE 1=1"
    params: dict = {"limit": limit}
    if status:
        filters += " AND i.status = :status"
        params["status"] = status
    if severity:
        filters += " AND i.severity = :severity"
        params["severity"] = severity

    rows = await db.execute(
        text(f"""
            SELECT i.incident_id, i.control_id, i.title, i.severity, i.status,
                   i.source_system, i.stage, i.ontology_entry_node,
                   i.created_at, i.rca_triggered, i.rca_completed, i.confidence_score,
                   i.root_cause_node,
                   i.failed_control_objective_node, i.failed_rule_node,
                   i.failed_transformation_node, i.implicated_pipeline_node,
                   i.implicated_script_node, i.implicated_system_node,
                   i.implicated_job_node, i.owner_node
            FROM incidents i
            {filters}
            ORDER BY i.severity ASC, i.created_at DESC
            LIMIT :limit
        """),
        params,
    )
    return [dict(r) for r in rows.mappings().fetchall()]


@router.get("/incidents/{incident_id}")
async def get_incident(incident_id: str, db: AsyncSession = Depends(get_db)):
    row = await db.execute(
        text("""
            SELECT i.*,
                   (
                       SELECT json_agg(json_build_object(
                           'evidence_id', e.evidence_id,
                           'evidence_type', e.evidence_type,
                           'source_system', e.source_system,
                           'artifact_ref', e.artifact_ref,
                           'content_json', e.content_json,
                           'collected_at', e.collected_at
                       ))
                       FROM incident_evidence e WHERE e.incident_id = i.incident_id
                   ) AS evidence
            FROM incidents i
            WHERE i.incident_id = :incident_id
        """),
        {"incident_id": incident_id},
    )
    data = row.mappings().fetchone()
    if not data:
        raise HTTPException(status_code=404, detail="Incident not found")
    return dict(data)


# ─────────────────────────────────────────────────────────────────────
# RCA context — external agent entry point
# ─────────────────────────────────────────────────────────────────────

@router.get("/rca/context/{incident_id}")
async def get_rca_context(incident_id: str, db: AsyncSession = Depends(get_db)):
    """
    Returns structured RCA context for an incident, ready for consumption
    by an external AI agent. Includes:
    - Incident details
    - Full v2 control chain (regulation → control_objective → rule → transformation → pipeline → script → owner)
    - Implicated entities (system, job, tables, columns, code_event, log_source)
    - All collected evidence
    - Ontology BFS traversal from entry_node
    - Agent instructions
    """
    # 1. Incident
    inc_row = await db.execute(
        text("SELECT * FROM incidents WHERE incident_id = :id"),
        {"id": incident_id},
    )
    incident = inc_row.mappings().fetchone()
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")
    incident = dict(incident)

    # 2. Control metadata (v2 chain columns)
    ctl_row = await db.execute(
        text("""
            SELECT control_id, control_name, description, source_ref, cfr_citation,
                   stage, system, rule_type, severity, ontology_node_id,
                   remediation, applicable_fields,
                   regulation_node_id, control_objective_node_id, rule_node_id,
                   transformation_node_id, pipeline_node_id, script_node_id, owner_node_id
            FROM controls WHERE control_id = :cid
        """),
        {"cid": incident.get("control_id")},
    )
    control = dict(ctl_row.mappings().fetchone() or {})

    # 3. Evidence
    ev_rows = await db.execute(
        text("""
            SELECT evidence_type, source_system, artifact_ref, content_json, collected_at
            FROM incident_evidence WHERE incident_id = :id
        """),
        {"id": incident_id},
    )
    evidence = [dict(r) for r in ev_rows.mappings().fetchall()]

    # Helper: look up a single ontology node
    async def _fetch_node(node_id: str | None) -> dict | None:
        if not node_id:
            return None
        r = await db.execute(
            text("""
                SELECT node_id, node_type, label, description, system, properties
                FROM ontology_nodes WHERE node_id = :nid
            """),
            {"nid": node_id},
        )
        row = r.mappings().fetchone()
        return dict(row) if row else None

    # Helper: look up multiple ontology nodes by ID list
    async def _fetch_nodes(node_ids: list[str]) -> list[dict]:
        result = []
        for nid in node_ids:
            n = await _fetch_node(nid)
            if n:
                result.append(n)
        return result

    # 4. Build v2 control chain from incident fields (prefer incident, fall back to control table)
    chain_reg_id    = control.get("regulation_node_id")
    chain_co_id     = incident.get("failed_control_objective_node") or control.get("control_objective_node_id")
    chain_rule_id   = incident.get("failed_rule_node") or control.get("rule_node_id")
    chain_trn_id    = incident.get("failed_transformation_node") or control.get("transformation_node_id")
    chain_pl_id     = incident.get("implicated_pipeline_node") or control.get("pipeline_node_id")
    chain_scr_id    = incident.get("implicated_script_node") or control.get("script_node_id")
    chain_owner_id  = incident.get("owner_node") or control.get("owner_node_id")

    control_chain = {
        "regulation":         await _fetch_node(chain_reg_id),
        "control_objective":  await _fetch_node(chain_co_id),
        "rule":               await _fetch_node(chain_rule_id),
        "transformation":     await _fetch_node(chain_trn_id),
        "pipeline":           await _fetch_node(chain_pl_id),
        "script":             await _fetch_node(chain_scr_id),
        "owner":              await _fetch_node(chain_owner_id),
    }

    # 5. Implicated entities from control_result (latest for this incident's control_result_id)
    cr_row = await db.execute(
        text("""
            SELECT implicated_pipeline_node, implicated_script_node,
                   implicated_transformation_node, implicated_table_nodes,
                   implicated_column_nodes, code_event_node, log_source_node
            FROM control_results
            WHERE result_id = :rid
        """),
        {"rid": str(incident.get("control_result_id"))},
    )
    cr = cr_row.mappings().fetchone()
    cr = dict(cr) if cr else {}

    table_ids  = cr.get("implicated_table_nodes") or []
    column_ids = cr.get("implicated_column_nodes") or []
    if isinstance(table_ids, str):
        table_ids = json.loads(table_ids)
    if isinstance(column_ids, str):
        column_ids = json.loads(column_ids)

    implicated_entities = {
        "system":     await _fetch_node(incident.get("implicated_system_node")),
        "job":        await _fetch_node(incident.get("implicated_job_node")),
        "tables":     await _fetch_nodes(table_ids),
        "columns":    await _fetch_nodes(column_ids),
        "code_event": await _fetch_node(cr.get("code_event_node")),
        "log_source": await _fetch_node(cr.get("log_source_node")),
    }

    # 6. Ontology BFS traversal — up to 5 hops from entry_node
    entry_node_id = incident.get("ontology_entry_node") or control.get("ontology_node_id")
    ontology_path = []
    if entry_node_id:
        visited = {entry_node_id}
        queue = [entry_node_id]
        hops = 0
        while queue and hops < 5:
            current_level = queue[:]
            queue = []
            for nid in current_level:
                node_row = await db.execute(
                    text("""
                        SELECT n.node_id, n.node_type, n.label, n.description,
                               n.evidence_fields, n.stop_condition, n.confidence_boost, n.properties
                        FROM ontology_nodes n WHERE n.node_id = :nid
                    """),
                    {"nid": nid},
                )
                node = node_row.mappings().fetchone()
                if node:
                    edge_rows = await db.execute(
                        text("""
                            SELECT e.to_node_id, e.relationship, e.weight, e.description,
                                   tn.node_type AS to_node_type, tn.label AS to_label
                            FROM ontology_edges e
                            JOIN ontology_nodes tn ON tn.node_id = e.to_node_id
                            WHERE e.from_node_id = :nid
                            ORDER BY e.weight DESC
                        """),
                        {"nid": nid},
                    )
                    edges = [dict(r) for r in edge_rows.mappings().fetchall()]
                    ontology_path.append({
                        **dict(node),
                        "out_edges": edges,
                    })
                    for edge in edges:
                        next_id = edge["to_node_id"]
                        if next_id not in visited:
                            visited.add(next_id)
                            queue.append(next_id)
            hops += 1

    # 7. Related incidents (same control_id, last 30 days)
    hist_rows = await db.execute(
        text("""
            SELECT incident_id, status, created_at, rca_completed, root_cause_node
            FROM incidents
            WHERE control_id = :cid
              AND incident_id <> :iid
              AND created_at > now() - INTERVAL '30 days'
            ORDER BY created_at DESC
            LIMIT 5
        """),
        {"cid": incident.get("control_id"), "iid": incident_id},
    )
    control_history = [dict(r) for r in hist_rows.mappings().fetchall()]

    # Mark rca_triggered
    await db.execute(
        text("UPDATE incidents SET rca_triggered = true WHERE incident_id = :id"),
        {"id": incident_id},
    )
    await db.commit()

    return {
        "incident": incident,
        "control": control,
        "control_chain": control_chain,
        "implicated_entities": implicated_entities,
        "evidence": evidence,
        "ontology_traversal": {
            "entry_node_id": entry_node_id,
            "path": ontology_path,
        },
        "control_history": control_history,
        "agent_instructions": {
            "objective": (
                "Identify the root cause entity responsible for the control failure. "
                "The control_chain gives you the full regulatory chain from regulation to script. "
                "The implicated_entities give you the specific operational artefacts (system, job, tables, columns, code_event). "
                "Confirm which entity (Script, Transformation, Rule, Table, Column, CodeEvent) is the precise root cause "
                "and submit your result to POST /rca/results/{incident_id} with root_cause_entity_type and root_cause_entity_id."
            ),
            "submit_url": f"/rca/results/{incident_id}",
            "required_fields": [
                "root_cause", "root_cause_entity_type", "root_cause_entity_id",
                "confidence_score", "recommendation", "evidence_summary",
                "supporting_entities", "traversal_path", "reasoning_summary", "submitted_by",
            ],
        },
    }


# ─────────────────────────────────────────────────────────────────────
# RCA result submission — external agent writes back
# ─────────────────────────────────────────────────────────────────────

class RCAResultPayload(BaseModel):
    root_cause: str
    root_cause_entity_type: str               # e.g. "Script" | "Transformation" | "Rule"
    root_cause_entity_id: str                 # e.g. "SCR-ORC-ASSIGNMENT"
    root_cause_node: Optional[str] = None     # legacy compat — ontology node ID string
    confidence_score: float
    recommendation: str
    evidence_summary: str
    supporting_entities: Optional[list] = None  # [{entity_type, entity_id, rationale}]
    traversal_path: Optional[list] = None
    reasoning_summary: Optional[str] = None
    submitted_by: str = "external_agent"


@router.post("/rca/results/{incident_id}")
async def submit_rca_result(
    incident_id: str,
    payload: RCAResultPayload,
    db: AsyncSession = Depends(get_db),
):
    """Accept an RCA result submitted by an external agent."""
    # Verify incident exists
    inc_row = await db.execute(
        text("SELECT incident_id FROM incidents WHERE incident_id = :id"),
        {"id": incident_id},
    )
    if not inc_row.scalar():
        raise HTTPException(status_code=404, detail="Incident not found")

    # Resolve root_cause_node: prefer explicit field, fall back to entity_id
    root_cause_node = payload.root_cause_node or payload.root_cause_entity_id

    rca_id = uuid.uuid4()
    await db.execute(
        text("""
            INSERT INTO rca_results
                (rca_result_id, incident_id, root_cause, root_cause_node,
                 root_cause_entity_type, root_cause_entity_id, supporting_entities,
                 confidence_score, recommendation, evidence_summary,
                 traversal_path, reasoning_summary, status, submitted_by)
            VALUES
                (:rca_result_id, :incident_id, :root_cause, :root_cause_node,
                 :root_cause_entity_type, :root_cause_entity_id, CAST(:supporting_entities AS jsonb),
                 :confidence_score, :recommendation, :evidence_summary,
                 CAST(:traversal_path AS jsonb), :reasoning_summary, 'SUBMITTED', :submitted_by)
            ON CONFLICT (incident_id) DO UPDATE SET
                root_cause = EXCLUDED.root_cause,
                root_cause_node = EXCLUDED.root_cause_node,
                root_cause_entity_type = EXCLUDED.root_cause_entity_type,
                root_cause_entity_id = EXCLUDED.root_cause_entity_id,
                supporting_entities = EXCLUDED.supporting_entities,
                confidence_score = EXCLUDED.confidence_score,
                recommendation = EXCLUDED.recommendation,
                reasoning_summary = EXCLUDED.reasoning_summary,
                status = 'REVISED',
                submitted_by = EXCLUDED.submitted_by
        """),
        {
            "rca_result_id": str(rca_id),
            "incident_id": incident_id,
            "root_cause": payload.root_cause,
            "root_cause_node": root_cause_node,
            "root_cause_entity_type": payload.root_cause_entity_type,
            "root_cause_entity_id": payload.root_cause_entity_id,
            "supporting_entities": json.dumps(payload.supporting_entities or []),
            "confidence_score": payload.confidence_score,
            "recommendation": payload.recommendation,
            "evidence_summary": payload.evidence_summary,
            "traversal_path": json.dumps(payload.traversal_path or []),
            "reasoning_summary": payload.reasoning_summary,
            "submitted_by": payload.submitted_by,
        },
    )

    await db.execute(
        text("""
            UPDATE incidents
            SET rca_completed = true,
                confidence_score = :confidence_score,
                root_cause_node = :root_cause_node,
                status = 'RCA_COMPLETE',
                updated_at = now()
            WHERE incident_id = :id
        """),
        {
            "id": incident_id,
            "confidence_score": payload.confidence_score,
            "root_cause_node": root_cause_node,
        },
    )

    await db.commit()
    log.info(
        "RCA submitted | incident=%s entity_type=%s entity_id=%s confidence=%.2f",
        incident_id, payload.root_cause_entity_type, payload.root_cause_entity_id, payload.confidence_score,
    )
    return {
        "rca_result_id": str(rca_id),
        "incident_id": incident_id,
        "status": "SUBMITTED",
        "root_cause_node": root_cause_node,
        "root_cause_entity_type": payload.root_cause_entity_type,
        "root_cause_entity_id": payload.root_cause_entity_id,
        "confidence_score": payload.confidence_score,
    }


@router.get("/rca/results/{incident_id}")
async def get_rca_result(incident_id: str, db: AsyncSession = Depends(get_db)):
    row = await db.execute(
        text("SELECT * FROM rca_results WHERE incident_id = :id"),
        {"id": incident_id},
    )
    data = row.mappings().fetchone()
    if not data:
        raise HTTPException(status_code=404, detail="No RCA result for this incident")
    return dict(data)
