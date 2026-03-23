"""
Control engine — deterministic evaluators for all 22 controls.

Each control evaluator returns a ControlEvalResult dict:
    {
        "status": "PASS" | "FAIL" | "WARN" | "SKIP",
        "affected_records": int,
        "fail_count": int,
        "pass_count": int,
        "warn_count": int,
        "evidence_json": dict | None,
    }

evaluate_stage_controls() is called by the pipeline runner for each
stage/system combination and dispatches to the appropriate evaluators.
"""
import json
import logging
import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings

log = logging.getLogger(__name__)

SMDIA = settings.smdia

# ─────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────

async def _save_control_result(
    db: AsyncSession,
    control_id: str,
    run_id: uuid.UUID,
    stage_run_id: uuid.UUID,
    result: dict,
    implicated: dict | None = None,
) -> uuid.UUID:
    cr_id = uuid.uuid4()
    imp = implicated or {}
    await db.execute(
        text("""
            INSERT INTO control_results
                (result_id, control_id, run_id, stage_run_id, evaluated_at,
                 status, affected_records, fail_count, pass_count, warn_count, evidence_json,
                 implicated_pipeline_node, implicated_script_node, implicated_transformation_node,
                 implicated_table_nodes, implicated_column_nodes,
                 code_event_node, log_source_node)
            VALUES
                (:result_id, :control_id, :run_id, :stage_run_id, now(),
                 :status, :affected_records, :fail_count, :pass_count, :warn_count,
                 CAST(:evidence_json AS jsonb),
                 :implicated_pipeline_node, :implicated_script_node, :implicated_transformation_node,
                 CAST(:implicated_table_nodes AS jsonb), CAST(:implicated_column_nodes AS jsonb),
                 :code_event_node, :log_source_node)
        """),
        {
            "result_id": str(cr_id),
            "control_id": control_id,
            "run_id": str(run_id),
            "stage_run_id": str(stage_run_id),
            "status": result["status"],
            "affected_records": result.get("affected_records", 0),
            "fail_count": result.get("fail_count", 0),
            "pass_count": result.get("pass_count", 0),
            "warn_count": result.get("warn_count", 0),
            "evidence_json": json.dumps(result.get("evidence_json") or {}),
            "implicated_pipeline_node": imp.get("implicated_pipeline_node"),
            "implicated_script_node": imp.get("implicated_script_node"),
            "implicated_transformation_node": imp.get("implicated_transformation_node"),
            "implicated_table_nodes": json.dumps(imp.get("implicated_table_nodes", [])),
            "implicated_column_nodes": json.dumps(imp.get("implicated_column_nodes", [])),
            "code_event_node": imp.get("code_event_node"),
            "log_source_node": imp.get("log_source_node"),
        },
    )
    return cr_id


# ─────────────────────────────────────────────────────────────────────
# Per-control implicated entity metadata (v2 ontology)
# ─────────────────────────────────────────────────────────────────────

CONTROL_IMPLICATED_ENTITIES: dict[str, dict] = {
    "CTL-DEP-001": {
        "implicated_pipeline_node": "PL-DEPOSIT-INSURANCE-CALC",
        "implicated_script_node": "SCR-DEPOSIT-INSURANCE-CALC",
        "implicated_transformation_node": "TRN-INSURANCE-CALCULATION",
        "implicated_table_nodes": ["TBL-ACCOUNTS"],
        "implicated_column_nodes": ["COL-INSURED-AMOUNT"],
        "code_event_node": None,
        "log_source_node": "LOG-JCL-SYSOUT",
    },
    "CTL-DEP-002": {
        "implicated_pipeline_node": "PL-DEPOSIT-ORC-ASSIGN",
        "implicated_script_node": "SCR-ORC-ASSIGNMENT",
        "implicated_transformation_node": "TRN-ORC-CLASSIFICATION",
        "implicated_table_nodes": ["TBL-ACCOUNTS"],
        "implicated_column_nodes": ["COL-ORC-CODE"],
        "code_event_node": "EVT-ORC-ASSIGN-LAST-CHANGE",
        "log_source_node": "LOG-JCL-SYSOUT",
    },
    "CTL-DEP-003": {
        "implicated_pipeline_node": "PL-DEPOSIT-ORC-ASSIGN",
        "implicated_script_node": "SCR-ORC-ASSIGNMENT",
        "implicated_transformation_node": "TRN-ORC-CLASSIFICATION",
        "implicated_table_nodes": ["TBL-ACCOUNTS"],
        "implicated_column_nodes": ["COL-ORC-CODE"],
        "code_event_node": None,
        "log_source_node": "LOG-JCL-SYSOUT",
    },
    "CTL-DEP-004": {
        "implicated_pipeline_node": "PL-DEPOSIT-INSURANCE-CALC",
        "implicated_script_node": "SCR-SP-CALC-INSURANCE",
        "implicated_transformation_node": "TRN-INSURANCE-CALCULATION",
        "implicated_table_nodes": ["TBL-INSURANCE-RESULTS"],
        "implicated_column_nodes": ["COL-INSURED-AMOUNT"],
        "code_event_node": "EVT-SMDIA-CONFIG-SET",
        "log_source_node": "LOG-API-STDOUT",
    },
    "CTL-DEP-005": {
        "implicated_pipeline_node": "PL-DEPOSIT-ORC-ASSIGN",
        "implicated_script_node": "SCR-ORC-ASSIGNMENT",
        "implicated_transformation_node": "TRN-ORC-CLASSIFICATION",
        "implicated_table_nodes": ["TBL-ACCOUNTS"],
        "implicated_column_nodes": ["COL-ORC-CODE"],
        "code_event_node": None,
        "log_source_node": "LOG-JCL-SYSOUT",
    },
    "CTL-DEP-006": {
        "implicated_pipeline_node": "PL-DEPOSIT-INSURANCE-CALC",
        "implicated_script_node": "SCR-DEPOSIT-INSURANCE-CALC",
        "implicated_transformation_node": "TRN-INSURANCE-CALCULATION",
        "implicated_table_nodes": ["TBL-ACCOUNTS"],
        "implicated_column_nodes": [],
        "code_event_node": None,
        "log_source_node": "LOG-JCL-SYSOUT",
    },
    "CTL-DEP-007": {
        "implicated_pipeline_node": "PL-DEPOSIT-ORC-ASSIGN",
        "implicated_script_node": "SCR-ORC-ASSIGNMENT",
        "implicated_transformation_node": "TRN-ORC-CLASSIFICATION",
        "implicated_table_nodes": ["TBL-ACCOUNTS"],
        "implicated_column_nodes": ["COL-ORC-CODE"],
        "code_event_node": None,
        "log_source_node": "LOG-JCL-SYSOUT",
    },
    "CTL-DEP-008": {
        "implicated_pipeline_node": "PL-DEPOSIT-INSURANCE-CALC",
        "implicated_script_node": "SCR-DEPOSIT-INSURANCE-CALC",
        "implicated_transformation_node": "TRN-INSURANCE-CALCULATION",
        "implicated_table_nodes": ["TBL-ACCOUNTS"],
        "implicated_column_nodes": [],
        "code_event_node": None,
        "log_source_node": "LOG-JCL-SYSOUT",
    },
    "CTL-TRUST-001": {
        "implicated_pipeline_node": "PL-TRUST-ORC-ASSIGN",
        "implicated_script_node": "SCR-ORC-ASSIGNMENT",
        "implicated_transformation_node": "TRN-ORC-CLASSIFICATION",
        "implicated_table_nodes": ["TBL-TRUST-ACCOUNTS"],
        "implicated_column_nodes": ["COL-ORC-ASSIGNED", "COL-TRUST-TYPE"],
        "code_event_node": "EVT-ORC-ASSIGN-LAST-CHANGE",
        "log_source_node": "LOG-JCL-SYSOUT",
    },
    "CTL-TRUST-002": {
        "implicated_pipeline_node": "PL-TRUST-INSURANCE-CALC",
        "implicated_script_node": "SCR-TRUST-INSURANCE-CALC",
        "implicated_transformation_node": "TRN-BENEFICIARY-INTEREST-CALC",
        "implicated_table_nodes": ["TBL-TRUST-ACCOUNTS", "TBL-BENEFICIARIES"],
        "implicated_column_nodes": [],
        "code_event_node": None,
        "log_source_node": "LOG-JCL-SYSOUT",
    },
    "CTL-TRUST-003": {
        "implicated_pipeline_node": "PL-TRUST-INSURANCE-CALC",
        "implicated_script_node": "SCR-TRUST-INSURANCE-CALC",
        "implicated_transformation_node": "TRN-BENEFICIARY-INTEREST-CALC",
        "implicated_table_nodes": ["TBL-TRUST-ACCOUNTS"],
        "implicated_column_nodes": ["COL-INSURED-AMOUNT"],
        "code_event_node": None,
        "log_source_node": "LOG-JCL-SYSOUT",
    },
    "CTL-TRUST-004": {
        "implicated_pipeline_node": "PL-TRUST-INSURANCE-CALC",
        "implicated_script_node": "SCR-TRUST-INSURANCE-CALC",
        "implicated_transformation_node": "TRN-BENEFICIARY-INTEREST-CALC",
        "implicated_table_nodes": ["TBL-BENEFICIARIES"],
        "implicated_column_nodes": [],
        "code_event_node": None,
        "log_source_node": "LOG-JCL-SYSOUT",
    },
    "CTL-TRUST-005": {
        "implicated_pipeline_node": "PL-TRUST-INSURANCE-CALC",
        "implicated_script_node": "SCR-TRUST-INSURANCE-CALC",
        "implicated_transformation_node": "TRN-BENEFICIARY-INTEREST-CALC",
        "implicated_table_nodes": ["TBL-BENEFICIARIES"],
        "implicated_column_nodes": [],
        "code_event_node": None,
        "log_source_node": "LOG-JCL-SYSOUT",
    },
    "CTL-TRUST-006": {
        "implicated_pipeline_node": "PL-TRUST-INSURANCE-CALC",
        "implicated_script_node": "SCR-TRUST-INSURANCE-CALC",
        "implicated_transformation_node": "TRN-BENEFICIARY-INTEREST-CALC",
        "implicated_table_nodes": ["TBL-TRUST-ACCOUNTS"],
        "implicated_column_nodes": [],
        "code_event_node": None,
        "log_source_node": "LOG-JCL-SYSOUT",
    },
    "CTL-WIRE-001": {
        "implicated_pipeline_node": "PL-WIRE-SCREENING",
        "implicated_script_node": "SCR-OFAC-SCREENING",
        "implicated_transformation_node": "TRN-OFAC-SCREENING-CHECK",
        "implicated_table_nodes": ["TBL-WIRE-TRANSACTIONS"],
        "implicated_column_nodes": ["COL-OFAC-STATUS"],
        "code_event_node": "EVT-OFAC-BATCH-ORDER-DEFECT",
        "log_source_node": "LOG-WIRE-AUDIT",
    },
    "CTL-WIRE-002": {
        "implicated_pipeline_node": "PL-WIRE-SCREENING",
        "implicated_script_node": "SCR-OFAC-SCREENING",
        "implicated_transformation_node": "TRN-OFAC-SCREENING-CHECK",
        "implicated_table_nodes": ["TBL-WIRE-TRANSACTIONS"],
        "implicated_column_nodes": ["COL-OFAC-STATUS"],
        "code_event_node": "EVT-OFAC-BATCH-ORDER-DEFECT",
        "log_source_node": "LOG-WIRE-AUDIT",
    },
    "CTL-WIRE-003": {
        "implicated_pipeline_node": "PL-WIRE-RECONCILIATION",
        "implicated_script_node": None,
        "implicated_transformation_node": None,
        "implicated_table_nodes": ["TBL-WIRE-TRANSACTIONS"],
        "implicated_column_nodes": [],
        "code_event_node": None,
        "log_source_node": "LOG-WIRE-AUDIT",
    },
    "CTL-WIRE-004": {
        "implicated_pipeline_node": "PL-WIRE-RECONCILIATION",
        "implicated_script_node": None,
        "implicated_transformation_node": None,
        "implicated_table_nodes": ["TBL-WIRE-TRANSACTIONS"],
        "implicated_column_nodes": [],
        "code_event_node": None,
        "log_source_node": "LOG-WIRE-AUDIT",
    },
    "CTL-WIRE-005": {
        "implicated_pipeline_node": "PL-WIRE-SCREENING",
        "implicated_script_node": None,
        "implicated_transformation_node": "TRN-ORC-CLASSIFICATION",
        "implicated_table_nodes": ["TBL-WIRE-TRANSACTIONS"],
        "implicated_column_nodes": ["COL-ORC-CODE"],
        "code_event_node": None,
        "log_source_node": "LOG-WIRE-AUDIT",
    },
    "CTL-WIRE-006": {
        "implicated_pipeline_node": "PL-WIRE-RECONCILIATION",
        "implicated_script_node": None,
        "implicated_transformation_node": None,
        "implicated_table_nodes": ["TBL-WIRE-TRANSACTIONS", "TBL-INSURANCE-RESULTS"],
        "implicated_column_nodes": ["COL-INSURED-AMOUNT"],
        "code_event_node": None,
        "log_source_node": "LOG-WIRE-AUDIT",
    },
    "CTL-WIRE-007": {
        "implicated_pipeline_node": "PL-WIRE-RECONCILIATION",
        "implicated_script_node": None,
        "implicated_transformation_node": None,
        "implicated_table_nodes": ["TBL-WIRE-TRANSACTIONS"],
        "implicated_column_nodes": [],
        "code_event_node": None,
        "log_source_node": "LOG-WIRE-AUDIT",
    },
}


# ─────────────────────────────────────────────────────────────────────
# DEPOSIT SYSTEM CONTROLS
# ─────────────────────────────────────────────────────────────────────

async def ctl_dep_001(db: AsyncSession) -> dict:
    """
    CTL-DEP-001 — Per-Depositor ORC Aggregation.
    FAIL if any account has insured_amount > SMDIA (per-account calc instead of aggregated).
    Source: sp_calculate_insurance.sql:44 / DEPOSIT-INSURANCE-CALC.cob KNOWN_ISSUES
    """
    rows = await db.execute(
        text("""
            SELECT party_id, orc_code, SUM(balance) AS total_balance,
                   COUNT(*) AS account_count
            FROM accounts
            WHERE is_active = true AND party_id IS NOT NULL
            GROUP BY party_id, orc_code
            HAVING SUM(balance) > :smdia
        """),
        {"smdia": str(SMDIA)},
    )
    violations = rows.fetchall()
    total = await db.execute(text("SELECT COUNT(DISTINCT (party_id, orc_code)) FROM accounts WHERE is_active = true"))
    total_count = total.scalar() or 0

    if violations:
        return {
            "status": "FAIL",
            "affected_records": total_count,
            "fail_count": len(violations),
            "pass_count": total_count - len(violations),
            "warn_count": 0,
            "evidence_json": {
                "violation_summary": [
                    {"party_id": str(r.party_id), "orc_code": r.orc_code, "total_balance": str(r.total_balance)}
                    for r in violations[:10]
                ],
                "smdia": str(SMDIA),
                "rule": "Aggregate balances by (party_id, orc_code) before applying SMDIA",
            },
        }
    return {"status": "PASS", "affected_records": total_count, "fail_count": 0, "pass_count": total_count, "warn_count": 0}


async def ctl_dep_002(db: AsyncSession) -> dict:
    """
    CTL-DEP-002 — IRR ORC Falls Through to SGL.
    FAIL if any ACCOUNT has orc_code='SGL' but its trust_name suggests IRR classification.
    Also checks TRUST accounts separately (CTL-TRUST-001 handles trusts — this catches
    deposit accounts linked to IRR trusts).
    Source: ORC-ASSIGNMENT.cob — no WHEN 'IRR' PERFORM branch exists.
    """
    # For accounts: look for trust-linked accounts where the trust is IRR but ORC is SGL
    rows = await db.execute(
        text("""
            SELECT a.account_number, a.orc_code, t.trust_id, t.trust_type, t.orc_assigned
            FROM accounts a
            JOIN trust_accounts t ON a.trust_name = t.trust_name
            WHERE a.orc_code = 'SGL'
              AND t.trust_type = 'IRR'
        """),
    )
    violations = rows.fetchall()

    total_result = await db.execute(text("SELECT COUNT(*) FROM accounts WHERE is_active = true"))
    total_count = total_result.scalar() or 0

    if violations:
        return {
            "status": "FAIL",
            "affected_records": total_count,
            "fail_count": len(violations),
            "pass_count": total_count - len(violations),
            "warn_count": 0,
            "evidence_json": {
                "description": "IRR trust accounts assigned SGL ORC due to missing IRR branch in ORC-ASSIGNMENT.cob",
                "affected_accounts": [
                    {
                        "account_number": r.account_number,
                        "orc_code_assigned": r.orc_code,
                        "trust_id": r.trust_id,
                        "trust_type": r.trust_type,
                    }
                    for r in violations[:10]
                ],
                "source_defect": "ORC-ASSIGNMENT.cob — no WHEN 'IRR' PERFORM ORC-IRR-PROC branch",
                "cfr_citation": "12 CFR 330.13",
            },
        }
    return {"status": "PASS", "affected_records": total_count, "fail_count": 0, "pass_count": total_count, "warn_count": 0}


async def ctl_dep_003(db: AsyncSession) -> dict:
    """
    CTL-DEP-003 — Pending ORC Disposition.
    WARN if any accounts still have orc_pending_flag = 'Y' after pipeline run.
    Source: ACCOUNT-MASTER.cpy ORC-PENDING-FLAG.
    """
    rows = await db.execute(
        text("SELECT COUNT(*) FROM accounts WHERE orc_pending_flag = 'Y'")
    )
    pending = rows.scalar() or 0
    total_r = await db.execute(text("SELECT COUNT(*) FROM accounts"))
    total = total_r.scalar() or 0

    if pending > 0:
        return {
            "status": "WARN",
            "affected_records": total,
            "fail_count": 0,
            "pass_count": total - pending,
            "warn_count": pending,
            "evidence_json": {"pending_orc_accounts": pending, "flag_field": "orc_pending_flag"},
        }
    return {"status": "PASS", "affected_records": total, "fail_count": 0, "pass_count": total, "warn_count": 0}


async def ctl_dep_004(db: AsyncSession) -> dict:
    """
    CTL-DEP-004 — SMDIA Hardcode Drift.
    WARN if the SMDIA env var doesn't match the regulatory constant $250,000.
    Source: DEPOSIT-INSURANCE-CALC.cob:SMDIA-AMOUNT COMP-3 VALUE 250000.
    """
    regulatory_smdia = Decimal("250000.00")
    if SMDIA != regulatory_smdia:
        return {
            "status": "FAIL",
            "affected_records": 1,
            "fail_count": 1,
            "pass_count": 0,
            "warn_count": 0,
            "evidence_json": {
                "configured_smdia": str(SMDIA),
                "regulatory_smdia": str(regulatory_smdia),
                "source": "DEPOSIT-INSURANCE-CALC.cob:SMDIA-AMOUNT COMP-3 VALUE 250000",
            },
        }
    return {"status": "PASS", "affected_records": 1, "fail_count": 0, "pass_count": 1, "warn_count": 0}


async def ctl_dep_005(db: AsyncSession) -> dict:
    """
    CTL-DEP-005 — JNT Account Two-Owner Requirement.
    FAIL if any account with orc_code='JNT' has no joint_owner_id.
    Source: 12 CFR 330.9(a).
    """
    rows = await db.execute(
        text("""
            SELECT COUNT(*) FROM accounts
            WHERE orc_code = 'JNT' AND joint_owner_id IS NULL AND is_active = true
        """)
    )
    violations = rows.scalar() or 0
    total_r = await db.execute(text("SELECT COUNT(*) FROM accounts WHERE orc_code = 'JNT'"))
    total = total_r.scalar() or 0

    status = "FAIL" if violations > 0 else "PASS"
    return {
        "status": status,
        "affected_records": total,
        "fail_count": violations,
        "pass_count": total - violations,
        "warn_count": 0,
        "evidence_json": {"jnt_without_joint_owner": violations} if violations else None,
    }


async def ctl_dep_006(db: AsyncSession) -> dict:
    """
    CTL-DEP-006 — REV Trust Deceased Grantor.
    WARN if any REV trust accounts exist where related party has a death_date.
    Source: 12 CFR 330.10.
    """
    rows = await db.execute(
        text("""
            SELECT COUNT(*) FROM trust_accounts t
            JOIN parties p ON p.source_system = 'legacy_deposit'
            WHERE t.trust_type = 'REV'
              AND p.death_date IS NOT NULL
        """)
    )
    violations = rows.scalar() or 0
    return {
        "status": "WARN" if violations > 0 else "PASS",
        "affected_records": violations,
        "fail_count": 0,
        "pass_count": 0,
        "warn_count": violations,
        "evidence_json": {"rev_trusts_with_deceased_grantor": violations} if violations else None,
    }


async def ctl_dep_007(db: AsyncSession) -> dict:
    """
    CTL-DEP-007 — GOV Account Entity Type.
    FAIL if accounts with orc_code starting with 'GOV' have no govt_entity_type.
    Source: 12 CFR 330.15.
    """
    rows = await db.execute(
        text("""
            SELECT COUNT(*) FROM accounts
            WHERE orc_code LIKE 'GOV%'
              AND (govt_entity_type IS NULL OR govt_entity_type = '')
              AND is_active = true
        """)
    )
    violations = rows.scalar() or 0
    total_r = await db.execute(text("SELECT COUNT(*) FROM accounts WHERE orc_code LIKE 'GOV%'"))
    total = total_r.scalar() or 0
    return {
        "status": "FAIL" if violations > 0 else "PASS",
        "affected_records": total,
        "fail_count": violations,
        "pass_count": total - violations,
        "warn_count": 0,
    }


async def ctl_dep_008(db: AsyncSession) -> dict:
    """
    CTL-DEP-008 — Collateral Pledge Exclusion.
    FAIL if any collaterally pledged balances are included in the insured calculation.
    Source: 12 CFR 330.7(b).
    """
    rows = await db.execute(
        text("SELECT COUNT(*) FROM accounts WHERE collateral_pledged = true AND is_active = true")
    )
    flagged = rows.scalar() or 0
    return {
        "status": "WARN" if flagged > 0 else "PASS",
        "affected_records": flagged,
        "fail_count": 0,
        "pass_count": 0,
        "warn_count": flagged,
        "evidence_json": {"collateral_pledged_count": flagged} if flagged else None,
    }


# ─────────────────────────────────────────────────────────────────────
# TRUST CONTROLS
# ─────────────────────────────────────────────────────────────────────

async def ctl_trust_001(db: AsyncSession) -> dict:
    """
    CTL-TRUST-001 — IRR Trust ORC Misclassification.
    FAIL if trust_type='IRR' but orc_assigned <> 'IRR'.
    This is the primary SCN-002 control.
    Source: TRUST-INSURANCE-CALC.cob + ORC-ASSIGNMENT.cob no IRR branch.
    """
    rows = await db.execute(
        text("""
            SELECT trust_id, trust_name, trust_type, orc_assigned, balance
            FROM trust_accounts
            WHERE trust_type = 'IRR'
              AND (orc_assigned IS NULL OR orc_assigned <> 'IRR')
              AND trust_status = 'A'
        """)
    )
    violations = rows.fetchall()
    total_r = await db.execute(text("SELECT COUNT(*) FROM trust_accounts WHERE trust_type = 'IRR'"))
    total = total_r.scalar() or 0

    if violations:
        return {
            "status": "FAIL",
            "affected_records": total,
            "fail_count": len(violations),
            "pass_count": total - len(violations),
            "warn_count": 0,
            "evidence_json": {
                "description": "Irrevocable trusts assigned SGL ORC — missing IRR branch in ORC-ASSIGNMENT.cob",
                "affected_trusts": [
                    {
                        "trust_id": r.trust_id,
                        "trust_name": r.trust_name,
                        "trust_type": r.trust_type,
                        "orc_assigned": r.orc_assigned,
                        "balance": str(r.balance),
                    }
                    for r in violations
                ],
                "root_cause_hint": "ORC-ASSIGNMENT.cob EVALUATE ORC-TYPE WHEN 'IRR' branch is missing",
                "ontology_entry_node": "ONT-SYM-ORC-MISCLASSIFICATION",
                "cfr_citation": "12 CFR 330.13",
            },
        }
    return {"status": "PASS", "affected_records": total, "fail_count": 0, "pass_count": total, "warn_count": 0}


async def ctl_trust_002(db: AsyncSession) -> dict:
    """
    CTL-TRUST-002 — Beneficiary Coverage per Trust.
    WARN if a trust has 0 beneficiaries recorded.
    Source: TRUST-INSURANCE-CALC.cob:BENE-COUNT-CHECK.
    """
    rows = await db.execute(
        text("SELECT COUNT(*) FROM trust_accounts WHERE trust_status = 'A' AND beneficiary_count = 0")
    )
    zero_bene = rows.scalar() or 0
    total_r = await db.execute(text("SELECT COUNT(*) FROM trust_accounts WHERE trust_status = 'A'"))
    total = total_r.scalar() or 0
    return {
        "status": "WARN" if zero_bene > 0 else "PASS",
        "affected_records": total,
        "fail_count": 0,
        "pass_count": total - zero_bene,
        "warn_count": zero_bene,
        "evidence_json": {"trusts_with_no_beneficiaries": zero_bene} if zero_bene else None,
    }


async def ctl_trust_003(db: AsyncSession) -> dict:
    """
    CTL-TRUST-003 — EBP Coverage Cap Check ($250K per participant).
    WARN if EBP trust balance / participant_count > SMDIA.
    Source: TRUST-INSURANCE-CALC.cob:EBP-PROC.
    """
    rows = await db.execute(
        text("""
            SELECT trust_id, trust_name, balance, participant_count,
                   balance / NULLIF(participant_count, 0) AS per_participant
            FROM trust_accounts
            WHERE trust_type = 'EBP'
              AND trust_status = 'A'
              AND participant_count > 0
              AND balance / participant_count > :smdia
        """),
        {"smdia": str(SMDIA)},
    )
    violations = rows.fetchall()
    total_r = await db.execute(text("SELECT COUNT(*) FROM trust_accounts WHERE trust_type = 'EBP'"))
    total = total_r.scalar() or 0
    return {
        "status": "WARN" if violations else "PASS",
        "affected_records": total,
        "fail_count": 0,
        "pass_count": total - len(violations),
        "warn_count": len(violations),
        "evidence_json": {
            "ebp_over_cap": [
                {
                    "trust_id": r.trust_id,
                    "per_participant": str(r.per_participant),
                }
                for r in violations
            ]
        } if violations else None,
    }


async def ctl_trust_004(db: AsyncSession) -> dict:
    """
    CTL-TRUST-004 — Deceased Beneficiary.
    WARN if any beneficiary is flagged as deceased.
    Source: TRUST-BENEFICIARY.cpy BENE-DECEASED-FLAG.
    """
    rows = await db.execute(text("SELECT COUNT(*) FROM beneficiaries WHERE is_deceased = true"))
    deceased = rows.scalar() or 0
    return {
        "status": "WARN" if deceased > 0 else "PASS",
        "affected_records": deceased,
        "fail_count": 0, "pass_count": 0, "warn_count": deceased,
    }


async def ctl_trust_005(db: AsyncSession) -> dict:
    """
    CTL-TRUST-005 — Trust Allocation Percentage Sum.
    FAIL if any trust's beneficiaries don't sum to 100%.
    Source: TRUST-BENEFICIARY.cpy BENE-ALLOCATION-PCT.
    """
    rows = await db.execute(
        text("""
            SELECT trust_id, SUM(allocation_pct) AS total_pct
            FROM beneficiaries
            WHERE allocation_pct IS NOT NULL
            GROUP BY trust_id
            HAVING ABS(SUM(allocation_pct) - 100.0) > 0.01
        """)
    )
    violations = rows.fetchall()
    return {
        "status": "FAIL" if violations else "PASS",
        "affected_records": len(violations),
        "fail_count": len(violations), "pass_count": 0, "warn_count": 0,
        "evidence_json": {
            "trusts_with_bad_allocation": [
                {"trust_id": r.trust_id, "total_pct": str(r.total_pct)} for r in violations
            ]
        } if violations else None,
    }


async def ctl_trust_006(db: AsyncSession) -> dict:
    """
    CTL-TRUST-006 — CUS/UTMA Trust Custodian Check.
    WARN if custodial/UTMA trusts have no trustee assigned.
    Source: TRUST-ACCOUNT-MASTER.cpy TRUSTEE-ID.
    """
    rows = await db.execute(
        text("""
            SELECT COUNT(*) FROM trust_accounts
            WHERE trust_type IN ('CUS','UTMA')
              AND trustee_id IS NULL
              AND trust_status = 'A'
        """)
    )
    missing = rows.scalar() or 0
    return {
        "status": "WARN" if missing > 0 else "PASS",
        "affected_records": missing,
        "fail_count": 0, "pass_count": 0, "warn_count": missing,
    }


# ─────────────────────────────────────────────────────────────────────
# WIRE CONTROLS
# ─────────────────────────────────────────────────────────────────────

async def ctl_wire_001(db: AsyncSession) -> dict:
    """
    CTL-WIRE-001 — OFAC Screening Gap.
    FAIL if any settled wire has ofac_status = 'NOT_SCREENED'.
    Source: ofac_screening.py.
    """
    rows = await db.execute(
        text("""
            SELECT COUNT(*) FROM wire_transactions
            WHERE status = 'SETTLED' AND ofac_status = 'NOT_SCREENED'
        """)
    )
    unscreened = rows.scalar() or 0
    total_r = await db.execute(text("SELECT COUNT(*) FROM wire_transactions WHERE status = 'SETTLED'"))
    total = total_r.scalar() or 0
    return {
        "status": "FAIL" if unscreened > 0 else "PASS",
        "affected_records": total,
        "fail_count": unscreened,
        "pass_count": total - unscreened,
        "warn_count": 0,
        "evidence_json": {"settled_wires_not_screened": unscreened} if unscreened else None,
    }


async def ctl_wire_002(db: AsyncSession) -> dict:
    """
    CTL-WIRE-002 — Large Value Wire Coverage.
    FAIL if any settled wire > SMDIA has no OFAC screening.
    Source: ofac_screening.py + 31 CFR 1010.410.
    """
    rows = await db.execute(
        text("""
            SELECT COUNT(*) FROM wire_transactions
            WHERE amount > :smdia
              AND status = 'SETTLED'
              AND (ofac_status IS NULL OR ofac_status = 'NOT_SCREENED')
        """),
        {"smdia": str(SMDIA)},
    )
    violations = rows.scalar() or 0
    return {
        "status": "FAIL" if violations > 0 else "PASS",
        "affected_records": violations,
        "fail_count": violations, "pass_count": 0, "warn_count": 0,
        "evidence_json": {"large_value_unscreened": violations} if violations else None,
    }


async def ctl_wire_003(db: AsyncSession) -> dict:
    """
    CTL-WIRE-003 — SWIFT Message Integrity.
    WARN if any wire with message_type like MT% lacks a settlement_channel.
    Source: swift_parser.py.
    """
    rows = await db.execute(
        text("""
            SELECT COUNT(*) FROM wire_transactions
            WHERE message_type LIKE 'MT%'
              AND (settlement_channel IS NULL OR settlement_channel = '')
        """)
    )
    missing = rows.scalar() or 0
    return {
        "status": "WARN" if missing > 0 else "PASS",
        "affected_records": missing,
        "fail_count": 0, "pass_count": 0, "warn_count": missing,
    }


async def ctl_wire_004(db: AsyncSession) -> dict:
    """
    CTL-WIRE-004 — ACH Batch Settlement Reconciliation.
    FAIL if any ACH wires are PENDING with a value_date older than 2 business days.
    Source: acb_batch_settlement.sh / reconciliation.py.
    """
    rows = await db.execute(
        text("""
            SELECT COUNT(*) FROM wire_transactions
            WHERE settlement_channel = 'ACH'
              AND status = 'PENDING'
              AND value_date < CURRENT_DATE - INTERVAL '2 days'
        """)
    )
    stale = rows.scalar() or 0
    return {
        "status": "FAIL" if stale > 0 else "PASS",
        "affected_records": stale,
        "fail_count": stale, "pass_count": 0, "warn_count": 0,
        "evidence_json": {"stale_ach_pending": stale} if stale else None,
    }


async def ctl_wire_005(db: AsyncSession) -> dict:
    """
    CTL-WIRE-005 — Wire ORC Assignment.
    WARN if any wire has no orc_type assigned.
    Source: PaymentRouter.java + WireTransactionService.java.
    """
    rows = await db.execute(
        text("SELECT COUNT(*) FROM wire_transactions WHERE orc_type IS NULL OR orc_type = ''")
    )
    missing = rows.scalar() or 0
    return {
        "status": "WARN" if missing > 0 else "PASS",
        "affected_records": missing,
        "fail_count": 0, "pass_count": 0, "warn_count": missing,
    }


async def ctl_wire_006(db: AsyncSession) -> dict:
    """
    CTL-WIRE-006 — Wire Insurance Calculation.
    WARN if any wire > SMDIA is SETTLED without an insurance result.
    Source: sp_calculate_wire_insurance.sql.
    """
    rows = await db.execute(
        text("""
            SELECT COUNT(*) FROM wire_transactions w
            LEFT JOIN insurance_results ir ON ir.wire_id = w.wire_id
            WHERE w.amount > :smdia
              AND w.status = 'SETTLED'
              AND ir.result_id IS NULL
        """),
        {"smdia": str(SMDIA)},
    )
    missing = rows.scalar() or 0
    return {
        "status": "WARN" if missing > 0 else "PASS",
        "affected_records": missing,
        "fail_count": 0, "pass_count": 0, "warn_count": missing,
        "evidence_json": {"large_wires_without_insurance_result": missing} if missing else None,
    }


async def ctl_wire_007(db: AsyncSession) -> dict:
    """
    CTL-WIRE-007 — HELD Wire Disposition.
    WARN if any wires are in HELD status.
    Source: PaymentRouter.java ROUTE-BY-ORC.
    """
    rows = await db.execute(text("SELECT COUNT(*) FROM wire_transactions WHERE status = 'HELD'"))
    held = rows.scalar() or 0
    return {
        "status": "WARN" if held > 0 else "PASS",
        "affected_records": held,
        "fail_count": 0, "pass_count": 0, "warn_count": held,
    }


# ─────────────────────────────────────────────────────────────────────
# Stage dispatch map
# ─────────────────────────────────────────────────────────────────────

# Maps (stage_name, system) → list of (control_id, evaluator_fn)
STAGE_CONTROL_MAP: dict[tuple[str, str], list[tuple[str, Any]]] = {
    ("ingestion",     "legacy_deposit"):  [
        ("CTL-DEP-003", ctl_dep_003),
    ],
    ("orc_assign",    "legacy_deposit"):  [
        ("CTL-DEP-002", ctl_dep_002),
        ("CTL-DEP-005", ctl_dep_005),
        ("CTL-DEP-007", ctl_dep_007),
        ("CTL-DEP-008", ctl_dep_008),
    ],
    ("aggregation",   "legacy_deposit"):  [
        ("CTL-DEP-001", ctl_dep_001),
        ("CTL-DEP-004", ctl_dep_004),
        ("CTL-DEP-006", ctl_dep_006),
    ],
    ("orc_assign",    "trust_custody"):   [
        ("CTL-TRUST-001", ctl_trust_001),
    ],
    ("ingestion",     "trust_custody"):   [
        ("CTL-TRUST-002", ctl_trust_002),
        ("CTL-TRUST-004", ctl_trust_004),
        ("CTL-TRUST-005", ctl_trust_005),
        ("CTL-TRUST-006", ctl_trust_006),
    ],
    ("insurance",     "trust_custody"):   [
        ("CTL-TRUST-003", ctl_trust_003),
    ],
    ("screening",     "wire_transfer"):   [
        ("CTL-WIRE-001", ctl_wire_001),
        ("CTL-WIRE-002", ctl_wire_002),
    ],
    ("ingestion",     "wire_transfer"):   [
        ("CTL-WIRE-003", ctl_wire_003),
        ("CTL-WIRE-005", ctl_wire_005),
    ],
    ("reconciliation","wire_transfer"):   [
        ("CTL-WIRE-004", ctl_wire_004),
        ("CTL-WIRE-006", ctl_wire_006),
        ("CTL-WIRE-007", ctl_wire_007),
    ],
}


# ─────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────

async def evaluate_stage_controls(
    db: AsyncSession,
    run_id: uuid.UUID,
    stage_run_id: uuid.UUID,
    stage_name: str,
    system: str,
) -> dict:
    """
    Evaluate all controls for a given (stage, system) pair.
    Save ControlResult rows; if FAIL/WARN, trigger incident creation.
    Returns summary dict for the pipeline runner.
    """
    from app.services.incident_service import create_incident_from_control  # local import

    evaluators = STAGE_CONTROL_MAP.get((stage_name, system), [])
    if not evaluators:
        return {"records_evaluated": 0, "records_passed": 0, "failures": 0, "controls_run": 0}

    total_failures = 0
    controls_run = 0
    records_evaluated = 0

    for control_id, evaluator_fn in evaluators:
        try:
            result = await evaluator_fn(db)
            implicated = CONTROL_IMPLICATED_ENTITIES.get(control_id)
            cr_id = await _save_control_result(db, control_id, run_id, stage_run_id, result, implicated)
            controls_run += 1
            records_evaluated += result.get("affected_records", 0)

            if result["status"] in ("FAIL", "WARN"):
                total_failures += result.get("fail_count", 0)
                await create_incident_from_control(
                    db,
                    control_id=control_id,
                    control_result_id=cr_id,
                    run_id=run_id,
                    stage=stage_name,
                    result=result,
                )
                log.warning(
                    "Control %s: %s | fail=%d warn=%d",
                    control_id, result["status"],
                    result.get("fail_count", 0),
                    result.get("warn_count", 0),
                )
            else:
                log.info("Control %s: PASS | records=%d", control_id, result.get("affected_records", 0))
        except Exception as exc:
            log.exception("Control %s evaluation error: %s", control_id, exc)
            total_failures += 1

    return {
        "records_evaluated": records_evaluated,
        "records_passed": records_evaluated - total_failures,
        "failures": total_failures,
        "controls_run": controls_run,
    }
