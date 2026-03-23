"""
Incident service — creates incidents from control failures
and collects supporting evidence.
"""
import json
import logging
import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

log = logging.getLogger(__name__)


# Map control_id → full v2 ontology entity chain for incidents
_CONTROL_META: dict[str, dict] = {
    "CTL-DEP-001": {
        "severity": "P1",
        "ontology_entry_node": "CO-INSURANCE-ACCURACY",
        "failed_control_objective_node": "CO-INSURANCE-ACCURACY",
        "failed_rule_node": "RULE-PER-DEPOSITOR-CALC",
        "failed_transformation_node": "TRN-INSURANCE-CALCULATION",
        "implicated_pipeline_node": "PL-DEPOSIT-INSURANCE-CALC",
        "implicated_script_node": "SCR-DEPOSIT-INSURANCE-CALC",
        "implicated_system_node": "SYS-LEGACY-DEPOSIT",
        "implicated_job_node": "JOB-DAILY-DEPOSIT",
        "owner_node": "OWN-DEPOSIT-COMPLIANCE",
    },
    "CTL-DEP-002": {
        "severity": "P1",
        "ontology_entry_node": "CO-ORC-COMPLETENESS",
        "failed_control_objective_node": "CO-ORC-COMPLETENESS",
        "failed_rule_node": "RULE-IRR-ORC-BRANCH",
        "failed_transformation_node": "TRN-ORC-CLASSIFICATION",
        "implicated_pipeline_node": "PL-DEPOSIT-ORC-ASSIGN",
        "implicated_script_node": "SCR-ORC-ASSIGNMENT",
        "implicated_system_node": "SYS-LEGACY-DEPOSIT",
        "implicated_job_node": "JOB-DAILY-DEPOSIT",
        "owner_node": "OWN-DEPOSIT-COMPLIANCE",
    },
    "CTL-DEP-003": {
        "severity": "P3",
        "ontology_entry_node": "CO-ORC-COMPLETENESS",
        "failed_control_objective_node": "CO-ORC-COMPLETENESS",
        "failed_rule_node": "RULE-IRR-ORC-BRANCH",
        "failed_transformation_node": "TRN-ORC-CLASSIFICATION",
        "implicated_pipeline_node": "PL-DEPOSIT-ORC-ASSIGN",
        "implicated_script_node": "SCR-ORC-ASSIGNMENT",
        "implicated_system_node": "SYS-LEGACY-DEPOSIT",
        "implicated_job_node": "JOB-DAILY-DEPOSIT",
        "owner_node": "OWN-DEPOSIT-COMPLIANCE",
    },
    "CTL-DEP-004": {
        "severity": "P1",
        "ontology_entry_node": "CO-INSURANCE-ACCURACY",
        "failed_control_objective_node": "CO-INSURANCE-ACCURACY",
        "failed_rule_node": "RULE-SMDIA-CONFIGURABLE",
        "failed_transformation_node": "TRN-INSURANCE-CALCULATION",
        "implicated_pipeline_node": "PL-DEPOSIT-INSURANCE-CALC",
        "implicated_script_node": "SCR-SP-CALC-INSURANCE",
        "implicated_system_node": "SYS-LEGACY-DEPOSIT",
        "implicated_job_node": "JOB-DAILY-DEPOSIT",
        "owner_node": "OWN-DEPOSIT-COMPLIANCE",
    },
    "CTL-DEP-005": {
        "severity": "P2",
        "ontology_entry_node": "CO-INSURANCE-ACCURACY",
        "failed_control_objective_node": "CO-INSURANCE-ACCURACY",
        "failed_rule_node": "RULE-PER-DEPOSITOR-CALC",
        "failed_transformation_node": "TRN-ORC-CLASSIFICATION",
        "implicated_pipeline_node": "PL-DEPOSIT-ORC-ASSIGN",
        "implicated_script_node": "SCR-ORC-ASSIGNMENT",
        "implicated_system_node": "SYS-LEGACY-DEPOSIT",
        "implicated_job_node": "JOB-DAILY-DEPOSIT",
        "owner_node": "OWN-DEPOSIT-COMPLIANCE",
    },
    "CTL-DEP-006": {
        "severity": "P3",
        "ontology_entry_node": "CO-INSURANCE-ACCURACY",
        "failed_control_objective_node": "CO-INSURANCE-ACCURACY",
        "failed_rule_node": "RULE-PER-DEPOSITOR-CALC",
        "failed_transformation_node": "TRN-INSURANCE-CALCULATION",
        "implicated_pipeline_node": "PL-DEPOSIT-INSURANCE-CALC",
        "implicated_script_node": "SCR-DEPOSIT-INSURANCE-CALC",
        "implicated_system_node": "SYS-LEGACY-DEPOSIT",
        "implicated_job_node": "JOB-DAILY-DEPOSIT",
        "owner_node": "OWN-DEPOSIT-COMPLIANCE",
    },
    "CTL-DEP-007": {
        "severity": "P2",
        "ontology_entry_node": "CO-ORC-COMPLETENESS",
        "failed_control_objective_node": "CO-ORC-COMPLETENESS",
        "failed_rule_node": "RULE-IRR-ORC-BRANCH",
        "failed_transformation_node": "TRN-ORC-CLASSIFICATION",
        "implicated_pipeline_node": "PL-DEPOSIT-ORC-ASSIGN",
        "implicated_script_node": "SCR-ORC-ASSIGNMENT",
        "implicated_system_node": "SYS-LEGACY-DEPOSIT",
        "implicated_job_node": "JOB-DAILY-DEPOSIT",
        "owner_node": "OWN-DEPOSIT-COMPLIANCE",
    },
    "CTL-DEP-008": {
        "severity": "P2",
        "ontology_entry_node": "CO-INSURANCE-ACCURACY",
        "failed_control_objective_node": "CO-INSURANCE-ACCURACY",
        "failed_rule_node": "RULE-PER-DEPOSITOR-CALC",
        "failed_transformation_node": "TRN-INSURANCE-CALCULATION",
        "implicated_pipeline_node": "PL-DEPOSIT-INSURANCE-CALC",
        "implicated_script_node": "SCR-DEPOSIT-INSURANCE-CALC",
        "implicated_system_node": "SYS-LEGACY-DEPOSIT",
        "implicated_job_node": "JOB-DAILY-DEPOSIT",
        "owner_node": "OWN-DEPOSIT-COMPLIANCE",
    },
    "CTL-TRUST-001": {
        "severity": "P1",
        "ontology_entry_node": "CO-ORC-COMPLETENESS",
        "failed_control_objective_node": "CO-ORC-COMPLETENESS",
        "failed_rule_node": "RULE-IRR-ORC-BRANCH",
        "failed_transformation_node": "TRN-ORC-CLASSIFICATION",
        "implicated_pipeline_node": "PL-TRUST-ORC-ASSIGN",
        "implicated_script_node": "SCR-ORC-ASSIGNMENT",
        "implicated_system_node": "SYS-TRUST-CUSTODY",
        "implicated_job_node": "JOB-DAILY-TRUST",
        "owner_node": "OWN-TRUST-OPERATIONS",
    },
    "CTL-TRUST-002": {
        "severity": "P3",
        "ontology_entry_node": "CO-TRUST-BENEFICIARY",
        "failed_control_objective_node": "CO-TRUST-BENEFICIARY",
        "failed_rule_node": "RULE-PER-DEPOSITOR-CALC",
        "failed_transformation_node": "TRN-BENEFICIARY-INTEREST-CALC",
        "implicated_pipeline_node": "PL-TRUST-INSURANCE-CALC",
        "implicated_script_node": "SCR-TRUST-INSURANCE-CALC",
        "implicated_system_node": "SYS-TRUST-CUSTODY",
        "implicated_job_node": "JOB-DAILY-TRUST",
        "owner_node": "OWN-TRUST-OPERATIONS",
    },
    "CTL-TRUST-003": {
        "severity": "P2",
        "ontology_entry_node": "CO-TRUST-BENEFICIARY",
        "failed_control_objective_node": "CO-TRUST-BENEFICIARY",
        "failed_rule_node": "RULE-PER-DEPOSITOR-CALC",
        "failed_transformation_node": "TRN-BENEFICIARY-INTEREST-CALC",
        "implicated_pipeline_node": "PL-TRUST-INSURANCE-CALC",
        "implicated_script_node": "SCR-TRUST-INSURANCE-CALC",
        "implicated_system_node": "SYS-TRUST-CUSTODY",
        "implicated_job_node": "JOB-DAILY-TRUST",
        "owner_node": "OWN-TRUST-OPERATIONS",
    },
    "CTL-TRUST-004": {
        "severity": "P3",
        "ontology_entry_node": "CO-TRUST-BENEFICIARY",
        "failed_control_objective_node": "CO-TRUST-BENEFICIARY",
        "failed_rule_node": "RULE-PER-DEPOSITOR-CALC",
        "failed_transformation_node": "TRN-BENEFICIARY-INTEREST-CALC",
        "implicated_pipeline_node": "PL-TRUST-INSURANCE-CALC",
        "implicated_script_node": "SCR-TRUST-INSURANCE-CALC",
        "implicated_system_node": "SYS-TRUST-CUSTODY",
        "implicated_job_node": "JOB-DAILY-TRUST",
        "owner_node": "OWN-TRUST-OPERATIONS",
    },
    "CTL-TRUST-005": {
        "severity": "P2",
        "ontology_entry_node": "CO-TRUST-BENEFICIARY",
        "failed_control_objective_node": "CO-TRUST-BENEFICIARY",
        "failed_rule_node": "RULE-PER-DEPOSITOR-CALC",
        "failed_transformation_node": "TRN-BENEFICIARY-INTEREST-CALC",
        "implicated_pipeline_node": "PL-TRUST-INSURANCE-CALC",
        "implicated_script_node": "SCR-TRUST-INSURANCE-CALC",
        "implicated_system_node": "SYS-TRUST-CUSTODY",
        "implicated_job_node": "JOB-DAILY-TRUST",
        "owner_node": "OWN-TRUST-OPERATIONS",
    },
    "CTL-TRUST-006": {
        "severity": "P3",
        "ontology_entry_node": "CO-TRUST-BENEFICIARY",
        "failed_control_objective_node": "CO-TRUST-BENEFICIARY",
        "failed_rule_node": "RULE-PER-DEPOSITOR-CALC",
        "failed_transformation_node": "TRN-BENEFICIARY-INTEREST-CALC",
        "implicated_pipeline_node": "PL-TRUST-INSURANCE-CALC",
        "implicated_script_node": "SCR-TRUST-INSURANCE-CALC",
        "implicated_system_node": "SYS-TRUST-CUSTODY",
        "implicated_job_node": "JOB-DAILY-TRUST",
        "owner_node": "OWN-TRUST-OPERATIONS",
    },
    "CTL-WIRE-001": {
        "severity": "P1",
        "ontology_entry_node": "CO-OFAC-SCREENING",
        "failed_control_objective_node": "CO-OFAC-SCREENING",
        "failed_rule_node": "RULE-OFAC-BEFORE-SETTLE",
        "failed_transformation_node": "TRN-OFAC-SCREENING-CHECK",
        "implicated_pipeline_node": "PL-WIRE-SCREENING",
        "implicated_script_node": "SCR-OFAC-SCREENING",
        "implicated_system_node": "SYS-WIRE-TRANSFER",
        "implicated_job_node": "JOB-WIRE-SETTLEMENT",
        "owner_node": "OWN-WIRE-OPERATIONS",
    },
    "CTL-WIRE-002": {
        "severity": "P1",
        "ontology_entry_node": "CO-OFAC-SCREENING",
        "failed_control_objective_node": "CO-OFAC-SCREENING",
        "failed_rule_node": "RULE-OFAC-BEFORE-SETTLE",
        "failed_transformation_node": "TRN-OFAC-SCREENING-CHECK",
        "implicated_pipeline_node": "PL-WIRE-SCREENING",
        "implicated_script_node": "SCR-OFAC-SCREENING",
        "implicated_system_node": "SYS-WIRE-TRANSFER",
        "implicated_job_node": "JOB-WIRE-SETTLEMENT",
        "owner_node": "OWN-WIRE-OPERATIONS",
    },
    "CTL-WIRE-003": {
        "severity": "P2",
        "ontology_entry_node": "CO-OFAC-SCREENING",
        "failed_control_objective_node": "CO-OFAC-SCREENING",
        "failed_rule_node": "RULE-OFAC-BEFORE-SETTLE",
        "failed_transformation_node": "TRN-OFAC-SCREENING-CHECK",
        "implicated_pipeline_node": "PL-WIRE-RECONCILIATION",
        "implicated_script_node": "SCR-OFAC-SCREENING",
        "implicated_system_node": "SYS-WIRE-TRANSFER",
        "implicated_job_node": "JOB-WIRE-SETTLEMENT",
        "owner_node": "OWN-WIRE-OPERATIONS",
    },
    "CTL-WIRE-004": {
        "severity": "P1",
        "ontology_entry_node": "CO-OFAC-SCREENING",
        "failed_control_objective_node": "CO-OFAC-SCREENING",
        "failed_rule_node": "RULE-OFAC-BEFORE-SETTLE",
        "failed_transformation_node": "TRN-OFAC-SCREENING-CHECK",
        "implicated_pipeline_node": "PL-WIRE-RECONCILIATION",
        "implicated_script_node": "SCR-OFAC-SCREENING",
        "implicated_system_node": "SYS-WIRE-TRANSFER",
        "implicated_job_node": "JOB-WIRE-SETTLEMENT",
        "owner_node": "OWN-WIRE-OPERATIONS",
    },
    "CTL-WIRE-005": {
        "severity": "P3",
        "ontology_entry_node": "CO-ORC-COMPLETENESS",
        "failed_control_objective_node": "CO-ORC-COMPLETENESS",
        "failed_rule_node": "RULE-IRR-ORC-BRANCH",
        "failed_transformation_node": "TRN-ORC-CLASSIFICATION",
        "implicated_pipeline_node": "PL-WIRE-SCREENING",
        "implicated_script_node": "SCR-ORC-CLASSIFIER",
        "implicated_system_node": "SYS-WIRE-TRANSFER",
        "implicated_job_node": "JOB-WIRE-SETTLEMENT",
        "owner_node": "OWN-WIRE-OPERATIONS",
    },
    "CTL-WIRE-006": {
        "severity": "P2",
        "ontology_entry_node": "CO-INSURANCE-ACCURACY",
        "failed_control_objective_node": "CO-INSURANCE-ACCURACY",
        "failed_rule_node": "RULE-PER-DEPOSITOR-CALC",
        "failed_transformation_node": "TRN-INSURANCE-CALCULATION",
        "implicated_pipeline_node": "PL-WIRE-RECONCILIATION",
        "implicated_script_node": "SCR-OFAC-SCREENING",
        "implicated_system_node": "SYS-WIRE-TRANSFER",
        "implicated_job_node": "JOB-WIRE-SETTLEMENT",
        "owner_node": "OWN-WIRE-OPERATIONS",
    },
    "CTL-WIRE-007": {
        "severity": "P3",
        "ontology_entry_node": "CO-OFAC-SCREENING",
        "failed_control_objective_node": "CO-OFAC-SCREENING",
        "failed_rule_node": "RULE-OFAC-BEFORE-SETTLE",
        "failed_transformation_node": "TRN-OFAC-SCREENING-CHECK",
        "implicated_pipeline_node": "PL-WIRE-RECONCILIATION",
        "implicated_script_node": "SCR-OFAC-SCREENING",
        "implicated_system_node": "SYS-WIRE-TRANSFER",
        "implicated_job_node": "JOB-WIRE-SETTLEMENT",
        "owner_node": "OWN-WIRE-OPERATIONS",
    },
}

_CONTROL_TITLES: dict[str, str] = {
    "CTL-DEP-001": "Per-Depositor ORC Aggregation Failure",
    "CTL-DEP-002": "IRR Trust ORC Falls Through to SGL (Deposit)",
    "CTL-DEP-003": "Pending ORC Accounts Detected",
    "CTL-DEP-004": "SMDIA Value Drift from Regulatory Constant",
    "CTL-DEP-005": "JNT Account Missing Second Owner",
    "CTL-DEP-006": "Revocable Trust with Deceased Grantor",
    "CTL-DEP-007": "GOV Account Missing Entity Type",
    "CTL-DEP-008": "Collaterally Pledged Balance in Insurance Pool",
    "CTL-TRUST-001": "IRR Trust ORC Misclassification — Missing IRR Branch",
    "CTL-TRUST-002": "Trust Has No Beneficiaries Recorded",
    "CTL-TRUST-003": "EBP Per-Participant Coverage Exceeds SMDIA",
    "CTL-TRUST-004": "Deceased Beneficiary in Active Trust",
    "CTL-TRUST-005": "Beneficiary Allocation Sum ≠ 100%",
    "CTL-TRUST-006": "Custodial/UTMA Trust Missing Trustee",
    "CTL-WIRE-001": "Settled Wire Processed Without OFAC Screening",
    "CTL-WIRE-002": "Large-Value Wire Not OFAC Screened",
    "CTL-WIRE-003": "SWIFT Wire Missing Settlement Channel",
    "CTL-WIRE-004": "Stale ACH Wire Pending Beyond Settlement Window",
    "CTL-WIRE-005": "Wire Transaction Missing ORC Assignment",
    "CTL-WIRE-006": "Large Wire Without Insurance Calculation",
    "CTL-WIRE-007": "Wire Transaction in HELD Status",
}

_SYSTEM_MAP: dict[str, str] = {
    "CTL-DEP": "legacy_deposit",
    "CTL-TRUST": "trust_custody",
    "CTL-WIRE": "wire_transfer",
}


def _system_for(control_id: str) -> str:
    prefix = "-".join(control_id.split("-")[:2])
    return _SYSTEM_MAP.get(prefix, "unknown")


async def create_incident_from_control(
    db: AsyncSession,
    control_id: str,
    control_result_id: uuid.UUID,
    run_id: uuid.UUID,
    stage: str,
    result: dict[str, Any],
) -> uuid.UUID:
    """
    Create an Incident row and attach evidence from the control result.
    Returns the new incident_id.
    """
    meta = _CONTROL_META.get(control_id, {})
    severity = meta.get("severity", "P3")
    ontology_node = meta.get("ontology_entry_node")
    title = _CONTROL_TITLES.get(control_id, f"Control Failure: {control_id}")
    source_system = _system_for(control_id)
    incident_id = uuid.uuid4()

    fail_count = result.get("fail_count", 0)
    warn_count = result.get("warn_count", 0)

    description = (
        f"{title}. "
        f"Fail count: {fail_count}, Warn count: {warn_count}, "
        f"Affected records: {result.get('affected_records', 0)}."
    )

    await db.execute(
        text("""
            INSERT INTO incidents
                (incident_id, control_result_id, control_id, run_id,
                 title, description, severity, status,
                 source_system, stage, ontology_entry_node,
                 entity_type, entity_ids, rca_triggered, rca_completed,
                 failed_control_objective_node, failed_rule_node,
                 failed_transformation_node, implicated_pipeline_node,
                 implicated_script_node, implicated_system_node,
                 implicated_job_node, owner_node)
            VALUES
                (:incident_id, :control_result_id, :control_id, :run_id,
                 :title, :description, :severity, 'OPEN',
                 :source_system, :stage, :ontology_entry_node,
                 'control', CAST(:entity_ids AS jsonb), false, false,
                 :failed_control_objective_node, :failed_rule_node,
                 :failed_transformation_node, :implicated_pipeline_node,
                 :implicated_script_node, :implicated_system_node,
                 :implicated_job_node, :owner_node)
        """),
        {
            "incident_id": str(incident_id),
            "control_result_id": str(control_result_id),
            "control_id": control_id,
            "run_id": str(run_id),
            "title": title,
            "description": description,
            "severity": severity,
            "source_system": source_system,
            "stage": stage,
            "ontology_entry_node": ontology_node,
            "entity_ids": json.dumps([str(control_result_id)]),
            "failed_control_objective_node": meta.get("failed_control_objective_node"),
            "failed_rule_node": meta.get("failed_rule_node"),
            "failed_transformation_node": meta.get("failed_transformation_node"),
            "implicated_pipeline_node": meta.get("implicated_pipeline_node"),
            "implicated_script_node": meta.get("implicated_script_node"),
            "implicated_system_node": meta.get("implicated_system_node"),
            "implicated_job_node": meta.get("implicated_job_node"),
            "owner_node": meta.get("owner_node"),
        },
    )

    # Attach evidence from the control result
    evidence_content = result.get("evidence_json") or {}
    await db.execute(
        text("""
            INSERT INTO incident_evidence
                (evidence_id, incident_id, evidence_type,
                 source_system, artifact_ref, content_json)
            VALUES
                (gen_random_uuid(), :incident_id, 'control_result',
                 :source_system, :artifact_ref, CAST(:content_json AS jsonb))
        """),
        {
            "incident_id": str(incident_id),
            "source_system": source_system,
            "artifact_ref": f"control_results/{control_result_id}",
            "content_json": json.dumps(evidence_content),
        },
    )

    log.info("Incident created | id=%s control=%s severity=%s", incident_id, control_id, severity)
    return incident_id
