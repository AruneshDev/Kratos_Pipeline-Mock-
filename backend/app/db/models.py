import uuid
from datetime import datetime, date
from decimal import Decimal
from typing import Optional, List
from sqlalchemy import (
    String, Boolean, Integer, Numeric, Date, DateTime,
    Text, ForeignKey, ARRAY, func, JSON
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.db.session import Base


# ─────────────────────────────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────────────────────────────

class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_name: Mapped[Optional[str]] = mapped_column(String(100))
    scenario_id: Mapped[Optional[str]] = mapped_column(String(50))
    triggered_by: Mapped[Optional[str]] = mapped_column(String(50), default="manual")
    status: Mapped[str] = mapped_column(String(20), default="RUNNING")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    total_records: Mapped[int] = mapped_column(Integer, default=0)
    error_count: Mapped[int] = mapped_column(Integer, default=0)
    # v2 — maps this run to ontology Job + System nodes
    job_node_id: Mapped[Optional[str]] = mapped_column(String(80))    # JOB-* (e.g. JOB-NIGHTLY-PIPELINE-RUN)
    system_node_id: Mapped[Optional[str]] = mapped_column(String(80)) # SYS-* (e.g. SYS-PIPELINE-API)
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSONB)

    stage_runs: Mapped[List["StageRun"]] = relationship("StageRun", back_populates="pipeline_run", cascade="all, delete-orphan")
    control_results: Mapped[List["ControlResult"]] = relationship("ControlResult", back_populates="pipeline_run")
    incidents: Mapped[List["Incident"]] = relationship("Incident", back_populates="pipeline_run")


class StageRun(Base):
    __tablename__ = "stage_runs"

    stage_run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("pipeline_runs.run_id"))
    stage_name: Mapped[str] = mapped_column(String(50))
    system: Mapped[Optional[str]] = mapped_column(String(50))
    status: Mapped[str] = mapped_column(String(20), default="PENDING")
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    records_in: Mapped[int] = mapped_column(Integer, default=0)
    records_out: Mapped[int] = mapped_column(Integer, default=0)
    records_pending: Mapped[int] = mapped_column(Integer, default=0)
    records_errored: Mapped[int] = mapped_column(Integer, default=0)
    # v2 — maps stage to ontology Pipeline node
    pipeline_node_id: Mapped[Optional[str]] = mapped_column(String(80))  # PL-*
    stage_metadata: Mapped[Optional[dict]] = mapped_column(JSONB)

    pipeline_run: Mapped["PipelineRun"] = relationship("PipelineRun", back_populates="stage_runs")
    control_results: Mapped[List["ControlResult"]] = relationship("ControlResult", back_populates="stage_run")


# ─────────────────────────────────────────────────────────────────────
# CANONICAL BANKING DATA
# ─────────────────────────────────────────────────────────────────────

class Party(Base):
    __tablename__ = "parties"

    party_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[Optional[str]] = mapped_column(String(200))
    party_type: Mapped[Optional[str]] = mapped_column(String(30))   # Individual|Business|Government|Trust
    party_status: Mapped[Optional[str]] = mapped_column(String(20))
    natural_person: Mapped[Optional[bool]] = mapped_column(Boolean)
    govt_id: Mapped[Optional[str]] = mapped_column(String(20))       # SSN / EIN / TIN
    death_date: Mapped[Optional[date]] = mapped_column(Date)
    source_system: Mapped[Optional[str]] = mapped_column(String(50))
    source_record_id: Mapped[Optional[str]] = mapped_column(String(50))
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())

    accounts: Mapped[List["Account"]] = relationship("Account", foreign_keys="Account.party_id", back_populates="party")


class Account(Base):
    __tablename__ = "accounts"

    account_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    account_number: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    account_type: Mapped[Optional[str]] = mapped_column(String(30))
    product_code: Mapped[Optional[str]] = mapped_column(String(10))
    party_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("parties.party_id"))
    orc_code: Mapped[Optional[str]] = mapped_column(String(5))
    orc_pending_flag: Mapped[str] = mapped_column(String(1), default="N")
    orc_pending_code: Mapped[Optional[str]] = mapped_column(String(3))
    balance: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    accrued_interest: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)
    hold_amount: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)
    pending_amount: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)
    currency: Mapped[str] = mapped_column(String(3), default="USD")
    joint_owner_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("parties.party_id"))
    beneficiary_count: Mapped[int] = mapped_column(Integer, default=0)
    participant_count: Mapped[int] = mapped_column(Integer, default=0)
    trust_name: Mapped[Optional[str]] = mapped_column(String(200))
    trust_date: Mapped[Optional[date]] = mapped_column(Date)
    collateral_pledged: Mapped[bool] = mapped_column(Boolean, default=False)
    govt_entity_type: Mapped[Optional[str]] = mapped_column(String(10))  # FEDERAL|STATE|MUNICIPAL
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    open_date: Mapped[Optional[date]] = mapped_column(Date)
    maturity_date: Mapped[Optional[date]] = mapped_column(Date)
    interest_rate: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 4))
    last_activity_date: Mapped[Optional[date]] = mapped_column(Date)
    source_system: Mapped[Optional[str]] = mapped_column(String(50))
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())

    party: Mapped[Optional["Party"]] = relationship("Party", foreign_keys=[party_id], back_populates="accounts")
    joint_owner: Mapped[Optional["Party"]] = relationship("Party", foreign_keys=[joint_owner_id])


class TrustAccount(Base):
    __tablename__ = "trust_accounts"

    trust_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    trust_name: Mapped[Optional[str]] = mapped_column(String(200))
    trust_type: Mapped[Optional[str]] = mapped_column(String(10))    # REV|IRR|EBP|CUS|UTMA
    grantor_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("parties.party_id"))
    trustee_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("parties.party_id"))
    balance: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    accrued_interest: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=0)
    beneficiary_count: Mapped[int] = mapped_column(Integer, default=0)
    participant_count: Mapped[int] = mapped_column(Integer, default=0)
    orc_assigned: Mapped[Optional[str]] = mapped_column(String(5))
    trust_status: Mapped[Optional[str]] = mapped_column(String(1))   # A|I|C
    open_date: Mapped[Optional[date]] = mapped_column(Date)
    source_system: Mapped[Optional[str]] = mapped_column(String(50))
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())

    beneficiaries: Mapped[List["Beneficiary"]] = relationship("Beneficiary", back_populates="trust_account", cascade="all, delete-orphan")


class Beneficiary(Base):
    __tablename__ = "beneficiaries"

    beneficiary_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    trust_id: Mapped[str] = mapped_column(String(20), ForeignKey("trust_accounts.trust_id"))
    party_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("parties.party_id"))
    beneficiary_type: Mapped[Optional[str]] = mapped_column(String(15))  # PRIMARY|CONTINGENT|REMAINDER
    allocation_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    is_deceased: Mapped[bool] = mapped_column(Boolean, default=False)
    vested_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))

    trust_account: Mapped["TrustAccount"] = relationship("TrustAccount", back_populates="beneficiaries")


class WireTransaction(Base):
    __tablename__ = "wire_transactions"

    wire_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    reference: Mapped[str] = mapped_column(String(30), nullable=False, index=True, unique=True)
    message_type: Mapped[Optional[str]] = mapped_column(String(10))   # MT103|MT202|ACH
    direction: Mapped[Optional[str]] = mapped_column(String(10))
    ordering_customer_id: Mapped[Optional[str]] = mapped_column(String(20))
    ordering_name: Mapped[Optional[str]] = mapped_column(String(200))
    beneficiary_name: Mapped[Optional[str]] = mapped_column(String(200))
    beneficiary_account: Mapped[Optional[str]] = mapped_column(String(30))
    amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3), default="USD")
    status: Mapped[Optional[str]] = mapped_column(String(20))         # SETTLED|PENDING|RETURNED|HELD
    orc_type: Mapped[Optional[str]] = mapped_column(String(5))
    settlement_channel: Mapped[Optional[str]] = mapped_column(String(20))
    ofac_status: Mapped[Optional[str]] = mapped_column(String(20))    # CLEARED|NOT_SCREENED|HELD|PENDING
    value_date: Mapped[Optional[date]] = mapped_column(Date)
    source_system: Mapped[Optional[str]] = mapped_column(String(50))
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())


# ─────────────────────────────────────────────────────────────────────
# INSURANCE RESULTS
# ─────────────────────────────────────────────────────────────────────

class InsuranceResult(Base):
    __tablename__ = "insurance_results"

    result_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("pipeline_runs.run_id"))
    account_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("accounts.account_id"))
    trust_id: Mapped[Optional[str]] = mapped_column(String(20), ForeignKey("trust_accounts.trust_id"))
    wire_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("wire_transactions.wire_id"))
    depositor_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("parties.party_id"))
    orc_type: Mapped[Optional[str]] = mapped_column(String(5))
    balance: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    accrued_interest: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    insured_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    uninsured_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    calc_method: Mapped[Optional[str]] = mapped_column(String(30))
    error_flag: Mapped[str] = mapped_column(String(1), default="N")
    error_code: Mapped[Optional[str]] = mapped_column(String(10))
    calc_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())


# ─────────────────────────────────────────────────────────────────────
# CONTROLS
# ─────────────────────────────────────────────────────────────────────

class Control(Base):
    __tablename__ = "controls"

    control_id: Mapped[str] = mapped_column(String(30), primary_key=True)
    control_name: Mapped[str] = mapped_column(String(200))
    description: Mapped[Optional[str]] = mapped_column(Text)
    source_ref: Mapped[Optional[str]] = mapped_column(String(300))
    cfr_citation: Mapped[Optional[str]] = mapped_column(String(100))
    stage: Mapped[Optional[str]] = mapped_column(String(30))
    system: Mapped[Optional[str]] = mapped_column(String(30))
    rule_type: Mapped[Optional[str]] = mapped_column(String(30))
    applicable_fields: Mapped[Optional[list]] = mapped_column(JSONB)
    severity: Mapped[Optional[str]] = mapped_column(String(10))       # CRITICAL|HIGH|MEDIUM|LOW
    control_type: Mapped[Optional[str]] = mapped_column(String(15))   # PREVENTIVE|DETECTIVE
    # v2 ontology references
    regulation_node_id: Mapped[Optional[str]] = mapped_column(String(80))    # REG-* node
    control_objective_node_id: Mapped[Optional[str]] = mapped_column(String(80))  # CO-* node
    rule_node_id: Mapped[Optional[str]] = mapped_column(String(80))           # RULE-* node
    transformation_node_id: Mapped[Optional[str]] = mapped_column(String(80)) # TRN-* node
    pipeline_node_id: Mapped[Optional[str]] = mapped_column(String(80))       # PL-* node
    script_node_id: Mapped[Optional[str]] = mapped_column(String(80))         # SCR-* node
    owner_node_id: Mapped[Optional[str]] = mapped_column(String(80))          # OWN-* node
    # legacy field retained
    ontology_node_id: Mapped[Optional[str]] = mapped_column(String(80))
    remediation: Mapped[Optional[str]] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())

    results: Mapped[List["ControlResult"]] = relationship("ControlResult", back_populates="control")


class ControlResult(Base):
    __tablename__ = "control_results"

    result_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    control_id: Mapped[str] = mapped_column(String(30), ForeignKey("controls.control_id"))
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("pipeline_runs.run_id"))
    stage_run_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("stage_runs.stage_run_id"))
    evaluated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    status: Mapped[str] = mapped_column(String(10), default="PENDING")  # PASS|FAIL|WARN|SKIP
    affected_records: Mapped[int] = mapped_column(Integer, default=0)
    fail_count: Mapped[int] = mapped_column(Integer, default=0)
    pass_count: Mapped[int] = mapped_column(Integer, default=0)
    warn_count: Mapped[int] = mapped_column(Integer, default=0)
    evidence_json: Mapped[Optional[dict]] = mapped_column(JSONB)
    # v2 — ontology entity references from the failed control evaluation
    implicated_pipeline_node: Mapped[Optional[str]] = mapped_column(String(80))    # PL-*
    implicated_script_node: Mapped[Optional[str]] = mapped_column(String(80))      # SCR-*
    implicated_transformation_node: Mapped[Optional[str]] = mapped_column(String(80))  # TRN-*
    implicated_table_nodes: Mapped[Optional[list]] = mapped_column(JSONB)          # [TBL-*, ...]
    implicated_column_nodes: Mapped[Optional[list]] = mapped_column(JSONB)         # [COL-*, ...]
    code_event_node: Mapped[Optional[str]] = mapped_column(String(80))             # EVT-*
    log_source_node: Mapped[Optional[str]] = mapped_column(String(80))             # LOG-*

    control: Mapped["Control"] = relationship("Control", back_populates="results")
    pipeline_run: Mapped["PipelineRun"] = relationship("PipelineRun", back_populates="control_results")
    stage_run: Mapped[Optional["StageRun"]] = relationship("StageRun", back_populates="control_results")
    incidents: Mapped[List["Incident"]] = relationship("Incident", back_populates="control_result")


# ─────────────────────────────────────────────────────────────────────
# INCIDENTS
# ─────────────────────────────────────────────────────────────────────

class Incident(Base):
    __tablename__ = "incidents"

    incident_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    control_result_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("control_results.result_id"))
    control_id: Mapped[Optional[str]] = mapped_column(String(30), ForeignKey("controls.control_id"))
    run_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("pipeline_runs.run_id"))
    title: Mapped[Optional[str]] = mapped_column(String(300))
    description: Mapped[Optional[str]] = mapped_column(Text)
    severity: Mapped[Optional[str]] = mapped_column(String(5))        # P1|P2|P3|P4
    status: Mapped[str] = mapped_column(String(20), default="OPEN")
    source_system: Mapped[Optional[str]] = mapped_column(String(50))
    stage: Mapped[Optional[str]] = mapped_column(String(30))
    entity_type: Mapped[Optional[str]] = mapped_column(String(30))
    entity_ids: Mapped[Optional[list]] = mapped_column(JSONB)
    # v2 — ontology entity references (replaces flat ontology_entry_node)
    ontology_entry_node: Mapped[Optional[str]] = mapped_column(String(80))       # kept for compat
    failed_control_objective_node: Mapped[Optional[str]] = mapped_column(String(80))  # CO-*
    failed_rule_node: Mapped[Optional[str]] = mapped_column(String(80))               # RULE-*
    failed_transformation_node: Mapped[Optional[str]] = mapped_column(String(80))     # TRN-*
    implicated_pipeline_node: Mapped[Optional[str]] = mapped_column(String(80))       # PL-*
    implicated_script_node: Mapped[Optional[str]] = mapped_column(String(80))         # SCR-*
    implicated_system_node: Mapped[Optional[str]] = mapped_column(String(80))         # SYS-*
    implicated_job_node: Mapped[Optional[str]] = mapped_column(String(80))            # JOB-*
    owner_node: Mapped[Optional[str]] = mapped_column(String(80))                     # OWN-*
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    rca_triggered: Mapped[bool] = mapped_column(Boolean, default=False)
    rca_completed: Mapped[bool] = mapped_column(Boolean, default=False)
    confidence_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 2))
    root_cause_node: Mapped[Optional[str]] = mapped_column(String(80))

    control_result: Mapped[Optional["ControlResult"]] = relationship("ControlResult", back_populates="incidents")
    pipeline_run: Mapped[Optional["PipelineRun"]] = relationship("PipelineRun", back_populates="incidents")
    evidence: Mapped[List["IncidentEvidence"]] = relationship("IncidentEvidence", back_populates="incident", cascade="all, delete-orphan")
    rca_result: Mapped[Optional["RCAResult"]] = relationship("RCAResult", back_populates="incident", uselist=False)


class IncidentEvidence(Base):
    __tablename__ = "incident_evidence"

    evidence_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    incident_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("incidents.incident_id"))
    evidence_type: Mapped[Optional[str]] = mapped_column(String(30))  # record_sample|config_snapshot|log_excerpt|code_reference|control_history
    source_system: Mapped[Optional[str]] = mapped_column(String(50))
    artifact_ref: Mapped[Optional[str]] = mapped_column(String(300))
    content_json: Mapped[Optional[dict]] = mapped_column(JSONB)
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())

    incident: Mapped["Incident"] = relationship("Incident", back_populates="evidence")


# ─────────────────────────────────────────────────────────────────────
# RCA RESULTS
# ─────────────────────────────────────────────────────────────────────

class RCAResult(Base):
    __tablename__ = "rca_results"

    rca_result_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    incident_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("incidents.incident_id"), unique=True)
    root_cause: Mapped[Optional[str]] = mapped_column(Text)
    # v2 — root cause resolves to a real ontology entity (Script|Transformation|Rule|Table|Column|...)
    root_cause_entity_type: Mapped[Optional[str]] = mapped_column(String(30))  # Script|Transformation|Rule|...
    root_cause_entity_id: Mapped[Optional[str]] = mapped_column(String(80))    # e.g. SCR-ORC-ASSIGNMENT
    root_cause_node: Mapped[Optional[str]] = mapped_column(String(80))         # legacy compat
    confidence_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 2))
    recommendation: Mapped[Optional[str]] = mapped_column(Text)
    evidence_summary: Mapped[Optional[str]] = mapped_column(Text)
    # v2 — supporting evidence as structured ontology entity list
    supporting_entities: Mapped[Optional[list]] = mapped_column(JSONB)  # [{entity_type, entity_id, rationale}]
    traversal_path: Mapped[Optional[list]] = mapped_column(JSONB)
    reasoning_summary: Mapped[Optional[str]] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(20), default="SUBMITTED")
    submitted_by: Mapped[Optional[str]] = mapped_column(String(100))
    submitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())

    incident: Mapped["Incident"] = relationship("Incident", back_populates="rca_result")


# ─────────────────────────────────────────────────────────────────────
# ONTOLOGY  (v2 — canonical 16-type operational ontology)
# Node types: System|Job|Pipeline|Script|Transformation|CodeEvent|
#             DataSource|Dataset|Table|Column|LogSource|
#             Regulation|ControlObjective|Rule|Owner
# ─────────────────────────────────────────────────────────────────────

class OntologyNode(Base):
    __tablename__ = "ontology_nodes"

    node_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    # v2 canonical types: System|Job|Pipeline|Script|Transformation|CodeEvent|
    #                     DataSource|Dataset|Table|Column|LogSource|
    #                     Regulation|ControlObjective|Rule|Owner
    node_type: Mapped[Optional[str]] = mapped_column(String(30))
    label: Mapped[Optional[str]] = mapped_column(String(200))
    description: Mapped[Optional[str]] = mapped_column(Text)
    system: Mapped[Optional[str]] = mapped_column(String(50))
    properties: Mapped[Optional[dict]] = mapped_column(JSONB)
    # legacy fields retained for backward compat
    evidence_fields: Mapped[Optional[list]] = mapped_column(JSONB)
    stop_condition: Mapped[Optional[str]] = mapped_column(Text)
    confidence_boost: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 2), default=Decimal("0.10"))


class OntologyEdge(Base):
    __tablename__ = "ontology_edges"

    edge_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    from_node_id: Mapped[str] = mapped_column(String(80), ForeignKey("ontology_nodes.node_id"))
    to_node_id: Mapped[str] = mapped_column(String(80), ForeignKey("ontology_nodes.node_id"))
    # v2 canonical relationships:
    # RUNS_JOB|DEPENDS_ON|EXECUTES|USES_SCRIPT|HAS_TRANSFORMATION|TYPICALLY_IMPLEMENTS|
    # READS|WRITES|CONTAINS|HAS_COLUMN|DERIVED_FROM|SOURCED_FROM|CHANGED_BY|LOGGED_IN|
    # MANDATES|IMPLEMENTED_BY|ENFORCED_BY|OWNS_PIPELINE|OWNS_JOB|OWNS_SYSTEM|OWNS_CONTROL
    relationship: Mapped[Optional[str]] = mapped_column(String(30))
    weight: Mapped[Decimal] = mapped_column(Numeric(3, 2), default=Decimal("0.50"))
    description: Mapped[Optional[str]] = mapped_column(Text)


# ─────────────────────────────────────────────────────────────────────
# AUDIT LOG
# ─────────────────────────────────────────────────────────────────────

class AuditLog(Base):
    __tablename__ = "audit_log"

    log_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("pipeline_runs.run_id"))
    stage_run_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("stage_runs.stage_run_id"))
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    event_type: Mapped[Optional[str]] = mapped_column(String(50))
    source_system: Mapped[Optional[str]] = mapped_column(String(50))
    message: Mapped[Optional[str]] = mapped_column(Text)
    severity: Mapped[Optional[str]] = mapped_column(String(10))
    record_ref: Mapped[Optional[str]] = mapped_column(String(100))
    details: Mapped[Optional[dict]] = mapped_column(JSONB)
