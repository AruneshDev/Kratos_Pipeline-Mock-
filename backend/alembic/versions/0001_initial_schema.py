"""initial schema

Revision ID: 0001
Revises: 
Create Date: 2024-01-01 00:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── parties ──────────────────────────────────────────────────────
    op.create_table(
        "parties",
        sa.Column("party_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(200)),
        sa.Column("party_type", sa.String(30)),
        sa.Column("party_status", sa.String(20)),
        sa.Column("natural_person", sa.Boolean()),
        sa.Column("govt_id", sa.String(20)),
        sa.Column("death_date", sa.Date()),
        sa.Column("source_system", sa.String(50)),
        sa.Column("source_record_id", sa.String(50)),
        sa.Column("ingested_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )

    # ── accounts ─────────────────────────────────────────────────────
    op.create_table(
        "accounts",
        sa.Column("account_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("account_number", sa.String(30), nullable=False),
        sa.Column("account_type", sa.String(30)),
        sa.Column("product_code", sa.String(10)),
        sa.Column("party_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("parties.party_id")),
        sa.Column("orc_code", sa.String(5)),
        sa.Column("orc_pending_flag", sa.String(1), server_default="N"),
        sa.Column("orc_pending_code", sa.String(3)),
        sa.Column("balance", sa.Numeric(15, 2)),
        sa.Column("accrued_interest", sa.Numeric(15, 2), server_default="0"),
        sa.Column("hold_amount", sa.Numeric(15, 2), server_default="0"),
        sa.Column("pending_amount", sa.Numeric(15, 2), server_default="0"),
        sa.Column("currency", sa.String(3), server_default="USD"),
        sa.Column("joint_owner_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("parties.party_id")),
        sa.Column("beneficiary_count", sa.Integer(), server_default="0"),
        sa.Column("participant_count", sa.Integer(), server_default="0"),
        sa.Column("trust_name", sa.String(200)),
        sa.Column("trust_date", sa.Date()),
        sa.Column("collateral_pledged", sa.Boolean(), server_default="false"),
        sa.Column("govt_entity_type", sa.String(10)),
        sa.Column("is_active", sa.Boolean(), server_default="true"),
        sa.Column("open_date", sa.Date()),
        sa.Column("maturity_date", sa.Date()),
        sa.Column("interest_rate", sa.Numeric(6, 4)),
        sa.Column("last_activity_date", sa.Date()),
        sa.Column("source_system", sa.String(50)),
        sa.Column("ingested_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_accounts_account_number", "accounts", ["account_number"])

    # ── trust_accounts ───────────────────────────────────────────────
    op.create_table(
        "trust_accounts",
        sa.Column("trust_id", sa.String(20), primary_key=True),
        sa.Column("trust_name", sa.String(200)),
        sa.Column("trust_type", sa.String(10)),
        sa.Column("grantor_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("parties.party_id")),
        sa.Column("trustee_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("parties.party_id")),
        sa.Column("balance", sa.Numeric(15, 2)),
        sa.Column("accrued_interest", sa.Numeric(15, 2), server_default="0"),
        sa.Column("beneficiary_count", sa.Integer(), server_default="0"),
        sa.Column("participant_count", sa.Integer(), server_default="0"),
        sa.Column("orc_assigned", sa.String(5)),
        sa.Column("trust_status", sa.String(1)),
        sa.Column("open_date", sa.Date()),
        sa.Column("source_system", sa.String(50)),
        sa.Column("ingested_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )

    # ── beneficiaries ────────────────────────────────────────────────
    op.create_table(
        "beneficiaries",
        sa.Column("beneficiary_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("trust_id", sa.String(20), sa.ForeignKey("trust_accounts.trust_id")),
        sa.Column("party_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("parties.party_id")),
        sa.Column("beneficiary_type", sa.String(15)),
        sa.Column("allocation_pct", sa.Numeric(5, 2)),
        sa.Column("is_deceased", sa.Boolean(), server_default="false"),
        sa.Column("vested_amount", sa.Numeric(15, 2)),
    )

    # ── wire_transactions ────────────────────────────────────────────
    op.create_table(
        "wire_transactions",
        sa.Column("wire_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("reference", sa.String(30), nullable=False),
        sa.Column("message_type", sa.String(10)),
        sa.Column("direction", sa.String(10)),
        sa.Column("ordering_customer_id", sa.String(20)),
        sa.Column("ordering_name", sa.String(200)),
        sa.Column("beneficiary_name", sa.String(200)),
        sa.Column("beneficiary_account", sa.String(30)),
        sa.Column("amount", sa.Numeric(18, 2)),
        sa.Column("currency", sa.String(3), server_default="USD"),
        sa.Column("status", sa.String(20)),
        sa.Column("orc_type", sa.String(5)),
        sa.Column("settlement_channel", sa.String(20)),
        sa.Column("ofac_status", sa.String(20)),
        sa.Column("value_date", sa.Date()),
        sa.Column("source_system", sa.String(50)),
        sa.Column("ingested_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_wire_transactions_reference", "wire_transactions", ["reference"], unique=True)

    # ── pipeline_runs ────────────────────────────────────────────────
    op.create_table(
        "pipeline_runs",
        sa.Column("run_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("run_name", sa.String(100)),
        sa.Column("scenario_id", sa.String(50)),
        sa.Column("triggered_by", sa.String(50), server_default="manual"),
        sa.Column("status", sa.String(20), server_default="RUNNING"),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("total_records", sa.Integer(), server_default="0"),
        sa.Column("error_count", sa.Integer(), server_default="0"),
        sa.Column("metadata", postgresql.JSONB()),
    )

    # ── stage_runs ───────────────────────────────────────────────────
    op.create_table(
        "stage_runs",
        sa.Column("stage_run_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("pipeline_runs.run_id")),
        sa.Column("stage_name", sa.String(50)),
        sa.Column("system", sa.String(50)),
        sa.Column("status", sa.String(20), server_default="PENDING"),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("records_in", sa.Integer(), server_default="0"),
        sa.Column("records_out", sa.Integer(), server_default="0"),
        sa.Column("records_pending", sa.Integer(), server_default="0"),
        sa.Column("records_errored", sa.Integer(), server_default="0"),
        sa.Column("stage_metadata", postgresql.JSONB()),
    )

    # ── insurance_results ────────────────────────────────────────────
    op.create_table(
        "insurance_results",
        sa.Column("result_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("pipeline_runs.run_id")),
        sa.Column("account_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("accounts.account_id")),
        sa.Column("trust_id", sa.String(20), sa.ForeignKey("trust_accounts.trust_id")),
        sa.Column("wire_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("wire_transactions.wire_id")),
        sa.Column("depositor_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("parties.party_id")),
        sa.Column("orc_type", sa.String(5)),
        sa.Column("balance", sa.Numeric(15, 2)),
        sa.Column("accrued_interest", sa.Numeric(15, 2)),
        sa.Column("insured_amount", sa.Numeric(15, 2)),
        sa.Column("uninsured_amount", sa.Numeric(15, 2)),
        sa.Column("calc_method", sa.String(30)),
        sa.Column("error_flag", sa.String(1), server_default="N"),
        sa.Column("error_code", sa.String(10)),
        sa.Column("calc_timestamp", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )

    # ── controls ─────────────────────────────────────────────────────
    op.create_table(
        "controls",
        sa.Column("control_id", sa.String(30), primary_key=True),
        sa.Column("control_name", sa.String(200), nullable=False),
        sa.Column("description", sa.Text()),
        sa.Column("source_ref", sa.String(300)),
        sa.Column("cfr_citation", sa.String(100)),
        sa.Column("stage", sa.String(30)),
        sa.Column("system", sa.String(30)),
        sa.Column("rule_type", sa.String(30)),
        sa.Column("applicable_fields", postgresql.JSONB()),
        sa.Column("severity", sa.String(10)),
        sa.Column("control_type", sa.String(15)),
        sa.Column("ontology_node_id", sa.String(60)),
        sa.Column("remediation", sa.Text()),
        sa.Column("is_active", sa.Boolean(), server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )

    # ── control_results ───────────────────────────────────────────────
    op.create_table(
        "control_results",
        sa.Column("result_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("control_id", sa.String(30), sa.ForeignKey("controls.control_id")),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("pipeline_runs.run_id")),
        sa.Column("stage_run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("stage_runs.stage_run_id")),
        sa.Column("evaluated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("status", sa.String(10), server_default="PENDING"),
        sa.Column("affected_records", sa.Integer(), server_default="0"),
        sa.Column("fail_count", sa.Integer(), server_default="0"),
        sa.Column("pass_count", sa.Integer(), server_default="0"),
        sa.Column("warn_count", sa.Integer(), server_default="0"),
        sa.Column("evidence_json", postgresql.JSONB()),
    )

    # ── incidents ────────────────────────────────────────────────────
    op.create_table(
        "incidents",
        sa.Column("incident_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("control_result_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("control_results.result_id")),
        sa.Column("control_id", sa.String(30), sa.ForeignKey("controls.control_id")),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("pipeline_runs.run_id")),
        sa.Column("title", sa.String(300)),
        sa.Column("description", sa.Text()),
        sa.Column("severity", sa.String(5)),
        sa.Column("status", sa.String(20), server_default="OPEN"),
        sa.Column("source_system", sa.String(50)),
        sa.Column("stage", sa.String(30)),
        sa.Column("entity_type", sa.String(30)),
        sa.Column("entity_ids", postgresql.JSONB()),
        sa.Column("ontology_entry_node", sa.String(60)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("resolved_at", sa.DateTime(timezone=True)),
        sa.Column("rca_triggered", sa.Boolean(), server_default="false"),
        sa.Column("rca_completed", sa.Boolean(), server_default="false"),
        sa.Column("confidence_score", sa.Numeric(3, 2)),
        sa.Column("root_cause_node", sa.String(60)),
    )

    # ── incident_evidence ────────────────────────────────────────────
    op.create_table(
        "incident_evidence",
        sa.Column("evidence_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("incident_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("incidents.incident_id")),
        sa.Column("evidence_type", sa.String(30)),
        sa.Column("source_system", sa.String(50)),
        sa.Column("artifact_ref", sa.String(300)),
        sa.Column("content_json", postgresql.JSONB()),
        sa.Column("collected_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )

    # ── rca_results ──────────────────────────────────────────────────
    op.create_table(
        "rca_results",
        sa.Column("rca_result_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("incident_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("incidents.incident_id"), unique=True),
        sa.Column("root_cause", sa.Text()),
        sa.Column("root_cause_node", sa.String(60)),
        sa.Column("confidence_score", sa.Numeric(3, 2)),
        sa.Column("recommendation", sa.Text()),
        sa.Column("evidence_summary", sa.Text()),
        sa.Column("traversal_path", postgresql.JSONB()),
        sa.Column("reasoning_summary", sa.Text()),
        sa.Column("status", sa.String(20), server_default="SUBMITTED"),
        sa.Column("submitted_by", sa.String(100)),
        sa.Column("submitted_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )

    # ── ontology_nodes ───────────────────────────────────────────────
    op.create_table(
        "ontology_nodes",
        sa.Column("node_id", sa.String(60), primary_key=True),
        sa.Column("node_type", sa.String(20)),
        sa.Column("label", sa.String(200)),
        sa.Column("description", sa.Text()),
        sa.Column("system", sa.String(30)),
        sa.Column("evidence_fields", postgresql.JSONB()),
        sa.Column("stop_condition", sa.Text()),
        sa.Column("confidence_boost", sa.Numeric(3, 2), server_default="0.10"),
    )

    # ── ontology_edges ───────────────────────────────────────────────
    op.create_table(
        "ontology_edges",
        sa.Column("edge_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("from_node_id", sa.String(60), sa.ForeignKey("ontology_nodes.node_id")),
        sa.Column("to_node_id", sa.String(60), sa.ForeignKey("ontology_nodes.node_id")),
        sa.Column("relationship", sa.String(20)),
        sa.Column("weight", sa.Numeric(3, 2), server_default="0.50"),
        sa.Column("description", sa.Text()),
    )

    # ── audit_log ────────────────────────────────────────────────────
    op.create_table(
        "audit_log",
        sa.Column("log_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("pipeline_runs.run_id")),
        sa.Column("stage_run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("stage_runs.stage_run_id")),
        sa.Column("event_time", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("event_type", sa.String(50)),
        sa.Column("source_system", sa.String(50)),
        sa.Column("message", sa.Text()),
        sa.Column("severity", sa.String(10)),
        sa.Column("record_ref", sa.String(100)),
        sa.Column("details", postgresql.JSONB()),
    )


def downgrade() -> None:
    op.drop_table("audit_log")
    op.drop_table("ontology_edges")
    op.drop_table("ontology_nodes")
    op.drop_table("rca_results")
    op.drop_table("incident_evidence")
    op.drop_table("incidents")
    op.drop_table("control_results")
    op.drop_table("controls")
    op.drop_table("insurance_results")
    op.drop_table("stage_runs")
    op.drop_table("pipeline_runs")
    op.drop_table("wire_transactions")
    op.drop_table("beneficiaries")
    op.drop_table("trust_accounts")
    op.drop_table("accounts")
    op.drop_table("parties")
