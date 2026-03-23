"""0002_ontology_v2 — ontology-first redesign migrations

Adds ontology entity reference columns to runtime tables:
- pipeline_runs: job_node_id, system_node_id
- stage_runs: pipeline_node_id
- controls: regulation_node_id, control_objective_node_id, rule_node_id,
            transformation_node_id, pipeline_node_id, script_node_id, owner_node_id
- control_results: implicated_pipeline_node, implicated_script_node,
                   implicated_transformation_node, implicated_table_nodes,
                   implicated_column_nodes, code_event_node, log_source_node
- incidents: failed_control_objective_node, failed_rule_node, failed_transformation_node,
             implicated_pipeline_node, implicated_script_node, implicated_system_node,
             implicated_job_node, owner_node
- rca_results: root_cause_entity_type, root_cause_entity_id, supporting_entities
- ontology_nodes: properties column, node_id widened to VARCHAR(80)

Revision ID: 0002
Revises: 0001
Create Date: 2026-03-23
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── ontology_nodes: add properties column, widen node_id to VARCHAR(80) ──
    # node_id is PK so we can't alter in-place easily; add properties column
    op.add_column("ontology_nodes", sa.Column("properties", JSONB, nullable=True))
    # Widen VARCHAR columns that reference ontology nodes
    op.alter_column("ontology_nodes", "node_id", type_=sa.String(80), existing_type=sa.String(60))
    op.alter_column("ontology_edges", "from_node_id", type_=sa.String(80), existing_type=sa.String(60))
    op.alter_column("ontology_edges", "to_node_id", type_=sa.String(80), existing_type=sa.String(60))
    op.alter_column("ontology_edges", "relationship", type_=sa.String(30), existing_type=sa.String(20))

    # ── pipeline_runs: add ontology job/system node refs ──
    op.add_column("pipeline_runs", sa.Column("job_node_id", sa.String(80), nullable=True))
    op.add_column("pipeline_runs", sa.Column("system_node_id", sa.String(80), nullable=True))

    # ── stage_runs: add ontology pipeline node ref ──
    op.add_column("stage_runs", sa.Column("pipeline_node_id", sa.String(80), nullable=True))

    # ── controls: add v2 ontology chain columns, widen ontology_node_id ──
    op.alter_column("controls", "ontology_node_id", type_=sa.String(80), existing_type=sa.String(60))
    op.add_column("controls", sa.Column("regulation_node_id", sa.String(80), nullable=True))
    op.add_column("controls", sa.Column("control_objective_node_id", sa.String(80), nullable=True))
    op.add_column("controls", sa.Column("rule_node_id", sa.String(80), nullable=True))
    op.add_column("controls", sa.Column("transformation_node_id", sa.String(80), nullable=True))
    op.add_column("controls", sa.Column("pipeline_node_id", sa.String(80), nullable=True))
    op.add_column("controls", sa.Column("script_node_id", sa.String(80), nullable=True))
    op.add_column("controls", sa.Column("owner_node_id", sa.String(80), nullable=True))

    # ── control_results: add ontology entity refs ──
    op.add_column("control_results", sa.Column("implicated_pipeline_node", sa.String(80), nullable=True))
    op.add_column("control_results", sa.Column("implicated_script_node", sa.String(80), nullable=True))
    op.add_column("control_results", sa.Column("implicated_transformation_node", sa.String(80), nullable=True))
    op.add_column("control_results", sa.Column("implicated_table_nodes", JSONB, nullable=True))
    op.add_column("control_results", sa.Column("implicated_column_nodes", JSONB, nullable=True))
    op.add_column("control_results", sa.Column("code_event_node", sa.String(80), nullable=True))
    op.add_column("control_results", sa.Column("log_source_node", sa.String(80), nullable=True))

    # ── incidents: widen ontology_entry_node, add v2 ontology columns ──
    op.alter_column("incidents", "ontology_entry_node", type_=sa.String(80), existing_type=sa.String(60))
    op.alter_column("incidents", "root_cause_node", type_=sa.String(80), existing_type=sa.String(60))
    op.add_column("incidents", sa.Column("failed_control_objective_node", sa.String(80), nullable=True))
    op.add_column("incidents", sa.Column("failed_rule_node", sa.String(80), nullable=True))
    op.add_column("incidents", sa.Column("failed_transformation_node", sa.String(80), nullable=True))
    op.add_column("incidents", sa.Column("implicated_pipeline_node", sa.String(80), nullable=True))
    op.add_column("incidents", sa.Column("implicated_script_node", sa.String(80), nullable=True))
    op.add_column("incidents", sa.Column("implicated_system_node", sa.String(80), nullable=True))
    op.add_column("incidents", sa.Column("implicated_job_node", sa.String(80), nullable=True))
    op.add_column("incidents", sa.Column("owner_node", sa.String(80), nullable=True))

    # ── rca_results: widen root_cause_node, add entity-typed root cause + supporting entities ──
    op.alter_column("rca_results", "root_cause_node", type_=sa.String(80), existing_type=sa.String(60))
    op.add_column("rca_results", sa.Column("root_cause_entity_type", sa.String(30), nullable=True))
    op.add_column("rca_results", sa.Column("root_cause_entity_id", sa.String(80), nullable=True))
    op.add_column("rca_results", sa.Column("supporting_entities", JSONB, nullable=True))


def downgrade() -> None:
    # rca_results
    op.drop_column("rca_results", "supporting_entities")
    op.drop_column("rca_results", "root_cause_entity_id")
    op.drop_column("rca_results", "root_cause_entity_type")

    # incidents
    for col in ["owner_node", "implicated_job_node", "implicated_system_node",
                "implicated_script_node", "implicated_pipeline_node",
                "failed_transformation_node", "failed_rule_node", "failed_control_objective_node"]:
        op.drop_column("incidents", col)

    # control_results
    for col in ["log_source_node", "code_event_node", "implicated_column_nodes",
                "implicated_table_nodes", "implicated_transformation_node",
                "implicated_script_node", "implicated_pipeline_node"]:
        op.drop_column("control_results", col)

    # controls
    for col in ["owner_node_id", "script_node_id", "pipeline_node_id",
                "transformation_node_id", "rule_node_id",
                "control_objective_node_id", "regulation_node_id"]:
        op.drop_column("controls", col)

    op.drop_column("stage_runs", "pipeline_node_id")
    op.drop_column("pipeline_runs", "system_node_id")
    op.drop_column("pipeline_runs", "job_node_id")
    op.drop_column("ontology_nodes", "properties")
