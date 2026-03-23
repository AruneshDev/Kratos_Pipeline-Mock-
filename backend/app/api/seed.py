"""
Seed router — POST /seed/load

Loads all seed data from mounted JSON files + operational system CSVs
into the database. All loads are idempotent (upsert on primary key).
"""
import csv
import json
import logging
import uuid
from decimal import Decimal
from pathlib import Path
from datetime import date

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.session import get_db

log = logging.getLogger(__name__)
router = APIRouter(prefix="/seed", tags=["seed"])

_SEED_DIR = Path(settings.seed_data_path)
_OPS_DIR = Path("/app/app/seed/ops")   # symlink target — see docker-compose volume


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

def _parse_decimal(value: str | None) -> Decimal | None:
    if not value or value.strip() == "":
        return None
    try:
        return Decimal(value.strip().replace(",", ""))
    except Exception:
        return None


def _parse_date(value: str | None) -> date | None:
    if not value or value.strip() == "":
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y"):
        try:
            from datetime import datetime
            return datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _parse_int(value: str | None) -> int:
    try:
        return int(value or 0)
    except (ValueError, TypeError):
        return 0


def _parse_bool(value: str | None) -> bool:
    return str(value or "").upper() in ("TRUE", "Y", "YES", "1")


# ─────────────────────────────────────────────────────────────────────
# Ontology
# ─────────────────────────────────────────────────────────────────────

async def _load_ontology(db: AsyncSession) -> dict:
    # Try v2 first, fall back to v1
    path = _SEED_DIR / "ontology_seed_v2.json"
    if not path.exists():
        path = _SEED_DIR / "ontology_seed.json"
    data = json.loads(path.read_text())

    node_count = 0
    for n in data["nodes"]:
        await db.execute(
            text("""
                INSERT INTO ontology_nodes
                    (node_id, node_type, label, description, system,
                     evidence_fields, stop_condition, confidence_boost, properties)
                VALUES
                    (:node_id, :node_type, :label, :description, :system,
                     CAST(:evidence_fields AS jsonb), :stop_condition, :confidence_boost,
                     CAST(:properties AS jsonb))
                ON CONFLICT (node_id) DO UPDATE SET
                    label = EXCLUDED.label,
                    description = EXCLUDED.description,
                    node_type = EXCLUDED.node_type,
                    system = EXCLUDED.system,
                    properties = EXCLUDED.properties
            """),
            {
                "node_id": n["node_id"],
                "node_type": n.get("node_type"),
                "label": n.get("label"),
                "description": n.get("description"),
                "system": n.get("system"),
                "evidence_fields": json.dumps(n.get("evidence_fields", [])),
                "stop_condition": n.get("stop_condition"),
                "confidence_boost": n.get("confidence_boost", 0.10),
                "properties": json.dumps(n.get("properties", {})),
            },
        )
        node_count += 1

    edge_count = 0
    for e in data["edges"]:
        await db.execute(
            text("""
                INSERT INTO ontology_edges
                    (edge_id, from_node_id, to_node_id, relationship, weight, description)
                VALUES
                    (gen_random_uuid(), :from_node_id, :to_node_id, :relationship, :weight, :description)
                ON CONFLICT DO NOTHING
            """),
            {
                "from_node_id": e["from_node_id"],
                "to_node_id": e["to_node_id"],
                "relationship": e.get("relationship"),
                "weight": e.get("weight", 0.5),
                "description": e.get("description"),
            },
        )
        edge_count += 1

    return {"nodes": node_count, "edges": edge_count}


# ─────────────────────────────────────────────────────────────────────
# Controls
# ─────────────────────────────────────────────────────────────────────

async def _load_controls(db: AsyncSession) -> int:
    # Try v2 registry first, fall back to v1
    path = _SEED_DIR / "control_registry_v2.json"
    if not path.exists():
        path = _SEED_DIR / "control_registry.json"
    controls = json.loads(path.read_text())
    for c in controls:
        await db.execute(
            text("""
                INSERT INTO controls
                    (control_id, control_name, description, source_ref, cfr_citation,
                     stage, system, rule_type, applicable_fields, severity,
                     control_type, ontology_node_id, remediation, is_active,
                     regulation_node_id, control_objective_node_id, rule_node_id,
                     transformation_node_id, pipeline_node_id, script_node_id, owner_node_id)
                VALUES
                    (:control_id, :control_name, :description, :source_ref, :cfr_citation,
                     :stage, :system, :rule_type, CAST(:applicable_fields AS jsonb), :severity,
                     :control_type, :ontology_node_id, :remediation, :is_active,
                     :regulation_node_id, :control_objective_node_id, :rule_node_id,
                     :transformation_node_id, :pipeline_node_id, :script_node_id, :owner_node_id)
                ON CONFLICT (control_id) DO UPDATE SET
                    control_name = EXCLUDED.control_name,
                    is_active = EXCLUDED.is_active,
                    regulation_node_id = EXCLUDED.regulation_node_id,
                    control_objective_node_id = EXCLUDED.control_objective_node_id,
                    rule_node_id = EXCLUDED.rule_node_id,
                    transformation_node_id = EXCLUDED.transformation_node_id,
                    pipeline_node_id = EXCLUDED.pipeline_node_id,
                    script_node_id = EXCLUDED.script_node_id,
                    owner_node_id = EXCLUDED.owner_node_id
            """),
            {
                "control_id": c["control_id"],
                "control_name": c["control_name"],
                "description": c.get("description"),
                "source_ref": c.get("source_ref"),
                "cfr_citation": c.get("cfr_citation"),
                "stage": c.get("stage"),
                "system": c.get("system"),
                "rule_type": c.get("rule_type"),
                "applicable_fields": json.dumps(c.get("applicable_fields", [])),
                "severity": c.get("severity"),
                "control_type": c.get("control_type"),
                "ontology_node_id": c.get("ontology_node_id"),
                "remediation": c.get("remediation"),
                "is_active": c.get("is_active", True),
                "regulation_node_id": c.get("regulation_node_id"),
                "control_objective_node_id": c.get("control_objective_node_id"),
                "rule_node_id": c.get("rule_node_id"),
                "transformation_node_id": c.get("transformation_node_id"),
                "pipeline_node_id": c.get("pipeline_node_id"),
                "script_node_id": c.get("script_node_id"),
                "owner_node_id": c.get("owner_node_id"),
            },
        )
    return len(controls)


# ─────────────────────────────────────────────────────────────────────
# Sample Accounts (legacy deposit)
# ─────────────────────────────────────────────────────────────────────

async def _load_accounts(db: AsyncSession) -> int:
    csv_path = _SEED_DIR / "sample_accounts.csv"
    if not csv_path.exists():
        # Fall back to operational system data
        csv_path = Path("/app/app/seed/ops/legacy_deposit/data/sample_accounts.csv")
    if not csv_path.exists():
        log.warning("sample_accounts.csv not found, skipping")
        return 0

    count = 0
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            party_exists = await db.execute(
                text("SELECT 1 FROM parties WHERE source_record_id = :sid AND source_system = 'legacy_deposit'"),
                {"sid": row.get("customer_id", "")},
            )
            if not party_exists.scalar():
                party_id = uuid.uuid4()
                await db.execute(
                    text("""
                        INSERT INTO parties
                            (party_id, name, party_type, source_system, source_record_id)
                        VALUES (:party_id, :name, 'Individual', 'legacy_deposit', :sid)
                        ON CONFLICT DO NOTHING
                    """),
                    {
                        "party_id": str(party_id),
                        "name": row.get("customer_name", "Unknown"),
                        "sid": row.get("customer_id", ""),
                    },
                )
            else:
                party_row = await db.execute(
                    text("SELECT party_id FROM parties WHERE source_record_id = :sid AND source_system = 'legacy_deposit'"),
                    {"sid": row.get("customer_id", "")},
                )
                party_id = party_row.scalar()

            await db.execute(
                text("""
                    INSERT INTO accounts
                        (account_id, account_number, account_type, party_id,
                         orc_code, balance, accrued_interest, beneficiary_count,
                         collateral_pledged, open_date, source_system)
                    VALUES
                        (gen_random_uuid(), :account_number, :account_type, :party_id,
                         :orc_code, :balance, :accrued_interest, :beneficiary_count,
                         :collateral_pledged, :open_date, 'legacy_deposit')
                    ON CONFLICT DO NOTHING
                """),
                {
                    "account_number": row.get("account_id", ""),
                    "account_type": row.get("account_type", ""),
                    "party_id": str(party_id),
                    "orc_code": row.get("orc_type", row.get("orc_code", "SGL")),
                    "balance": _parse_decimal(row.get("balance")),
                    "accrued_interest": _parse_decimal(row.get("accrued_interest")) or Decimal(0),
                    "beneficiary_count": _parse_int(row.get("beneficiary_count")),
                    "collateral_pledged": _parse_bool(row.get("collateral_pledged")),
                    "open_date": _parse_date(row.get("open_date")),
                },
            )
            count += 1
    return count


# ─────────────────────────────────────────────────────────────────────
# Trust Accounts
# ─────────────────────────────────────────────────────────────────────

async def _load_trusts(db: AsyncSession) -> int:
    csv_path = _SEED_DIR / "sample_trust_accounts.csv"
    if not csv_path.exists():
        csv_path = Path("/app/app/seed/ops/trust_custody/data/sample_trust_accounts.csv")
    if not csv_path.exists():
        log.warning("sample_trust_accounts.csv not found, skipping")
        return 0

    count = 0
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            await db.execute(
                text("""
                    INSERT INTO trust_accounts
                        (trust_id, trust_name, trust_type, balance, accrued_interest,
                         beneficiary_count, orc_assigned, trust_status, open_date, source_system)
                    VALUES
                        (:trust_id, :trust_name, :trust_type, :balance, :accrued_interest,
                         :beneficiary_count, :orc_assigned, :trust_status, :open_date, 'trust_custody')
                    ON CONFLICT (trust_id) DO UPDATE SET
                        trust_name = EXCLUDED.trust_name,
                        orc_assigned = EXCLUDED.orc_assigned,
                        balance = EXCLUDED.balance
                """),
                {
                    "trust_id": row.get("trust_id", ""),
                    "trust_name": row.get("trust_name", ""),
                    "trust_type": row.get("trust_type", ""),
                    "balance": _parse_decimal(row.get("balance")),
                    "accrued_interest": _parse_decimal(row.get("accrued_interest")) or Decimal(0),
                    "beneficiary_count": _parse_int(row.get("beneficiary_count")),
                    "orc_assigned": row.get("orc_assigned", row.get("orc_code", "SGL")),
                    "trust_status": row.get("trust_status", row.get("status", "A")),
                    "open_date": _parse_date(row.get("open_date")),
                },
            )
            count += 1
    return count


# ─────────────────────────────────────────────────────────────────────
# Wire Transactions
# ─────────────────────────────────────────────────────────────────────

async def _load_wires(db: AsyncSession) -> int:
    csv_path = _SEED_DIR / "sample_wire_transactions.csv"
    if not csv_path.exists():
        csv_path = Path("/app/app/seed/ops/wire_transfer/data/sample_wire_transactions.csv")
    if not csv_path.exists():
        log.warning("sample_wire_transactions.csv not found, skipping")
        return 0

    count = 0
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            await db.execute(
                text("""
                    INSERT INTO wire_transactions
                        (wire_id, reference, message_type, direction,
                         ordering_customer_id, ordering_name, beneficiary_name, beneficiary_account,
                         amount, currency, status, orc_type, settlement_channel,
                         ofac_status, value_date, source_system)
                    VALUES
                        (gen_random_uuid(), :reference, :message_type, :direction,
                         :ordering_customer_id, :ordering_name, :beneficiary_name, :beneficiary_account,
                         :amount, :currency, :status, :orc_type, :settlement_channel,
                         :ofac_status, :value_date, 'wire_transfer')
                    ON CONFLICT (reference) DO UPDATE SET
                        status = EXCLUDED.status,
                        ofac_status = EXCLUDED.ofac_status
                """),
                {
                    "reference": row.get("wire_id", row.get("reference", str(uuid.uuid4())[:12])),
                    "message_type": row.get("message_type", "MT103"),
                    "direction": row.get("direction", "OUTBOUND"),
                    "ordering_customer_id": row.get("customer_id", row.get("ordering_customer_id")),
                    "ordering_name": row.get("ordering_name", row.get("customer_name")),
                    "beneficiary_name": row.get("beneficiary_name"),
                    "beneficiary_account": row.get("beneficiary_account"),
                    "amount": _parse_decimal(row.get("amount")),
                    "currency": row.get("currency", "USD"),
                    "status": row.get("status", "SETTLED"),
                    "orc_type": row.get("orc_type", row.get("orc_code")),
                    "settlement_channel": row.get("settlement_channel"),
                    "ofac_status": row.get("ofac_status", "CLEARED"),
                    "value_date": _parse_date(row.get("value_date")),
                },
            )
            count += 1
    return count


# ─────────────────────────────────────────────────────────────────────
# Scenario SCN-002 seed — IRR trusts misclassified as SGL
# ─────────────────────────────────────────────────────────────────────

_IRR_TRUSTS = [
    {
        "trust_id": "TR-003",
        "trust_name": "Irrevocable Life Insurance Trust - Hendricks",
        "trust_type": "IRR",
        "balance": Decimal("2200000.00"),
        "accrued_interest": Decimal("4500.00"),
        "beneficiary_count": 3,
        "orc_assigned": "SGL",   # BUG: should be IRR — IRR branch not implemented in ORC-ASSIGNMENT.cob
        "trust_status": "A",
    },
    {
        "trust_id": "TR-008",
        "trust_name": "Charitable Remainder Trust - Morrison Foundation",
        "trust_type": "IRR",
        "balance": Decimal("750000.00"),
        "accrued_interest": Decimal("1200.00"),
        "beneficiary_count": 2,
        "orc_assigned": "SGL",   # BUG: should be IRR
        "trust_status": "A",
    },
    {
        "trust_id": "TR-015",
        "trust_name": "Grandchildren Education Irrevocable Trust - Kim",
        "trust_type": "IRR",
        "balance": Decimal("500000.00"),
        "accrued_interest": Decimal("875.00"),
        "beneficiary_count": 4,
        "orc_assigned": "SGL",   # BUG: should be IRR
        "trust_status": "A",
    },
]


async def _seed_scn002(db: AsyncSession) -> int:
    """Upsert the three IRR trusts that trigger SCN-002."""
    for t in _IRR_TRUSTS:
        await db.execute(
            text("""
                INSERT INTO trust_accounts
                    (trust_id, trust_name, trust_type, balance, accrued_interest,
                     beneficiary_count, orc_assigned, trust_status, source_system)
                VALUES
                    (:trust_id, :trust_name, :trust_type, :balance, :accrued_interest,
                     :beneficiary_count, :orc_assigned, :trust_status, 'scn002_seed')
                ON CONFLICT (trust_id) DO UPDATE SET
                    orc_assigned = EXCLUDED.orc_assigned,
                    trust_type = EXCLUDED.trust_type,
                    balance = EXCLUDED.balance
            """),
            {**{k: str(v) if isinstance(v, Decimal) else v for k, v in t.items()}},
        )
    return len(_IRR_TRUSTS)


# ─────────────────────────────────────────────────────────────────────
# Router endpoint
# ─────────────────────────────────────────────────────────────────────

@router.post("/load")
async def load_seed(db: AsyncSession = Depends(get_db)):
    """
    Idempotent seed load. Safe to call multiple times.
    Loads: ontology nodes/edges, controls, sample accounts, trusts, wires,
    and the SCN-002 scenario IRR trusts.
    """
    results = {}
    try:
        ontology = await _load_ontology(db)
        results["ontology"] = ontology

        results["controls"] = await _load_controls(db)
        results["accounts"] = await _load_accounts(db)
        results["trusts"] = await _load_trusts(db)
        results["wires"] = await _load_wires(db)
        results["scn002_trusts"] = await _seed_scn002(db)

        await db.commit()
        log.info("Seed load complete: %s", results)
        return {"status": "ok", "loaded": results}
    except Exception as exc:
        await db.rollback()
        log.exception("Seed load failed")
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/status")
async def seed_status(db: AsyncSession = Depends(get_db)):
    """Return row counts for every domain table."""
    tables = [
        "parties", "accounts", "trust_accounts", "beneficiaries",
        "wire_transactions", "controls", "ontology_nodes", "ontology_edges",
        "pipeline_runs", "incidents",
    ]
    counts = {}
    for table in tables:
        row = await db.execute(text(f"SELECT COUNT(*) FROM {table}"))  # noqa: S608 — table names are hardcoded
        counts[table] = row.scalar()
    return {"counts": counts}
