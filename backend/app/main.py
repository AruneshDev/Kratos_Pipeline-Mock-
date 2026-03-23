import uuid
from typing import Optional
from fastapi import FastAPI, APIRouter, Depends
from fastapi.responses import RedirectResponse
from contextlib import asynccontextmanager
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logging import setup_logging
from app.db.session import engine, get_db
from app.api import seed as seed_router_module
from app.api import runs as runs_router_module
from app.api import rca as rca_router_module


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging(settings.log_level)
    import logging
    log = logging.getLogger("app.startup")
    log.info("bank-pipeline-api starting | env=%s | smdia=%s", settings.app_env, settings.smdia)
    # Verify DB connection
    try:
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        log.info("Database connection verified")
    except Exception as exc:
        log.error("Database connection failed: %s", exc)
    yield
    await engine.dispose()
    log.info("bank-pipeline-api shutdown complete")


app = FastAPI(
    title="bank-pipeline-api",
    version="0.1.0",
    description=(
        "Mock banking pipeline that evaluates deterministic controls, creates incidents "
        "from failures, and exposes evidence-rich RCA context to an external agent."
    ),
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")


# ── health router (defined inline — promoted to its own file in Phase 4+) ──
health_router = APIRouter(prefix="/health", tags=["health"])


@health_router.get("")
async def health(db: AsyncSession = Depends(get_db)):
    db_status = "connected"
    try:
        await db.execute(text("SELECT 1"))
    except Exception:
        db_status = "error"
    return {
        "status": "ok" if db_status == "connected" else "degraded",
        "env": settings.app_env,
        "db": db_status,
        "smdia": str(settings.smdia),
    }


app.include_router(health_router)
app.include_router(seed_router_module.router)
app.include_router(runs_router_module.router)
app.include_router(rca_router_module.router)
