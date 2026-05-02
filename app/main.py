from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from app.auth_router import router as auth_router
from app.config import settings
from app.dashboard.router import router as dashboard_router
from app.db import init_db
from app.logging import configure_logging
from app.security import BasicAuthMiddleware
from app.webhook import router as webhook_router

log = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    await init_db()
    log.info("startup", db=str(settings.database_path), base_url=settings.public_base_url)
    yield


app = FastAPI(title="StravaFit", lifespan=lifespan)
app.add_middleware(BasicAuthMiddleware)
app.include_router(dashboard_router)
app.include_router(auth_router)
app.include_router(webhook_router)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}
