"""ilinkERP Fabric Accelerate — FastAPI backend."""
import asyncio
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import erp_data, fabric_api, erp_scraper

app = FastAPI(
    title="ilinkERP Fabric Accelerate",
    version="1.0.0",
    description="ERP → Microsoft Fabric integration accelerator",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(erp_data.router,    prefix="/api/erp",    tags=["ERP Data"])
app.include_router(fabric_api.router,  prefix="/api/fabric", tags=["Fabric"])
app.include_router(erp_scraper.router, prefix="/api/erp",    tags=["erp-scraper"])


async def _background_refresh():
    await asyncio.sleep(5)  # let the app start fully
    try:
        from app.routes.erp_scraper import refresh_all_sources
        await refresh_all_sources()
    except Exception as e:
        logging.getLogger(__name__).warning("Background ERP doc refresh failed: %s", e)


@app.on_event("startup")
async def startup_refresh():
    """Kick off a background cache refresh for any stale ERP doc entries."""
    asyncio.create_task(_background_refresh())


@app.get("/health")
async def health():
    return {"status": "ok", "app": "ilinkERP Fabric Accelerate"}
