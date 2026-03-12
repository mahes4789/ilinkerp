"""ilinkERP Fabric Accelerate — FastAPI backend."""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import erp_data, fabric_api

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

app.include_router(erp_data.router,   prefix="/api/erp",    tags=["ERP Data"])
app.include_router(fabric_api.router, prefix="/api/fabric",  tags=["Fabric"])


@app.get("/health")
async def health():
    return {"status": "ok", "app": "ilinkERP Fabric Accelerate"}
