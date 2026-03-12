"""Fabric Connection Types, Create Connection, and Full Deploy endpoints."""
from __future__ import annotations
import base64, json, uuid
import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter()

FABRIC_API = "https://api.fabric.microsoft.com/v1"

# Cached MS365 OAuth token (set via /api/fabric/set-token)
_ms_token: dict = {}

# ── Fabric Connection Types per ERP source ────────────────────────────────────
ERP_FABRIC_CONNECTION_TYPES: dict[str, list[dict]] = {
    "oracle_ebs": [
        {
            "type": "Oracle", "label": "Oracle Database (JDBC)",
            "protocol": "jdbc", "recommended": True,
            "description": "Direct JDBC connection to Oracle EBS database. Best for on-premise deployments with DB access.",
            "fields": [
                {"name": "server",   "label": "Host / Server",      "placeholder": "10.0.0.100"},
                {"name": "port",     "label": "Port",               "placeholder": "1521"},
                {"name": "database", "label": "Service Name / SID", "placeholder": "PROD"},
            ],
            "credentialType": "Basic",
        },
        {
            "type": "OracleDatabase", "label": "Oracle DB (Fabric Native Connector)",
            "protocol": "oracle_native", "recommended": False,
            "description": "Fabric-native Oracle connector with query push-down optimization.",
            "fields": [
                {"name": "server",      "label": "Host / Server",  "placeholder": "10.0.0.100"},
                {"name": "port",        "label": "Port",           "placeholder": "1521"},
                {"name": "serviceName", "label": "Service Name",   "placeholder": "PROD"},
            ],
            "credentialType": "Basic",
        },
    ],
    "oracle_fusion": [
        {
            "type": "OracleCloudFinancials", "label": "Oracle Fusion Cloud REST API",
            "protocol": "rest", "recommended": True,
            "description": "Oracle Fusion Cloud ERP REST API — recommended for cloud ERP. No database credentials needed.",
            "fields": [
                {"name": "serviceUrl", "label": "Cloud Service URL", "placeholder": "https://myinstance.fa.oraclecloud.com"},
            ],
            "credentialType": "Basic",
        },
        {
            "type": "Oracle", "label": "Oracle Database (JDBC)",
            "protocol": "jdbc", "recommended": False,
            "description": "Direct JDBC to Oracle Fusion Cloud DB tier — requires DBA privileges.",
            "fields": [
                {"name": "server",   "label": "Host / Server",  "placeholder": "db.oraclecloud.com"},
                {"name": "port",     "label": "Port",           "placeholder": "1521"},
                {"name": "database", "label": "Service / SID",  "placeholder": "FUSION"},
            ],
            "credentialType": "Basic",
        },
    ],
    "sap_s4hana": [
        {
            "type": "SapHana", "label": "SAP S/4HANA HANA DB",
            "protocol": "hana", "recommended": True,
            "description": "Direct HANA in-memory DB connection — highest performance for S/4HANA extraction.",
            "fields": [
                {"name": "server", "label": "HANA Host", "placeholder": "s4hana.corp.local"},
                {"name": "port",   "label": "Port",      "placeholder": "30015"},
            ],
            "credentialType": "Basic",
        },
        {
            "type": "OData", "label": "SAP S/4HANA OData API",
            "protocol": "odata", "recommended": False,
            "description": "OData service connector — best for cloud-to-cloud S/4HANA integration.",
            "fields": [
                {"name": "url", "label": "OData Service URL", "placeholder": "https://s4hana.corp.com/sap/opu/odata"},
            ],
            "credentialType": "Basic",
        },
    ],
    "sap_ecc": [
        {
            "type": "SapBw", "label": "SAP RFC (Open Hub / Table)",
            "protocol": "rfc", "recommended": True,
            "description": "SAP RFC connector for ECC table extraction via RFC_READ_TABLE or Open Hub.",
            "fields": [
                {"name": "server",       "label": "Application Server", "placeholder": "10.0.0.50"},
                {"name": "systemNumber", "label": "System Number",      "placeholder": "00"},
                {"name": "clientId",     "label": "Client (Mandant)",   "placeholder": "100"},
            ],
            "credentialType": "Basic",
        },
        {
            "type": "SapHana", "label": "SAP HANA (JDBC)",
            "protocol": "hana", "recommended": False,
            "description": "SAP HANA DB connector for ECC-on-HANA scenarios.",
            "fields": [
                {"name": "server", "label": "HANA Host", "placeholder": "10.0.0.50"},
                {"name": "port",   "label": "Port",      "placeholder": "30015"},
            ],
            "credentialType": "Basic",
        },
    ],
    "dynamics_365_fo": [
        {
            "type": "DynamicsCrm", "label": "Dynamics 365 Finance & Operations",
            "protocol": "dynamics", "recommended": True,
            "description": "D365 F&O connector via Azure AD service principal — recommended.",
            "fields": [
                {"name": "organizationUrl", "label": "Environment URL",  "placeholder": "https://myorg.operations.dynamics.com"},
                {"name": "tenantId",        "label": "Azure Tenant ID",  "placeholder": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"},
                {"name": "clientId",        "label": "Azure Client ID",  "placeholder": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"},
            ],
            "credentialType": "ServicePrincipal",
        },
        {
            "type": "OData", "label": "D365 F&O OData API",
            "protocol": "odata", "recommended": False,
            "description": "OData entities endpoint for Dynamics 365 Finance and Operations.",
            "fields": [
                {"name": "url",      "label": "OData Base URL",   "placeholder": "https://myorg.operations.dynamics.com/data"},
                {"name": "tenantId", "label": "Azure Tenant ID",  "placeholder": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"},
            ],
            "credentialType": "ServicePrincipal",
        },
    ],
    "dynamics_bc": [
        {
            "type": "DynamicsNAV", "label": "Dynamics 365 Business Central",
            "protocol": "bc", "recommended": True,
            "description": "BC API connector via Azure AD — supports cloud and on-premise.",
            "fields": [
                {"name": "serverUrl", "label": "API Server URL",   "placeholder": "https://api.businesscentral.dynamics.com/v2.0"},
                {"name": "tenantId",  "label": "Azure Tenant ID",  "placeholder": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"},
            ],
            "credentialType": "ServicePrincipal",
        },
        {
            "type": "OData", "label": "Business Central OData V4",
            "protocol": "odata", "recommended": False,
            "description": "OData V4 connector for Business Central entities.",
            "fields": [
                {"name": "url",      "label": "OData URL",         "placeholder": "https://api.businesscentral.dynamics.com/v2.0/{tenant}/api/v2.0"},
                {"name": "tenantId", "label": "Azure Tenant ID",   "placeholder": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"},
            ],
            "credentialType": "ServicePrincipal",
        },
    ],
    "netsuite": [
        {
            "type": "RestService", "label": "NetSuite REST API (TBA)",
            "protocol": "rest", "recommended": True,
            "description": "NetSuite REST API with Token-Based Authentication — best for SuiteQL extraction.",
            "fields": [
                {"name": "accountId", "label": "Account ID",    "placeholder": "1234567"},
                {"name": "baseUrl",   "label": "REST Base URL", "placeholder": "https://1234567.suitetalk.api.netsuite.com"},
            ],
            "credentialType": "TBA",
        },
    ],
    "workday": [
        {
            "type": "Workday", "label": "Workday Connector (Native)",
            "protocol": "workday", "recommended": True,
            "description": "Native Workday connector for HCM and Financials via RAAS and REST.",
            "fields": [
                {"name": "tenantName", "label": "Tenant Name", "placeholder": "mycompany"},
                {"name": "tenantUrl",  "label": "Tenant URL",  "placeholder": "https://wd3.myworkday.com"},
            ],
            "credentialType": "Basic",
        },
        {
            "type": "RestService", "label": "Workday REST API",
            "protocol": "rest", "recommended": False,
            "description": "Workday REST API for programmatic access to HCM and financial data.",
            "fields": [
                {"name": "tenantName", "label": "Tenant Name",   "placeholder": "mycompany"},
                {"name": "serviceUrl", "label": "Service URL",   "placeholder": "https://wd3-services1.workday.com/ccx/service"},
            ],
            "credentialType": "Basic",
        },
    ],
}


# ── Helpers ───────────────────────────────────────────────────────────────────
def _get_token() -> str:
    token = _ms_token.get("access_token", "")
    if not token:
        raise HTTPException(401, "Microsoft 365 SSO not active. Set token via /api/fabric/set-token.")
    return token


async def _fabric_post(token: str, url: str, payload: dict) -> dict:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            url, json=payload,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        )
        if r.status_code not in (200, 201, 202):
            raise HTTPException(r.status_code, f"Fabric API {r.status_code}: {r.text[:300]}")
        return r.json() if r.text else {}


def _make_notebook_b64(name: str, cells: list[str]) -> str:
    nb = {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {"kernelspec": {"display_name": "PySpark", "language": "python", "name": "synapse_pyspark"},
                     "language_info": {"name": "python", "version": "3.11.0"}},
        "cells": [{"cell_type": "code", "execution_count": None, "metadata": {},
                   "outputs": [], "source": c} for c in cells],
    }
    return base64.b64encode(json.dumps(nb).encode()).decode()


def _bronze_cells(source: str, module: str, tables: list[str]) -> list[str]:
    return [
        f"# Bronze Layer — {source} {module} raw extraction\n"
        "from pyspark.sql import SparkSession\n"
        "spark = SparkSession.builder.appName('Bronze_Extract').getOrCreate()\n"
        "JDBC = f\"jdbc:oracle:thin:@{{dbutils.secrets.get('erp','host')}}:1521/{{dbutils.secrets.get('erp','svc')}}\"\n"
        "OPTS = {'url': JDBC, 'user': dbutils.secrets.get('erp','user'), 'password': dbutils.secrets.get('erp','pwd'), 'driver': 'oracle.jdbc.OracleDriver'}"
    ] + [
        f"# Extract {t}\ndf_{t.lower()} = spark.read.format('jdbc').options(**OPTS, dbtable='{t}').load()\n"
        f"df_{t.lower()}.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('bronze.{t.lower()}')\n"
        f"print('bronze.{t.lower()} written')"
        for t in (tables or ["HEADER_TABLE", "LINE_TABLE"])
    ]


def _silver_cells(source: str, module: str) -> list[str]:
    return [
        f"# Silver Layer — {source} {module} cleanse & conform\n"
        "from pyspark.sql import SparkSession, functions as F\n"
        "spark = SparkSession.builder.appName('Silver_Transform').getOrCreate()\n\n"
        "# Load bronze tables and apply transformations\n"
        f"bronze_tables = spark.catalog.listTables('bronze')\n"
        f"for t in bronze_tables:\n"
        f"    df = spark.read.table(f'bronze.{{t.name}}')\n"
        f"    df_clean = df.dropDuplicates().na.fill('')\n"
        f"    df_clean.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(f'silver.{{t.name}}')\n"
        f"    print(f'silver.{{t.name}} written')"
    ]


def _gold_cells(source: str, module: str) -> list[str]:
    return [
        f"# Gold Layer — {source} {module} business KPIs\n"
        "from pyspark.sql import SparkSession, functions as F\n"
        "spark = SparkSession.builder.appName('Gold_Aggregation').getOrCreate()\n\n"
        f"# Build {module} summary metrics\n"
        f"# TODO: Define aggregations specific to {module} business requirements\n"
        "print('Gold layer transformation complete')"
    ]


# ── Endpoints ─────────────────────────────────────────────────────────────────
@router.get("/connection-types")
async def get_connection_types(source: str = Query(...)):
    types = ERP_FABRIC_CONNECTION_TYPES.get(source, [
        {"type": "RestService", "label": "REST API", "protocol": "rest", "recommended": True,
         "description": "Generic REST connector.",
         "fields": [{"name": "url", "label": "Base URL", "placeholder": "https://api.example.com"}],
         "credentialType": "Basic"}
    ])
    return {"source": source, "connection_types": types, "total": len(types)}


@router.post("/set-token")
async def set_ms_token(payload: dict):
    """Set the Microsoft 365 OAuth access token for Fabric API calls."""
    token = payload.get("access_token", "")
    if not token:
        raise HTTPException(400, "access_token required")
    _ms_token["access_token"] = token
    return {"status": "ok", "message": "MS365 token set — Fabric API calls will use live credentials."}


class CreateConnectionRequest(BaseModel):
    workspace_id:      str
    display_name:      str
    source_type:       str
    connection_type:   str
    protocol:          str = ""
    connection_fields: dict = {}
    credential_type:   str = "Basic"
    username:          str = ""
    password:          str = ""
    client_secret:     str = ""
    privacy_level:     str = "Organizational"


@router.post("/create-connection")
async def create_connection(req: CreateConnectionRequest):
    """
    Create a Fabric shareable connection for an ERP source.
    Calls POST https://api.fabric.microsoft.com/v1/connections if MS365 token is active.
    Returns a simulated response when token is not set.
    """
    conn_id = str(uuid.uuid4())
    try:
        token  = _get_token()
        params = [{"name": k, "value": v} for k, v in req.connection_fields.items()]
        if req.credential_type == "ServicePrincipal":
            creds = {"credentialType": "ServicePrincipal", "clientId": req.username,
                     "clientSecret": req.client_secret,
                     "tenantId": req.connection_fields.get("tenantId", "")}
        elif req.credential_type == "TBA":
            creds = {"credentialType": "Anonymous"}
        else:
            creds = {"credentialType": "Basic", "username": req.username, "password": req.password}
        payload = {
            "connectivityType":  "ShareableCloud",
            "displayName":        req.display_name,
            "connectionDetails": {"type": req.connection_type, "parameters": params},
            "privacyLevel":       req.privacy_level,
            "credentialDetails":  creds,
        }
        result = await _fabric_post(token, f"{FABRIC_API}/connections", payload)
        return {
            "status": "created", "live": True,
            "connection_id": result.get("id", conn_id),
            "display_name": req.display_name,
            "connection_type": req.connection_type,
            "workspace_id": req.workspace_id,
        }
    except HTTPException:
        return {
            "status": "simulated", "live": False,
            "connection_id": conn_id,
            "display_name": req.display_name,
            "connection_type": req.connection_type,
            "workspace_id": req.workspace_id,
            "note": "MS365 SSO not active — connection simulated. Use /api/fabric/set-token for live deployment.",
        }


class DeployRequest(BaseModel):
    workspace_id:    str
    lakehouse_id:    str = ""
    source_type:     str
    module:          str
    connection_id:   str = ""
    connection_name: str = ""
    selected_tables: list[str] = []
    create_bronze:   bool = True
    create_silver:   bool = True
    create_gold:     bool = True
    create_pipeline: bool = True


@router.post("/deploy")
async def deploy(req: DeployRequest):
    """
    Full ERP → Fabric deployment:
    Creates Bronze / Silver / Gold notebooks + Data Pipeline.
    Simulates when MS365 SSO is not active.
    """
    try:
        token = _get_token()
        live  = True
    except HTTPException:
        token = None
        live  = False

    ws     = req.workspace_id
    prefix = f"{req.source_type}_{req.module}".replace(" ", "_").replace("(", "").replace(")", "")
    tables = req.selected_tables or ["TABLE_A", "TABLE_B", "TABLE_C"]

    artifacts: list[dict] = []
    nb_ids: dict[str, str] = {}

    async def _make(name: str, item_type: str, definition: dict) -> dict:
        if live:
            try:
                result = await _fabric_post(token, f"{FABRIC_API}/workspaces/{ws}/items",
                                            {"displayName": name, "type": item_type, "definition": definition})
                return {"name": name, "type": item_type, "id": result.get("id", str(uuid.uuid4())),
                        "status": "created", "live": True}
            except Exception as exc:
                return {"name": name, "type": item_type, "id": str(uuid.uuid4()),
                        "status": "failed", "error": str(exc)}
        return {"name": name, "type": item_type, "id": str(uuid.uuid4()), "status": "simulated", "live": False}

    # Notebooks
    layers = []
    if req.create_bronze:
        layers.append(("Bronze", _bronze_cells(req.source_type, req.module, tables)))
    if req.create_silver:
        layers.append(("Silver", _silver_cells(req.source_type, req.module)))
    if req.create_gold:
        layers.append(("Gold",   _gold_cells(req.source_type, req.module)))

    for layer, cells in layers:
        nb_name = f"{prefix}_{layer}_Notebook"
        nb64    = _make_notebook_b64(nb_name, cells)
        art     = await _make(nb_name, "Notebook", {
            "format": "ipynb",
            "parts": [{"path": "artifact.content.ipynb", "payload": nb64, "payloadType": "InlineBase64"}],
        })
        artifacts.append(art)
        nb_ids[layer] = art["id"]

    # Data Pipeline
    if req.create_pipeline:
        activities, prev = [], None
        for layer in ("Bronze", "Silver", "Gold"):
            if layer not in nb_ids:
                continue
            act = {"name": f"Run_{layer}", "type": "TridentNotebook",
                   "dependsOn": [{"activity": prev, "dependencyConditions": ["Succeeded"]}] if prev else [],
                   "typeProperties": {"notebookId": nb_ids[layer], "workspaceId": ws}}
            activities.append(act)
            prev = act["name"]
        pl_def = base64.b64encode(json.dumps({"properties": {"activities": activities}}).encode()).decode()
        art = await _make(f"{prefix}_Pipeline", "DataPipeline", {
            "format": "json",
            "parts": [{"path": "pipeline-content.json", "payload": pl_def, "payloadType": "InlineBase64"}],
        })
        artifacts.append(art)

    return {
        "live":       live,
        "total":      len(artifacts),
        "artifacts":  artifacts,
        "workspace_id": ws,
        "note": "" if live else "MS365 SSO not active — artifacts simulated. Use /api/fabric/set-token.",
    }
