"""
Fabric REST API routes
 · Token management   → POST /set-token, GET /validate-token
 · Workspaces         → GET /workspaces, GET /workspaces/{id}/lakehouses,
                        GET /workspaces/{id}/items
 · Connections        → GET /connections, GET /connections/{id},
                        POST /create-connection
 · Deploy             → POST /deploy  (notebooks + pipeline, LRO-aware)
"""
from __future__ import annotations
import asyncio
import base64, json, uuid
import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional

router = APIRouter()

FABRIC_API = "https://api.fabric.microsoft.com/v1"

# In-memory token store (single-user; replace with session store for multi-user)
_ms_token: dict = {}

# ── ERP → Fabric Connection Types ─────────────────────────────────────────────
ERP_FABRIC_CONNECTION_TYPES: dict[str, list[dict]] = {
    "oracle_ebs": [
        {
            "type": "Oracle", "label": "Oracle Database (JDBC)",
            "protocol": "jdbc", "recommended": True,
            "description": "Direct JDBC connection to Oracle EBS database. Best for on-premise deployments.",
            "fields": [
                {"name": "server",   "label": "Host / Server",      "placeholder": "10.0.0.100"},
                {"name": "port",     "label": "Port",               "placeholder": "1521"},
                {"name": "database", "label": "Service Name / SID", "placeholder": "PROD"},
            ],
            "credentialType": "Basic",
        },
        {
            "type": "OracleDatabase", "label": "Oracle DB (Fabric Native)",
            "protocol": "oracle_native", "recommended": False,
            "description": "Fabric-native Oracle connector with query push-down optimisation.",
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
            "description": "Oracle Fusion Cloud ERP REST API — recommended for cloud ERP.",
            "fields": [
                {"name": "serviceUrl", "label": "Cloud Service URL",
                 "placeholder": "https://myinstance.fa.oraclecloud.com"},
            ],
            "credentialType": "Basic",
        },
        {
            "type": "Oracle", "label": "Oracle Database (JDBC)",
            "protocol": "jdbc", "recommended": False,
            "description": "Direct JDBC to Oracle Fusion Cloud DB tier — requires DBA access.",
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
            "description": "Direct HANA in-memory DB connection — highest performance for S/4HANA.",
            "fields": [
                {"name": "server", "label": "HANA Host", "placeholder": "s4hana.corp.local"},
                {"name": "port",   "label": "Port",      "placeholder": "30015"},
            ],
            "credentialType": "Basic",
        },
        {
            "type": "OData", "label": "SAP S/4HANA OData API",
            "protocol": "odata", "recommended": False,
            "description": "OData service connector — best for cloud-to-cloud S/4HANA.",
            "fields": [
                {"name": "url", "label": "OData Service URL",
                 "placeholder": "https://s4hana.corp.com/sap/opu/odata"},
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
                {"name": "organizationUrl", "label": "Environment URL",
                 "placeholder": "https://myorg.operations.dynamics.com"},
                {"name": "tenantId",        "label": "Azure Tenant ID",
                 "placeholder": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"},
                {"name": "clientId",        "label": "Azure Client ID",
                 "placeholder": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"},
            ],
            "credentialType": "ServicePrincipal",
        },
        {
            "type": "OData", "label": "D365 F&O OData API",
            "protocol": "odata", "recommended": False,
            "description": "OData entities endpoint for Dynamics 365 Finance and Operations.",
            "fields": [
                {"name": "url",      "label": "OData Base URL",
                 "placeholder": "https://myorg.operations.dynamics.com/data"},
                {"name": "tenantId", "label": "Azure Tenant ID",
                 "placeholder": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"},
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
                {"name": "serverUrl", "label": "API Server URL",
                 "placeholder": "https://api.businesscentral.dynamics.com/v2.0"},
                {"name": "tenantId",  "label": "Azure Tenant ID",
                 "placeholder": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"},
            ],
            "credentialType": "ServicePrincipal",
        },
        {
            "type": "OData", "label": "Business Central OData V4",
            "protocol": "odata", "recommended": False,
            "description": "OData V4 connector for Business Central entities.",
            "fields": [
                {"name": "url",      "label": "OData URL",
                 "placeholder": "https://api.businesscentral.dynamics.com/v2.0/{tenant}/api/v2.0"},
                {"name": "tenantId", "label": "Azure Tenant ID",
                 "placeholder": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"},
            ],
            "credentialType": "ServicePrincipal",
        },
    ],
    "netsuite": [
        {
            "type": "RestService", "label": "NetSuite REST API (TBA)",
            "protocol": "rest", "recommended": True,
            "description": "NetSuite REST API with Token-Based Authentication — best for SuiteQL.",
            "fields": [
                {"name": "accountId", "label": "Account ID",    "placeholder": "1234567"},
                {"name": "baseUrl",   "label": "REST Base URL",
                 "placeholder": "https://1234567.suitetalk.api.netsuite.com"},
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
                {"name": "tenantUrl",  "label": "Tenant URL",
                 "placeholder": "https://wd3.myworkday.com"},
            ],
            "credentialType": "Basic",
        },
        {
            "type": "RestService", "label": "Workday REST API",
            "protocol": "rest", "recommended": False,
            "description": "Workday REST API for programmatic access to HCM and financial data.",
            "fields": [
                {"name": "tenantName", "label": "Tenant Name",  "placeholder": "mycompany"},
                {"name": "serviceUrl", "label": "Service URL",
                 "placeholder": "https://wd3-services1.workday.com/ccx/service"},
            ],
            "credentialType": "Basic",
        },
    ],
}


# ── Internal helpers ───────────────────────────────────────────────────────────
def _get_token() -> str:
    token = _ms_token.get("access_token", "")
    if not token:
        raise HTTPException(401, "MS365 token not active. Set it via /api/fabric/set-token.")
    return token


async def _fabric_get(token: str, url: str) -> dict:
    """GET to Fabric REST API — raises HTTPException on non-200."""
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, headers={"Authorization": f"Bearer {token}"})
        if r.status_code != 200:
            raise HTTPException(r.status_code, f"Fabric API {r.status_code}: {r.text[:300]}")
        return r.json() if r.text else {}


async def _poll_lro(token: str, location: str, fallback_id: str,
                    max_wait: int = 90) -> dict:
    """
    Poll a Fabric Long-Running Operation (LRO) until succeeded, failed,
    or max_wait seconds have elapsed.
    Fabric LRO responses have: { "status": "Running|Succeeded|Failed", "result": {...} }
    """
    async with httpx.AsyncClient(timeout=20) as client:
        elapsed, delay = 0, 2
        while elapsed < max_wait:
            await asyncio.sleep(delay)
            elapsed += delay
            delay = min(delay * 1.5, 10)
            r = await client.get(location, headers={"Authorization": f"Bearer {token}"})
            if r.status_code == 200:
                data  = r.json()
                state = data.get("status", "").lower()
                if state == "succeeded":
                    result = data.get("result", {})
                    return result if result else {"id": fallback_id, "status": "succeeded"}
                if state in ("failed", "cancelled"):
                    err = data.get("error", {})
                    raise HTTPException(
                        500, f"Fabric LRO {state}: {err.get('message', 'unknown error')}")
                # state == "running" | "notstarted" → keep polling
    return {"id": fallback_id, "status": "pending",
            "note": "LRO still running — check the Fabric workspace manually"}


async def _fabric_post(token: str, url: str, payload: dict) -> dict:
    """
    POST to Fabric API.
      · 200/201 → parse JSON body
      · 202     → Long-Running Operation; poll Location header until done
    """
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            url, json=payload,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        )
        if r.status_code not in (200, 201, 202):
            raise HTTPException(r.status_code, f"Fabric API {r.status_code}: {r.text[:300]}")

        if r.status_code == 202:
            location = r.headers.get("Location", "")
            op_id    = r.headers.get("x-ms-operation-id", str(uuid.uuid4()))
            if location:
                return await _poll_lro(token, location, op_id)
            return {"id": op_id, "status": "accepted"}

        return r.json() if r.text else {}


# ── Notebook / pipeline cell helpers ──────────────────────────────────────────
def _make_notebook_b64(name: str, cells: list[str]) -> str:
    nb = {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {"display_name": "PySpark", "language": "python",
                           "name": "synapse_pyspark"},
            "language_info": {"name": "python", "version": "3.11.0"},
        },
        "cells": [
            {"cell_type": "code", "execution_count": None,
             "metadata": {}, "outputs": [], "source": c}
            for c in cells
        ],
    }
    return base64.b64encode(json.dumps(nb).encode()).decode()


def _bronze_cells(source: str, module: str, tables: list[str]) -> list[str]:
    return [
        f"# Bronze Layer — {source} {module} raw extraction\n"
        "from pyspark.sql import SparkSession\n"
        "spark = SparkSession.builder.appName('Bronze_Extract').getOrCreate()\n"
        "JDBC = f\"jdbc:oracle:thin:@{{dbutils.secrets.get('erp','host')}}:"
        "1521/{{dbutils.secrets.get('erp','svc')}}\"\n"
        "OPTS = {'url': JDBC, 'user': dbutils.secrets.get('erp','user'), "
        "'password': dbutils.secrets.get('erp','pwd'), 'driver': 'oracle.jdbc.OracleDriver'}"
    ] + [
        f"# Extract {t}\n"
        f"df_{t.lower()} = spark.read.format('jdbc').options(**OPTS, dbtable='{t}').load()\n"
        f"df_{t.lower()}.write.format('delta').mode('overwrite')"
        f".option('overwriteSchema','true').saveAsTable('bronze.{t.lower()}')\n"
        f"print('bronze.{t.lower()} written')"
        for t in (tables or ["HEADER_TABLE", "LINE_TABLE"])
    ]


def _silver_cells(source: str, module: str) -> list[str]:
    return [
        f"# Silver Layer — {source} {module} cleanse & conform\n"
        "from pyspark.sql import SparkSession, functions as F\n"
        "spark = SparkSession.builder.appName('Silver_Transform').getOrCreate()\n\n"
        "bronze_tables = spark.catalog.listTables('bronze')\n"
        "for t in bronze_tables:\n"
        "    df = spark.read.table(f'bronze.{t.name}')\n"
        "    df_clean = df.dropDuplicates().na.fill('')\n"
        "    df_clean.write.format('delta').mode('overwrite')"
        ".option('overwriteSchema','true').saveAsTable(f'silver.{t.name}')\n"
        "    print(f'silver.{t.name} written')"
    ]


def _gold_cells(source: str, module: str) -> list[str]:
    return [
        f"# Gold Layer — {source} {module} business KPIs\n"
        "from pyspark.sql import SparkSession, functions as F\n"
        "spark = SparkSession.builder.appName('Gold_Aggregation').getOrCreate()\n\n"
        f"# Build {module} summary metrics\n"
        f"# TODO: Add aggregations specific to {module} business requirements\n"
        "print('Gold layer transformation complete')"
    ]


# ══════════════════════════════════════════════════════════════════════════════
# Endpoints
# ══════════════════════════════════════════════════════════════════════════════

# ── Connection types ───────────────────────────────────────────────────────────
@router.get("/connection-types")
async def get_connection_types(source: str = Query(...)):
    types = ERP_FABRIC_CONNECTION_TYPES.get(source, [
        {
            "type": "RestService", "label": "REST API", "protocol": "rest",
            "recommended": True, "description": "Generic REST connector.",
            "fields": [{"name": "url", "label": "Base URL",
                        "placeholder": "https://api.example.com"}],
            "credentialType": "Basic",
        }
    ])
    return {"source": source, "connection_types": types, "total": len(types)}


# ── Token management ───────────────────────────────────────────────────────────
@router.post("/set-token")
async def set_ms_token(payload: dict):
    """Store the Microsoft 365 Bearer token for subsequent Fabric API calls."""
    token = payload.get("access_token", "")
    if not token:
        raise HTTPException(400, "access_token required")
    _ms_token["access_token"] = token
    return {"status": "ok", "message": "MS365 token stored — live Fabric calls enabled."}


@router.get("/validate-token")
async def validate_token():
    """
    Validate the stored MS365 token by calling GET /workspaces.
    Returns valid flag, workspace count and first 5 workspace names.
    """
    try:
        token = _get_token()
    except HTTPException:
        return {
            "valid": False, "token_set": False,
            "message": "No token configured. Paste a Bearer token in Settings.",
        }
    try:
        data       = await _fabric_get(token, f"{FABRIC_API}/workspaces")
        workspaces = data.get("value", [])
        return {
            "valid":           True,
            "token_set":       True,
            "workspace_count": len(workspaces),
            "workspaces": [
                {"id": w["id"], "name": w.get("displayName", "")}
                for w in workspaces[:10]
            ],
            "message": f"Token valid — {len(workspaces)} workspace(s) accessible",
        }
    except HTTPException as exc:
        return {
            "valid":     False,
            "token_set": True,
            "message":   f"Token rejected by Fabric API: {exc.detail}",
        }


# ── Workspace & item discovery ─────────────────────────────────────────────────
@router.get("/workspaces")
async def list_workspaces():
    """List all Fabric workspaces the current token has access to."""
    token = _get_token()
    data  = await _fabric_get(token, f"{FABRIC_API}/workspaces")
    return {
        "workspaces": [
            {
                "id":          w["id"],
                "name":        w.get("displayName", ""),
                "type":        w.get("type", "Workspace"),
                "capacity_id": w.get("capacityId", ""),
            }
            for w in data.get("value", [])
        ],
        "total": len(data.get("value", [])),
    }


@router.get("/workspaces/{workspace_id}/lakehouses")
async def list_lakehouses(workspace_id: str):
    """List Lakehouse items inside a specific Fabric workspace."""
    token = _get_token()
    data  = await _fabric_get(
        token, f"{FABRIC_API}/workspaces/{workspace_id}/items?type=Lakehouse"
    )
    items = data.get("value", [])
    return {
        "workspace_id": workspace_id,
        "lakehouses":   [{"id": i["id"], "name": i.get("displayName", "")} for i in items],
        "total":        len(items),
    }


@router.get("/workspaces/{workspace_id}/items")
async def list_workspace_items(workspace_id: str, type: Optional[str] = None):
    """
    List items in a workspace.
    Use ?type=Notebook, ?type=DataPipeline, ?type=Lakehouse to filter.
    """
    token = _get_token()
    url   = f"{FABRIC_API}/workspaces/{workspace_id}/items"
    if type:
        url += f"?type={type}"
    data = await _fabric_get(token, url)
    return {
        "workspace_id": workspace_id,
        "items": [
            {
                "id":   i["id"],
                "name": i.get("displayName", ""),
                "type": i.get("type", ""),
            }
            for i in data.get("value", [])
        ],
        "total": len(data.get("value", [])),
    }


# ── Connection management ─────────────────────────────────────────────────────
@router.get("/connections")
async def list_connections():
    """List all Fabric shareable connections the token can access."""
    token = _get_token()
    data  = await _fabric_get(token, f"{FABRIC_API}/connections")
    return {
        "connections": [
            {
                "id":               c["id"],
                "name":             c.get("displayName", ""),
                "type":             c.get("connectionDetails", {}).get("type", ""),
                "connectivity_type": c.get("connectivityType", ""),
            }
            for c in data.get("value", [])
        ],
        "total": len(data.get("value", [])),
    }


@router.get("/connections/{connection_id}")
async def get_connection(connection_id: str):
    """
    Verify a Fabric connection exists and return its details.
    Returns 404 if the connection_id does not exist in Fabric.
    """
    token = _get_token()
    data  = await _fabric_get(token, f"{FABRIC_API}/connections/{connection_id}")
    return {
        "id":               data.get("id", connection_id),
        "name":             data.get("displayName", ""),
        "type":             data.get("connectionDetails", {}).get("type", ""),
        "connectivity_type": data.get("connectivityType", ""),
        "privacy_level":    data.get("privacyLevel", ""),
        "status":           "exists",
    }


# ── Create connection ─────────────────────────────────────────────────────────
class CreateConnectionRequest(BaseModel):
    workspace_id:      str
    display_name:      str
    source_type:       str
    connection_type:   str
    protocol:          str = ""
    connection_fields: dict = {}
    credential_type:   str = "Basic"    # Basic | ServicePrincipal | TBA
    username:          str = ""
    password:          str = ""
    client_secret:     str = ""
    privacy_level:     str = "Organizational"


@router.post("/create-connection")
async def create_connection(req: CreateConnectionRequest):
    """
    Create a Fabric shareable cloud connection for an ERP source.

    Payload conforms to the Fabric REST API spec:
      POST /v1/connections
      credentialDetails.credentials (nested, not flat)

    Falls back to simulated response when the MS365 token is not set.
    """
    conn_id = str(uuid.uuid4())
    try:
        token  = _get_token()
        params = [{"name": k, "value": v}
                  for k, v in req.connection_fields.items() if v]

        # ── Build credentials per Fabric API spec ──────────────────────────────
        if req.credential_type == "ServicePrincipal":
            credentials = {
                "credentialType": "ServicePrincipal",
                "tenantId":       req.connection_fields.get("tenantId", ""),
                "clientId":       req.username,
                "clientSecret":   req.client_secret,
            }
        elif req.credential_type == "TBA":
            credentials = {"credentialType": "Anonymous"}
        else:
            credentials = {
                "credentialType": "Basic",
                "username":       req.username,
                "password":       req.password,
            }

        payload = {
            "connectivityType": "ShareableCloud",
            "displayName":       req.display_name,
            "connectionDetails": {
                "type":       req.connection_type,
                "parameters": params,
            },
            "privacyLevel": req.privacy_level,
            # Fabric API requires nested credentialDetails.credentials
            "credentialDetails": {
                "singleSignOnType":     "None",
                "connectionEncryption": "NotEncrypted",
                "skipTestConnection":   False,
                "credentials":          credentials,
            },
        }

        result         = await _fabric_post(token, f"{FABRIC_API}/connections", payload)
        fabric_conn_id = result.get("id", conn_id)
        return {
            "status":          "created",
            "live":            True,
            "connection_id":   fabric_conn_id,
            "display_name":    req.display_name,
            "connection_type": req.connection_type,
            "workspace_id":    req.workspace_id,
            "verify_url":      f"/api/fabric/connections/{fabric_conn_id}",
        }

    except HTTPException:
        # Token not set → simulate so the UI wizard can still be demo'd
        return {
            "status":          "simulated",
            "live":            False,
            "connection_id":   conn_id,
            "display_name":    req.display_name,
            "connection_type": req.connection_type,
            "workspace_id":    req.workspace_id,
            "note":            "MS365 token not active — connection simulated. "
                               "Set a token in Settings for live deployment.",
        }


# ── Deploy ─────────────────────────────────────────────────────────────────────
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
      1. Creates Bronze extraction notebook (raw SQL → Delta)
      2. Creates Silver cleanse notebook
      3. Creates Gold KPI notebook
      4. Creates a Data Pipeline chaining Bronze → Silver → Gold

    Uses LRO polling for async 202 responses.
    Simulates when MS365 token is not set.
    """
    try:
        token = _get_token()
        live  = True
    except HTTPException:
        token = None
        live  = False

    ws     = req.workspace_id
    prefix = f"{req.source_type}_{req.module}"
    tables = req.selected_tables or ["HEADER_TABLE", "LINE_TABLE", "TABLE_C"]

    artifacts: list[dict] = []
    nb_ids:    dict[str, str] = {}

    async def _make(name: str, item_type: str, definition: dict) -> dict:
        if live:
            try:
                result  = await _fabric_post(
                    token,
                    f"{FABRIC_API}/workspaces/{ws}/items",
                    {"displayName": name, "type": item_type, "definition": definition},
                )
                item_id = result.get("id", str(uuid.uuid4()))
                return {
                    "name":       name,
                    "type":       item_type,
                    "id":         item_id,
                    "status":     "created",
                    "live":       True,
                    "verify_url": f"/api/fabric/workspaces/{ws}/items",
                }
            except Exception as exc:
                return {
                    "name":   name, "type": item_type,
                    "id":     str(uuid.uuid4()),
                    "status": "failed",
                    "error":  str(exc),
                }
        # Simulated
        return {
            "name":   name, "type": item_type,
            "id":     str(uuid.uuid4()),
            "status": "simulated",
            "live":   False,
        }

    # ── Notebooks ──────────────────────────────────────────────────────────────
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
            "parts": [{"path": "artifact.content.ipynb",
                       "payload": nb64, "payloadType": "InlineBase64"}],
        })
        artifacts.append(art)
        nb_ids[layer] = art["id"]

    # ── Data Pipeline ──────────────────────────────────────────────────────────
    if req.create_pipeline:
        activities, prev = [], None
        for layer in ("Bronze", "Silver", "Gold"):
            if layer not in nb_ids:
                continue
            act = {
                "name":       f"Run_{layer}",
                "type":       "TridentNotebook",
                "dependsOn":  ([{"activity": prev,
                                  "dependencyConditions": ["Succeeded"]}]
                               if prev else []),
                "typeProperties": {
                    "notebookId":  nb_ids[layer],
                    "workspaceId": ws,
                },
            }
            activities.append(act)
            prev = act["name"]

        pl_def = base64.b64encode(
            json.dumps({"properties": {"activities": activities}}).encode()
        ).decode()
        art = await _make(f"{prefix}_Pipeline", "DataPipeline", {
            "format": "json",
            "parts": [{"path": "pipeline-content.json",
                       "payload": pl_def, "payloadType": "InlineBase64"}],
        })
        artifacts.append(art)

    return {
        "live":         live,
        "total":        len(artifacts),
        "artifacts":    artifacts,
        "workspace_id": ws,
        "note": ("" if live
                 else "MS365 token not active — all artifacts simulated. "
                      "Set a token in Settings to deploy to real Fabric."),
    }
