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
from datetime import datetime
from pathlib import Path
import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional

router = APIRouter()

FABRIC_API = "https://api.fabric.microsoft.com/v1"

# In-memory token store (legacy — kept for /set-token backward compatibility)
_ms_token: dict = {}

# ── Persistent UI Connection Store ────────────────────────────────────────────
_STORE_FILE = Path(__file__).parent.parent.parent / "connections.json"


class _ConnectionStore:
    """
    Stores named Fabric connections in connections.json (persists across restarts).
    Each entry: { id, name, token, workspace_id, note, created_at, status }
    One connection can be marked 'active'; its token is used for all Fabric calls.
    """

    def __init__(self):
        self._data: dict = {"connections": [], "active_id": None}
        self._load()

    def _load(self):
        try:
            if _STORE_FILE.exists():
                self._data = json.loads(_STORE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass

    def _save(self):
        try:
            _STORE_FILE.write_text(
                json.dumps(self._data, indent=2), encoding="utf-8"
            )
        except Exception:
            pass

    # ── Accessors ──────────────────────────────────────────────────────────────
    def all(self) -> list:
        return self._data.get("connections", [])

    def get(self, cid: str) -> dict | None:
        return next((c for c in self.all() if c["id"] == cid), None)

    @property
    def active_id(self) -> str | None:
        return self._data.get("active_id")

    def active_token(self) -> str | None:
        """Return the token for the active connection, falling back to legacy _ms_token."""
        aid = self.active_id
        if aid:
            conn = self.get(aid)
            if conn and conn.get("token"):
                return conn["token"]
        return _ms_token.get("access_token") or None

    # ── Mutators ───────────────────────────────────────────────────────────────
    def add(self, name: str, token: str, workspace_id: str = "", note: str = "") -> dict:
        cid = str(uuid.uuid4())
        conn = {
            "id":           cid,
            "name":         name,
            "token":        token,
            "workspace_id": workspace_id,
            "note":         note,
            "created_at":   datetime.utcnow().isoformat(),
            "status":       "unknown",
        }
        self._data.setdefault("connections", []).append(conn)
        if len(self.all()) == 1:          # auto-activate first connection
            self._data["active_id"] = cid
        self._save()
        return conn

    def delete(self, cid: str):
        self._data["connections"] = [c for c in self.all() if c["id"] != cid]
        if self.active_id == cid:
            remaining = self.all()
            self._data["active_id"] = remaining[0]["id"] if remaining else None
        self._save()

    def activate(self, cid: str):
        if not self.get(cid):
            raise KeyError(cid)
        self._data["active_id"] = cid
        self._save()

    def set_status(self, cid: str, status: str):
        conn = self.get(cid)
        if conn:
            conn["status"] = status
            self._save()

    def update_workspace(self, cid: str, workspace_id: str):
        conn = self.get(cid)
        if conn:
            conn["workspace_id"] = workspace_id
            self._save()


_conn_store = _ConnectionStore()

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
    """
    Return the active Fabric Bearer token.
    Priority: active UI connection → legacy /set-token store.
    """
    token = _conn_store.active_token()
    if not token:
        raise HTTPException(
            401,
            "MS365 token not active — connection simulated. "
            "Add a Fabric connection in Settings for live deployment.",
        )
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


async def _fabric_update_definition(
    token: str, ws: str, endpoint: str, item_id: str, definition: dict
) -> dict:
    """
    POST .../updateDefinition for an existing Fabric Notebook or DataPipeline.
    Reuses _fabric_post() which already handles 200 (immediate) and 202 (LRO).
    """
    url = f"{FABRIC_API}/workspaces/{ws}/{endpoint}/{item_id}/updateDefinition"
    return await _fabric_post(token, url, {"definition": definition})


async def _list_existing_items(token: str, ws: str, *types: str) -> dict[str, str]:
    """
    Returns {displayName: itemId} for all workspace items matching the given
    types (e.g. "Notebook", "DataPipeline").
    Non-fatal: silently returns {} on any error so deploy is never aborted by
    a failed list call — _make() will fall back to always-create mode.
    """
    result: dict[str, str] = {}
    try:
        for t in types:
            url  = f"{FABRIC_API}/workspaces/{ws}/items?type={t}"
            data = await _fabric_get(token, url)
            for item in data.get("value", []):
                name = item.get("displayName", "")
                iid  = item.get("id", "")
                if name and iid:
                    result[name] = iid
    except Exception:
        pass  # non-fatal — caller falls back to create
    return result


# ── Notebook / pipeline cell helpers ──────────────────────────────────────────
# Fabric kernel specs by type
_KERNEL_SPECS: dict[str, dict] = {
    "pyspark": {
        "kernelspec":  {"display_name": "PySpark", "language": "python", "name": "synapse_pyspark"},
        "language_info": {"name": "python", "version": "3.11.0"},
        "cell_type":   "code",
    },
    "sql": {
        "kernelspec":  {"display_name": "SparkSQL", "language": "sparksql", "name": "synapse_spark"},
        "language_info": {"name": "sparksql"},
        "cell_type":   "code",
    },
}

# Separator used when joining/splitting cells for preview editing
_CELL_SEP = "\n\n# ─── Next Cell ───\n\n"


def _make_notebook_b64(name: str, cells: list[str], kernel: str = "pyspark") -> str:
    """
    Build a valid Jupyter nbformat v4 notebook and return it as a base64 string.
    Fabric requires cell `source` to be a list of strings (one per line, each
    ending with \\n except the last), not a single multi-line string.

    kernel: "pyspark" (default) or "sql"
    """
    spec = _KERNEL_SPECS.get(kernel, _KERNEL_SPECS["pyspark"])
    nb = {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {
            "kernelspec":    spec["kernelspec"],
            "language_info": spec["language_info"],
        },
        "cells": [
            {
                "cell_type": spec["cell_type"], "execution_count": None,
                "metadata": {}, "outputs": [],
                # Fabric nbformat parser expects source as list-of-lines
                "source": c.splitlines(keepends=True) if isinstance(c, str) else c,
            }
            for c in cells
        ],
    }
    return base64.b64encode(json.dumps(nb).encode()).decode()


def _code_to_cells(code: str) -> list[str]:
    """Split user-edited code (from frontend) back into individual cells."""
    return [c.strip() for c in code.split(_CELL_SEP.strip()) if c.strip()]


def _bronze_cells(
    source: str, module: str, tables: list[str],
    custom_sql: dict[str, str] | None = None,
) -> list[str]:
    """
    Generate PySpark cells for the Bronze notebook.

    custom_sql: optional dict of {table_name: SQL_query}.
      When provided for a table, the SQL is used as a JDBC subquery
      instead of reading the whole table: dbtable='(SELECT ...) t_alias'.

    Credentials resolution order (Fabric-compatible):
      1. notebookutils Key Vault  — production (uncomment KV section)
      2. Spark config             — passed by Data Pipeline → Parameters
      3. os.environ               — Docker env_file / VM environment
      4. Hardcoded placeholders   — fallback (replace before running)
    """
    if custom_sql is None:
        custom_sql = {}

    def _table_cell(t: str) -> str:
        sql = custom_sql.get(t)
        dbtable = f"({sql}) t_{t.lower()}" if sql else t
        note    = f"  # custom SQL override\n" if sql else "\n"
        return (
            f"# Extract {t}{note}"
            f"df_{t.lower()} = spark.read.format('jdbc').options(**OPTS, dbtable='{dbtable}').load()\n"
            f"df_{t.lower()}.write.format('delta').mode('overwrite')"
            f".option('overwriteSchema','true').saveAsTable('bronze.{t.lower()}')\n"
            f"print(f'bronze.{t.lower()} written \u2014 {{df_{t.lower()}.count()}} rows')"
        )

    return [
        # ── Cell 0: setup & credentials ─────────────────────────────────────
        f"# Bronze Layer — {source} {module} raw extraction\n"
        "# Microsoft Fabric Notebook — PySpark\n"
        "import os\n"
        "from pyspark.sql import SparkSession\n\n"
        "spark = SparkSession.builder.appName('Bronze_Extract').getOrCreate()\n\n"
        "# ── Option A: notebookutils Key Vault (recommended for production) ──\n"
        "# Uncomment the 5 lines below and set your Key Vault URL:\n"
        "# from notebookutils import mssparkutils\n"
        "# KV = 'https://YOUR-KEYVAULT.vault.azure.net/'\n"
        "# ORACLE_HOST = mssparkutils.credentials.getSecret(KV, 'oracle-host')\n"
        "# ORACLE_USER = mssparkutils.credentials.getSecret(KV, 'oracle-user')\n"
        "# ORACLE_PWD  = mssparkutils.credentials.getSecret(KV, 'oracle-pwd')\n\n"
        "# ── Option B: Pipeline parameters / environment variables (default) ─\n"
        "ORACLE_HOST = spark.conf.get('spark.oracle.host',    os.getenv('ORACLE_HOST', 'your-oracle-host'))\n"
        "ORACLE_PORT = spark.conf.get('spark.oracle.port',    os.getenv('ORACLE_PORT', '1521'))\n"
        "ORACLE_SVC  = spark.conf.get('spark.oracle.service', os.getenv('ORACLE_SVC',  'orcl'))\n"
        "ORACLE_USER = spark.conf.get('spark.oracle.user',    os.getenv('ORACLE_USER', ''))\n"
        "ORACLE_PWD  = spark.conf.get('spark.oracle.password',os.getenv('ORACLE_PWD',  ''))\n\n"
        "JDBC = f'jdbc:oracle:thin:@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SVC}'\n"
        "OPTS = {\n"
        "    'url':      JDBC,\n"
        "    'user':     ORACLE_USER,\n"
        "    'password': ORACLE_PWD,\n"
        "    'driver':   'oracle.jdbc.OracleDriver',\n"
        "}\n"
        "print(f'Bronze: Connecting to {ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SVC}')",
    ] + [
        _table_cell(t) for t in (tables or ["HEADER_TABLE", "LINE_TABLE"])
    ]


def _silver_cells(
    source: str, module: str,
    tables: list[str] | None = None,
    silver_sql: dict[str, str] | None = None,
) -> list[str]:
    """
    Generate PySpark cells for the Silver notebook.

    silver_sql: optional dict of {table_name: SQL_query}.
      When provided, emits spark.sql(query) instead of the default
      dropDuplicates/na.fill cleansing loop for that table.
    """
    if silver_sql is None:
        silver_sql = {}

    cells = [
        f"# Silver Layer — {source} {module} cleanse & conform\n"
        "from pyspark.sql import SparkSession, functions as F\n"
        "spark = SparkSession.builder.appName('Silver_Transform').getOrCreate()\n",
    ]

    if tables:
        for t in tables:
            sql = silver_sql.get(t)
            if sql:
                cells.append(
                    f"# Silver transform — {t} (custom SQL)\n"
                    f"df_{t.lower()} = spark.sql(\"\"\"\n{sql}\n\"\"\")\n"
                    f"df_{t.lower()}.write.format('delta').mode('overwrite')"
                    f".option('overwriteSchema','true').saveAsTable('silver.{t.lower()}')\n"
                    f"print('silver.{t.lower()} written')"
                )
            else:
                cells.append(
                    f"# Silver cleanse — {t} (default: dedupe + fill nulls)\n"
                    f"df_{t.lower()} = spark.read.table('bronze.{t.lower()}')\n"
                    f"df_{t.lower()}_clean = df_{t.lower()}.dropDuplicates().na.fill('')\n"
                    f"df_{t.lower()}_clean.write.format('delta').mode('overwrite')"
                    f".option('overwriteSchema','true').saveAsTable('silver.{t.lower()}')\n"
                    f"print('silver.{t.lower()} written')"
                )
    else:
        # Fallback: iterate all bronze tables dynamically
        cells.append(
            "bronze_tables = spark.catalog.listTables('bronze')\n"
            "for t in bronze_tables:\n"
            "    df = spark.read.table(f'bronze.{t.name}')\n"
            "    df_clean = df.dropDuplicates().na.fill('')\n"
            "    df_clean.write.format('delta').mode('overwrite')"
            ".option('overwriteSchema','true').saveAsTable(f'silver.{t.name}')\n"
            "    print(f'silver.{t.name} written')"
        )

    return cells


def _gold_cells(
    source: str, module: str,
    tables: list[str] | None = None,
    gold_sql: dict[str, str] | None = None,
) -> list[str]:
    """
    Generate PySpark cells for the Gold notebook.

    gold_sql: optional dict of {table_name: SQL_query}.
      When provided, emits a spark.sql(query) aggregation cell writing
      the result to gold.<table>. Otherwise emits a TODO stub cell.
    """
    if gold_sql is None:
        gold_sql = {}

    cells = [
        f"# Gold Layer — {source} {module} business KPIs\n"
        "from pyspark.sql import SparkSession, functions as F\n"
        "spark = SparkSession.builder.appName('Gold_Aggregation').getOrCreate()\n",
    ]

    custom_tables = [t for t in (tables or []) if t in gold_sql]
    stub_tables   = [t for t in (tables or []) if t not in gold_sql]

    for t in custom_tables:
        cells.append(
            f"# Gold KPI — {t} (custom SQL)\n"
            f"df_gold_{t.lower()} = spark.sql(\"\"\"\n{gold_sql[t]}\n\"\"\")\n"
            f"df_gold_{t.lower()}.write.format('delta').mode('overwrite')"
            f".option('overwriteSchema','true').saveAsTable('gold.{t.lower()}')\n"
            f"print('gold.{t.lower()} written')"
        )

    if stub_tables or not tables:
        table_list = stub_tables if stub_tables else ["<your_table>"]
        cells.append(
            f"# TODO: Add aggregations specific to {module} business requirements\n"
            f"# Tables without custom SQL: {table_list}\n"
            "# Example:\n"
            "# df_kpi = spark.sql(\"\"\"\n"
            "#   SELECT date_trunc('month', invoice_date) AS month,\n"
            "#          SUM(amount) AS total_revenue\n"
            "#   FROM silver.<table>\n"
            "#   GROUP BY 1\n"
            "# \"\"\")\n"
            "# df_kpi.write.format('delta').mode('overwrite').saveAsTable('gold.revenue_monthly')\n"
            "print('Gold layer transformation complete')"
        )

    return cells


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
    # Custom SQL per table per layer (table_name → SQL string)
    custom_sql:      dict[str, str] = {}   # Bronze: JDBC extraction queries
    silver_sql:      dict[str, str] = {}   # Silver: transform queries
    gold_sql:        dict[str, str] = {}   # Gold: aggregation/KPI queries
    # Notebook kernel per layer: "pyspark" (default) or "sql"
    notebook_types:  dict[str, str] = {}   # e.g. {"bronze": "sql", "silver": "pyspark"}
    # User-edited notebook code per layer — overrides auto-generated cells when set
    # Each value is cells joined by _CELL_SEP; empty = use auto-generated code
    custom_notebook_code: dict[str, str] = {}   # bronze/silver/gold → edited code string
    # User-edited pipeline JSON — overrides auto-generated pipeline when set
    custom_pipeline_json: str = ""


def _bronze_sql_cells(source: str, module: str, tables: list[str],
                      custom_sql: dict[str, str] | None = None) -> list[str]:
    """
    Generate SparkSQL cells for the Bronze notebook (SQL kernel variant).
    Uses %sql magic cells — reads from JDBC via CREATE TABLE AS SELECT.
    """
    if custom_sql is None:
        custom_sql = {}
    setup = (
        f"-- Bronze Layer — {source} {module} raw extraction (SparkSQL)\n"
        "-- Microsoft Fabric Notebook — SparkSQL\n"
        "-- NOTE: Configure your JDBC connection credentials via Spark config before running.\n"
        f"-- Tables: {', '.join(tables or ['HEADER_TABLE', 'LINE_TABLE'])}"
    )
    cells = [setup]
    for t in (tables or ["HEADER_TABLE", "LINE_TABLE"]):
        sql = custom_sql.get(t, f"SELECT * FROM {t}")
        cells.append(
            f"-- Extract {t} via JDBC\n"
            f"CREATE OR REPLACE TABLE bronze.{t.lower()} USING delta AS\n"
            f"{sql};"
        )
    return cells


def _silver_sql_cells(source: str, module: str, tables: list[str] | None = None,
                      silver_sql: dict[str, str] | None = None) -> list[str]:
    """Generate SparkSQL cells for the Silver notebook (SQL kernel variant)."""
    if silver_sql is None:
        silver_sql = {}
    cells = [f"-- Silver Layer — {source} {module} cleanse & conform (SparkSQL)"]
    for t in (tables or []):
        sql = silver_sql.get(t,
            f"SELECT DISTINCT *\nFROM bronze.{t.lower()}\nWHERE 1=1 -- add filters here")
        cells.append(
            f"-- Silver cleanse — {t}\n"
            f"CREATE OR REPLACE TABLE silver.{t.lower()} USING delta AS\n"
            f"{sql};"
        )
    if not tables:
        cells.append(
            "-- TODO: Add silver transform queries\n"
            "-- Example:\n"
            "-- CREATE OR REPLACE TABLE silver.my_table USING delta AS\n"
            "-- SELECT DISTINCT * FROM bronze.my_table WHERE 1=1;"
        )
    return cells


def _gold_sql_cells(source: str, module: str, tables: list[str] | None = None,
                    gold_sql: dict[str, str] | None = None) -> list[str]:
    """Generate SparkSQL cells for the Gold notebook (SQL kernel variant)."""
    if gold_sql is None:
        gold_sql = {}
    cells = [f"-- Gold Layer — {source} {module} business KPIs (SparkSQL)"]
    for t in (tables or []):
        if t in gold_sql:
            cells.append(
                f"-- Gold KPI — {t} (custom SQL)\n"
                f"CREATE OR REPLACE TABLE gold.{t.lower()} USING delta AS\n"
                f"{gold_sql[t]};"
            )
    stub_tables = [t for t in (tables or []) if t not in (gold_sql or {})]
    if stub_tables or not tables:
        cells.append(
            f"-- TODO: Add Gold aggregation queries for {module}\n"
            "-- Example:\n"
            "-- CREATE OR REPLACE TABLE gold.revenue_monthly USING delta AS\n"
            "-- SELECT date_trunc('month', invoice_date) AS month,\n"
            "--        SUM(amount) AS total_revenue\n"
            f"-- FROM silver.{(stub_tables or ['<table>'])[0].lower()}\n"
            "-- GROUP BY 1;"
        )
    return cells


class PreviewNotebooksRequest(BaseModel):
    source_type:    str
    module:         str
    selected_tables: list[str] = []
    create_bronze:  bool = True
    create_silver:  bool = True
    create_gold:    bool = True
    create_pipeline: bool = True
    custom_sql:     dict[str, str] = {}
    silver_sql:     dict[str, str] = {}
    gold_sql:       dict[str, str] = {}
    notebook_types: dict[str, str] = {}   # bronze/silver/gold → "pyspark" or "sql"


@router.post("/preview-notebooks")
async def preview_notebooks(req: PreviewNotebooksRequest):
    """
    Return auto-generated notebook code strings (not base64) for frontend preview.
    Cells are joined by _CELL_SEP so the frontend can display and edit them.
    Also returns the pipeline JSON template.
    """
    tables = req.selected_tables or ["HEADER_TABLE", "LINE_TABLE", "TABLE_C"]
    result: dict[str, str] = {}

    def _get_cells(layer: str) -> list[str]:
        kernel = req.notebook_types.get(layer.lower(), "pyspark")
        if layer == "Bronze":
            if kernel == "sql":
                return _bronze_sql_cells(req.source_type, req.module, tables, req.custom_sql)
            return _bronze_cells(req.source_type, req.module, tables, req.custom_sql)
        if layer == "Silver":
            if kernel == "sql":
                return _silver_sql_cells(req.source_type, req.module, tables, req.silver_sql)
            return _silver_cells(req.source_type, req.module, tables, req.silver_sql)
        if layer == "Gold":
            if kernel == "sql":
                return _gold_sql_cells(req.source_type, req.module, tables, req.gold_sql)
            return _gold_cells(req.source_type, req.module, tables, req.gold_sql)
        return []

    if req.create_bronze:
        result["bronze"] = _CELL_SEP.join(_get_cells("Bronze"))
    if req.create_silver:
        result["silver"] = _CELL_SEP.join(_get_cells("Silver"))
    if req.create_gold:
        result["gold"]   = _CELL_SEP.join(_get_cells("Gold"))

    if req.create_pipeline:
        # Return a preview of the pipeline JSON with placeholder notebook IDs
        prefix = f"{req.source_type}_{req.module}"
        activities, prev = [], None
        for layer in ("Bronze", "Silver", "Gold"):
            if not getattr(req, f"create_{layer.lower()}", False):
                continue
            act_name = f"Run_{layer}"
            act = {
                "name":      act_name,
                "type":      "TridentNotebook",
                "dependsOn": ([{"activity": prev,
                                "dependencyConditions": ["Succeeded"]}] if prev else []),
                "policy": {
                    "timeout":              "0.12:00:00",
                    "retry":                0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput":         False,
                    "secureInput":          False,
                },
                "typeProperties": {
                    "notebookId":  f"<{layer.lower()}-notebook-id>",
                    # Use zero GUID for same-workspace notebooks per MS docs
                    "workspaceId": "00000000-0000-0000-0000-000000000000",
                },
            }
            activities.append(act)
            prev = act_name
        result["pipeline"] = json.dumps(
            {"properties": {"activities": activities}}, indent=2)

    return {"layers": result, "cell_separator": _CELL_SEP.strip()}


# Type-specific Fabric API endpoints per official MS docs
# https://learn.microsoft.com/en-us/rest/api/fabric/notebook/items/create-notebook
# https://learn.microsoft.com/en-us/rest/api/fabric/datapipeline/items/create-data-pipeline
_FABRIC_ITEM_ENDPOINTS: dict[str, str] = {
    "Notebook":     "notebooks",
    "DataPipeline": "dataPipelines",
}


@router.post("/deploy")
async def deploy(req: DeployRequest):
    """
    Full ERP → Fabric deployment:
      1. Creates Bronze extraction notebook (raw SQL → Delta)
      2. Creates Silver cleanse notebook
      3. Creates Gold KPI notebook
      4. Creates a Data Pipeline chaining Bronze → Silver → Gold

    Uses type-specific Fabric API endpoints (confirmed per MS docs).
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

    # Pre-fetch existing workspace items so _make() can upsert (update if
    # found, create if not). Non-fatal: {} means always-create fallback.
    existing_ids: dict[str, str] = {}
    if live:
        existing_ids = await _list_existing_items(
            token, ws, "Notebook", "DataPipeline"
        )

    async def _make(name: str, item_type: str, definition: dict) -> dict:
        if live:
            try:
                endpoint = _FABRIC_ITEM_ENDPOINTS.get(item_type, "items")

                if name in existing_ids:
                    # ── UPDATE existing item ──────────────────────────────────
                    item_id = existing_ids[name]
                    result  = await _fabric_update_definition(
                        token, ws, endpoint, item_id, definition
                    )
                    if result.get("status") == "pending":
                        return {
                            "name":   name, "type": item_type, "id": item_id,
                            "status": "pending", "live": True,
                            "note":   result.get("note", "LRO still running — check Fabric workspace"),
                        }
                    return {
                        "name":       name, "type": item_type, "id": item_id,
                        "status":     "updated", "live": True,
                        "verify_url": f"/api/fabric/workspaces/{ws}/items",
                    }

                else:
                    # ── CREATE new item ───────────────────────────────────────
                    url  = f"{FABRIC_API}/workspaces/{ws}/{endpoint}"
                    body: dict = {"displayName": name, "definition": definition}
                    if endpoint == "items":
                        body["type"] = item_type  # generic /items needs type
                    result = await _fabric_post(token, url, body)

                    if result.get("status") == "pending":
                        return {
                            "name":   name, "type": item_type,
                            "id":     result.get("id", str(uuid.uuid4())),
                            "status": "pending", "live": True,
                            "note":   result.get("note", "LRO still running — check Fabric workspace"),
                        }
                    item_id = result.get("id", str(uuid.uuid4()))
                    return {
                        "name":       name, "type": item_type, "id": item_id,
                        "status":     "created", "live": True,
                        "verify_url": f"/api/fabric/workspaces/{ws}/items",
                    }

            except HTTPException as exc:
                detail = exc.detail if hasattr(exc, "detail") else str(exc)
                return {
                    "name":        name, "type": item_type,
                    "id":          str(uuid.uuid4()),
                    "status":      "failed",
                    "http_status": exc.status_code,
                    "error":       detail,
                }
            except Exception as exc:
                return {
                    "name":   name, "type": item_type,
                    "id":     str(uuid.uuid4()),
                    "status": "failed",
                    "error":  str(exc),
                }
        # Simulated (no active token)
        return {
            "name":   name, "type": item_type,
            "id":     str(uuid.uuid4()),
            "status": "simulated",
            "live":   False,
        }

    try:
        # ── Notebooks ──────────────────────────────────────────────────────────
        def _build_cells(layer: str) -> list[str]:
            """Return cells — from user-edited code if provided, else auto-generated."""
            key    = layer.lower()
            kernel = req.notebook_types.get(key, "pyspark")
            # User-edited code overrides auto-generated cells
            custom_code = req.custom_notebook_code.get(key, "").strip()
            if custom_code:
                return _code_to_cells(custom_code)
            if layer == "Bronze":
                return (
                    _bronze_sql_cells(req.source_type, req.module, tables, req.custom_sql)
                    if kernel == "sql" else
                    _bronze_cells(req.source_type, req.module, tables, req.custom_sql)
                )
            if layer == "Silver":
                return (
                    _silver_sql_cells(req.source_type, req.module, tables, req.silver_sql)
                    if kernel == "sql" else
                    _silver_cells(req.source_type, req.module, tables, req.silver_sql)
                )
            if layer == "Gold":
                return (
                    _gold_sql_cells(req.source_type, req.module, tables, req.gold_sql)
                    if kernel == "sql" else
                    _gold_cells(req.source_type, req.module, tables, req.gold_sql)
                )
            return []

        layers = []
        if req.create_bronze: layers.append("Bronze")
        if req.create_silver: layers.append("Silver")
        if req.create_gold:   layers.append("Gold")

        for layer in layers:
            kernel  = req.notebook_types.get(layer.lower(), "pyspark")
            cells   = _build_cells(layer)
            nb_name = f"{prefix}_{layer}_Notebook"
            nb64    = _make_notebook_b64(nb_name, cells, kernel=kernel)
            art     = await _make(nb_name, "Notebook", {
                # Correct path per MS Fabric Notebook Items API docs
                "format": "ipynb",
                "parts": [{"path": "notebook-content.ipynb",
                           "payload": nb64, "payloadType": "InlineBase64"}],
            })
            artifacts.append(art)
            # Register notebook ID so the pipeline can reference it.
            # Covers created (first deploy) and updated (re-deploy) — the pipeline
            # should orchestrate exactly the layers the user selected this run.
            # Also covers simulated (no token) and pending (LRO still running).
            if art["status"] in ("created", "updated", "simulated", "pending"):
                nb_ids[layer] = art["id"]

        # ── Resolve actual Fabric notebook item IDs before building pipeline ────
        # On a first deploy the notebook create LRO may not return the actual
        # item.id in its result body — only an operation/fallback ID.  Using a
        # wrong ID in the pipeline's notebookId field causes a Fabric 400 error.
        # After all notebooks have been created/updated, re-list the workspace to
        # fetch the real item IDs by display name and overwrite nb_ids so the
        # pipeline always references valid, existing Fabric notebook item IDs.
        if live and nb_ids:
            try:
                fresh_nb_ids = await _list_existing_items(token, ws, "Notebook")
                for layer in list(nb_ids.keys()):
                    nb_name = f"{prefix}_{layer}_Notebook"
                    if nb_name in fresh_nb_ids:
                        nb_ids[layer] = fresh_nb_ids[nb_name]
            except Exception:
                pass  # non-fatal — fall back to IDs already in nb_ids

        # nb_ids now contains ONLY the user-selected layers for this deploy run,
        # with real Fabric item IDs confirmed from the workspace.
        # The pipeline activities below chain them in Bronze → Silver → Gold order.

        # ── Data Pipeline ──────────────────────────────────────────────────────
        if req.create_pipeline:
            # Use user-edited pipeline JSON if provided, else auto-generate
            custom_pl = req.custom_pipeline_json.strip()
            if custom_pl:
                try:
                    pl_content = json.loads(custom_pl)
                except json.JSONDecodeError as e:
                    raise HTTPException(400, f"Invalid pipeline JSON: {e}") from e
            else:
                activities, prev = [], None
                for layer in ("Bronze", "Silver", "Gold"):
                    if layer not in nb_ids:
                        continue
                    act = {
                        "name":      f"Run_{layer}",
                        "type":      "TridentNotebook",
                        "dependsOn": ([{"activity": prev,
                                        "dependencyConditions": ["Succeeded"]}]
                                      if prev else []),
                        # policy is required per Fabric DataPipeline activity spec
                        "policy": {
                            "timeout":              "0.12:00:00",
                            "retry":                0,
                            "retryIntervalInSeconds": 30,
                            "secureOutput":         False,
                            "secureInput":          False,
                        },
                        "typeProperties": {
                            "notebookId": nb_ids[layer],
                            # Per MS docs: use zero GUID when notebook is in the
                            # same workspace as the pipeline
                            "workspaceId": "00000000-0000-0000-0000-000000000000",
                        },
                    }
                    activities.append(act)
                    prev = act["name"]
                pl_content = {"properties": {"activities": activities}}

            pl_name = f"{prefix}_Pipeline"
            pl_def = base64.b64encode(json.dumps(pl_content).encode()).decode()

            # .platform metadata part — required alongside pipeline-content.json
            platform_meta = {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
                "metadata": {
                    "type":        "DataPipeline",
                    "displayName": pl_name,
                },
                "config": {
                    "version":   "2.0",
                    "logicalId": "00000000-0000-0000-0000-000000000000",
                },
            }
            pl_platform = base64.b64encode(
                json.dumps(platform_meta).encode()
            ).decode()

            art = await _make(pl_name, "DataPipeline", {
                "parts": [
                    {"path": "pipeline-content.json",
                     "payload": pl_def,      "payloadType": "InlineBase64"},
                    {"path": ".platform",
                     "payload": pl_platform, "payloadType": "InlineBase64"},
                ],
            })
            artifacts.append(art)

        return {
            "live":         live,
            "total":        len(artifacts),
            "artifacts":    artifacts,
            "workspace_id": ws,
            "note": ("" if live
                     else "No active Fabric connection — all artifacts simulated. "
                          "Add a connection in Settings → Fabric Connections."),
        }

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, f"Deployment error: {exc}") from exc


# ══════════════════════════════════════════════════════════════════════════════
# UI-managed Fabric Connections  (persisted to connections.json)
# ══════════════════════════════════════════════════════════════════════════════

def _mask(token: str) -> str:
    """Return last-8 chars preceded by bullets, safe for display."""
    if not token:
        return ""
    suffix = token[-8:] if len(token) >= 8 else token
    return "•" * 20 + suffix


class SaveConnectionRequest(BaseModel):
    name:         str
    token:        str
    workspace_id: str = ""
    note:         str = ""


@router.get("/fabric-connections")
async def list_fabric_connections():
    """Return all UI-saved connections (tokens masked) + which is active."""
    conns = _conn_store.all()
    aid   = _conn_store.active_id
    return {
        "connections": [
            {
                **{k: v for k, v in c.items() if k != "token"},
                "token_masked": _mask(c.get("token", "")),
                "active":       c["id"] == aid,
            }
            for c in conns
        ],
        "active_id": aid,
        "count":     len(conns),
    }


@router.post("/fabric-connections")
async def save_fabric_connection(req: SaveConnectionRequest):
    """Add a new named Fabric connection and auto-test it."""
    if not req.name or not req.token:
        raise HTTPException(400, "name and token are required")
    conn = _conn_store.add(req.name, req.token, req.workspace_id, req.note)
    # Quick test
    status = "unknown"
    workspaces: list = []
    try:
        data       = await _fabric_get(req.token, f"{FABRIC_API}/workspaces")
        workspaces = data.get("value", [])
        status     = "valid"
        _conn_store.set_status(conn["id"], "valid")
        if req.workspace_id == "" and workspaces:
            # auto-fill workspace_id from first workspace if not provided
            pass
    except Exception:
        status = "invalid"
        _conn_store.set_status(conn["id"], "invalid")

    return {
        "status":      "saved",
        "id":          conn["id"],
        "name":        conn["name"],
        "test_result": status,
        "workspaces":  [{"id": w["id"], "name": w.get("displayName", "")}
                        for w in workspaces[:10]],
    }


@router.delete("/fabric-connections/{cid}")
async def delete_fabric_connection(cid: str):
    """Delete a saved connection by ID."""
    if not _conn_store.get(cid):
        raise HTTPException(404, f"Connection {cid} not found")
    _conn_store.delete(cid)
    return {"status": "deleted", "id": cid}


@router.post("/fabric-connections/{cid}/activate")
async def activate_fabric_connection(cid: str):
    """Set a connection as the active one used for all Fabric API calls."""
    try:
        _conn_store.activate(cid)
    except KeyError:
        raise HTTPException(404, f"Connection {cid} not found")
    conn = _conn_store.get(cid)
    return {"status": "activated", "id": cid, "name": conn["name"] if conn else ""}


@router.post("/fabric-connections/{cid}/test")
async def test_fabric_connection(cid: str):
    """Test a saved connection by calling GET /workspaces with its token."""
    conn = _conn_store.get(cid)
    if not conn:
        raise HTTPException(404, f"Connection {cid} not found")
    try:
        data       = await _fabric_get(conn["token"], f"{FABRIC_API}/workspaces")
        workspaces = data.get("value", [])
        _conn_store.set_status(cid, "valid")
        return {
            "status":          "valid",
            "workspace_count": len(workspaces),
            "workspaces": [
                {"id": w["id"], "name": w.get("displayName", "")}
                for w in workspaces[:10]
            ],
        }
    except HTTPException as exc:
        _conn_store.set_status(cid, "invalid")
        return {"status": "invalid", "error": exc.detail}
    except Exception as exc:
        _conn_store.set_status(cid, "invalid")
        return {"status": "invalid", "error": str(exc)}


@router.patch("/fabric-connections/{cid}")
async def update_fabric_connection(cid: str, body: dict):
    """Update name / workspace_id / note for a saved connection."""
    conn = _conn_store.get(cid)
    if not conn:
        raise HTTPException(404, f"Connection {cid} not found")
    if "name"         in body: conn["name"]         = body["name"]
    if "workspace_id" in body: conn["workspace_id"] = body["workspace_id"]
    if "note"         in body: conn["note"]         = body["note"]
    _conn_store._save()
    return {"status": "updated", "id": cid}


# ══════════════════════════════════════════════════════════════════════════════
# OAuth2 / Auth method endpoints
# Supports: Bearer Token (direct), Service Principal, Device Code,
#           Managed Identity, Username/Password (ROPC)
# ══════════════════════════════════════════════════════════════════════════════

_AAD_TOKEN    = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
_AAD_DEVICE   = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/devicecode"
_FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"

# In-memory device-code session store (keyed by session_id, expires with flow)
_device_sessions: dict = {}


async def _save_and_test(name: str, token: str, workspace_id: str,
                         note: str, auth_method: str, expires_in: int) -> dict:
    """Save connection, test it against Fabric API, return uniform response dict."""
    conn = _conn_store.add(name, token, workspace_id, note)
    status, workspaces = "unknown", []
    try:
        data       = await _fabric_get(token, f"{FABRIC_API}/workspaces")
        workspaces = data.get("value", [])
        status     = "valid"
        _conn_store.set_status(conn["id"], "valid")
    except Exception:
        status = "invalid"
        _conn_store.set_status(conn["id"], "invalid")
    return {
        "status":      "saved",
        "id":          conn["id"],
        "name":        conn["name"],
        "auth_method": auth_method,
        "expires_in":  expires_in,
        "test_result": status,
        "workspaces":  [{"id": w["id"], "name": w.get("displayName", "")}
                        for w in workspaces[:10]],
    }


# ── Service Principal (Client Credentials) ────────────────────────────────────
class ServicePrincipalRequest(BaseModel):
    name:          str
    tenant_id:     str
    client_id:     str
    client_secret: str
    workspace_id:  str = ""
    note:          str = ""


@router.post("/auth/service-principal")
async def auth_service_principal(req: ServicePrincipalRequest):
    """Obtain a Fabric Bearer token via Azure AD Service Principal (client credentials)."""
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            _AAD_TOKEN.format(tenant=req.tenant_id),
            data={
                "grant_type":    "client_credentials",
                "client_id":     req.client_id,
                "client_secret": req.client_secret,
                "scope":         _FABRIC_SCOPE,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    body = r.json()
    if r.status_code != 200 or "access_token" not in body:
        raise HTTPException(
            401, f"Azure AD rejected credentials: {body.get('error_description', r.text[:200])}"
        )
    return await _save_and_test(
        req.name, body["access_token"], req.workspace_id,
        req.note or f"Service Principal · {req.client_id[:8]}…",
        "service_principal", body.get("expires_in", 3600),
    )


# ── Device Code (OAuth2 interactive) ─────────────────────────────────────────
class DeviceCodeStartRequest(BaseModel):
    tenant_id:    str
    client_id:    str
    name:         str = "Device Code Connection"
    workspace_id: str = ""
    note:         str = ""


class DeviceCodePollRequest(BaseModel):
    session_id:   str
    name:         str = ""
    workspace_id: str = ""
    note:         str = ""


@router.post("/auth/device-code/start")
async def auth_device_code_start(req: DeviceCodeStartRequest):
    """
    Start OAuth2 Device Code flow.
    Returns user_code + verification_uri — user visits the URL and enters the code.
    """
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            _AAD_DEVICE.format(tenant=req.tenant_id),
            data={
                "client_id": req.client_id,
                "scope":     f"{_FABRIC_SCOPE} offline_access",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    body = r.json()
    if r.status_code != 200:
        raise HTTPException(
            400, f"Device code error: {body.get('error_description', r.text[:200])}"
        )
    session_id = str(uuid.uuid4())
    _device_sessions[session_id] = {
        "device_code":  body["device_code"],
        "client_id":    req.client_id,
        "tenant_id":    req.tenant_id,
        "name":         req.name,
        "workspace_id": req.workspace_id,
        "note":         req.note,
        "expires_at":   datetime.utcnow().timestamp() + body.get("expires_in", 900),
        "interval":     body.get("interval", 5),
    }
    return {
        "session_id":       session_id,
        "user_code":        body["user_code"],
        "verification_uri": body["verification_uri"],
        "expires_in":       body.get("expires_in", 900),
        "interval":         body.get("interval", 5),
        "message":          body.get("message",
            f"Open {body['verification_uri']} and enter code {body['user_code']}"),
    }


@router.post("/auth/device-code/poll")
async def auth_device_code_poll(req: DeviceCodePollRequest):
    """
    Poll for device code token.
    Returns status=pending while waiting, saves connection on success.
    """
    sess = _device_sessions.get(req.session_id)
    if not sess:
        raise HTTPException(404, "Session not found or expired. Start a new device code flow.")
    if datetime.utcnow().timestamp() > sess["expires_at"]:
        _device_sessions.pop(req.session_id, None)
        raise HTTPException(410, "Device code expired — start a new flow.")

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            _AAD_TOKEN.format(tenant=sess["tenant_id"]),
            data={
                "grant_type":  "urn:ietf:params:oauth:grant-type:device_code",
                "client_id":   sess["client_id"],
                "device_code": sess["device_code"],
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    body = r.json()
    err  = body.get("error", "")

    if err in ("authorization_pending", "slow_down"):
        return {"status": "pending", "message": "Waiting for user to complete authentication…"}

    if err:
        _device_sessions.pop(req.session_id, None)
        raise HTTPException(401, f"Auth failed: {body.get('error_description', err)}")

    _device_sessions.pop(req.session_id, None)
    return await _save_and_test(
        req.name or sess["name"],
        body["access_token"],
        req.workspace_id or sess.get("workspace_id", ""),
        req.note or sess.get("note", "") or "Device Code OAuth2",
        "device_code", body.get("expires_in", 3600),
    )


# ── Managed Identity (Azure VM / Container) ───────────────────────────────────
class ManagedIdentityRequest(BaseModel):
    name:         str = "Managed Identity"
    resource:     str = "https://api.fabric.microsoft.com"
    workspace_id: str = ""
    note:         str = ""


@router.post("/auth/managed-identity")
async def auth_managed_identity(req: ManagedIdentityRequest):
    """
    Obtain token from Azure Managed Identity IMDS endpoint (169.254.169.254).
    Requires an Azure VM or container with a system/user-assigned managed identity.
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "http://169.254.169.254/metadata/identity/oauth2/token",
                params={"api-version": "2018-02-01", "resource": req.resource},
                headers={"Metadata": "true"},
            )
    except (httpx.ConnectError, httpx.TimeoutException):
        raise HTTPException(
            503,
            "Managed Identity endpoint (169.254.169.254) is not reachable. "
            "This method only works on Azure VMs or containers with a managed identity assigned.",
        )
    body = r.json()
    if r.status_code != 200 or "access_token" not in body:
        raise HTTPException(500, f"Managed Identity error: {body.get('error', r.text[:200])}")
    return await _save_and_test(
        req.name, body["access_token"], req.workspace_id,
        req.note or "Azure Managed Identity",
        "managed_identity", int(body.get("expires_in", 3600)),
    )


# ── Username / Password (ROPC) ────────────────────────────────────────────────
class ROPCRequest(BaseModel):
    name:         str
    tenant_id:    str
    client_id:    str
    username:     str
    password:     str
    workspace_id: str = ""
    note:         str = ""


@router.post("/auth/username-password")
async def auth_username_password(req: ROPCRequest):
    """
    Obtain token via Username/Password (ROPC flow).
    Accounts with MFA enforced will fail — use Device Code instead.
    """
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            _AAD_TOKEN.format(tenant=req.tenant_id),
            data={
                "grant_type": "password",
                "client_id":  req.client_id,
                "username":   req.username,
                "password":   req.password,
                "scope":      _FABRIC_SCOPE,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    body = r.json()
    if r.status_code != 200 or "access_token" not in body:
        raise HTTPException(
            401, f"Auth failed: {body.get('error_description', body.get('error', r.text[:200]))}"
        )
    return await _save_and_test(
        req.name, body["access_token"], req.workspace_id,
        req.note or f"Username/Password · {req.username}",
        "username_password", body.get("expires_in", 3600),
    )
