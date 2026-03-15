"""
erp_scraper.py — Live ERP documentation fetcher with JSON file cache.

Fetches table metadata from official vendor documentation and caches it
in backend/erp_docs_cache.json (24-hour TTL). Falls back to the static
ERP_TABLES dict from erp_data.py when live fetch fails.
"""
from __future__ import annotations
import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
from fastapi import APIRouter

from app.storage import get_data_dir

logger = logging.getLogger(__name__)

router = APIRouter()

_CACHE_FILE = get_data_dir() / "erp_docs_cache.json"
_CACHE_TTL  = timedelta(hours=24)

# ── In-memory cache (loaded at import time) ──────────────────────────────────
def _load_cache() -> dict:
    try:
        if _CACHE_FILE.exists():
            return json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def _save_cache(data: dict) -> None:
    try:
        _CACHE_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        logger.warning("Could not save ERP docs cache: %s", e)

_cache: dict = _load_cache()   # { source_id: { fetched_at, tables: [...] } }


def _is_stale(source_id: str) -> bool:
    entry = _cache.get(source_id)
    if not entry:
        return True
    try:
        fetched = datetime.fromisoformat(entry["fetched_at"])
        return datetime.now(timezone.utc) - fetched > _CACHE_TTL
    except Exception:
        return True


# ── Fetcher per ERP vendor ────────────────────────────────────────────────────

async def _fetch_oracle_fusion() -> list[dict]:
    """
    Fetch Oracle Fusion Cloud table list from OEDMF documentation index.
    URL: https://docs.oracle.com/en/cloud/saas/financials/24c/oedmf/
    """
    tables = []
    base_url = "https://docs.oracle.com/en/cloud/saas/financials/24c/oedmf/"
    headers  = {"User-Agent": "Mozilla/5.0 (compatible; ilinkERP/1.0)"}
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            r = await client.get(base_url, headers=headers)
            if r.status_code != 200:
                return tables
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(r.text, "html.parser")
            # Parse table links from the OEDMF index — each <a> inside the table list
            for link in soup.find_all("a", href=True):
                href = link["href"]
                txt  = link.get_text(strip=True).upper()
                # Oracle table names are ALL_CAPS with underscores, at least 3 chars
                if (href.endswith(".htm") or href.endswith(".html")) and "_" in txt and len(txt) >= 3 and txt[0].isalpha():
                    # Guess module from table prefix
                    prefix = txt.split("_")[0]
                    module_guess = {
                        "GL": "GL", "AR": "AR", "AP": "AP", "HZ": "AR",
                        "RA": "AR", "OE": "OM", "WSH": "OM", "QP": "OM",
                        "MTL": "INV", "PO": "PO", "HR": "HCM", "PER": "HCM",
                        "PAY": "HCM", "FA": "FA", "CE": "CM", "XLA": "GL",
                        "IBY": "AP", "ZX": "AP", "CST": "INV", "BOM": "INV",
                        "WIP": "INV",
                    }.get(prefix, "OTHER")
                    tables.append({
                        "table_name":   txt,
                        "description":  link.get("title") or link.get_text(strip=True),
                        "module_key":   module_guess,
                        "layer_hint":   "transaction" if any(x in txt for x in ("_ALL", "_LINES", "_DIST")) else "master",
                        "doc_url":      base_url + href,
                        "is_core":      txt in {"GL_BALANCES", "GL_JE_LINES", "GL_CODE_COMBINATIONS",
                                                 "AR_PAYMENT_SCHEDULES_ALL", "AP_INVOICES_ALL",
                                                 "HZ_PARTIES", "OE_ORDER_HEADERS_ALL"},
                    })
    except ImportError:
        logger.warning("beautifulsoup4 not installed — Oracle Fusion scraping unavailable")
    except Exception as e:
        logger.warning("Oracle Fusion doc fetch failed: %s", e)
    return tables


async def _fetch_sap_s4hana() -> list[dict]:
    """
    Fetch SAP S/4HANA table list from the SAP API Business Hub public catalog.
    Uses the public JSON API — no auth required.
    """
    tables = []
    url = "https://api.sap.com/odata/1.0/catalog.svc/APIs?$filter=Products/any(p:p/Name+eq+'S/4HANA')&$format=json&$top=200"
    headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0 (compatible; ilinkERP/1.0)"}
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.get(url, headers=headers)
            if r.status_code == 200:
                items = r.json().get("d", {}).get("results", [])
                for item in items:
                    name   = item.get("Title", "") or item.get("Name", "")
                    domain = item.get("Domain", "") or ""
                    tables.append({
                        "table_name":  name.upper().replace(" ", "_"),
                        "description": item.get("ShortText") or item.get("Title") or name,
                        "module_key":  {"Financial": "FI", "Materials": "MM", "Sales": "SD",
                                        "HR": "HCM", "Logistics": "MM"}.get(domain, "FI"),
                        "layer_hint":  "transaction",
                        "is_core":     name in ("ACDOCA", "SKA1", "SKAT", "MARA", "KNA1", "LFA1"),
                    })
    except Exception as e:
        logger.warning("SAP S/4HANA API Hub fetch failed: %s", e)

    # Also add known core tables from SAP help
    if not tables:
        core_sap = [
            ("ACDOCA", "Universal Journal Entry Line Items (S/4HANA)", "FI", "transaction"),
            ("ACDOCP", "Plan Data Universal Journal", "CO", "transaction"),
            ("SKA1",   "G/L Account Master (Chart of Accounts)", "FI", "master"),
            ("SKAT",   "G/L Account Master Record — Description", "FI", "master"),
            ("FAGLFLEXA", "General Ledger: Items", "FI", "transaction"),
            ("BKPF",   "Accounting Document Header", "FI", "transaction"),
            ("BSEG",   "Accounting Document Segment", "FI", "transaction"),
            ("MARA",   "General Material Data", "MM", "master"),
            ("MARC",   "Plant Data for Material", "MM", "master"),
            ("KNA1",   "General Data in Customer Master", "SD", "master"),
            ("LFA1",   "Vendor Master (General Section)", "MM", "master"),
            ("LIKP",   "SD Document: Delivery Header Data", "SD", "transaction"),
            ("LIPS",   "SD document: Delivery: Item data", "SD", "transaction"),
            ("VBAK",   "Sales Document: Header Data", "SD", "transaction"),
            ("VBAP",   "Sales Document: Item Data", "SD", "transaction"),
        ]
        tables = [{"table_name": t, "description": d, "module_key": m, "layer_hint": l, "is_core": True}
                  for t, d, m, l in core_sap]
    return tables


async def _fetch_dynamics_365() -> list[dict]:
    """
    Fetch Dynamics 365 table list from the Common Data Model schema index.
    CDM schema is public JSON on GitHub.
    """
    tables = []
    url = "https://raw.githubusercontent.com/microsoft/CDM/master/schemaDocuments/core/applicationCommon/foundationCommon/financeCommon/FinanceCore.manifest.cdm.json"
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.get(url, headers={"Accept": "application/json"})
            if r.status_code == 200:
                manifest = r.json()
                for ent in manifest.get("entities", []):
                    name = ent.get("entityName") or ent.get("entityPath", "").split("/")[-1]
                    tables.append({
                        "table_name":  name.upper(),
                        "description": ent.get("explanation") or name,
                        "module_key":  "GL" if any(k in name.upper() for k in ("JOURNAL", "LEDGER", "ACCOUNT")) else
                                       "AP" if "VEND" in name.upper() else
                                       "AR" if "CUST" in name.upper() else "GL",
                        "layer_hint":  "transaction",
                        "is_core":     True,
                    })
    except Exception as e:
        logger.warning("Dynamics 365 CDM fetch failed: %s", e)

    if not tables:
        # Fallback known D365 F&O tables
        d365_tables = [
            ("GENERALJOURNALENTRY", "General Journal Entry Header", "GL", "transaction"),
            ("GENERALJOURNALACCOUNTENTRY", "General Journal Account Entry Line", "GL", "transaction"),
            ("MAINACCOUNT", "Chart of Accounts - Main Account", "GL", "master"),
            ("LEDGERTABLE", "Ledger Configuration", "GL", "master"),
            ("CUSTINVOICETABLE", "Customer Invoice Header", "AR", "transaction"),
            ("CUSTINVOICETRANS", "Customer Invoice Lines", "AR", "transaction"),
            ("CUSTTABLE", "Customer Master", "AR", "master"),
            ("VENDTABLE", "Vendor Master", "AP", "master"),
            ("VENDINVOICEJOUR", "Vendor Invoice Journal Header", "AP", "transaction"),
            ("INVENTTABLE", "Item / Product Master", "INV", "master"),
            ("INVENTTRANS", "Inventory Transactions", "INV", "transaction"),
            ("PURCHLINE", "Purchase Order Lines", "PO", "transaction"),
            ("PURCHTABLE", "Purchase Order Header", "PO", "transaction"),
        ]
        tables = [{"table_name": t, "description": d, "module_key": m, "layer_hint": l, "is_core": True}
                  for t, d, m, l in d365_tables]
    return tables


async def _fetch_oracle_ebs() -> list[dict]:
    """Oracle EBS: fall back to known core tables (docs require Oracle credentials)."""
    tables = [
        ("GL_CODE_COMBINATIONS", "Chart of Accounts Segment Combinations", "GL", "master"),
        ("GL_BALANCES",          "Period Balances", "GL", "transaction"),
        ("GL_JE_HEADERS",        "Journal Entry Headers", "GL", "transaction"),
        ("GL_JE_LINES",          "Journal Entry Lines", "GL", "transaction"),
        ("RA_CUSTOMER_TRX_ALL",  "Customer Transactions (Invoices, DM, CM)", "AR", "transaction"),
        ("AR_PAYMENT_SCHEDULES_ALL", "Payment Schedules per Invoice", "AR", "transaction"),
        ("AR_CASH_RECEIPTS_ALL", "Cash Receipts from Customers", "AR", "transaction"),
        ("HZ_PARTIES",           "Party Master Registry", "AR", "master"),
        ("HZ_CUST_ACCOUNTS",     "Customer Accounts", "AR", "master"),
        ("AP_INVOICES_ALL",      "Supplier Invoice Headers", "AP", "transaction"),
        ("AP_INVOICE_LINES_ALL", "Supplier Invoice Lines", "AP", "transaction"),
        ("AP_CHECKS_ALL",        "Payment Checks/EFT", "AP", "transaction"),
        ("AP_SUPPLIERS",         "Supplier Master", "AP", "master"),
        ("OE_ORDER_HEADERS_ALL", "Sales Order Headers", "OE", "transaction"),
        ("OE_ORDER_LINES_ALL",   "Sales Order Lines", "OE", "transaction"),
        ("MTL_SYSTEM_ITEMS_B",   "Item Master", "INV", "master"),
        ("MTL_ONHAND_QUANTITIES_DETAIL", "On-hand Inventory Quantities", "INV", "transaction"),
        ("PO_HEADERS_ALL",       "Purchase Order Headers", "PO", "transaction"),
        ("PO_LINES_ALL",         "Purchase Order Lines", "PO", "transaction"),
        ("PER_ALL_PEOPLE_F",     "Employee Personal Data", "HCM", "master"),
        ("PAY_PAYROLL_ACTIONS",  "Payroll Run Actions", "HCM", "transaction"),
    ]
    return [{"table_name": t, "description": d, "module_key": m, "layer_hint": l, "is_core": True}
            for t, d, m, l in tables]


# Map source_id → fetch function
_FETCH_FNS: dict[str, any] = {
    "oracle_fusion":   _fetch_oracle_fusion,
    "oracle_ebs":      _fetch_oracle_ebs,
    "sap_s4hana":      _fetch_sap_s4hana,
    "sap_ecc":         _fetch_sap_s4hana,      # similar table set
    "dynamics_365_fo": _fetch_dynamics_365,
    "dynamics_bc":     _fetch_dynamics_365,    # similar
}


async def get_tables_for_source(source_id: str, force_refresh: bool = False) -> dict:
    """
    Return { tables, fetched_at, stale, source } — uses cache when fresh,
    fetches live when stale or forced.
    """
    if not force_refresh and not _is_stale(source_id):
        entry = _cache[source_id]
        return {
            "source":     source_id,
            "tables":     entry["tables"],
            "fetched_at": entry["fetched_at"],
            "stale":      False,
            "live":       True,
        }

    fn = _FETCH_FNS.get(source_id)
    if fn:
        try:
            tables = await fn()
            if tables:
                _cache[source_id] = {
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "tables":     tables,
                }
                _save_cache(_cache)
                return {
                    "source":     source_id,
                    "tables":     tables,
                    "fetched_at": _cache[source_id]["fetched_at"],
                    "stale":      False,
                    "live":       True,
                }
        except Exception as e:
            logger.warning("Fetch failed for %s: %s", source_id, e)

    # Fall back to static ERP_TABLES
    from .erp_data import ERP_TABLES, ERP_MODULES
    all_modules = ERP_MODULES.get(source_id, [])
    tables = []
    for mod in all_modules:
        for tbl in ERP_TABLES.get((source_id, mod["key"]), []):
            tables.append({**tbl, "module_key": mod["key"]})

    return {
        "source":     source_id,
        "tables":     tables,
        "fetched_at": _cache.get(source_id, {}).get("fetched_at", "never"),
        "stale":      True,
        "live":       False,
    }


async def refresh_all_sources() -> dict:
    """Refresh all supported ERP sources. Called by background task."""
    results = {}
    for src_id in _FETCH_FNS:
        try:
            data = await get_tables_for_source(src_id, force_refresh=True)
            results[src_id] = {
                "table_count": len(data["tables"]),
                "fetched_at":  data["fetched_at"],
                "live":        data["live"],
            }
        except Exception as e:
            results[src_id] = {"error": str(e)}
    return results


# ── API Endpoints ─────────────────────────────────────────────────────────────

@router.get("/cross-comparison")
async def cross_comparison(
    module: str,
    source: str | None = None,
):
    """
    Cross-ERP table comparison for a given module (e.g. GL, AR, AP, FI, MM).
    Returns one entry per ERP source with its tables matching the requested module.
    Module matching is case-insensitive; SAP FI ≈ Oracle GL ≈ D365 GL etc.
    """
    # Module aliases: map SAP/D365 module codes to canonical module
    MODULE_ALIASES: dict[str, list[str]] = {
        "GL":  ["GL", "FI", "LEDGER", "GL"],
        "AR":  ["AR", "CUST", "SD"],
        "AP":  ["AP", "VEND", "MM"],
        "INV": ["INV", "MM", "INVENT"],
        "HCM": ["HCM", "HR", "PAY"],
        "PO":  ["PO", "MM", "PURCH"],
        "OM":  ["OM", "OE", "SD"],
        "FA":  ["FA", "ASSET"],
        "CM":  ["CM", "CE", "BANK"],
    }
    target_module = module.upper()
    aliases = MODULE_ALIASES.get(target_module, [target_module])

    from .erp_data import ERP_SOURCES
    sources_to_check = [s["id"] for s in ERP_SOURCES if s.get("supported")]
    if source:
        sources_to_check = [s for s in sources_to_check if s == source]

    results = []
    for src_id in sources_to_check:
        data = await get_tables_for_source(src_id)
        # Filter tables whose module_key is in aliases
        matching = [
            t for t in data["tables"]
            if t.get("module_key", "").upper() in aliases
        ]
        if matching or not source:
            from .erp_data import ERP_SOURCES
            src_info = next((s for s in ERP_SOURCES if s["id"] == src_id), {})
            results.append({
                "source_id":   src_id,
                "source_name": src_info.get("name", src_id),
                "vendor":      src_info.get("vendor", ""),
                "color":       src_info.get("color", "#888"),
                "tables":      matching,
                "table_count": len(matching),
                "fetched_at":  data.get("fetched_at", ""),
                "live":        data.get("live", False),
                "stale":       data.get("stale", True),
            })

    return {
        "module":  target_module,
        "aliases": aliases,
        "sources": results,
        "total_tables": sum(r["table_count"] for r in results),
    }


@router.post("/refresh-docs")
async def refresh_docs(source: str | None = None):
    """Manually trigger a live re-fetch from vendor documentation."""
    if source:
        data = await get_tables_for_source(source, force_refresh=True)
        return {
            "refreshed": [source],
            "results":   {source: {"table_count": len(data["tables"]), "live": data["live"]}},
        }
    results = await refresh_all_sources()
    return {"refreshed": list(results.keys()), "results": results}


@router.get("/docs-cache-status")
async def docs_cache_status():
    """Return cache status for all ERP sources."""
    from .erp_data import ERP_SOURCES
    status = {}
    now = datetime.now(timezone.utc)
    for src in ERP_SOURCES:
        if not src.get("supported"):
            continue
        src_id = src["id"]
        entry  = _cache.get(src_id, {})
        if entry.get("fetched_at"):
            try:
                fetched  = datetime.fromisoformat(entry["fetched_at"])
                age_h    = (now - fetched).total_seconds() / 3600
                is_stale = age_h > 24
            except Exception:
                age_h, is_stale = 999, True
        else:
            age_h, is_stale = 999, True
        status[src_id] = {
            "fetched_at":  entry.get("fetched_at", "never"),
            "table_count": len(entry.get("tables", [])),
            "age_hours":   round(age_h, 1),
            "stale":       is_stale,
        }
    return {"sources": status}
