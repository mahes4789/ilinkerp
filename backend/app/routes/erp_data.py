"""ERP Sources, Modules, and Data Dictionary tables."""
from __future__ import annotations
import asyncio
import json
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, HTTPException, Query
import httpx

from app.storage import get_data_dir

router = APIRouter()

# ── ERP Source catalogue ──────────────────────────────────────────────────────
ERP_SOURCES = [
    {"id": "oracle_fusion",   "name": "Oracle Fusion Cloud ERP",         "vendor": "Oracle",    "color": "#cc2222", "supported": True,  "desc": "Cloud ERP for enterprise financials, procurement and HCM"},
    {"id": "oracle_ebs",      "name": "Oracle E-Business Suite",         "vendor": "Oracle",    "color": "#cc2222", "supported": True,  "desc": "On-premise Oracle ERP (11i, R12.x)"},
    {"id": "sap_s4hana",      "name": "SAP S/4HANA",                    "vendor": "SAP",       "color": "#0077cc", "supported": True,  "desc": "Intelligent ERP on HANA in-memory database"},
    {"id": "sap_ecc",         "name": "SAP ECC 6.0",                    "vendor": "SAP",       "color": "#0077cc", "supported": True,  "desc": "Classic SAP ERP — on-premise or hosted"},
    {"id": "dynamics_365_fo", "name": "Dynamics 365 Finance",           "vendor": "Microsoft", "color": "#0078d4", "supported": True,  "desc": "Cloud ERP for enterprise finance and operations"},
    {"id": "dynamics_bc",     "name": "Dynamics 365 Business Central",  "vendor": "Microsoft", "color": "#0078d4", "supported": True,  "desc": "ERP for small and medium businesses"},
    {"id": "netsuite",        "name": "NetSuite ERP",                   "vendor": "Oracle",    "color": "#cc2222", "supported": True,  "desc": "Cloud ERP for growing businesses"},
    {"id": "workday",         "name": "Workday HCM & Financials",       "vendor": "Workday",   "color": "#009fd9", "supported": True,  "desc": "Cloud HCM and financial management platform"},
    {"id": "infor_cloud",     "name": "Infor CloudSuite",               "vendor": "Infor",     "color": "#ff6900", "supported": False, "desc": "Industry-specific cloud ERP suite"},
    {"id": "epicor",          "name": "Epicor Kinetic",                 "vendor": "Epicor",    "color": "#e61e28", "supported": False, "desc": "ERP for manufacturing and distribution"},
    {"id": "ifs",             "name": "IFS Applications",               "vendor": "IFS",       "color": "#003b6f", "supported": False, "desc": "Service, manufacturing and asset management ERP"},
    {"id": "sage_300",        "name": "Sage 300",                       "vendor": "Sage",      "color": "#00bc63", "supported": False, "desc": "Financial management for mid-sized companies"},
]

# ── Module registry ───────────────────────────────────────────────────────────
ERP_MODULES: dict[str, list[dict]] = {
    "oracle_fusion": [
        {"key": "AR",  "label": "Accounts Receivable (AR)",    "desc": "Customer invoicing, receipts and collections"},
        {"key": "AP",  "label": "Accounts Payable (AP)",       "desc": "Supplier invoices, payments and holds"},
        {"key": "GL",  "label": "General Ledger (GL)",         "desc": "Journal entries, chart of accounts and balances"},
        {"key": "OM",  "label": "Order Management (OM)",       "desc": "Sales orders, pricing and fulfilment"},
        {"key": "INV", "label": "Inventory Management (INV)",  "desc": "Item master, on-hand quantities and transactions"},
        {"key": "PO",  "label": "Purchasing (PO)",             "desc": "Purchase orders, receipts and suppliers"},
        {"key": "HCM", "label": "Human Capital Management",    "desc": "Employee data, payroll and HR processes"},
        {"key": "FA",  "label": "Fixed Assets (FA)",           "desc": "Asset lifecycle, depreciation and disposals"},
        {"key": "CM",  "label": "Cash Management (CM)",        "desc": "Bank statements, reconciliation and forecasting"},
        {"key": "PPM", "label": "Project Portfolio Mgmt",      "desc": "Projects, tasks, costs and billing"},
        {"key": "SCM", "label": "Supply Chain Management",     "desc": "Supply chain planning and execution"},
        {"key": "FBL", "label": "Financial Budgeting",         "desc": "Budgets, forecasts and variance analysis"},
    ],
    "oracle_ebs": [
        {"key": "AR",  "label": "Accounts Receivable (AR)",    "desc": "RA/HZ module for customer billing and receipts"},
        {"key": "AP",  "label": "Accounts Payable (AP)",       "desc": "Supplier invoices and payment processing"},
        {"key": "GL",  "label": "General Ledger (GL)",         "desc": "Core financial accounting"},
        {"key": "OE",  "label": "Order Management (OE/OM)",    "desc": "Sales orders and fulfilment"},
        {"key": "INV", "label": "Inventory (INV)",             "desc": "Materials and warehouse management"},
        {"key": "PO",  "label": "Purchasing (PO)",             "desc": "Procurement and supplier management"},
        {"key": "HCM", "label": "HR & Payroll",                "desc": "PER/PAY module — employee and payroll"},
        {"key": "FA",  "label": "Fixed Assets (FA)",           "desc": "Asset management and depreciation"},
        {"key": "CM",  "label": "Cash Management (CE)",        "desc": "Bank reconciliation and cash forecasting"},
        {"key": "PA",  "label": "Projects (PA)",               "desc": "Project accounting and billing"},
    ],
    "sap_s4hana": [
        {"key": "FI",   "label": "Financial Accounting (FI)",  "desc": "General ledger, AR, AP and asset accounting"},
        {"key": "CO",   "label": "Controlling (CO)",           "desc": "Cost centers, profit centers and internal orders"},
        {"key": "SD",   "label": "Sales & Distribution (SD)",  "desc": "Sales orders, deliveries and billing"},
        {"key": "MM",   "label": "Materials Management (MM)",  "desc": "Procurement, inventory and warehouse"},
        {"key": "HCM",  "label": "Human Capital Mgmt (HCM)",   "desc": "Personnel, org structure and payroll"},
        {"key": "PP",   "label": "Production Planning (PP)",   "desc": "MRP, BOM and production orders"},
        {"key": "PM",   "label": "Plant Maintenance (PM)",     "desc": "Equipment, maintenance orders and notifications"},
        {"key": "PS",   "label": "Project System (PS)",        "desc": "WBS, networks and project cost accounting"},
    ],
    "sap_ecc": [
        {"key": "FI",  "label": "Financial Accounting (FI)",   "desc": "GL, AR, AP — core finance module"},
        {"key": "CO",  "label": "Controlling (CO)",            "desc": "Cost accounting and management reporting"},
        {"key": "SD",  "label": "Sales & Distribution (SD)",   "desc": "Order-to-cash process"},
        {"key": "MM",  "label": "Materials Management (MM)",   "desc": "Procure-to-pay process"},
        {"key": "HCM", "label": "HR & Payroll (HCM)",          "desc": "Human resources and payroll"},
        {"key": "PP",  "label": "Production Planning (PP)",    "desc": "Manufacturing and production"},
        {"key": "QM",  "label": "Quality Management (QM)",     "desc": "Quality inspection and notifications"},
        {"key": "PM",  "label": "Plant Maintenance (PM)",      "desc": "Asset and equipment maintenance"},
    ],
    "dynamics_365_fo": [
        {"key": "GL",  "label": "General Ledger",              "desc": "Chart of accounts, journals and balances"},
        {"key": "AR",  "label": "Accounts Receivable",         "desc": "Customer invoicing and collection"},
        {"key": "AP",  "label": "Accounts Payable",            "desc": "Vendor invoices and payments"},
        {"key": "INV", "label": "Inventory Management",        "desc": "Items, warehouses and on-hand"},
        {"key": "SCM", "label": "Supply Chain Management",     "desc": "Procurement, production and logistics"},
        {"key": "HRM", "label": "HR & Payroll",                "desc": "Workers, positions and compensation"},
        {"key": "PRJ", "label": "Project Management",          "desc": "Projects, costs and revenue recognition"},
    ],
    "dynamics_bc": [
        {"key": "FIN", "label": "Finance",                     "desc": "General ledger, AR, AP and budgeting"},
        {"key": "SLS", "label": "Sales",                       "desc": "Sales orders, invoices and customers"},
        {"key": "PUR", "label": "Purchasing",                  "desc": "Purchase orders and vendor management"},
        {"key": "INV", "label": "Inventory",                   "desc": "Items, locations and stock tracking"},
        {"key": "MFG", "label": "Manufacturing",               "desc": "Production orders, BOM and routings"},
        {"key": "SVC", "label": "Service Management",          "desc": "Service orders and contracts"},
    ],
    "netsuite": [
        {"key": "FM",  "label": "Financial Management",        "desc": "GL, multi-currency and period close"},
        {"key": "AR",  "label": "Accounts Receivable",         "desc": "Invoices, payments and collections"},
        {"key": "AP",  "label": "Accounts Payable",            "desc": "Vendor bills and payments"},
        {"key": "INV", "label": "Inventory & Supply Chain",    "desc": "Items, fulfillment and warehouse"},
        {"key": "OM",  "label": "Order Management",            "desc": "Sales orders, returns and pricing"},
        {"key": "HR",  "label": "HR & Payroll",                "desc": "Employees, payroll and benefits"},
    ],
    "workday": [
        {"key": "HCM", "label": "HCM Core",                   "desc": "Worker profiles, org structure and HRBPs"},
        {"key": "PAY", "label": "Payroll",                     "desc": "Payroll processing, deductions and compliance"},
        {"key": "BEN", "label": "Benefits Administration",     "desc": "Benefits plans, enrollment and life events"},
        {"key": "REC", "label": "Recruiting",                  "desc": "Job requisitions, applications and offers"},
        {"key": "FIN", "label": "Financial Management",        "desc": "Accounting, budgets and expenses"},
        {"key": "EXP", "label": "Expense Management",          "desc": "Employee expenses, travel and reimbursements"},
    ],
}

# ── Table registry ────────────────────────────────────────────────────────────
# Key: (source_id, module_key)
ERP_TABLES: dict[tuple[str, str], list[dict]] = {

    # ─── Oracle Fusion — AR ──────────────────────────────────────────────────
    ("oracle_fusion", "AR"): [
        {"table_name": "HZ_PARTIES",                    "description": "Master registry of all parties (customers, suppliers, employees)",  "is_core": True},
        {"table_name": "HZ_CUST_ACCOUNTS",              "description": "Customer account linking a party to a business account",            "is_core": True},
        {"table_name": "HZ_CUST_ACCT_SITES_ALL",        "description": "Customer account sites (bill-to and ship-to locations)",           "is_core": True},
        {"table_name": "HZ_CUST_SITE_USES_ALL",         "description": "Site use type (BILL_TO, SHIP_TO, STATEMENTS)",                     "is_core": True},
        {"table_name": "RA_CUSTOMER_TRX_ALL",           "description": "Customer transaction header (invoices, credit memos, debit memos)", "is_core": True},
        {"table_name": "RA_CUSTOMER_TRX_LINES_ALL",     "description": "Invoice lines with item, quantity, unit price",                     "is_core": True},
        {"table_name": "RA_CUST_TRX_TYPES_ALL",         "description": "Transaction type definitions (INV, CM, DM, CB)",                   "is_core": False},
        {"table_name": "AR_CASH_RECEIPTS_ALL",          "description": "Cash receipts — payments received from customers",                  "is_core": True},
        {"table_name": "AR_RECEIVABLE_APPLICATIONS_ALL","description": "Application of receipts to invoices",                               "is_core": True},
        {"table_name": "AR_PAYMENT_SCHEDULES_ALL",      "description": "Payment schedule — amount due, due date, status per invoice",       "is_core": True},
        {"table_name": "AR_ADJUSTMENTS_ALL",            "description": "Manual adjustments to invoice balances",                            "is_core": False},
        {"table_name": "HZ_LOCATIONS",                  "description": "Physical address details for parties and sites",                    "is_core": False},
        {"table_name": "RA_TERMS_B",                    "description": "Payment terms (NET30, 2/10 NET30, etc.)",                           "is_core": False},
        {"table_name": "AR_COLLECTORS",                 "description": "Collector assignments for dunning and collection management",        "is_core": False},
        {"table_name": "AR_STATEMENT_HEADERS_ALL",      "description": "Customer account statement headers",                                "is_core": False},
    ],

    # ─── Oracle Fusion — AP ──────────────────────────────────────────────────
    ("oracle_fusion", "AP"): [
        {"table_name": "AP_SUPPLIERS",                  "description": "Supplier master (replaces PO_VENDORS in Fusion)",                   "is_core": True},
        {"table_name": "AP_SUPPLIER_SITES_ALL",         "description": "Supplier site details (remit-to, pay-to)",                         "is_core": True},
        {"table_name": "AP_INVOICES_ALL",               "description": "AP invoice headers — all types",                                   "is_core": True},
        {"table_name": "AP_INVOICE_LINES_ALL",          "description": "Invoice lines (ITEM, TAX, FREIGHT, MISCELLANEOUS)",                 "is_core": True},
        {"table_name": "AP_INVOICE_DISTRIBUTIONS_ALL",  "description": "Accounting distributions for each invoice line",                   "is_core": True},
        {"table_name": "AP_CHECKS_ALL",                 "description": "Payment checks / EFT records",                                     "is_core": True},
        {"table_name": "AP_INVOICE_PAYMENTS_ALL",       "description": "Links invoices to payments",                                       "is_core": True},
        {"table_name": "AP_PAYMENT_SCHEDULES_ALL",      "description": "Payment schedule — due dates and amounts per invoice",             "is_core": True},
        {"table_name": "AP_HOLDS_ALL",                  "description": "Invoice validation holds preventing payment",                      "is_core": False},
        {"table_name": "AP_LOOKUP_CODES",               "description": "AP configuration lookup values",                                   "is_core": False},
        {"table_name": "AP_TERMS",                      "description": "Payment terms definitions",                                        "is_core": False},
        {"table_name": "IBY_EXT_BANK_ACCOUNTS",         "description": "External (supplier/employee) bank accounts",                       "is_core": False},
        {"table_name": "AP_BANK_ACCOUNTS_ALL",          "description": "Internal bank accounts used for payments",                         "is_core": False},
    ],

    # ─── Oracle Fusion — GL ──────────────────────────────────────────────────
    ("oracle_fusion", "GL"): [
        {"table_name": "GL_LEDGERS",                    "description": "Ledger definitions — primary and secondary ledgers",               "is_core": True},
        {"table_name": "GL_CODE_COMBINATIONS",          "description": "Chart of accounts — all account segment combinations",             "is_core": True},
        {"table_name": "GL_JE_HEADERS",                 "description": "Journal entry batch headers",                                      "is_core": True},
        {"table_name": "GL_JE_LINES",                   "description": "Journal entry line details (debit / credit amounts)",              "is_core": True},
        {"table_name": "GL_BALANCES",                   "description": "Period balances by ledger and account combination",                 "is_core": True},
        {"table_name": "GL_PERIODS",                    "description": "Fiscal calendar period definitions",                               "is_core": True},
        {"table_name": "GL_DAILY_RATES",                "description": "Daily foreign currency conversion rates",                          "is_core": True},
        {"table_name": "GL_BUDGET_VERSIONS",            "description": "Budget version definitions (original, forecast, etc.)",            "is_core": False},
        {"table_name": "GL_BUDGET_INTERFACE",           "description": "Budget upload interface table",                                    "is_core": False},
        {"table_name": "GL_ACCOUNT_HIERARCHIES",        "description": "Account hierarchy structure for reporting",                        "is_core": False},
        {"table_name": "GL_LOOKUPS",                    "description": "General Ledger configuration lookup values",                       "is_core": False},
        {"table_name": "XLA_AE_HEADERS",                "description": "Subledger accounting event headers",                               "is_core": False},
        {"table_name": "XLA_AE_LINES",                  "description": "Subledger accounting event lines",                                 "is_core": False},
    ],

    # ─── Oracle Fusion — OM ──────────────────────────────────────────────────
    ("oracle_fusion", "OM"): [
        {"table_name": "OE_ORDER_HEADERS_ALL",          "description": "Sales order headers",                                              "is_core": True},
        {"table_name": "OE_ORDER_LINES_ALL",            "description": "Sales order lines with item, qty, price",                          "is_core": True},
        {"table_name": "OE_TRANSACTION_TYPES_ALL",      "description": "Order transaction type definitions",                               "is_core": True},
        {"table_name": "OE_PRICE_ADJUSTMENTS",          "description": "Price adjustments (discounts, surcharges) on order lines",         "is_core": False},
        {"table_name": "OE_HOLD_SOURCES_ALL",           "description": "Order hold definitions",                                           "is_core": False},
        {"table_name": "OE_ORDER_HOLDS_ALL",            "description": "Holds applied to specific orders or lines",                        "is_core": False},
        {"table_name": "WSH_DELIVERY_DETAILS",          "description": "Shipping delivery detail lines",                                   "is_core": True},
        {"table_name": "WSH_DELIVERIES",                "description": "Shipment delivery headers",                                        "is_core": True},
        {"table_name": "QP_LIST_HEADERS_B",             "description": "Price list headers",                                               "is_core": False},
        {"table_name": "QP_LIST_LINES",                 "description": "Price list line details (item prices)",                            "is_core": False},
        {"table_name": "MTL_SALES_ORDERS",              "description": "MTL sales order interface to inventory",                            "is_core": False},
    ],

    # ─── Oracle Fusion — INV ─────────────────────────────────────────────────
    ("oracle_fusion", "INV"): [
        {"table_name": "MTL_SYSTEM_ITEMS_B",            "description": "Item master — all inventory items",                                "is_core": True},
        {"table_name": "MTL_ONHAND_QUANTITIES_DETAIL",  "description": "Current on-hand inventory quantities by locator",                  "is_core": True},
        {"table_name": "MTL_MATERIAL_TRANSACTIONS",     "description": "All inventory movement transactions",                              "is_core": True},
        {"table_name": "MTL_TRANSACTION_TYPES",         "description": "Transaction type definitions (issues, receipts, transfers)",       "is_core": True},
        {"table_name": "MTL_ITEM_LOCATIONS",            "description": "Warehouse locator definitions",                                   "is_core": True},
        {"table_name": "MTL_ITEM_CATEGORIES",           "description": "Item category assignments",                                        "is_core": False},
        {"table_name": "MTL_CATEGORIES_B",              "description": "Category definitions",                                             "is_core": False},
        {"table_name": "MTL_LOT_NUMBERS",               "description": "Lot number master",                                                "is_core": False},
        {"table_name": "MTL_SERIAL_NUMBERS",            "description": "Serial number tracking",                                           "is_core": False},
        {"table_name": "MTL_DEMAND",                    "description": "Demand records from sales orders and forecasts",                   "is_core": False},
        {"table_name": "CST_ITEM_COSTS",                "description": "Item cost details per cost type",                                  "is_core": False},
    ],

    # ─── SAP S/4HANA — FI ────────────────────────────────────────────────────
    ("sap_s4hana", "FI"): [
        {"table_name": "BKPF",                          "description": "Accounting document header",                                       "is_core": True},
        {"table_name": "BSEG",                          "description": "Accounting document line items",                                   "is_core": True},
        {"table_name": "SKA1",                          "description": "G/L account master (chart of accounts)",                           "is_core": True},
        {"table_name": "SKB1",                          "description": "G/L account master (company code data)",                           "is_core": True},
        {"table_name": "FAGLFLEXT",                     "description": "General ledger account totals (new GL)",                           "is_core": True},
        {"table_name": "FAGLFLEXA",                     "description": "General ledger actual line items",                                 "is_core": True},
        {"table_name": "KNA1",                          "description": "Customer master (general data)",                                   "is_core": True},
        {"table_name": "KNB1",                          "description": "Customer master (company code)",                                   "is_core": True},
        {"table_name": "LFA1",                          "description": "Vendor master (general data)",                                     "is_core": True},
        {"table_name": "LFB1",                          "description": "Vendor master (company code)",                                     "is_core": True},
        {"table_name": "BSID",                          "description": "Accounting: secondary index for customers (open items)",           "is_core": True},
        {"table_name": "BSAD",                          "description": "Accounting: secondary index for customers (cleared items)",        "is_core": False},
        {"table_name": "BSIK",                          "description": "Accounting: secondary index for vendors (open items)",             "is_core": True},
        {"table_name": "BSAK",                          "description": "Accounting: secondary index for vendors (cleared items)",          "is_core": False},
        {"table_name": "T001",                          "description": "Company codes",                                                    "is_core": False},
        {"table_name": "T004",                          "description": "Chart of accounts directory",                                      "is_core": False},
    ],

    # ─── SAP S/4HANA — MM ────────────────────────────────────────────────────
    ("sap_s4hana", "MM"): [
        {"table_name": "MARA",                          "description": "Material master (general data)",                                   "is_core": True},
        {"table_name": "MARC",                          "description": "Material master (plant data / MRP)",                               "is_core": True},
        {"table_name": "MARD",                          "description": "Material master (storage location data)",                          "is_core": True},
        {"table_name": "EKKO",                          "description": "Purchase order header",                                            "is_core": True},
        {"table_name": "EKPO",                          "description": "Purchase order line items",                                        "is_core": True},
        {"table_name": "MSEG",                          "description": "Document segment — material movements",                            "is_core": True},
        {"table_name": "MKPF",                          "description": "Material document header",                                         "is_core": True},
        {"table_name": "MCHB",                          "description": "Batch stocks (quantity by batch and storage location)",            "is_core": False},
        {"table_name": "MBEW",                          "description": "Material valuation data",                                          "is_core": True},
        {"table_name": "T001W",                         "description": "Plants table",                                                     "is_core": False},
        {"table_name": "T001L",                         "description": "Storage locations",                                                "is_core": False},
    ],

    # ─── SAP S/4HANA — SD ────────────────────────────────────────────────────
    ("sap_s4hana", "SD"): [
        {"table_name": "VBAK",                          "description": "Sales document header data",                                       "is_core": True},
        {"table_name": "VBAP",                          "description": "Sales document item data",                                         "is_core": True},
        {"table_name": "LIKP",                          "description": "Delivery header data",                                             "is_core": True},
        {"table_name": "LIPS",                          "description": "Delivery item data",                                               "is_core": True},
        {"table_name": "VBRK",                          "description": "Billing document header",                                          "is_core": True},
        {"table_name": "VBRP",                          "description": "Billing document items",                                           "is_core": True},
        {"table_name": "VBEP",                          "description": "Sales document schedule line data",                                "is_core": False},
        {"table_name": "KONP",                          "description": "Conditions (pricing procedure items)",                              "is_core": False},
        {"table_name": "KNA1",                          "description": "Customer master (general data)",                                   "is_core": True},
        {"table_name": "TVKWZ",                         "description": "Allowed plants per sales organization",                            "is_core": False},
    ],

    # ─── SAP ECC — FI ────────────────────────────────────────────────────────
    ("sap_ecc", "FI"): [
        {"table_name": "BKPF",                          "description": "Accounting document header",                                       "is_core": True},
        {"table_name": "BSEG",                          "description": "Accounting document line items",                                   "is_core": True},
        {"table_name": "SKA1",                          "description": "G/L account master (chart of accounts)",                           "is_core": True},
        {"table_name": "SKB1",                          "description": "G/L account master (company code)",                                "is_core": True},
        {"table_name": "GLT0",                          "description": "G/L account transaction figures (classic GL)",                     "is_core": True},
        {"table_name": "KNA1",                          "description": "Customer master (general data)",                                   "is_core": True},
        {"table_name": "KNB1",                          "description": "Customer master (company code)",                                   "is_core": True},
        {"table_name": "LFA1",                          "description": "Vendor master (general data)",                                     "is_core": True},
        {"table_name": "LFB1",                          "description": "Vendor master (company code)",                                     "is_core": True},
        {"table_name": "BSID",                          "description": "Customer open items",                                              "is_core": True},
        {"table_name": "BSIK",                          "description": "Vendor open items",                                                "is_core": True},
        {"table_name": "T001",                          "description": "Company codes",                                                    "is_core": False},
        {"table_name": "T004",                          "description": "Chart of accounts",                                                "is_core": False},
    ],

    # ─── SAP ECC — MM ────────────────────────────────────────────────────────
    ("sap_ecc", "MM"): [
        {"table_name": "MARA",                          "description": "Material master (general data)",                                   "is_core": True},
        {"table_name": "MARC",                          "description": "Material master (plant / MRP data)",                               "is_core": True},
        {"table_name": "MARD",                          "description": "Material master (storage location data)",                          "is_core": True},
        {"table_name": "EKKO",                          "description": "Purchase order header",                                            "is_core": True},
        {"table_name": "EKPO",                          "description": "Purchase order line items",                                        "is_core": True},
        {"table_name": "MSEG",                          "description": "Material document segment",                                        "is_core": True},
        {"table_name": "MKPF",                          "description": "Material document header",                                         "is_core": True},
        {"table_name": "MBEW",                          "description": "Material valuation",                                               "is_core": True},
    ],

    # ─── Dynamics 365 F&O — GL ───────────────────────────────────────────────
    ("dynamics_365_fo", "GL"): [
        {"table_name": "LEDGER",                        "description": "Ledger entity",                                                    "is_core": True},
        {"table_name": "MAINACCOUNT",                   "description": "Main account (chart of accounts)",                                 "is_core": True},
        {"table_name": "GENERALJOURNALENTRY",           "description": "General journal entry header",                                     "is_core": True},
        {"table_name": "GENERALJOURNALACCOUNTENTRY",    "description": "General journal account entry lines",                              "is_core": True},
        {"table_name": "LEDGERENTRYJOURNAL",            "description": "Ledger entry journal (accounting entries)",                        "is_core": True},
        {"table_name": "FISCALCALENDAR",                "description": "Fiscal calendar periods",                                          "is_core": False},
        {"table_name": "DIMENSIONHIERARCHY",            "description": "Financial dimension hierarchy",                                    "is_core": False},
        {"table_name": "BUDGETENTRY",                   "description": "Budget register entries",                                          "is_core": False},
    ],

    # ─── Dynamics 365 F&O — AR ───────────────────────────────────────────────
    ("dynamics_365_fo", "AR"): [
        {"table_name": "CUSTINVOICEJOUR",               "description": "Customer invoice journal (posted invoices)",                       "is_core": True},
        {"table_name": "CUSTINVOICETRANS",              "description": "Customer invoice transaction lines",                               "is_core": True},
        {"table_name": "CUSTTABLE",                     "description": "Customer master table",                                            "is_core": True},
        {"table_name": "CUSTTRANS",                     "description": "Customer open transactions",                                       "is_core": True},
        {"table_name": "CUSTPAYMSCHED",                 "description": "Customer payment schedule",                                        "is_core": False},
        {"table_name": "CUSTSETTLEMENT",                "description": "Customer transaction settlement",                                  "is_core": False},
        {"table_name": "CUSTGROUP",                     "description": "Customer group definitions",                                       "is_core": False},
    ],

    # ─── NetSuite — AR ───────────────────────────────────────────────────────
    ("netsuite", "AR"): [
        {"table_name": "TRANSACTION",                   "description": "All transaction types (invoices, payments, credits)",              "is_core": True},
        {"table_name": "TRANSACTIONLINE",               "description": "Transaction line details",                                         "is_core": True},
        {"table_name": "CUSTOMER",                      "description": "Customer master record",                                           "is_core": True},
        {"table_name": "CUSTOMERTRANSACTION",           "description": "Customer-specific transaction view",                               "is_core": True},
        {"table_name": "PAYMENTTERM",                   "description": "Payment term definitions",                                         "is_core": False},
        {"table_name": "CURRENCY",                      "description": "Currency master",                                                  "is_core": False},
    ],

    # ─── Workday — HCM ───────────────────────────────────────────────────────
    ("workday", "HCM"): [
        {"table_name": "Worker",                        "description": "Worker (employee / contingent worker) profile",                    "is_core": True},
        {"table_name": "WorkerJobProfile",              "description": "Job profile assignments per worker",                               "is_core": True},
        {"table_name": "OrganizationHierarchy",         "description": "Organizational hierarchy structure",                               "is_core": True},
        {"table_name": "Position",                      "description": "Position definitions in the org structure",                        "is_core": True},
        {"table_name": "CompensationPlan",              "description": "Compensation plan assignments",                                    "is_core": False},
        {"table_name": "LeaveRequest",                  "description": "Employee leave of absence requests",                               "is_core": False},
        {"table_name": "TimeOff",                       "description": "Time-off balances and requests",                                   "is_core": False},
    ],

    # ─── Workday — PAY ───────────────────────────────────────────────────────
    ("workday", "PAY"): [
        {"table_name": "PayrollResult",                 "description": "Payroll calculation results per worker per period",                "is_core": True},
        {"table_name": "PayrollInputs",                 "description": "Payroll inputs (earnings, deductions) before processing",          "is_core": True},
        {"table_name": "PayComponent",                  "description": "Pay component definitions (base pay, bonus, etc.)",               "is_core": True},
        {"table_name": "TaxWithholding",                "description": "Tax withholding elections per worker",                             "is_core": False},
        {"table_name": "DirectDeposit",                 "description": "Worker bank account and direct deposit elections",                 "is_core": False},
    ],
}

# Default fallback tables for unsupported combinations
_DEFAULT_TABLES = [
    {"table_name": "HEADER_TABLE",     "description": "Transaction header records",      "is_core": True},
    {"table_name": "LINE_TABLE",       "description": "Transaction line details",        "is_core": True},
    {"table_name": "MASTER_TABLE",     "description": "Master data entity",              "is_core": True},
    {"table_name": "CONFIG_TABLE",     "description": "Configuration and lookup values", "is_core": False},
    {"table_name": "AUDIT_TABLE",      "description": "Audit trail and history",         "is_core": False},
]


# ── API Endpoints ─────────────────────────────────────────────────────────────
@router.get("/sources")
async def list_sources():
    return {"sources": ERP_SOURCES, "total": len(ERP_SOURCES)}


@router.get("/modules")
async def list_modules(source: str = Query(...)):
    modules = ERP_MODULES.get(source)
    if modules is None:
        raise HTTPException(404, f"Unknown ERP source: {source}")
    return {"source": source, "modules": modules, "total": len(modules)}


@router.get("/tables")
async def list_tables(source: str = Query(...), module: str = Query(...)):
    tables = ERP_TABLES.get((source, module)) or _DEFAULT_TABLES
    return {
        "source":      source,
        "module":      module,
        "tables":      tables,
        "total":       len(tables),
        "core_count":  sum(1 for t in tables if t.get("is_core")),
    }


# ── Live Connection Test ───────────────────────────────────────────────────────
@router.post("/test-connection")
async def test_connection(body: dict):
    """
    Test ERP database / API connectivity with the supplied credentials.

    Oracle EBS    → oracledb thin-mode JDBC connect
    Oracle Fusion → HTTP GET to REST API metadata URL
    SAP S/4HANA   → HTTP ping to HANA REST endpoint
    Dynamics 365  → Azure AD client-credentials token
    Others        → graceful informational response
    """
    erp_type  = body.get("erp_type", "")
    host      = body.get("host", "")
    port      = int(body.get("port") or 1521)
    service   = body.get("service", body.get("database", ""))
    username  = body.get("username", "")
    password  = body.get("password", "")

    # ── Oracle EBS / Oracle Fusion — JDBC via python-oracledb (thin mode) ───────
    # Both oracle_ebs and oracle_fusion use the same JDBC connect when port ≠ 443.
    # For Oracle Fusion Cloud (SaaS at port 443) we fall through to the REST test.
    if erp_type in ("oracle_ebs", "oracle_fusion") and port != 443:
        def _connect():
            import oracledb  # thin mode — no Oracle Client needed
            # Build DSN: prefer Easy Connect format  host:port/service
            dsn  = f"{host}:{port}/{service}" if service else f"{host}:{port}"
            conn = oracledb.connect(user=username, password=password, dsn=dsn)
            ver  = conn.version
            conn.close()
            return ver
        try:
            ver = await asyncio.get_event_loop().run_in_executor(None, _connect)
            return {
                "success": True,
                "message": f"Connected — Oracle Database {ver} at {host}:{port}/{service}",
                "version": ver,
            }
        except ImportError:
            return {
                "success": False,
                "message": "oracledb package not found in this container. "
                           "Add 'oracledb' to requirements.txt and rebuild.",
            }
        except Exception as exc:
            # Return the actual Oracle error (ORA-xxxxx, TNS, etc.)
            return {"success": False, "message": str(exc)}

    # ── Oracle Fusion Cloud SaaS — REST metadata endpoint (port 443 / HTTPS) ──
    if erp_type == "oracle_fusion":
        url = f"https://{host}/fscmRestApi/resources/v1"
        try:
            async with httpx.AsyncClient(timeout=12, verify=False) as client:
                r = await client.get(url, auth=(username, password))
            if r.status_code == 200:
                return {"success": True,
                        "message": "Oracle Fusion Cloud REST API reachable — credentials valid"}
            if r.status_code in (401, 403):
                return {"success": False,
                        "message": f"Oracle Fusion REST API responded HTTP {r.status_code} — "
                                   "check username / password"}
            return {"success": False,
                    "message": f"Oracle Fusion REST API returned HTTP {r.status_code}"}
        except Exception as exc:
            return {"success": False,
                    "message": f"Cannot reach Oracle Fusion REST at {host}: {exc}"}

    # ── SAP S/4HANA — HANA REST ping ──────────────────────────────────────────
    if erp_type == "sap_s4hana":
        url = f"https://{host}:{port}/api/v1/ping"
        try:
            async with httpx.AsyncClient(timeout=8, verify=False) as client:
                r = await client.get(url, auth=(username, password))
            return {
                "success": r.status_code < 400,
                "message": (f"SAP HANA REST API reachable — HTTP {r.status_code}"
                            if r.status_code < 400
                            else f"SAP HANA responded HTTP {r.status_code}"),
            }
        except Exception as exc:
            return {"success": False, "message": f"Cannot reach SAP HANA {host}:{port}: {exc}"}

    # ── Dynamics 365 F&O — Azure AD client-credentials ────────────────────────
    if erp_type == "dynamics_365_fo":
        tenant_id     = body.get("tenant_id", "")
        client_id     = body.get("client_id", "")
        client_secret = body.get("client_secret", "")
        if tenant_id and client_id and client_secret:
            try:
                async with httpx.AsyncClient(timeout=12) as client:
                    r = await client.post(
                        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
                        data={
                            "grant_type":    "client_credentials",
                            "client_id":     client_id,
                            "client_secret": client_secret,
                            "scope":         f"https://{host}/.default",
                        },
                    )
                data = r.json()
                if "access_token" in data:
                    return {"success": True,
                            "message": "Azure AD token obtained — credentials valid"}
                return {"success": False,
                        "message": f"Azure AD auth failed: "
                                   f"{data.get('error_description', data.get('error', 'unknown'))}"}
            except Exception as exc:
                return {"success": False, "message": str(exc)}
        return {"success": False,
                "message": "tenant_id, client_id and client_secret are required for Dynamics 365"}

    # ── Generic fallback ───────────────────────────────────────────────────────
    return {
        "success":   True,
        "simulated": True,
        "message":   (f"Credentials saved for {erp_type}. "
                      "Live connection test is not available for this ERP type — "
                      "credentials will be used when deploying Fabric notebooks."),
    }


# ── Live Table Scan ────────────────────────────────────────────────────────────
@router.post("/scan-tables")
async def scan_tables(body: dict):
    """
    Verify which module tables actually exist in the connected ERP database/API.

    Oracle EBS      → queries ALL_TABLES via oracledb thin mode
    Oracle Fusion   → all standard views assumed present (REST API, not direct DB)
    SAP S/4HANA     → queries SYS.TABLES via hdbcli (if installed)
    Others          → returns all tables as "assumed found" with a note
    """
    erp_type  = body.get("erp_type", "")
    host      = body.get("host", "")
    port      = int(body.get("port") or 1521)
    service   = body.get("service", body.get("database", ""))
    username  = body.get("username", "")
    password  = body.get("password", "")
    schema    = body.get("schema", "")
    tables    = body.get("tables", [])

    if not tables:
        raise HTTPException(400, "tables list is required")

    # ── Oracle EBS — query ALL_TABLES ─────────────────────────────────────────
    if erp_type == "oracle_ebs":
        def _scan():
            import oracledb
            conn = oracledb.connect(user=username, password=password,
                                    dsn=f"{host}:{port}/{service}")
            try:
                cur = conn.cursor()
                upper_names = [t.upper() for t in tables]
                # Build bind variables  (:0, :1, …) to avoid SQL injection
                binds     = ", ".join(f":{i}" for i in range(len(upper_names)))
                bind_vals = {str(i): t for i, t in enumerate(upper_names)}
                if schema:
                    sql = (f"SELECT TABLE_NAME FROM ALL_TABLES "
                           f"WHERE TABLE_NAME IN ({binds}) "
                           f"AND OWNER = :owner")
                    bind_vals["owner"] = schema.upper()
                else:
                    sql = f"SELECT TABLE_NAME FROM ALL_TABLES WHERE TABLE_NAME IN ({binds})"
                cur.execute(sql, bind_vals)
                found_set = {row[0] for row in cur.fetchall()}
                cur.close()
                return found_set
            finally:
                conn.close()

        try:
            found_set = await asyncio.get_event_loop().run_in_executor(None, _scan)
            found     = [t for t in tables if t.upper() in found_set]
            missing   = [t for t in tables if t.upper() not in found_set]
            return {
                "found":   found,
                "missing": missing,
                "total":   len(tables),
                "scanned": len(tables),
                "method":  "live_oracle",
            }
        except ImportError:
            return {
                "found": [], "missing": tables,
                "total": len(tables), "scanned": 0, "method": "error",
                "error": "oracledb not installed in container. "
                         "Add 'oracledb' to requirements.txt and rebuild.",
            }
        except Exception as exc:
            return {
                "found": [], "missing": tables,
                "total": len(tables), "scanned": 0, "method": "error",
                "error": str(exc),
            }

    # ── Oracle Fusion Cloud — REST views, not direct DB tables ─────────────────
    if erp_type == "oracle_fusion":
        return {
            "found":   tables,
            "missing": [],
            "total":   len(tables),
            "scanned": len(tables),
            "method":  "assumed",
            "note":    "Oracle Fusion Cloud exposes data via REST views. "
                       "All standard module views are assumed present. "
                       "Verify access after deployment.",
        }

    # ── SAP S/4HANA — query SYS.TABLES via hdbcli ─────────────────────────────
    if erp_type == "sap_s4hana":
        def _scan_hana():
            import hdbcli.dbapi as hdb  # pip install hdbcli
            conn = hdb.connect(address=host, port=port,
                               user=username, password=password)
            try:
                cur = conn.cursor()
                names_sql = ", ".join(f"'{t.upper()}'" for t in tables)
                cur.execute(
                    f'SELECT TABLE_NAME FROM "SYS"."TABLES" '
                    f'WHERE TABLE_NAME IN ({names_sql})'
                )
                found_set = {row[0] for row in cur.fetchall()}
                cur.close()
                return found_set
            finally:
                conn.close()

        try:
            found_set = await asyncio.get_event_loop().run_in_executor(None, _scan_hana)
            found     = [t for t in tables if t.upper() in found_set]
            missing   = [t for t in tables if t.upper() not in found_set]
            return {
                "found": found, "missing": missing,
                "total": len(tables), "scanned": len(tables), "method": "live_hana",
            }
        except ImportError:
            pass   # hdbcli not installed — fall through to assumed
        except Exception as exc:
            return {
                "found": [], "missing": tables,
                "total": len(tables), "scanned": 0, "method": "error",
                "error": str(exc),
            }

    # ── Generic fallback — all tables assumed present ──────────────────────────
    return {
        "found":   tables,
        "missing": [],
        "total":   len(tables),
        "scanned": len(tables),
        "method":  "assumed",
        "note":    (f"Live table scan is not available for {erp_type}. "
                    "All industry-standard module tables are assumed present. "
                    "Tables will be verified when the Bronze notebook runs."),
    }


# ── ERP Config Persistence ─────────────────────────────────────────────────────
_ERP_CONFIG_FILE = get_data_dir() / "erp_configs.json"
_erp_configs: dict = {}  # {name: {name, source, module, conn_fields_sans_password}}

# Load on startup
try:
    if _ERP_CONFIG_FILE.exists():
        _erp_configs = json.loads(_ERP_CONFIG_FILE.read_text(encoding="utf-8"))
except Exception:
    pass


@router.post("/configure")
async def save_erp_config(body: dict):
    """Save ERP connection config (without password) for quick reuse."""
    name = body.get("name") or f"{body.get('source', '')}_{body.get('host', '')}"
    _erp_configs[name] = {
        "name":       name,
        "source":     body.get("source", ""),
        "sourceName": body.get("sourceName", ""),
        "module":     body.get("module", ""),
        "moduleName": body.get("moduleName", ""),
        "host":       body.get("host", ""),
        "port":       body.get("port", ""),
        "service":    body.get("service", ""),
        "username":   body.get("username", ""),
        # Never save password
        "saved_at":   datetime.utcnow().isoformat(),
    }
    try:
        _ERP_CONFIG_FILE.write_text(json.dumps(_erp_configs, indent=2), encoding="utf-8")
    except Exception:
        pass
    return {"status": "saved", "name": name}


@router.get("/configs")
async def list_erp_configs():
    """Return saved ERP connection configs."""
    return {"configs": list(_erp_configs.values()), "total": len(_erp_configs)}


# ── Field-to-Field Cross-ERP Comparison Data ──────────────────────────────────
# Logical field mappings: for each module, maps canonical field names → ERP-specific columns.
# Each mapping entry: {table, column, type, is_key, nullable}

_FIELD_COMPARISON: dict[str, dict] = {
    "GL": {
        "label": "General Ledger",
        "description": "Core accounting journal entries, balances and chart of accounts",
        "fields": [
            {
                "logical_name": "Journal Entry ID",
                "description": "Unique identifier for the journal entry header",
                "category": "Identity",
                "mappings": {
                    "oracle_fusion":   {"table": "GL_JE_HEADERS",          "column": "JE_HEADER_ID",      "type": "NUMBER(15)",    "is_key": True,  "nullable": False},
                    "oracle_ebs":      {"table": "GL_JE_HEADERS",          "column": "JE_HEADER_ID",      "type": "NUMBER(15)",    "is_key": True,  "nullable": False},
                    "sap_s4hana":      {"table": "ACDOCA",                  "column": "BELNR",             "type": "CHAR(10)",      "is_key": True,  "nullable": False},
                    "sap_ecc":         {"table": "BKPF",                    "column": "BELNR",             "type": "CHAR(10)",      "is_key": True,  "nullable": False},
                    "dynamics_365_fo": {"table": "LedgerJournalTable",      "column": "JOURNALNUM",        "type": "VARCHAR(20)",   "is_key": True,  "nullable": False},
                    "dynamics_bc":     {"table": "G/L Entry",               "column": "Document No.",      "type": "CODE(20)",      "is_key": True,  "nullable": False},
                    "netsuite":        {"table": "TRANSACTION",             "column": "ID",                "type": "INTEGER",       "is_key": True,  "nullable": False},
                    "workday":         {"table": "Journal_Entry",           "column": "Journal_Number",    "type": "VARCHAR(50)",   "is_key": True,  "nullable": False},
                },
            },
            {
                "logical_name": "Accounting Date",
                "description": "The date the journal entry affects the accounting books",
                "category": "Date/Time",
                "mappings": {
                    "oracle_fusion":   {"table": "GL_JE_HEADERS",          "column": "DEFAULT_EFFECTIVE_DATE", "type": "DATE",      "is_key": False, "nullable": False},
                    "oracle_ebs":      {"table": "GL_JE_HEADERS",          "column": "DEFAULT_EFFECTIVE_DATE", "type": "DATE",      "is_key": False, "nullable": False},
                    "sap_s4hana":      {"table": "ACDOCA",                  "column": "BUDAT",             "type": "DATS(8)",       "is_key": False, "nullable": False},
                    "sap_ecc":         {"table": "BKPF",                    "column": "BUDAT",             "type": "DATS(8)",       "is_key": False, "nullable": False},
                    "dynamics_365_fo": {"table": "LedgerJournalTrans",      "column": "TRANSDATE",         "type": "DATE",          "is_key": False, "nullable": False},
                    "dynamics_bc":     {"table": "G/L Entry",               "column": "Posting Date",      "type": "DATE",          "is_key": False, "nullable": False},
                    "netsuite":        {"table": "TRANSACTION",             "column": "TRANDATE",          "type": "DATE",          "is_key": False, "nullable": False},
                    "workday":         {"table": "Journal_Entry",           "column": "Accounting_Date",   "type": "DATE",          "is_key": False, "nullable": False},
                },
            },
            {
                "logical_name": "Accounting Period",
                "description": "Fiscal period the journal entry is posted to",
                "category": "Date/Time",
                "mappings": {
                    "oracle_fusion":   {"table": "GL_JE_HEADERS",          "column": "PERIOD_NAME",       "type": "VARCHAR2(15)",  "is_key": False, "nullable": False},
                    "oracle_ebs":      {"table": "GL_JE_HEADERS",          "column": "PERIOD_NAME",       "type": "VARCHAR2(15)",  "is_key": False, "nullable": False},
                    "sap_s4hana":      {"table": "ACDOCA",                  "column": "POPER",            "type": "NUMC(3)",        "is_key": False, "nullable": False},
                    "sap_ecc":         {"table": "BKPF",                    "column": "MONAT",            "type": "NUMC(2)",        "is_key": False, "nullable": False},
                    "dynamics_365_fo": {"table": "LedgerJournalTable",      "column": "PERIODDATEFROM",   "type": "DATE",           "is_key": False, "nullable": True},
                    "dynamics_bc":     {"table": "G/L Entry",               "column": "Posting Date",     "type": "DATE",           "is_key": False, "nullable": False},
                    "netsuite":        {"table": "TRANSACTION",             "column": "POSTINGPERIOD",    "type": "INTEGER",        "is_key": False, "nullable": False},
                    "workday":         {"table": "Journal_Entry",           "column": "Accounting_Period", "type": "VARCHAR(20)",   "is_key": False, "nullable": False},
                },
            },
            {
                "logical_name": "Company / Entity Code",
                "description": "Company or legal entity for the journal entry",
                "category": "Organisation",
                "mappings": {
                    "oracle_fusion":   {"table": "GL_LEDGERS",              "column": "LEDGER_ID",         "type": "NUMBER(15)",    "is_key": True,  "nullable": False},
                    "oracle_ebs":      {"table": "GL_LEDGERS",              "column": "LEDGER_ID",         "type": "NUMBER(15)",    "is_key": True,  "nullable": False},
                    "sap_s4hana":      {"table": "ACDOCA",                  "column": "BUKRS",             "type": "CHAR(4)",        "is_key": True,  "nullable": False},
                    "sap_ecc":         {"table": "BKPF",                    "column": "BUKRS",             "type": "CHAR(4)",        "is_key": True,  "nullable": False},
                    "dynamics_365_fo": {"table": "LedgerJournalTable",      "column": "DATAAREAID",        "type": "VARCHAR(4)",     "is_key": True,  "nullable": False},
                    "dynamics_bc":     {"table": "G/L Entry",               "column": "Company Name",      "type": "TEXT(30)",       "is_key": True,  "nullable": False},
                    "netsuite":        {"table": "TRANSACTION",             "column": "SUBSIDIARY",        "type": "INTEGER",        "is_key": True,  "nullable": False},
                    "workday":         {"table": "Journal_Entry",           "column": "Company",           "type": "VARCHAR(50)",    "is_key": True,  "nullable": False},
                },
            },
            {
                "logical_name": "GL Account Code",
                "description": "Natural account / chart of accounts segment",
                "category": "Account",
                "mappings": {
                    "oracle_fusion":   {"table": "GL_CODE_COMBINATIONS",    "column": "SEGMENT1",          "type": "VARCHAR2(25)",   "is_key": False, "nullable": False},
                    "oracle_ebs":      {"table": "GL_CODE_COMBINATIONS",    "column": "SEGMENT1",          "type": "VARCHAR2(25)",   "is_key": False, "nullable": False},
                    "sap_s4hana":      {"table": "ACDOCA",                  "column": "RACCT",             "type": "CHAR(10)",        "is_key": False, "nullable": False},
                    "sap_ecc":         {"table": "BSEG",                    "column": "HKONT",             "type": "CHAR(10)",        "is_key": False, "nullable": False},
                    "dynamics_365_fo": {"table": "MainAccount",             "column": "DISPLAYVALUE",      "type": "VARCHAR(20)",    "is_key": False, "nullable": False},
                    "dynamics_bc":     {"table": "G/L Account",             "column": "No.",               "type": "CODE(20)",        "is_key": False, "nullable": False},
                    "netsuite":        {"table": "ACCOUNT",                 "column": "ACCTNUMBER",        "type": "VARCHAR(10)",     "is_key": False, "nullable": False},
                    "workday":         {"table": "Ledger_Account",          "column": "Account_ID",        "type": "VARCHAR(30)",     "is_key": False, "nullable": False},
                },
            },
            {
                "logical_name": "Debit Amount",
                "description": "Debit (Dr) amount in functional currency",
                "category": "Amount",
                "mappings": {
                    "oracle_fusion":   {"table": "GL_JE_LINES",             "column": "ENTERED_DR",        "type": "NUMBER",         "is_key": False, "nullable": True},
                    "oracle_ebs":      {"table": "GL_JE_LINES",             "column": "ENTERED_DR",        "type": "NUMBER",         "is_key": False, "nullable": True},
                    "sap_s4hana":      {"table": "ACDOCA",                  "column": "HSL",               "type": "CURR(23,2)",      "is_key": False, "nullable": True},
                    "sap_ecc":         {"table": "BSEG",                    "column": "DMBTR",             "type": "CURR(13,2)",      "is_key": False, "nullable": True},
                    "dynamics_365_fo": {"table": "LedgerJournalTrans",      "column": "AMOUNTCURDEBIT",    "type": "REAL",            "is_key": False, "nullable": True},
                    "dynamics_bc":     {"table": "G/L Entry",               "column": "Debit Amount",      "type": "DECIMAL(38,20)",  "is_key": False, "nullable": True},
                    "netsuite":        {"table": "TRANSACTION_LINES",       "column": "DEBITAMOUNT",       "type": "NUMERIC(28,2)",   "is_key": False, "nullable": True},
                    "workday":         {"table": "Journal_Line",            "column": "Debit_Amount",      "type": "DECIMAL(18,6)",   "is_key": False, "nullable": True},
                },
            },
            {
                "logical_name": "Credit Amount",
                "description": "Credit (Cr) amount in functional currency",
                "category": "Amount",
                "mappings": {
                    "oracle_fusion":   {"table": "GL_JE_LINES",             "column": "ENTERED_CR",        "type": "NUMBER",         "is_key": False, "nullable": True},
                    "oracle_ebs":      {"table": "GL_JE_LINES",             "column": "ENTERED_CR",        "type": "NUMBER",         "is_key": False, "nullable": True},
                    "sap_s4hana":      {"table": "ACDOCA",                  "column": "KSL",               "type": "CURR(23,2)",      "is_key": False, "nullable": True},
                    "sap_ecc":         {"table": "BSEG",                    "column": "WRBTR",             "type": "CURR(13,2)",      "is_key": False, "nullable": True},
                    "dynamics_365_fo": {"table": "LedgerJournalTrans",      "column": "AMOUNTCURCREDIT",   "type": "REAL",            "is_key": False, "nullable": True},
                    "dynamics_bc":     {"table": "G/L Entry",               "column": "Credit Amount",     "type": "DECIMAL(38,20)",  "is_key": False, "nullable": True},
                    "netsuite":        {"table": "TRANSACTION_LINES",       "column": "CREDITAMOUNT",      "type": "NUMERIC(28,2)",   "is_key": False, "nullable": True},
                    "workday":         {"table": "Journal_Line",            "column": "Credit_Amount",     "type": "DECIMAL(18,6)",   "is_key": False, "nullable": True},
                },
            },
            {
                "logical_name": "Currency Code",
                "description": "Transaction currency (ISO 4217 code)",
                "category": "Currency",
                "mappings": {
                    "oracle_fusion":   {"table": "GL_JE_HEADERS",          "column": "CURRENCY_CODE",     "type": "VARCHAR2(15)",   "is_key": False, "nullable": False},
                    "oracle_ebs":      {"table": "GL_JE_HEADERS",          "column": "CURRENCY_CODE",     "type": "VARCHAR2(15)",   "is_key": False, "nullable": False},
                    "sap_s4hana":      {"table": "ACDOCA",                  "column": "RHCUR",            "type": "CUKY(5)",         "is_key": False, "nullable": False},
                    "sap_ecc":         {"table": "BKPF",                    "column": "WAERS",            "type": "CUKY(5)",         "is_key": False, "nullable": False},
                    "dynamics_365_fo": {"table": "LedgerJournalTrans",      "column": "CURRENCYCODE",     "type": "VARCHAR(3)",      "is_key": False, "nullable": False},
                    "dynamics_bc":     {"table": "G/L Entry",               "column": "Currency Code",    "type": "CODE(10)",        "is_key": False, "nullable": True},
                    "netsuite":        {"table": "TRANSACTION",             "column": "CURRENCY",         "type": "INTEGER",         "is_key": False, "nullable": False},
                    "workday":         {"table": "Journal_Entry",           "column": "Currency",         "type": "VARCHAR(3)",      "is_key": False, "nullable": False},
                },
            },
            {
                "logical_name": "Journal Description",
                "description": "Free-text description of the journal entry",
                "category": "Description",
                "mappings": {
                    "oracle_fusion":   {"table": "GL_JE_HEADERS",          "column": "DESCRIPTION",       "type": "VARCHAR2(240)",  "is_key": False, "nullable": True},
                    "oracle_ebs":      {"table": "GL_JE_HEADERS",          "column": "DESCRIPTION",       "type": "VARCHAR2(240)",  "is_key": False, "nullable": True},
                    "sap_s4hana":      {"table": "ACDOCA",                  "column": "SGTXT",            "type": "CHAR(50)",        "is_key": False, "nullable": True},
                    "sap_ecc":         {"table": "BSEG",                    "column": "SGTXT",            "type": "CHAR(50)",        "is_key": False, "nullable": True},
                    "dynamics_365_fo": {"table": "LedgerJournalTrans",      "column": "TXT",              "type": "VARCHAR(30)",     "is_key": False, "nullable": True},
                    "dynamics_bc":     {"table": "G/L Entry",               "column": "Description",      "type": "TEXT(100)",       "is_key": False, "nullable": True},
                    "netsuite":        {"table": "TRANSACTION",             "column": "MEMO",             "type": "VARCHAR(999)",    "is_key": False, "nullable": True},
                    "workday":         {"table": "Journal_Entry",           "column": "Memo",             "type": "VARCHAR(4000)",   "is_key": False, "nullable": True},
                },
            },
            {
                "logical_name": "Cost Center",
                "description": "Cost centre / profit centre segment",
                "category": "Organisation",
                "mappings": {
                    "oracle_fusion":   {"table": "GL_CODE_COMBINATIONS",    "column": "SEGMENT2",          "type": "VARCHAR2(25)",   "is_key": False, "nullable": True},
                    "oracle_ebs":      {"table": "GL_CODE_COMBINATIONS",    "column": "SEGMENT2",          "type": "VARCHAR2(25)",   "is_key": False, "nullable": True},
                    "sap_s4hana":      {"table": "ACDOCA",                  "column": "KOSTL",            "type": "CHAR(10)",        "is_key": False, "nullable": True},
                    "sap_ecc":         {"table": "BSEG",                    "column": "KOSTL",            "type": "CHAR(10)",        "is_key": False, "nullable": True},
                    "dynamics_365_fo": {"table": "LedgerJournalTrans",      "column": "DIMENSIONDISPLAYVALUE", "type": "VARCHAR(255)", "is_key": False, "nullable": True},
                    "dynamics_bc":     {"table": "G/L Entry",               "column": "Department Code",  "type": "CODE(20)",        "is_key": False, "nullable": True},
                    "netsuite":        {"table": "TRANSACTION_LINES",       "column": "DEPARTMENT",       "type": "INTEGER",         "is_key": False, "nullable": True},
                    "workday":         {"table": "Journal_Line",            "column": "Cost_Center",      "type": "VARCHAR(50)",     "is_key": False, "nullable": True},
                },
            },
            {
                "logical_name": "Created By User",
                "description": "User who created the journal entry",
                "category": "Audit",
                "mappings": {
                    "oracle_fusion":   {"table": "GL_JE_HEADERS",          "column": "CREATED_BY",        "type": "VARCHAR2(64)",   "is_key": False, "nullable": False},
                    "oracle_ebs":      {"table": "GL_JE_HEADERS",          "column": "CREATED_BY",        "type": "NUMBER(15)",     "is_key": False, "nullable": False},
                    "sap_s4hana":      {"table": "ACDOCA",                  "column": "USNAM",            "type": "CHAR(12)",        "is_key": False, "nullable": True},
                    "sap_ecc":         {"table": "BKPF",                    "column": "USNAM",            "type": "CHAR(12)",        "is_key": False, "nullable": True},
                    "dynamics_365_fo": {"table": "LedgerJournalTable",      "column": "CREATEDBY",        "type": "VARCHAR(8)",      "is_key": False, "nullable": False},
                    "dynamics_bc":     {"table": "G/L Entry",               "column": "User ID",          "type": "CODE(50)",        "is_key": False, "nullable": True},
                    "netsuite":        {"table": "TRANSACTION",             "column": "CREATEDBY",        "type": "INTEGER",         "is_key": False, "nullable": True},
                    "workday":         {"table": "Journal_Entry",           "column": "Entered_By",       "type": "VARCHAR(100)",    "is_key": False, "nullable": True},
                },
            },
            {
                "logical_name": "Posted / Unposted Status",
                "description": "Whether the journal has been posted to the GL",
                "category": "Status",
                "mappings": {
                    "oracle_fusion":   {"table": "GL_JE_HEADERS",          "column": "STATUS",            "type": "VARCHAR2(1)",    "is_key": False, "nullable": False},
                    "oracle_ebs":      {"table": "GL_JE_HEADERS",          "column": "STATUS",            "type": "VARCHAR2(1)",    "is_key": False, "nullable": False},
                    "sap_s4hana":      {"table": "ACDOCA",                  "column": "BSTAT",            "type": "CHAR(1)",         "is_key": False, "nullable": True},
                    "sap_ecc":         {"table": "BKPF",                    "column": "BSTAT",            "type": "CHAR(1)",         "is_key": False, "nullable": True},
                    "dynamics_365_fo": {"table": "LedgerJournalTable",      "column": "POSTED",           "type": "ENUM",            "is_key": False, "nullable": False},
                    "dynamics_bc":     {"table": "G/L Entry",               "column": "Open",             "type": "BOOLEAN",         "is_key": False, "nullable": False},
                    "netsuite":        {"table": "TRANSACTION",             "column": "STATUS",           "type": "VARCHAR(10)",     "is_key": False, "nullable": False},
                    "workday":         {"table": "Journal_Entry",           "column": "Status",           "type": "VARCHAR(50)",     "is_key": False, "nullable": False},
                },
            },
        ],
    },
    "AR": {
        "label": "Accounts Receivable",
        "description": "Customer invoices, receipts and collections",
        "fields": [
            {
                "logical_name": "Invoice Number",
                "description": "Unique identifier/number for the customer invoice",
                "category": "Identity",
                "mappings": {
                    "oracle_fusion":   {"table": "RA_CUSTOMER_TRX_ALL",     "column": "TRX_NUMBER",        "type": "VARCHAR2(20)",   "is_key": True,  "nullable": False},
                    "oracle_ebs":      {"table": "RA_CUSTOMER_TRX_ALL",     "column": "TRX_NUMBER",        "type": "VARCHAR2(20)",   "is_key": True,  "nullable": False},
                    "sap_s4hana":      {"table": "BKPF",                    "column": "BELNR",             "type": "CHAR(10)",        "is_key": True,  "nullable": False},
                    "sap_ecc":         {"table": "BKPF",                    "column": "BELNR",             "type": "CHAR(10)",        "is_key": True,  "nullable": False},
                    "dynamics_365_fo": {"table": "CustInvoiceJour",         "column": "INVOICEID",         "type": "VARCHAR(20)",    "is_key": True,  "nullable": False},
                    "dynamics_bc":     {"table": "Sales Invoice Header",    "column": "No.",               "type": "CODE(20)",        "is_key": True,  "nullable": False},
                    "netsuite":        {"table": "TRANSACTION",             "column": "TRANID",            "type": "VARCHAR(64)",    "is_key": True,  "nullable": False},
                    "workday":         {"table": "Customer_Invoice",        "column": "Invoice_Number",    "type": "VARCHAR(50)",    "is_key": True,  "nullable": False},
                },
            },
            {
                "logical_name": "Customer ID",
                "description": "Unique identifier for the customer/party",
                "category": "Identity",
                "mappings": {
                    "oracle_fusion":   {"table": "HZ_CUST_ACCOUNTS",        "column": "CUST_ACCOUNT_ID",   "type": "NUMBER(15)",     "is_key": True,  "nullable": False},
                    "oracle_ebs":      {"table": "HZ_CUST_ACCOUNTS",        "column": "CUST_ACCOUNT_ID",   "type": "NUMBER(15)",     "is_key": True,  "nullable": False},
                    "sap_s4hana":      {"table": "KNA1",                    "column": "KUNNR",             "type": "CHAR(10)",        "is_key": True,  "nullable": False},
                    "sap_ecc":         {"table": "KNA1",                    "column": "KUNNR",             "type": "CHAR(10)",        "is_key": True,  "nullable": False},
                    "dynamics_365_fo": {"table": "CustTable",               "column": "ACCOUNTNUM",        "type": "VARCHAR(20)",    "is_key": True,  "nullable": False},
                    "dynamics_bc":     {"table": "Customer",                "column": "No.",               "type": "CODE(20)",        "is_key": True,  "nullable": False},
                    "netsuite":        {"table": "CUSTOMER",                "column": "ID",                "type": "INTEGER",        "is_key": True,  "nullable": False},
                    "workday":         {"table": "Customer",                "column": "Customer_ID",       "type": "VARCHAR(50)",    "is_key": True,  "nullable": False},
                },
            },
            {
                "logical_name": "Invoice Date",
                "description": "Date the invoice was created/issued",
                "category": "Date/Time",
                "mappings": {
                    "oracle_fusion":   {"table": "RA_CUSTOMER_TRX_ALL",     "column": "TRX_DATE",          "type": "DATE",           "is_key": False, "nullable": False},
                    "oracle_ebs":      {"table": "RA_CUSTOMER_TRX_ALL",     "column": "TRX_DATE",          "type": "DATE",           "is_key": False, "nullable": False},
                    "sap_s4hana":      {"table": "BKPF",                    "column": "BLDAT",             "type": "DATS(8)",         "is_key": False, "nullable": False},
                    "sap_ecc":         {"table": "BKPF",                    "column": "BLDAT",             "type": "DATS(8)",         "is_key": False, "nullable": False},
                    "dynamics_365_fo": {"table": "CustInvoiceJour",         "column": "INVOICEDATE",       "type": "DATE",           "is_key": False, "nullable": False},
                    "dynamics_bc":     {"table": "Sales Invoice Header",    "column": "Document Date",     "type": "DATE",           "is_key": False, "nullable": False},
                    "netsuite":        {"table": "TRANSACTION",             "column": "TRANDATE",          "type": "DATE",           "is_key": False, "nullable": False},
                    "workday":         {"table": "Customer_Invoice",        "column": "Invoice_Date",      "type": "DATE",           "is_key": False, "nullable": False},
                },
            },
            {
                "logical_name": "Due Date",
                "description": "Payment due date based on payment terms",
                "category": "Date/Time",
                "mappings": {
                    "oracle_fusion":   {"table": "AR_PAYMENT_SCHEDULES_ALL","column": "DUE_DATE",          "type": "DATE",           "is_key": False, "nullable": False},
                    "oracle_ebs":      {"table": "AR_PAYMENT_SCHEDULES_ALL","column": "DUE_DATE",          "type": "DATE",           "is_key": False, "nullable": False},
                    "sap_s4hana":      {"table": "BSID",                    "column": "FAEDT",             "type": "DATS(8)",         "is_key": False, "nullable": True},
                    "sap_ecc":         {"table": "BSID",                    "column": "FAEDT",             "type": "DATS(8)",         "is_key": False, "nullable": True},
                    "dynamics_365_fo": {"table": "CustInvoiceJour",         "column": "DUEDATE",           "type": "DATE",           "is_key": False, "nullable": True},
                    "dynamics_bc":     {"table": "Sales Invoice Header",    "column": "Due Date",          "type": "DATE",           "is_key": False, "nullable": True},
                    "netsuite":        {"table": "TRANSACTION",             "column": "DUEDATE",           "type": "DATE",           "is_key": False, "nullable": True},
                    "workday":         {"table": "Customer_Invoice",        "column": "Due_Date",          "type": "DATE",           "is_key": False, "nullable": True},
                },
            },
            {
                "logical_name": "Invoice Amount",
                "description": "Total invoice amount in transaction currency",
                "category": "Amount",
                "mappings": {
                    "oracle_fusion":   {"table": "RA_CUSTOMER_TRX_ALL",     "column": "INVOICE_CURRENCY_CODE", "type": "VARCHAR2(15)", "is_key": False, "nullable": False},
                    "oracle_ebs":      {"table": "AR_PAYMENT_SCHEDULES_ALL","column": "AMOUNT_DUE_ORIGINAL", "type": "NUMBER",       "is_key": False, "nullable": False},
                    "sap_s4hana":      {"table": "BKPF",                    "column": "DMBTR",             "type": "CURR(13,2)",     "is_key": False, "nullable": True},
                    "sap_ecc":         {"table": "BKPF",                    "column": "DMBTR",             "type": "CURR(13,2)",     "is_key": False, "nullable": True},
                    "dynamics_365_fo": {"table": "CustInvoiceJour",         "column": "INVOICEAMOUNT",     "type": "REAL",           "is_key": False, "nullable": False},
                    "dynamics_bc":     {"table": "Sales Invoice Header",    "column": "Amount Including VAT", "type": "DECIMAL(38,20)", "is_key": False, "nullable": False},
                    "netsuite":        {"table": "TRANSACTION",             "column": "TOTAL",             "type": "NUMERIC(28,2)",  "is_key": False, "nullable": False},
                    "workday":         {"table": "Customer_Invoice",        "column": "Total_Amount",      "type": "DECIMAL(18,6)",  "is_key": False, "nullable": False},
                },
            },
            {
                "logical_name": "Outstanding Balance",
                "description": "Amount remaining unpaid on the invoice",
                "category": "Amount",
                "mappings": {
                    "oracle_fusion":   {"table": "AR_PAYMENT_SCHEDULES_ALL","column": "AMOUNT_DUE_REMAINING", "type": "NUMBER",     "is_key": False, "nullable": False},
                    "oracle_ebs":      {"table": "AR_PAYMENT_SCHEDULES_ALL","column": "AMOUNT_DUE_REMAINING", "type": "NUMBER",     "is_key": False, "nullable": False},
                    "sap_s4hana":      {"table": "BSID",                    "column": "DMBTR",             "type": "CURR(13,2)",     "is_key": False, "nullable": True},
                    "sap_ecc":         {"table": "BSID",                    "column": "DMBTR",             "type": "CURR(13,2)",     "is_key": False, "nullable": True},
                    "dynamics_365_fo": {"table": "CustInvoiceJour",         "column": "REMAINAMOUNT",      "type": "REAL",           "is_key": False, "nullable": True},
                    "dynamics_bc":     {"table": "Cust. Ledger Entry",      "column": "Remaining Amount",  "type": "DECIMAL(38,20)", "is_key": False, "nullable": True},
                    "netsuite":        {"table": "TRANSACTION",             "column": "AMOUNTREMAINING",   "type": "NUMERIC(28,2)",  "is_key": False, "nullable": True},
                    "workday":         {"table": "Customer_Invoice",        "column": "Unpaid_Amount",     "type": "DECIMAL(18,6)",  "is_key": False, "nullable": True},
                },
            },
            {
                "logical_name": "Payment Terms",
                "description": "Payment terms code (e.g. NET30, 2/10 NET30)",
                "category": "Terms",
                "mappings": {
                    "oracle_fusion":   {"table": "RA_CUSTOMER_TRX_ALL",     "column": "TERM_ID",           "type": "NUMBER(15)",     "is_key": False, "nullable": True},
                    "oracle_ebs":      {"table": "RA_CUSTOMER_TRX_ALL",     "column": "TERM_ID",           "type": "NUMBER(15)",     "is_key": False, "nullable": True},
                    "sap_s4hana":      {"table": "KNA1",                    "column": "ZTERM",             "type": "CHAR(4)",         "is_key": False, "nullable": True},
                    "sap_ecc":         {"table": "KNA1",                    "column": "ZTERM",             "type": "CHAR(4)",         "is_key": False, "nullable": True},
                    "dynamics_365_fo": {"table": "CustTable",               "column": "PAYMTERMID",        "type": "VARCHAR(10)",    "is_key": False, "nullable": True},
                    "dynamics_bc":     {"table": "Customer",                "column": "Payment Terms Code", "type": "CODE(10)",      "is_key": False, "nullable": True},
                    "netsuite":        {"table": "CUSTOMER",                "column": "TERMS",             "type": "INTEGER",        "is_key": False, "nullable": True},
                    "workday":         {"table": "Customer",                "column": "Payment_Terms",     "type": "VARCHAR(50)",    "is_key": False, "nullable": True},
                },
            },
            {
                "logical_name": "Invoice Status",
                "description": "Current status of the invoice (Open, Closed, Void, etc.)",
                "category": "Status",
                "mappings": {
                    "oracle_fusion":   {"table": "AR_PAYMENT_SCHEDULES_ALL","column": "STATUS",            "type": "VARCHAR2(15)",   "is_key": False, "nullable": False},
                    "oracle_ebs":      {"table": "AR_PAYMENT_SCHEDULES_ALL","column": "STATUS",            "type": "VARCHAR2(15)",   "is_key": False, "nullable": False},
                    "sap_s4hana":      {"table": "BKPF",                    "column": "BSTAT",             "type": "CHAR(1)",         "is_key": False, "nullable": True},
                    "sap_ecc":         {"table": "BKPF",                    "column": "BSTAT",             "type": "CHAR(1)",         "is_key": False, "nullable": True},
                    "dynamics_365_fo": {"table": "CustInvoiceJour",         "column": "STATUS",            "type": "ENUM",           "is_key": False, "nullable": False},
                    "dynamics_bc":     {"table": "Sales Invoice Header",    "column": "Status",            "type": "OPTION",         "is_key": False, "nullable": False},
                    "netsuite":        {"table": "TRANSACTION",             "column": "STATUS",            "type": "VARCHAR(10)",    "is_key": False, "nullable": False},
                    "workday":         {"table": "Customer_Invoice",        "column": "Invoice_Status",    "type": "VARCHAR(50)",    "is_key": False, "nullable": False},
                },
            },
        ],
    },
    "AP": {
        "label": "Accounts Payable",
        "description": "Supplier invoices, payments and holds",
        "fields": [
            {
                "logical_name": "Invoice Number",
                "description": "Supplier-assigned invoice number",
                "category": "Identity",
                "mappings": {
                    "oracle_fusion":   {"table": "AP_INVOICES_ALL",         "column": "INVOICE_NUM",       "type": "VARCHAR2(50)",   "is_key": True,  "nullable": False},
                    "oracle_ebs":      {"table": "AP_INVOICES_ALL",         "column": "INVOICE_NUM",       "type": "VARCHAR2(50)",   "is_key": True,  "nullable": False},
                    "sap_s4hana":      {"table": "BKPF",                    "column": "BELNR",             "type": "CHAR(10)",        "is_key": True,  "nullable": False},
                    "sap_ecc":         {"table": "BKPF",                    "column": "BELNR",             "type": "CHAR(10)",        "is_key": True,  "nullable": False},
                    "dynamics_365_fo": {"table": "VendInvoiceJour",         "column": "INVOICEID",         "type": "VARCHAR(20)",    "is_key": True,  "nullable": False},
                    "dynamics_bc":     {"table": "Purch. Inv. Header",      "column": "No.",               "type": "CODE(20)",        "is_key": True,  "nullable": False},
                    "netsuite":        {"table": "TRANSACTION",             "column": "TRANID",            "type": "VARCHAR(64)",    "is_key": True,  "nullable": False},
                    "workday":         {"table": "Supplier_Invoice",        "column": "Invoice_Number",    "type": "VARCHAR(50)",    "is_key": True,  "nullable": False},
                },
            },
            {
                "logical_name": "Supplier ID",
                "description": "Unique identifier for the supplier/vendor",
                "category": "Identity",
                "mappings": {
                    "oracle_fusion":   {"table": "AP_SUPPLIERS",            "column": "VENDOR_ID",         "type": "NUMBER(15)",     "is_key": True,  "nullable": False},
                    "oracle_ebs":      {"table": "AP_SUPPLIERS",            "column": "VENDOR_ID",         "type": "NUMBER(15)",     "is_key": True,  "nullable": False},
                    "sap_s4hana":      {"table": "LFA1",                    "column": "LIFNR",             "type": "CHAR(10)",        "is_key": True,  "nullable": False},
                    "sap_ecc":         {"table": "LFA1",                    "column": "LIFNR",             "type": "CHAR(10)",        "is_key": True,  "nullable": False},
                    "dynamics_365_fo": {"table": "VendTable",               "column": "ACCOUNTNUM",        "type": "VARCHAR(20)",    "is_key": True,  "nullable": False},
                    "dynamics_bc":     {"table": "Vendor",                  "column": "No.",               "type": "CODE(20)",        "is_key": True,  "nullable": False},
                    "netsuite":        {"table": "VENDOR",                  "column": "ID",                "type": "INTEGER",        "is_key": True,  "nullable": False},
                    "workday":         {"table": "Supplier",                "column": "Supplier_ID",       "type": "VARCHAR(50)",    "is_key": True,  "nullable": False},
                },
            },
            {
                "logical_name": "Invoice Date",
                "description": "Date on the supplier invoice",
                "category": "Date/Time",
                "mappings": {
                    "oracle_fusion":   {"table": "AP_INVOICES_ALL",         "column": "INVOICE_DATE",      "type": "DATE",           "is_key": False, "nullable": False},
                    "oracle_ebs":      {"table": "AP_INVOICES_ALL",         "column": "INVOICE_DATE",      "type": "DATE",           "is_key": False, "nullable": False},
                    "sap_s4hana":      {"table": "BKPF",                    "column": "BLDAT",             "type": "DATS(8)",         "is_key": False, "nullable": False},
                    "sap_ecc":         {"table": "BKPF",                    "column": "BLDAT",             "type": "DATS(8)",         "is_key": False, "nullable": False},
                    "dynamics_365_fo": {"table": "VendInvoiceJour",         "column": "INVOICEDATE",       "type": "DATE",           "is_key": False, "nullable": False},
                    "dynamics_bc":     {"table": "Purch. Inv. Header",      "column": "Document Date",     "type": "DATE",           "is_key": False, "nullable": False},
                    "netsuite":        {"table": "TRANSACTION",             "column": "TRANDATE",          "type": "DATE",           "is_key": False, "nullable": False},
                    "workday":         {"table": "Supplier_Invoice",        "column": "Invoice_Date",      "type": "DATE",           "is_key": False, "nullable": False},
                },
            },
            {
                "logical_name": "Invoice Amount",
                "description": "Total invoice amount in transaction currency",
                "category": "Amount",
                "mappings": {
                    "oracle_fusion":   {"table": "AP_INVOICES_ALL",         "column": "INVOICE_AMOUNT",    "type": "NUMBER",         "is_key": False, "nullable": False},
                    "oracle_ebs":      {"table": "AP_INVOICES_ALL",         "column": "INVOICE_AMOUNT",    "type": "NUMBER",         "is_key": False, "nullable": False},
                    "sap_s4hana":      {"table": "BKPF",                    "column": "DMBTR",             "type": "CURR(13,2)",     "is_key": False, "nullable": True},
                    "sap_ecc":         {"table": "BKPF",                    "column": "DMBTR",             "type": "CURR(13,2)",     "is_key": False, "nullable": True},
                    "dynamics_365_fo": {"table": "VendInvoiceJour",         "column": "INVOICEAMOUNT",     "type": "REAL",           "is_key": False, "nullable": False},
                    "dynamics_bc":     {"table": "Purch. Inv. Header",      "column": "Amount Including VAT", "type": "DECIMAL(38,20)", "is_key": False, "nullable": False},
                    "netsuite":        {"table": "TRANSACTION",             "column": "TOTAL",             "type": "NUMERIC(28,2)",  "is_key": False, "nullable": False},
                    "workday":         {"table": "Supplier_Invoice",        "column": "Total_Amount",      "type": "DECIMAL(18,6)",  "is_key": False, "nullable": False},
                },
            },
            {
                "logical_name": "Due Date",
                "description": "Payment due date per payment terms",
                "category": "Date/Time",
                "mappings": {
                    "oracle_fusion":   {"table": "AP_PAYMENT_SCHEDULES_ALL","column": "DUE_DATE",          "type": "DATE",           "is_key": False, "nullable": True},
                    "oracle_ebs":      {"table": "AP_PAYMENT_SCHEDULES_ALL","column": "DUE_DATE",          "type": "DATE",           "is_key": False, "nullable": True},
                    "sap_s4hana":      {"table": "BSIK",                    "column": "FAEDT",             "type": "DATS(8)",         "is_key": False, "nullable": True},
                    "sap_ecc":         {"table": "BSIK",                    "column": "FAEDT",             "type": "DATS(8)",         "is_key": False, "nullable": True},
                    "dynamics_365_fo": {"table": "VendInvoiceJour",         "column": "DUEDATE",           "type": "DATE",           "is_key": False, "nullable": True},
                    "dynamics_bc":     {"table": "Purch. Inv. Header",      "column": "Due Date",          "type": "DATE",           "is_key": False, "nullable": True},
                    "netsuite":        {"table": "TRANSACTION",             "column": "DUEDATE",           "type": "DATE",           "is_key": False, "nullable": True},
                    "workday":         {"table": "Supplier_Invoice",        "column": "Due_Date",          "type": "DATE",           "is_key": False, "nullable": True},
                },
            },
            {
                "logical_name": "Payment Status",
                "description": "Whether the invoice is paid, unpaid, partially paid",
                "category": "Status",
                "mappings": {
                    "oracle_fusion":   {"table": "AP_INVOICES_ALL",         "column": "PAYMENT_STATUS_FLAG", "type": "VARCHAR2(1)", "is_key": False, "nullable": True},
                    "oracle_ebs":      {"table": "AP_INVOICES_ALL",         "column": "PAYMENT_STATUS_FLAG", "type": "VARCHAR2(1)", "is_key": False, "nullable": True},
                    "sap_s4hana":      {"table": "BKPF",                    "column": "BSTAT",             "type": "CHAR(1)",        "is_key": False, "nullable": True},
                    "sap_ecc":         {"table": "BKPF",                    "column": "BSTAT",             "type": "CHAR(1)",        "is_key": False, "nullable": True},
                    "dynamics_365_fo": {"table": "VendInvoiceJour",         "column": "APPROVED",          "type": "ENUM",           "is_key": False, "nullable": False},
                    "dynamics_bc":     {"table": "Purch. Inv. Header",      "column": "Status",            "type": "OPTION",         "is_key": False, "nullable": False},
                    "netsuite":        {"table": "TRANSACTION",             "column": "STATUS",            "type": "VARCHAR(10)",    "is_key": False, "nullable": False},
                    "workday":         {"table": "Supplier_Invoice",        "column": "Payment_Status",    "type": "VARCHAR(50)",    "is_key": False, "nullable": False},
                },
            },
            {
                "logical_name": "GL Account (Expense)",
                "description": "GL account charged for the expense/liability",
                "category": "Account",
                "mappings": {
                    "oracle_fusion":   {"table": "AP_INVOICE_DISTRIBUTIONS_ALL", "column": "DIST_CODE_COMBINATION_ID", "type": "NUMBER(15)", "is_key": False, "nullable": True},
                    "oracle_ebs":      {"table": "AP_INVOICE_DISTRIBUTIONS_ALL", "column": "DIST_CODE_COMBINATION_ID", "type": "NUMBER(15)", "is_key": False, "nullable": True},
                    "sap_s4hana":      {"table": "BSEG",                    "column": "HKONT",             "type": "CHAR(10)",       "is_key": False, "nullable": True},
                    "sap_ecc":         {"table": "BSEG",                    "column": "HKONT",             "type": "CHAR(10)",       "is_key": False, "nullable": True},
                    "dynamics_365_fo": {"table": "LedgerJournalTrans",      "column": "OFFSETLEDGERDIMENSION", "type": "BIGINT",     "is_key": False, "nullable": True},
                    "dynamics_bc":     {"table": "Purch. Inv. Line",        "column": "No.",               "type": "CODE(20)",       "is_key": False, "nullable": True},
                    "netsuite":        {"table": "TRANSACTION_LINES",       "column": "ACCOUNT",           "type": "INTEGER",        "is_key": False, "nullable": True},
                    "workday":         {"table": "Supplier_Invoice_Line",   "column": "Spend_Category",    "type": "VARCHAR(50)",    "is_key": False, "nullable": True},
                },
            },
        ],
    },
    "INV": {
        "label": "Inventory Management",
        "description": "Item master, on-hand quantities and stock transactions",
        "fields": [
            {
                "logical_name": "Item / Material Number",
                "description": "Unique identifier for the inventory item or material",
                "category": "Identity",
                "mappings": {
                    "oracle_fusion":   {"table": "EGP_SYSTEM_ITEMS_B",      "column": "INVENTORY_ITEM_ID", "type": "NUMBER(15)",     "is_key": True,  "nullable": False},
                    "oracle_ebs":      {"table": "MTL_SYSTEM_ITEMS_B",      "column": "INVENTORY_ITEM_ID", "type": "NUMBER(15)",     "is_key": True,  "nullable": False},
                    "sap_s4hana":      {"table": "MARA",                    "column": "MATNR",             "type": "CHAR(40)",        "is_key": True,  "nullable": False},
                    "sap_ecc":         {"table": "MARA",                    "column": "MATNR",             "type": "CHAR(18)",        "is_key": True,  "nullable": False},
                    "dynamics_365_fo": {"table": "InventTable",             "column": "ITEMID",            "type": "VARCHAR(20)",    "is_key": True,  "nullable": False},
                    "dynamics_bc":     {"table": "Item",                    "column": "No.",               "type": "CODE(20)",        "is_key": True,  "nullable": False},
                    "netsuite":        {"table": "ITEM",                    "column": "ID",                "type": "INTEGER",        "is_key": True,  "nullable": False},
                    "workday":         {"table": "Inventory_Item",          "column": "Item_ID",           "type": "VARCHAR(50)",    "is_key": True,  "nullable": False},
                },
            },
            {
                "logical_name": "Item Description",
                "description": "Short description / name of the inventory item",
                "category": "Description",
                "mappings": {
                    "oracle_fusion":   {"table": "EGP_SYSTEM_ITEMS_TL",     "column": "DESCRIPTION",       "type": "VARCHAR2(240)",  "is_key": False, "nullable": True},
                    "oracle_ebs":      {"table": "MTL_SYSTEM_ITEMS_B",      "column": "DESCRIPTION",       "type": "VARCHAR2(240)",  "is_key": False, "nullable": True},
                    "sap_s4hana":      {"table": "MAKT",                    "column": "MAKTX",             "type": "CHAR(40)",        "is_key": False, "nullable": True},
                    "sap_ecc":         {"table": "MAKT",                    "column": "MAKTX",             "type": "CHAR(40)",        "is_key": False, "nullable": True},
                    "dynamics_365_fo": {"table": "InventTable",             "column": "NAMEALIAS",         "type": "VARCHAR(60)",    "is_key": False, "nullable": True},
                    "dynamics_bc":     {"table": "Item",                    "column": "Description",       "type": "TEXT(100)",       "is_key": False, "nullable": True},
                    "netsuite":        {"table": "ITEM",                    "column": "DISPLAYNAME",       "type": "VARCHAR(256)",   "is_key": False, "nullable": True},
                    "workday":         {"table": "Inventory_Item",          "column": "Item_Name",         "type": "VARCHAR(200)",   "is_key": False, "nullable": True},
                },
            },
            {
                "logical_name": "Unit of Measure",
                "description": "Primary unit of measure for stocking/ordering",
                "category": "Measure",
                "mappings": {
                    "oracle_fusion":   {"table": "EGP_SYSTEM_ITEMS_B",      "column": "PRIMARY_UOM_CODE",  "type": "VARCHAR2(3)",    "is_key": False, "nullable": False},
                    "oracle_ebs":      {"table": "MTL_SYSTEM_ITEMS_B",      "column": "PRIMARY_UOM_CODE",  "type": "VARCHAR2(3)",    "is_key": False, "nullable": False},
                    "sap_s4hana":      {"table": "MARA",                    "column": "MEINS",             "type": "UNIT(3)",         "is_key": False, "nullable": False},
                    "sap_ecc":         {"table": "MARA",                    "column": "MEINS",             "type": "UNIT(3)",         "is_key": False, "nullable": False},
                    "dynamics_365_fo": {"table": "InventTable",             "column": "UNITID",            "type": "VARCHAR(10)",    "is_key": False, "nullable": False},
                    "dynamics_bc":     {"table": "Item",                    "column": "Base Unit of Measure", "type": "CODE(10)",   "is_key": False, "nullable": False},
                    "netsuite":        {"table": "ITEM",                    "column": "BASEUNIT",          "type": "INTEGER",        "is_key": False, "nullable": False},
                    "workday":         {"table": "Inventory_Item",          "column": "Unit_of_Measure",   "type": "VARCHAR(10)",    "is_key": False, "nullable": False},
                },
            },
            {
                "logical_name": "On-Hand Quantity",
                "description": "Current quantity available in stock",
                "category": "Quantity",
                "mappings": {
                    "oracle_fusion":   {"table": "INV_ONHAND_QUANTITIES_DETAIL", "column": "TRANSACTION_QUANTITY", "type": "NUMBER", "is_key": False, "nullable": False},
                    "oracle_ebs":      {"table": "MTL_ONHAND_QUANTITIES_DETAIL", "column": "TRANSACTION_QUANTITY", "type": "NUMBER", "is_key": False, "nullable": False},
                    "sap_s4hana":      {"table": "MARD",                    "column": "LABST",             "type": "QUAN(13,3)",     "is_key": False, "nullable": False},
                    "sap_ecc":         {"table": "MARD",                    "column": "LABST",             "type": "QUAN(13,3)",     "is_key": False, "nullable": False},
                    "dynamics_365_fo": {"table": "InventSum",               "column": "PHYSICALINVENT",    "type": "REAL",           "is_key": False, "nullable": False},
                    "dynamics_bc":     {"table": "Item Ledger Entry",       "column": "Quantity",          "type": "DECIMAL(38,20)", "is_key": False, "nullable": False},
                    "netsuite":        {"table": "ITEM",                    "column": "QUANTITYONHAND",    "type": "NUMERIC(28,2)",  "is_key": False, "nullable": True},
                    "workday":         {"table": "Inventory_Balance",       "column": "Quantity_On_Hand",  "type": "DECIMAL(18,6)",  "is_key": False, "nullable": False},
                },
            },
            {
                "logical_name": "Valuation / Unit Cost",
                "description": "Standard or average cost per unit",
                "category": "Amount",
                "mappings": {
                    "oracle_fusion":   {"table": "CST_ITEM_COSTS",          "column": "ITEM_COST",         "type": "NUMBER",         "is_key": False, "nullable": True},
                    "oracle_ebs":      {"table": "CST_ITEM_COSTS",          "column": "ITEM_COST",         "type": "NUMBER",         "is_key": False, "nullable": True},
                    "sap_s4hana":      {"table": "MBEW",                    "column": "STPRS",             "type": "CURR(13,2)",     "is_key": False, "nullable": True},
                    "sap_ecc":         {"table": "MBEW",                    "column": "STPRS",             "type": "CURR(13,2)",     "is_key": False, "nullable": True},
                    "dynamics_365_fo": {"table": "InventTable",             "column": "PRICE",             "type": "REAL",           "is_key": False, "nullable": True},
                    "dynamics_bc":     {"table": "Item",                    "column": "Unit Cost",         "type": "DECIMAL(38,20)", "is_key": False, "nullable": True},
                    "netsuite":        {"table": "ITEM",                    "column": "COST",              "type": "NUMERIC(28,2)",  "is_key": False, "nullable": True},
                    "workday":         {"table": "Inventory_Item",          "column": "Standard_Cost",     "type": "DECIMAL(18,6)",  "is_key": False, "nullable": True},
                },
            },
        ],
    },
}

SUPPORTED_FIELD_COMPARISON_MODULES = list(_FIELD_COMPARISON.keys())


@router.get("/field-comparison")
async def get_field_comparison(module: str = Query(..., description="Module key: GL, AR, AP, INV")):
    """
    Return field-to-field cross-ERP mapping for a given module.
    Shows logical field names mapped to ERP-specific table/column names.
    """
    module_upper = module.upper()
    data = _FIELD_COMPARISON.get(module_upper)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"Field comparison not available for module '{module}'. "
                   f"Supported: {SUPPORTED_FIELD_COMPARISON_MODULES}"
        )
    return {
        "module":      module_upper,
        "label":       data["label"],
        "description": data["description"],
        "erp_sources": [s["id"] for s in ERP_SOURCES if s["supported"]],
        "field_count": len(data["fields"]),
        "fields":      data["fields"],
    }


@router.get("/field-comparison-modules")
async def list_field_comparison_modules():
    """List modules that have field-level comparison data."""
    return [
        {"key": k, "label": v["label"], "field_count": len(v["fields"])}
        for k, v in _FIELD_COMPARISON.items()
    ]
