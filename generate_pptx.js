"use strict";
const pptxgen = require("pptxgenjs");

// ─── Palette ────────────────────────────────────────────────
const C = {
  darkBg:   "0F172A",
  cardBg:   "1E293B",
  lightBg:  "F8FAFC",
  teal:     "0891B2",
  cyan:     "06B6D4",
  white:    "FFFFFF",
  navy:     "0F172A",
  muted:    "94A3B8",
  dark:     "1E293B",
  gray:     "64748B",
  bronze:   "CD7F32",
  silver:   "94A3B8",
  gold:     "F59E0B",
  green:    "22C55E",
};

const pres = new pptxgen();
pres.layout = "LAYOUT_16x9";
pres.author  = "ilinkERP";
pres.title   = "ilinkERP Fabric Accelerate";

// ─── Slide 1 — Title ────────────────────────────────────────
{
  const sl = pres.addSlide();
  sl.background = { color: C.darkBg };

  // Left accent bar
  sl.addShape(pres.shapes.RECTANGLE, {
    x: 0, y: 0, w: 0.18, h: 5.625,
    fill: { color: C.teal }, line: { color: C.teal }
  });

  // Main title
  sl.addText("ilinkERP Fabric Accelerate", {
    x: 0.5, y: 1.4, w: 9, h: 1.0,
    fontSize: 44, fontFace: "Calibri", bold: true,
    color: C.white, margin: 0
  });

  // Subtitle
  sl.addText("Connecting ERP Systems to Microsoft Fabric — Fast", {
    x: 0.5, y: 2.55, w: 9, h: 0.6,
    fontSize: 22, fontFace: "Calibri", bold: false,
    color: C.cyan, margin: 0
  });

  // Tagline
  sl.addText("Accelerate your ERP → Microsoft Fabric integration journey", {
    x: 0.5, y: 3.3, w: 9, h: 0.5,
    fontSize: 16, fontFace: "Calibri", italic: true,
    color: C.muted, margin: 0
  });

  // Bottom right branding
  sl.addText("ilinkERP  |  Microsoft Fabric Integration", {
    x: 5.5, y: 5.1, w: 4.3, h: 0.35,
    fontSize: 11, fontFace: "Calibri",
    color: C.muted, align: "right", margin: 0
  });
}

// ─── Slide 2 — What is ilinkERP Fabric Accelerate? ──────────
{
  const sl = pres.addSlide();
  sl.background = { color: C.lightBg };

  // Left accent bar
  sl.addShape(pres.shapes.RECTANGLE, {
    x: 0.4, y: 0.55, w: 0.08, h: 4.6,
    fill: { color: C.teal }, line: { color: C.teal }
  });

  // Title
  sl.addText("What is ilinkERP Fabric Accelerate?", {
    x: 0.65, y: 0.3, w: 9, h: 0.55,
    fontSize: 28, fontFace: "Calibri", bold: true,
    color: C.navy, margin: 0
  });

  // Bullet rows with teal circle icons (left column)
  const bullets = [
    "Web-based accelerator connecting any ERP to Microsoft Fabric",
    "Eliminates manual data engineering effort entirely",
    "Supports Oracle EBS/Fusion, SAP ECC/S4HANA, Dynamics 365, NetSuite, Workday",
    "Guided 5-step wizard — ERP to deployed Fabric notebooks in minutes",
    "Role-based access with secure JWT authentication",
  ];
  const bulletY = [1.1, 1.8, 2.5, 3.2, 3.9];
  bullets.forEach((txt, i) => {
    // teal circle
    sl.addShape(pres.shapes.OVAL, {
      x: 0.7, y: bulletY[i] + 0.02, w: 0.28, h: 0.28,
      fill: { color: C.teal }, line: { color: C.teal }
    });
    // bullet number
    sl.addText(String(i + 1), {
      x: 0.7, y: bulletY[i] + 0.02, w: 0.28, h: 0.28,
      fontSize: 10, fontFace: "Calibri", bold: true,
      color: C.white, align: "center", valign: "middle", margin: 0
    });
    // bullet text
    sl.addText(txt, {
      x: 1.08, y: bulletY[i], w: 4.9, h: 0.45,
      fontSize: 13, fontFace: "Calibri",
      color: C.dark, valign: "middle", margin: 0
    });
  });

  // Right column stat box
  sl.addShape(pres.shapes.RECTANGLE, {
    x: 6.3, y: 0.9, w: 3.3, h: 3.8,
    fill: { color: C.darkBg }, line: { color: C.teal, width: 2 }
  });

  // Stats
  const stats = [
    { num: "8", label: "ERP Platforms" },
    { num: "5", label: "Guided Steps" },
    { num: "9", label: "ERP Modules" },
  ];
  stats.forEach((s, i) => {
    const statY = 1.1 + i * 1.15;
    sl.addText(s.num, {
      x: 6.3, y: statY, w: 3.3, h: 0.75,
      fontSize: 52, fontFace: "Calibri", bold: true,
      color: C.teal, align: "center", valign: "middle", margin: 0
    });
    sl.addText(s.label, {
      x: 6.3, y: statY + 0.72, w: 3.3, h: 0.32,
      fontSize: 13, fontFace: "Calibri",
      color: C.muted, align: "center", margin: 0
    });
  });
}

// ─── Slide 3 — App Navigation Overview ──────────────────────
{
  const sl = pres.addSlide();
  sl.background = { color: C.lightBg };

  sl.addShape(pres.shapes.RECTANGLE, {
    x: 0.4, y: 0.55, w: 0.08, h: 4.6,
    fill: { color: C.teal }, line: { color: C.teal }
  });

  sl.addText("App Navigation Overview", {
    x: 0.65, y: 0.3, w: 9, h: 0.55,
    fontSize: 28, fontFace: "Calibri", bold: true,
    color: C.navy, margin: 0
  });

  const navItems = [
    { icon: "DB", label: "Dashboard",          desc: "Real-time connection status, deployment health & activity feed" },
    { icon: "WZ", label: "ERP Source Wizard",  desc: "5-step guided: Select → Connect → Discover → Fabric Setup → Deploy" },
    { icon: "CP", label: "ERP Comparison",     desc: "Cross-ERP table & field-level comparison across all platforms" },
    { icon: "ST", label: "Settings",           desc: "Fabric connection manager (5 auth methods) + User Management" },
    { icon: "LK", label: "Secure Login",       desc: "JWT authentication, Admin / Viewer role-based access" },
  ];

  navItems.forEach((item, i) => {
    const rowY = 1.05 + i * 0.83;

    // Card background
    sl.addShape(pres.shapes.RECTANGLE, {
      x: 0.65, y: rowY, w: 9.1, h: 0.68,
      fill: { color: C.white }, line: { color: "E2E8F0", width: 1 }
    });

    // Teal icon box
    sl.addShape(pres.shapes.RECTANGLE, {
      x: 0.65, y: rowY, w: 0.68, h: 0.68,
      fill: { color: C.teal }, line: { color: C.teal }
    });
    sl.addText(item.icon, {
      x: 0.65, y: rowY, w: 0.68, h: 0.68,
      fontSize: 9, fontFace: "Calibri", bold: true,
      color: C.white, align: "center", valign: "middle", margin: 0
    });

    // Label
    sl.addText(item.label, {
      x: 1.45, y: rowY + 0.04, w: 2.2, h: 0.32,
      fontSize: 14, fontFace: "Calibri", bold: true,
      color: C.navy, valign: "middle", margin: 0
    });

    // Description
    sl.addText(item.desc, {
      x: 1.45, y: rowY + 0.34, w: 7.2, h: 0.3,
      fontSize: 11, fontFace: "Calibri",
      color: C.gray, valign: "middle", margin: 0
    });
  });
}

// ─── Slide 4 — ERP Source Wizard (dark bg) ──────────────────
{
  const sl = pres.addSlide();
  sl.background = { color: C.darkBg };

  sl.addText("ERP Source Wizard — 5 Steps", {
    x: 0.5, y: 0.28, w: 9, h: 0.6,
    fontSize: 30, fontFace: "Calibri", bold: true,
    color: C.white, margin: 0
  });

  const steps = [
    { num: "1", title: "Select ERP\n& Module",     sub: "GL, AR, AP,\nInventory & more" },
    { num: "2", title: "Configure\nConnection",    sub: "Enter credentials\nor skip to tables" },
    { num: "3", title: "Discover\nTables",         sub: "Auto-loaded;\nlive scan optional" },
    { num: "4", title: "Fabric\nSetup",            sub: "Connect workspace\nvia 5 auth methods" },
    { num: "5", title: "Deploy",                   sub: "Bronze/Silver/Gold\nnotebooks generated" },
  ];

  const startX = 0.5;
  const gapX   = 1.82;
  const circY  = 1.35;
  const circR  = 0.55;

  steps.forEach((s, i) => {
    const cx = startX + i * gapX;

    // Connecting arrow line (between circles)
    if (i < steps.length - 1) {
      sl.addShape(pres.shapes.LINE, {
        x: cx + circR * 2, y: circY + circR,
        w: gapX - circR * 2, h: 0,
        line: { color: C.cyan, width: 2.5 }
      });
    }

    // Circle
    sl.addShape(pres.shapes.OVAL, {
      x: cx, y: circY, w: circR * 2, h: circR * 2,
      fill: { color: C.teal }, line: { color: C.cyan, width: 2 }
    });

    // Step number
    sl.addText(s.num, {
      x: cx, y: circY, w: circR * 2, h: circR * 2,
      fontSize: 22, fontFace: "Calibri", bold: true,
      color: C.white, align: "center", valign: "middle", margin: 0
    });

    // Step title (below circle)
    sl.addText(s.title, {
      x: cx - 0.2, y: circY + circR * 2 + 0.1, w: circR * 2 + 0.4, h: 0.7,
      fontSize: 12, fontFace: "Calibri", bold: true,
      color: C.white, align: "center", margin: 0
    });

    // Step sub-text
    sl.addText(s.sub, {
      x: cx - 0.25, y: circY + circR * 2 + 0.78, w: circR * 2 + 0.5, h: 0.7,
      fontSize: 10, fontFace: "Calibri",
      color: C.muted, align: "center", margin: 0
    });
  });

  // Bottom legend note
  sl.addText("Each step builds on the last — from ERP selection to a fully deployed Microsoft Fabric pipeline", {
    x: 0.5, y: 4.95, w: 9, h: 0.35,
    fontSize: 11, fontFace: "Calibri", italic: true,
    color: C.muted, align: "center", margin: 0
  });
}

// ─── Slide 5 — ERP Comparison Page ──────────────────────────
{
  const sl = pres.addSlide();
  sl.background = { color: C.lightBg };

  sl.addShape(pres.shapes.RECTANGLE, {
    x: 0.4, y: 0.55, w: 0.08, h: 4.6,
    fill: { color: C.teal }, line: { color: C.teal }
  });

  sl.addText("ERP Comparison Page", {
    x: 0.65, y: 0.28, w: 9, h: 0.55,
    fontSize: 28, fontFace: "Calibri", bold: true,
    color: C.navy, margin: 0
  });

  // Left card
  sl.addShape(pres.shapes.RECTANGLE, {
    x: 0.65, y: 1.05, w: 4.1, h: 3.35,
    fill: { color: C.white }, line: { color: "E2E8F0", width: 1 }
  });
  sl.addShape(pres.shapes.RECTANGLE, {
    x: 0.65, y: 1.05, w: 4.1, h: 0.1,
    fill: { color: C.teal }, line: { color: C.teal }
  });
  sl.addText("Table Comparison", {
    x: 0.85, y: 1.15, w: 3.5, h: 0.42,
    fontSize: 15, fontFace: "Calibri", bold: true,
    color: C.teal, margin: 0
  });
  const leftBullets = [
    "Equivalent tables per module across all ERP platforms",
    "Live data from official vendor docs (24h cache)",
    "Filter by ERP, refresh on demand",
  ];
  leftBullets.forEach((b, i) => {
    sl.addText([
      { text: "▸  ", options: { bold: true, color: C.teal } },
      { text: b, options: { color: C.dark } }
    ], {
      x: 0.85, y: 1.7 + i * 0.65, w: 3.65, h: 0.55,
      fontSize: 12, fontFace: "Calibri", valign: "top", margin: 0
    });
  });

  // Right card
  sl.addShape(pres.shapes.RECTANGLE, {
    x: 5.1, y: 1.05, w: 4.65, h: 3.35,
    fill: { color: C.white }, line: { color: "E2E8F0", width: 1 }
  });
  sl.addShape(pres.shapes.RECTANGLE, {
    x: 5.1, y: 1.05, w: 4.65, h: 0.1,
    fill: { color: C.cyan }, line: { color: C.cyan }
  });
  sl.addText([
    { text: "Field Comparison  ", options: { bold: true, color: C.cyan } },
    { text: "★ New", options: { bold: true, color: C.gold } }
  ], {
    x: 5.3, y: 1.15, w: 4.2, h: 0.42,
    fontSize: 15, fontFace: "Calibri", margin: 0
  });
  const rightBullets = [
    "Logical field → ERP column mapping",
    "Table, column, data type, key indicators per ERP",
    "8 ERP platforms side-by-side in one view",
    "Download as CSV or Excel",
  ];
  rightBullets.forEach((b, i) => {
    sl.addText([
      { text: "▸  ", options: { bold: true, color: C.cyan } },
      { text: b, options: { color: C.dark } }
    ], {
      x: 5.3, y: 1.7 + i * 0.62, w: 4.3, h: 0.55,
      fontSize: 12, fontFace: "Calibri", valign: "top", margin: 0
    });
  });

  // Bottom module note
  sl.addShape(pres.shapes.RECTANGLE, {
    x: 0.65, y: 4.65, w: 9.1, h: 0.55,
    fill: { color: "EFF6FF" }, line: { color: C.teal, width: 1 }
  });
  sl.addText("Modules: GL · AR · AP · Inventory · Purchasing · Order Mgmt · HCM · Fixed Assets · Cash Mgmt", {
    x: 0.75, y: 4.67, w: 9.0, h: 0.5,
    fontSize: 11, fontFace: "Calibri", italic: true,
    color: C.navy, valign: "middle", margin: 0
  });
}

// ─── Slide 6 — Supported ERP Platforms (dark bg) ────────────
{
  const sl = pres.addSlide();
  sl.background = { color: C.darkBg };

  sl.addText("Supported ERP Platforms", {
    x: 0.5, y: 0.22, w: 9, h: 0.55,
    fontSize: 30, fontFace: "Calibri", bold: true,
    color: C.white, margin: 0
  });

  const platforms = [
    { name: "Oracle Fusion Cloud ERP", vendor: "Oracle" },
    { name: "Oracle EBS",              vendor: "Oracle" },
    { name: "SAP S/4HANA",            vendor: "SAP" },
    { name: "SAP ECC",                 vendor: "SAP" },
    { name: "Microsoft Dynamics 365 FO", vendor: "Microsoft" },
    { name: "Microsoft Dynamics BC",   vendor: "Microsoft" },
    { name: "NetSuite",                vendor: "Oracle NetSuite" },
    { name: "Workday",                 vendor: "Workday" },
  ];

  const cols = 4, rows = 2;
  const cardW = 2.2, cardH = 1.0;
  const startX = 0.5, startY = 1.0, gapX = 0.12, gapY = 0.12;

  platforms.forEach((p, i) => {
    const col = i % cols;
    const row = Math.floor(i / cols);
    const cx = startX + col * (cardW + gapX);
    const cy = startY + row * (cardH + gapY);

    sl.addShape(pres.shapes.RECTANGLE, {
      x: cx, y: cy, w: cardW, h: cardH,
      fill: { color: C.cardBg }, line: { color: C.teal, width: 1 }
    });
    // Teal top accent
    sl.addShape(pres.shapes.RECTANGLE, {
      x: cx, y: cy, w: cardW, h: 0.07,
      fill: { color: C.teal }, line: { color: C.teal }
    });
    sl.addText(p.name, {
      x: cx + 0.1, y: cy + 0.15, w: cardW - 0.2, h: 0.45,
      fontSize: 12, fontFace: "Calibri", bold: true,
      color: C.white, valign: "middle", margin: 0
    });
    sl.addText(p.vendor, {
      x: cx + 0.1, y: cy + 0.58, w: cardW - 0.2, h: 0.3,
      fontSize: 10, fontFace: "Calibri",
      color: C.teal, margin: 0
    });
  });

  // Modules label
  sl.addText("Modules Covered:", {
    x: 0.5, y: 3.18, w: 9, h: 0.3,
    fontSize: 12, fontFace: "Calibri", bold: true,
    color: C.white, align: "center", valign: "middle", margin: 0
  });

  // Module pills — two rows to fit within slide width
  const modRow1 = ["GL", "AR", "AP", "Inventory", "Purchasing"];
  const modRow2 = ["OM", "HCM", "Fixed Assets", "Cash Mgmt"];
  const pillH = 0.32;
  const pillGap = 0.1;

  [modRow1, modRow2].forEach((rowMods, rowIdx) => {
    // Calculate total row width to center it
    const pillWidths = rowMods.map(m => Math.max(m.length * 0.115 + 0.38, 0.6));
    const totalW = pillWidths.reduce((a, b) => a + b, 0) + pillGap * (rowMods.length - 1);
    let px = 0.5 + (9.0 - totalW) / 2;
    const py = 3.47 + rowIdx * (pillH + 0.1);
    rowMods.forEach((m, j) => {
      const pw = pillWidths[j];
      sl.addShape(pres.shapes.RECTANGLE, {
        x: px, y: py, w: pw, h: pillH,
        fill: { color: C.teal }, line: { color: C.teal }
      });
      sl.addText(m, {
        x: px, y: py, w: pw, h: pillH,
        fontSize: 10, fontFace: "Calibri", bold: true,
        color: C.white, align: "center", valign: "middle", margin: 0
      });
      px += pw + pillGap;
    });
  });

  // Bottom note
  sl.addText("All platforms support all 9 modules. Live connection optional — standard tables pre-loaded.", {
    x: 0.5, y: 4.45, w: 9, h: 0.35,
    fontSize: 11, fontFace: "Calibri", italic: true,
    color: C.muted, align: "center", margin: 0
  });
}

// ─── Slide 7 — Microsoft Fabric Integration ─────────────────
{
  const sl = pres.addSlide();
  sl.background = { color: C.lightBg };

  sl.addShape(pres.shapes.RECTANGLE, {
    x: 0.4, y: 0.55, w: 0.08, h: 4.6,
    fill: { color: C.teal }, line: { color: C.teal }
  });

  sl.addText("Microsoft Fabric Integration", {
    x: 0.65, y: 0.28, w: 9, h: 0.55,
    fontSize: 28, fontFace: "Calibri", bold: true,
    color: C.navy, margin: 0
  });

  // Medallion layers
  const layers = [
    { icon: "BRONZE", label: "BRONZE — Raw ERP Extraction", desc: "SQL → Delta Lake tables (raw)", color: C.bronze },
    { icon: "SILVER", label: "SILVER — Cleanse & Transform",  desc: "Validated, deduplicated, typed data", color: "9CA3AF" },
    { icon: "GOLD",   label: "GOLD — KPI Aggregation",        desc: "Business-ready KPIs & reporting tables", color: C.gold },
  ];

  layers.forEach((layer, i) => {
    const ly = 1.1 + i * 1.13;
    sl.addShape(pres.shapes.RECTANGLE, {
      x: 0.65, y: ly, w: 4.3, h: 0.9,
      fill: { color: C.white }, line: { color: "E2E8F0", width: 1 }
    });
    // Left border accent
    sl.addShape(pres.shapes.RECTANGLE, {
      x: 0.65, y: ly, w: 0.12, h: 0.9,
      fill: { color: layer.color }, line: { color: layer.color }
    });
    sl.addText(layer.label, {
      x: 0.9, y: ly + 0.06, w: 4.0, h: 0.38,
      fontSize: 13, fontFace: "Calibri", bold: true,
      color: C.navy, valign: "middle", margin: 0
    });
    sl.addText(layer.desc, {
      x: 0.9, y: ly + 0.44, w: 4.0, h: 0.35,
      fontSize: 11, fontFace: "Calibri",
      color: C.gray, valign: "middle", margin: 0
    });
    // Arrow between layers
    if (i < layers.length - 1) {
      sl.addShape(pres.shapes.LINE, {
        x: 2.45, y: ly + 0.9, w: 0, h: 0.23,
        line: { color: C.teal, width: 2 }
      });
      sl.addText("▼", {
        x: 2.3, y: ly + 1.05, w: 0.3, h: 0.2,
        fontSize: 11, color: C.teal, align: "center", margin: 0
      });
    }
  });

  // Right side — Auth methods
  sl.addShape(pres.shapes.RECTANGLE, {
    x: 5.5, y: 1.05, w: 4.2, h: 3.35,
    fill: { color: C.white }, line: { color: "E2E8F0", width: 1 }
  });
  sl.addShape(pres.shapes.RECTANGLE, {
    x: 5.5, y: 1.05, w: 4.2, h: 0.1,
    fill: { color: C.teal }, line: { color: C.teal }
  });
  sl.addText("Authentication Methods", {
    x: 5.7, y: 1.15, w: 3.8, h: 0.4,
    fontSize: 14, fontFace: "Calibri", bold: true,
    color: C.teal, margin: 0
  });

  const authMethods = [
    "Bearer Token",
    "Service Principal",
    "Device Code (MFA)",
    "Managed Identity",
    "Username / Password",
  ];
  authMethods.forEach((m, i) => {
    sl.addShape(pres.shapes.OVAL, {
      x: 5.7, y: 1.7 + i * 0.52 + 0.02, w: 0.22, h: 0.22,
      fill: { color: C.teal }, line: { color: C.teal }
    });
    sl.addText(m, {
      x: 6.02, y: 1.7 + i * 0.52, w: 3.45, h: 0.34,
      fontSize: 12, fontFace: "Calibri",
      color: C.dark, valign: "middle", margin: 0
    });
  });
  sl.addText("Demo-safe: simulates deployment without a Fabric token", {
    x: 5.7, y: 3.95, w: 3.8, h: 0.4,
    fontSize: 10, fontFace: "Calibri", italic: true,
    color: C.muted, margin: 0
  });

  // Bottom note
  sl.addText("Automated Bronze → Silver → Gold notebook generation with full Microsoft Fabric pipeline", {
    x: 0.65, y: 4.65, w: 9.1, h: 0.55,
    fontSize: 11, fontFace: "Calibri", italic: true,
    color: C.gray, align: "center", margin: 0
  });
}

// ─── Slide 8 — Security & User Management ───────────────────
{
  const sl = pres.addSlide();
  sl.background = { color: C.lightBg };

  sl.addShape(pres.shapes.RECTANGLE, {
    x: 0.4, y: 0.55, w: 0.08, h: 4.6,
    fill: { color: C.teal }, line: { color: C.teal }
  });

  sl.addText("Security & User Management", {
    x: 0.65, y: 0.28, w: 9, h: 0.55,
    fontSize: 28, fontFace: "Calibri", bold: true,
    color: C.navy, margin: 0
  });

  // Left security features
  const secFeatures = [
    { icon: "JWT", label: "JWT Authentication", desc: "8-hour token expiry, auto-refresh" },
    { icon: "BCR", label: "bcrypt Password Hashing", desc: "Never stored in plain text" },
    { icon: "RBA", label: "Role-Based Access Control", desc: "Granular per-feature permissions" },
    { icon: "ADM", label: "Admin / Viewer Separation", desc: "Admin manages users; Viewer reads data" },
  ];
  secFeatures.forEach((f, i) => {
    const fy = 1.08 + i * 0.88;
    sl.addShape(pres.shapes.OVAL, {
      x: 0.65, y: fy + 0.08, w: 0.5, h: 0.5,
      fill: { color: C.teal }, line: { color: C.teal }
    });
    sl.addText(f.icon, {
      x: 0.65, y: fy + 0.08, w: 0.5, h: 0.5,
      fontSize: 7, fontFace: "Calibri", bold: true,
      color: C.white, align: "center", valign: "middle", margin: 0
    });
    sl.addText(f.label, {
      x: 1.28, y: fy + 0.04, w: 3.5, h: 0.34,
      fontSize: 13, fontFace: "Calibri", bold: true,
      color: C.navy, valign: "middle", margin: 0
    });
    sl.addText(f.desc, {
      x: 1.28, y: fy + 0.36, w: 3.5, h: 0.34,
      fontSize: 11, fontFace: "Calibri",
      color: C.gray, valign: "middle", margin: 0
    });
  });

  // Right user management card
  sl.addShape(pres.shapes.RECTANGLE, {
    x: 5.5, y: 1.0, w: 4.2, h: 3.9,
    fill: { color: C.white }, line: { color: C.teal, width: 2 }
  });
  sl.addShape(pres.shapes.RECTANGLE, {
    x: 5.5, y: 1.0, w: 4.2, h: 0.1,
    fill: { color: C.teal }, line: { color: C.teal }
  });
  sl.addText("In-App User Management", {
    x: 5.65, y: 1.1, w: 3.8, h: 0.42,
    fontSize: 14, fontFace: "Calibri", bold: true,
    color: C.teal, margin: 0
  });
  sl.addText("No command-line required", {
    x: 5.65, y: 1.55, w: 3.8, h: 0.3,
    fontSize: 11, fontFace: "Calibri", italic: true,
    color: C.gray, margin: 0
  });
  const mgmtItems = [
    "Add new users",
    "Edit display name & role",
    "Reset passwords",
    "Delete users",
    "Role permissions legend",
  ];
  mgmtItems.forEach((item, i) => {
    // green check circle
    sl.addShape(pres.shapes.OVAL, {
      x: 5.65, y: 1.98 + i * 0.52 + 0.04, w: 0.22, h: 0.22,
      fill: { color: C.green }, line: { color: C.green }
    });
    sl.addText("✓", {
      x: 5.65, y: 1.98 + i * 0.52 + 0.04, w: 0.22, h: 0.22,
      fontSize: 8, bold: true, color: C.white,
      align: "center", valign: "middle", margin: 0
    });
    sl.addText(item, {
      x: 5.98, y: 1.98 + i * 0.52, w: 3.5, h: 0.4,
      fontSize: 12, fontFace: "Calibri",
      color: C.dark, valign: "middle", margin: 0
    });
  });
}

// ─── Slide 9 — Key Benefits (dark bg) ───────────────────────
{
  const sl = pres.addSlide();
  sl.background = { color: C.darkBg };

  sl.addText("Key Benefits", {
    x: 0.5, y: 0.22, w: 9, h: 0.55,
    fontSize: 30, fontFace: "Calibri", bold: true,
    color: C.white, margin: 0
  });

  const benefits = [
    { title: "Zero Coding Required",    desc: "Guided wizard for all ERP types" },
    { title: "Field-Level Comparison",  desc: "Understand schema differences instantly" },
    { title: "Export & Download",       desc: "CSV and Excel for offline analysis" },
    { title: "Works Offline",           desc: "Standard tables without live ERP connection" },
    { title: "Production-Ready",        desc: "Docker, nginx proxy, JWT auth" },
    { title: "Multi-ERP Support",       desc: "8 platforms, 9 modules covered" },
  ];

  const cols = 3;
  const cardW = 2.95, cardH = 1.55;
  const startX = 0.5, startY = 1.0;
  const gapX = 0.22, gapY = 0.2;

  benefits.forEach((b, i) => {
    const col = i % cols;
    const row = Math.floor(i / cols);
    const cx = startX + col * (cardW + gapX);
    const cy = startY + row * (cardH + gapY);

    sl.addShape(pres.shapes.RECTANGLE, {
      x: cx, y: cy, w: cardW, h: cardH,
      fill: { color: C.cardBg }, line: { color: C.teal, width: 1 }
    });
    // Teal top accent
    sl.addShape(pres.shapes.RECTANGLE, {
      x: cx, y: cy, w: cardW, h: 0.1,
      fill: { color: C.teal }, line: { color: C.teal }
    });
    // Number badge
    sl.addShape(pres.shapes.OVAL, {
      x: cx + 0.15, y: cy + 0.2, w: 0.38, h: 0.38,
      fill: { color: C.teal }, line: { color: C.teal }
    });
    sl.addText(String(i + 1), {
      x: cx + 0.15, y: cy + 0.2, w: 0.38, h: 0.38,
      fontSize: 11, fontFace: "Calibri", bold: true,
      color: C.white, align: "center", valign: "middle", margin: 0
    });
    sl.addText(b.title, {
      x: cx + 0.65, y: cy + 0.18, w: cardW - 0.78, h: 0.45,
      fontSize: 13, fontFace: "Calibri", bold: true,
      color: C.white, valign: "middle", margin: 0
    });
    sl.addText(b.desc, {
      x: cx + 0.18, y: cy + 0.75, w: cardW - 0.28, h: 0.65,
      fontSize: 11, fontFace: "Calibri",
      color: C.muted, valign: "top", margin: 0
    });
  });
}

// ─── Slide 10 — Thank You (dark bg) ─────────────────────────
{
  const sl = pres.addSlide();
  sl.background = { color: C.darkBg };

  // Left accent bar
  sl.addShape(pres.shapes.RECTANGLE, {
    x: 0, y: 0, w: 0.18, h: 5.625,
    fill: { color: C.teal }, line: { color: C.teal }
  });

  sl.addText("Thank You", {
    x: 0.5, y: 1.4, w: 9, h: 1.0,
    fontSize: 52, fontFace: "Calibri", bold: true,
    color: C.white, margin: 0
  });

  sl.addText("Questions & Discussion", {
    x: 0.5, y: 2.6, w: 9, h: 0.6,
    fontSize: 24, fontFace: "Calibri",
    color: C.cyan, margin: 0
  });

  // Divider line
  sl.addShape(pres.shapes.LINE, {
    x: 0.5, y: 3.45, w: 9, h: 0,
    line: { color: C.teal, width: 2 }
  });

  sl.addText("ilinkERP Fabric Accelerate  |  Microsoft Fabric Integration", {
    x: 0.5, y: 3.75, w: 9, h: 0.5,
    fontSize: 14, fontFace: "Calibri",
    color: C.muted, margin: 0
  });
}

// ─── Write file ──────────────────────────────────────────────
const outPath = "C:\\Users\\Maheswaran M\\ilinkERP-Fabric\\ilinkERP_FabricAccelerate_Presentation.pptx";
pres.writeFile({ fileName: outPath })
  .then(() => console.log("✅  Saved:", outPath))
  .catch(err => { console.error("❌  Error:", err); process.exit(1); });
