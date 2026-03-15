const pptxgen = require("pptxgenjs");
const path = require("path");
const fs = require("fs");

const SHOTS = path.join(__dirname, "screenshots");
const OUT = path.join(__dirname, "ilinkERP_FabricAccelerate_Presentation.pptx");

// ── Palette ──────────────────────────────────────────────────────────────────
const C = {
  navyDark:   "0B2447",   // dark bg
  navy:       "19376D",   // mid bg
  teal:       "0C6478",   // accent panels
  mint:       "02C39A",   // highlight / accent
  mintLight:  "E6FBF6",   // light mint tint
  white:      "FFFFFF",
  offWhite:   "F4F8FB",
  lightGray:  "E2E8F0",
  textDark:   "1E293B",
  textMid:    "475569",
  textLight:  "94A3B8",
};

// ── Helper: screenshot path ───────────────────────────────────────────────────
function shot(name) {
  return path.join(SHOTS, name);
}

// ── Step pill (numbered badge) ────────────────────────────────────────────────
function addStepBadge(slide, num, x, y) {
  slide.addShape("ellipse", { x, y, w: 0.38, h: 0.38, fill: { color: C.mint }, line: { color: C.mint } });
  slide.addText(String(num), { x: x - 0.01, y: y + 0.03, w: 0.40, h: 0.32, fontSize: 13, bold: true, color: C.navyDark, align: "center", margin: 0 });
}

// ── Callout box ───────────────────────────────────────────────────────────────
function addCallout(slide, lines, x, y, w, h, bg = C.teal) {
  slide.addShape("rect", { x, y, w, h, fill: { color: bg, transparency: 8 }, line: { color: bg } });
  slide.addText(lines, { x: x + 0.15, y, w: w - 0.2, h, fontSize: 11, color: C.white, valign: "middle" });
}

// ── Screenshot card (with thin frame) ─────────────────────────────────────────
function addScreenCard(slide, imgFile, x, y, w, h) {
  // shadow card
  slide.addShape("rect", {
    x: x + 0.04, y: y + 0.04, w, h,
    fill: { color: "000000", transparency: 80 }, line: { color: "000000", transparency: 80 }
  });
  // white frame
  slide.addShape("rect", {
    x, y, w, h,
    fill: { color: C.white }, line: { color: C.lightGray, pt: 1.5 }
  });
  // image inside frame
  slide.addImage({ path: imgFile, x: x + 0.04, y: y + 0.04, w: w - 0.08, h: h - 0.08, sizing: { type: "contain", w: w - 0.08, h: h - 0.08 } });
}

// ── Dark header bar ────────────────────────────────────────────────────────────
function addDarkHeader(slide, title, subtitle) {
  slide.addShape("rect", { x: 0, y: 0, w: 10, h: 1.1, fill: { color: C.navyDark }, line: { color: C.navyDark } });
  slide.addShape("rect", { x: 0, y: 1.1, w: 10, h: 0.05, fill: { color: C.mint }, line: { color: C.mint } });
  slide.addText(title, { x: 0.45, y: 0.12, w: 8, h: 0.55, fontSize: 22, bold: true, color: C.white, margin: 0 });
  if (subtitle) slide.addText(subtitle, { x: 0.45, y: 0.65, w: 8, h: 0.38, fontSize: 12, color: C.textLight, margin: 0 });
}

// ═════════════════════════════════════════════════════════════════════════════
const pres = new pptxgen();
pres.layout = "LAYOUT_16x9"; // 10 × 5.625
pres.title = "ilinkERP-Fabric Accelerator";
pres.author = "ilinkERP Team";

// ─────────────────────────────────────────────────────────────────────────────
// SLIDE 1 — Title
// ─────────────────────────────────────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.navyDark };

  // Left accent strip
  s.addShape("rect", { x: 0, y: 0, w: 0.18, h: 5.625, fill: { color: C.mint }, line: { color: C.mint } });

  // Large teal circle
  s.addShape("ellipse", { x: 6.0, y: -0.8, w: 5.5, h: 5.5, fill: { color: C.teal, transparency: 70 }, line: { color: C.teal, transparency: 70 } });

  s.addText("ilinkERP-Fabric", {
    x: 0.55, y: 1.35, w: 7, h: 1.0,
    fontSize: 44, bold: true, color: C.white, fontFace: "Calibri"
  });
  s.addText("Accelerator", {
    x: 0.55, y: 2.25, w: 7, h: 0.75,
    fontSize: 36, bold: false, color: C.mint, fontFace: "Calibri"
  });
  s.addText("Connect any ERP to Microsoft Fabric in minutes —\nnot months", {
    x: 0.55, y: 3.1, w: 6.5, h: 0.85,
    fontSize: 15, color: "CADCFC", fontFace: "Calibri"
  });

  s.addText("ERP → Fabric Accelerator  ·  Powered by Microsoft Fabric  ·  Free AI", {
    x: 0.55, y: 5.05, w: 9, h: 0.35,
    fontSize: 10, color: C.textLight, fontFace: "Calibri"
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// SLIDE 2 — Navigation Overview
// ─────────────────────────────────────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.offWhite };
  addDarkHeader(s, "App Navigation — What's Inside", "Six key areas covering the entire ERP-to-Fabric journey");

  const modules = [
    { icon: "⊕", title: "Dashboard", desc: "KPIs, pipeline status\n& ingestion metrics" },
    { icon: "⚙", title: "ERP Sources", desc: "Connect & configure\nyour ERP system" },
    { icon: "⇄", title: "ERP Analysis", desc: "Compare ERP schemas\nacross platforms" },
    { icon: "✦", title: "AI Query", desc: "Natural-language\nqueries over your data" },
    { icon: "◈", title: "Ontology", desc: "Canonical data model\n& field mapping" },
    { icon: "☰", title: "Settings", desc: "Users, connections\n& Fabric config" },
  ];

  modules.forEach((m, i) => {
    const col = i % 3;
    const row = Math.floor(i / 3);
    const x = 0.35 + col * 3.15;
    const y = 1.45 + row * 1.85;

    s.addShape("rect", { x, y, w: 2.9, h: 1.6, fill: { color: C.white }, line: { color: C.lightGray, pt: 1 },
      shadow: { type: "outer", blur: 5, offset: 2, angle: 135, color: "000000", opacity: 0.08 }
    });
    s.addShape("rect", { x, y, w: 0.07, h: 1.6, fill: { color: C.mint }, line: { color: C.mint } });
    s.addText(m.icon, { x: x + 0.2, y: y + 0.25, w: 0.45, h: 0.45, fontSize: 22, color: C.teal, margin: 0 });
    s.addText(m.title, { x: x + 0.22, y: y + 0.72, w: 2.4, h: 0.35, fontSize: 14, bold: true, color: C.textDark, margin: 0 });
    s.addText(m.desc, { x: x + 0.22, y: y + 1.06, w: 2.55, h: 0.46, fontSize: 10, color: C.textMid, margin: 0 });
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// SLIDE 3 — Login
// ─────────────────────────────────────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.offWhite };
  addDarkHeader(s, "Step 0 — Secure Login", "JWT-based authentication keeps your ERP data protected");

  addScreenCard(s, shot("01_login.png"), 0.35, 1.3, 5.6, 3.95);

  // Callouts on right
  const pts = [
    ["Role-based access", "Admin and analyst roles with separate permission levels"],
    ["JWT tokens", "Secure session management — tokens expire automatically"],
    ["Local auth", "No external auth provider needed; credentials stored securely with bcrypt"],
  ];
  pts.forEach(([title, body], i) => {
    const y = 1.3 + i * 1.27;
    s.addShape("rect", { x: 6.2, y, w: 3.45, h: 1.1,
      fill: { color: C.white }, line: { color: C.lightGray, pt: 1 },
      shadow: { type: "outer", blur: 4, offset: 1, angle: 135, color: "000000", opacity: 0.07 }
    });
    s.addShape("rect", { x: 6.2, y, w: 0.07, h: 1.1, fill: { color: C.mint }, line: { color: C.mint } });
    s.addText(title, { x: 6.35, y: y + 0.1, w: 3.15, h: 0.32, fontSize: 12, bold: true, color: C.textDark, margin: 0 });
    s.addText(body, { x: 6.35, y: y + 0.42, w: 3.15, h: 0.58, fontSize: 10, color: C.textMid, margin: 0 });
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// SLIDE 4 — Dashboard
// ─────────────────────────────────────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.offWhite };
  addDarkHeader(s, "Dashboard — Real-Time Pipeline Status", "After login, the dashboard shows ingestion health at a glance");

  addScreenCard(s, shot("02_dashboard.png"), 0.35, 1.3, 9.3, 4.0);

  s.addText("After logging in → you land here", { x: 0.35, y: 5.12, w: 9.3, h: 0.35,
    fontSize: 10, color: C.textLight, italic: true, align: "center", margin: 0
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// SLIDE 5 — ERP Source (pre-wizard)
// ─────────────────────────────────────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.offWhite };
  addDarkHeader(s, "ERP Sources — Your Connection Hub", "Manage all configured ERP connections from one place");

  addScreenCard(s, shot("03_erp_source_step1.png"), 0.35, 1.3, 5.8, 3.95);

  s.addText("What you can do here:", { x: 6.35, y: 1.4, w: 3.3, h: 0.35,
    fontSize: 13, bold: true, color: C.textDark, margin: 0
  });

  const items = [
    "View all connected ERP systems",
    "See sync status & last run time",
    "Launch the ERP Source Wizard",
    "Edit or delete existing connections",
    "Trigger manual sync runs",
  ];
  s.addText(items.map(t => ({ text: t, options: { bullet: true, breakLine: true } })), {
    x: 6.35, y: 1.82, w: 3.3, h: 2.5, fontSize: 11.5, color: C.textMid
  });

  s.addShape("rect", { x: 6.2, y: 4.5, w: 3.45, h: 0.55,
    fill: { color: C.mint, transparency: 15 }, line: { color: C.mint }
  });
  s.addText('Click "Start ERP Source Wizard" to connect a new ERP →', {
    x: 6.28, y: 4.5, w: 3.3, h: 0.55, fontSize: 10.5, bold: true, color: C.navyDark,
    valign: "middle", margin: 0
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// SLIDE 6 — Wizard Step 1: ERP Selection
// ─────────────────────────────────────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.offWhite };
  addDarkHeader(s, "Wizard — Step 1: Select Your ERP", "Choose from 8 supported ERP platforms + upcoming integrations");

  addScreenCard(s, shot("04_wizard_step1_erp_select.png"), 0.35, 1.3, 5.8, 3.95);

  // Step flow badges
  const steps = ["Select ERP", "Connect", "Discover Tables", "Fabric Setup", "Deploy"];
  steps.forEach((label, i) => {
    const x = 6.25 + (i % 3) * 1.18;
    const y = 1.35 + Math.floor(i / 3) * 0.72;
    addStepBadge(s, i + 1, x, y);
    s.addText(label, { x: x + 0.45, y: y + 0.04, w: 0.85, h: 0.3, fontSize: 9.5, color: C.textDark, bold: i === 0, margin: 0 });
  });

  s.addShape("rect", { x: 6.2, y: 2.95, w: 3.45, h: 1.55,
    fill: { color: C.white }, line: { color: C.lightGray, pt: 1 }
  });
  s.addShape("rect", { x: 6.2, y: 2.95, w: 0.07, h: 1.55, fill: { color: C.mint }, line: { color: C.mint } });
  s.addText("Supported ERPs", { x: 6.35, y: 3.05, w: 3.1, h: 0.3, fontSize: 12, bold: true, color: C.textDark, margin: 0 });
  const erps = ["Oracle Fusion Cloud ERP", "Oracle E-Business Suite", "SAP S/4HANA  ·  SAP ECC 6.0", "Microsoft Dynamics 365", "NetSuite ERP  ·  Workday HCM"];
  s.addText(erps.map(t => ({ text: t, options: { bullet: true, breakLine: true } })), {
    x: 6.35, y: 3.38, w: 3.15, h: 1.05, fontSize: 9.5, color: C.textMid
  });

  s.addText("Also select the modules (Finance, SCM, HR…) you want to ingest", {
    x: 6.25, y: 4.65, w: 3.4, h: 0.5, fontSize: 10, italic: true, color: C.textLight
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// SLIDE 7 — Wizard Step 2: Connection + SKIP
// ─────────────────────────────────────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.offWhite };
  addDarkHeader(s, "Wizard — Step 2: Configure Connection", "Enter ERP credentials OR skip to use standard industry tables");

  addScreenCard(s, shot("04_wizard_step1_erp_select.png"), 0.35, 1.3, 5.5, 3.9); // placeholder - reuse step1

  // Connection form description
  s.addShape("rect", { x: 6.1, y: 1.3, w: 3.55, h: 2.45,
    fill: { color: C.white }, line: { color: C.lightGray, pt: 1 }
  });
  s.addShape("rect", { x: 6.1, y: 1.3, w: 0.07, h: 2.45, fill: { color: C.teal }, line: { color: C.teal } });
  s.addText("Connection Fields", { x: 6.25, y: 1.42, w: 3.2, h: 0.3, fontSize: 12, bold: true, color: C.textDark, margin: 0 });
  const fields = ["Host / Service URL", "Port number", "Database / SID", "Username", "Password", "Schema (optional)"];
  s.addText(fields.map(t => ({ text: t, options: { bullet: true, breakLine: true } })), {
    x: 6.25, y: 1.76, w: 3.25, h: 1.85, fontSize: 10.5, color: C.textMid
  });

  // Skip button highlight
  s.addShape("rect", { x: 6.1, y: 3.88, w: 3.55, h: 1.25,
    fill: { color: C.mint, transparency: 10 }, line: { color: C.mint, pt: 1.5 }
  });
  s.addText("NEW  — Skip Option", { x: 6.22, y: 3.96, w: 3.2, h: 0.3, fontSize: 12, bold: true, color: C.navyDark, margin: 0 });
  s.addText(
    'No credentials? Click "Skip — use standard tables" to proceed with pre-built industry table schemas for your chosen ERP.',
    { x: 6.22, y: 4.29, w: 3.25, h: 0.75, fontSize: 9.5, color: C.textDark, margin: 0 }
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// SLIDE 8 — Wizard Steps 3–5 (flow diagram)
// ─────────────────────────────────────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.offWhite };
  addDarkHeader(s, "Wizard — Steps 3 to 5: Discover, Setup & Deploy", "Automated table discovery → Fabric configuration → one-click deploy");

  const cards = [
    {
      num: 3, title: "Discover Tables",
      lines: ["Auto-scans the ERP schema", "Lists all available tables", "Shows row counts & column info", "You select what to ingest"],
      color: "0C6478"
    },
    {
      num: 4, title: "Fabric Setup",
      lines: ["Enter OneLake storage details", "Set container & schema prefix", "Configure Delta table names", "Map ERP tables → Fabric tables"],
      color: "065A82"
    },
    {
      num: 5, title: "Deploy Pipeline",
      lines: ["Creates Fabric notebooks", "Sets up Synapse pipeline", "Runs first data load", "Schedules future syncs"],
      color: C.navy
    },
  ];

  cards.forEach((c, i) => {
    const x = 0.35 + i * 3.22;
    s.addShape("rect", { x, y: 1.3, w: 3.0, h: 3.85,
      fill: { color: C.white }, line: { color: C.lightGray, pt: 1 },
      shadow: { type: "outer", blur: 5, offset: 2, angle: 135, color: "000000", opacity: 0.08 }
    });
    s.addShape("rect", { x, y: 1.3, w: 3.0, h: 0.75, fill: { color: c.color }, line: { color: c.color } });
    s.addText(`${c.num}`, { x: x + 0.18, y: 1.38, w: 0.5, h: 0.55, fontSize: 28, bold: true, color: C.mint, margin: 0 });
    s.addText(c.title, { x: x + 0.62, y: 1.43, w: 2.2, h: 0.55, fontSize: 14, bold: true, color: C.white, valign: "middle", margin: 0 });
    s.addText(c.lines.map(l => ({ text: l, options: { bullet: true, breakLine: true } })), {
      x: x + 0.2, y: 2.15, w: 2.65, h: 2.8, fontSize: 11.5, color: C.textMid
    });

    if (i < 2) {
      s.addShape("rect", { x: x + 3.0, y: 2.97, w: 0.22, h: 0.05, fill: { color: C.mint }, line: { color: C.mint } });
      s.addText("›", { x: x + 3.18, y: 2.82, w: 0.2, h: 0.35, fontSize: 16, bold: true, color: C.mint, margin: 0 });
    }
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// SLIDE 9 — ERP Comparison
// ─────────────────────────────────────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.offWhite };
  addDarkHeader(s, "ERP Analysis & Comparison", "Side-by-side comparison of table and field schemas across ERP platforms");

  addScreenCard(s, shot("06_erp_comparison.png"), 0.35, 1.3, 9.3, 3.7);

  const tabs = [
    { name: "Table Comparison", desc: "Compare which tables exist in SAP vs Oracle vs Dynamics and spot gaps" },
    { name: "Field Comparison", desc: "Drill into field-level differences — data types, nullability, field names" },
    { name: "Adaptation Engine", desc: "AI-assisted suggestions to harmonise fields into the canonical ontology" },
  ];
  tabs.forEach((t, i) => {
    const x = 0.35 + i * 3.22;
    s.addShape("rect", { x, y: 5.12, w: 3.0, h: 0.38,
      fill: { color: i === 0 ? C.teal : C.white }, line: { color: C.lightGray }
    });
    s.addText(`${t.name}`, {
      x: x + 0.1, y: 5.12, w: 2.8, h: 0.38,
      fontSize: 9.5, bold: i === 0, color: i === 0 ? C.white : C.textMid, valign: "middle", margin: 0
    });
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// SLIDE 10 — AI Query
// ─────────────────────────────────────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.offWhite };
  addDarkHeader(s, "AI-Powered Natural Language Queries", "Ask questions in plain English — get SQL and chart results instantly");

  addScreenCard(s, shot("10_ai_query.png"), 0.35, 1.3, 5.8, 3.95);

  const pts = [
    { title: "No SQL needed", body: "Type questions like 'Show me top 10 customers by revenue last quarter'" },
    { title: "Free AI (Ollama)", body: "Runs locally — no API keys, no cloud AI costs, full data privacy" },
    { title: "Auto-generates SQL", body: "Converts NL query → SQL → executes against Delta tables in Fabric" },
  ];
  pts.forEach(({ title, body }, i) => {
    const y = 1.38 + i * 1.28;
    s.addShape("rect", { x: 6.3, y, w: 3.35, h: 1.12,
      fill: { color: C.white }, line: { color: C.lightGray, pt: 1 },
      shadow: { type: "outer", blur: 4, offset: 1, angle: 135, color: "000000", opacity: 0.07 }
    });
    s.addShape("rect", { x: 6.3, y, w: 0.07, h: 1.12, fill: { color: C.mint }, line: { color: C.mint } });
    s.addText(title, { x: 6.46, y: y + 0.1, w: 3.02, h: 0.3, fontSize: 12, bold: true, color: C.textDark, margin: 0 });
    s.addText(body, { x: 6.46, y: y + 0.44, w: 3.02, h: 0.58, fontSize: 10, color: C.textMid, margin: 0 });
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// SLIDE 11 — Settings
// ─────────────────────────────────────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.offWhite };
  addDarkHeader(s, "Settings — Users & Connections", "Manage user accounts, roles and ERP connection credentials");

  // Two screenshots side by side
  addScreenCard(s, shot("08_settings.png"), 0.3, 1.3, 4.65, 3.65);
  addScreenCard(s, shot("09_settings_users.png"), 5.1, 1.3, 4.65, 3.65);

  s.addText("Connections Tab", { x: 0.3, y: 5.1, w: 4.65, h: 0.35, fontSize: 10, color: C.textMid, align: "center", italic: true, margin: 0 });
  s.addText("Users & Roles Tab", { x: 5.1, y: 5.1, w: 4.65, h: 0.35, fontSize: 10, color: C.textMid, align: "center", italic: true, margin: 0 });
}

// ─────────────────────────────────────────────────────────────────────────────
// SLIDE 12 — End / Summary
// ─────────────────────────────────────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.navyDark };

  s.addShape("ellipse", { x: 6.5, y: -0.5, w: 4.5, h: 4.5, fill: { color: C.teal, transparency: 75 }, line: { color: C.teal, transparency: 75 } });
  s.addShape("rect", { x: 0, y: 0, w: 0.18, h: 5.625, fill: { color: C.mint }, line: { color: C.mint } });

  s.addText("End-to-End in One App", {
    x: 0.55, y: 0.6, w: 7, h: 0.65, fontSize: 30, bold: true, color: C.white, fontFace: "Calibri"
  });

  const benefits = [
    { icon: "✓", text: "Connect 8+ ERP platforms with a guided wizard" },
    { icon: "✓", text: "Skip connection details — use pre-built industry schemas" },
    { icon: "✓", text: "Compare schemas across ERPs to find gaps & differences" },
    { icon: "✓", text: "Auto-publish Delta tables to Microsoft Fabric OneLake" },
    { icon: "✓", text: "Query data in plain English with free AI (no API keys)" },
    { icon: "✓", text: "Role-based users + secure JWT authentication" },
  ];
  benefits.forEach(({ icon, text }, i) => {
    const y = 1.42 + i * 0.58;
    s.addText(icon, { x: 0.55, y, w: 0.35, h: 0.42, fontSize: 15, bold: true, color: C.mint, margin: 0 });
    s.addText(text, { x: 0.95, y, w: 6.5, h: 0.42, fontSize: 14, color: C.white, valign: "middle", margin: 0 });
  });

  s.addShape("rect", { x: 0.55, y: 5.1, w: 9, h: 0.3, fill: { color: C.teal, transparency: 50 }, line: { color: C.teal, transparency: 50 } });
  s.addText("ilinkERP-Fabric Accelerator  ·  Questions?", {
    x: 0.55, y: 5.1, w: 9, h: 0.3, fontSize: 10, color: "CADCFC", align: "center", valign: "middle", margin: 0
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Save
// ─────────────────────────────────────────────────────────────────────────────
pres.writeFile({ fileName: OUT }).then(() => {
  const size = fs.statSync(OUT).size;
  console.log(`✓ Saved: ${OUT}`);
  console.log(`  Size: ${(size / 1024).toFixed(0)} KB`);
  console.log(`  Slides: 12`);
}).catch(err => {
  console.error("Error:", err.message);
  process.exit(1);
});
