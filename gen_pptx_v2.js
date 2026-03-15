const pptxgen = require("pptxgenjs");
const path = require("path");
const fs = require("fs");

const SHOTS = path.join(__dirname, "screenshots");
const OUT = path.join(__dirname, "ilinkERP_FabricAccelerate_Presentation.pptx");

const C = {
  navyDark:  "0B2447",
  navy:      "19376D",
  teal:      "0C6478",
  mint:      "02C39A",
  white:     "FFFFFF",
  offWhite:  "F4F8FB",
  lightGray: "E2E8F0",
  textDark:  "1E293B",
  textMid:   "475569",
  textLight: "64748B",
};

const S = (name) => path.join(SHOTS, name);

// ── screen card (framed screenshot) ──────────────────────────────────────────
function sc(slide, img, x, y, w, h) {
  // shadow
  slide.addShape("rect", { x: x+0.05, y: y+0.05, w, h,
    fill: { color: "000000", transparency: 82 }, line: { color: "000000", transparency: 82 }
  });
  // white frame
  slide.addShape("rect", { x, y, w, h,
    fill: { color: C.white }, line: { color: C.lightGray, pt: 1.5 }
  });
  // image
  slide.addImage({ path: img, x: x+0.05, y: y+0.05, w: w-0.10, h: h-0.10,
    sizing: { type: "contain", w: w-0.10, h: h-0.10 }
  });
}

// ── dark header ───────────────────────────────────────────────────────────────
function hdr(s, title, sub) {
  s.addShape("rect", { x:0, y:0, w:10, h:1.05, fill:{color:C.navyDark}, line:{color:C.navyDark} });
  s.addShape("rect", { x:0, y:1.05, w:10, h:0.06, fill:{color:C.mint}, line:{color:C.mint} });
  s.addText(title, { x:0.45, y:0.08, w:8.5, h:0.55, fontSize:22, bold:true, color:C.white, margin:0 });
  if (sub) s.addText(sub, { x:0.45, y:0.63, w:8.5, h:0.36, fontSize:11.5, color:"94A3B8", margin:0 });
}

// ── right callout card ────────────────────────────────────────────────────────
function callout(s, x, y, w, h, title, body, accent) {
  const ac = accent || C.mint;
  s.addShape("rect", { x, y, w, h, fill:{color:C.white}, line:{color:C.lightGray,pt:1},
    shadow:{type:"outer",blur:4,offset:1,angle:135,color:"000000",opacity:0.07}
  });
  s.addShape("rect", { x, y, w:0.07, h, fill:{color:ac}, line:{color:ac} });
  s.addText(title, { x:x+0.17, y:y+0.1, w:w-0.22, h:0.3, fontSize:12, bold:true, color:C.textDark, margin:0 });
  s.addText(body,  { x:x+0.17, y:y+0.43, w:w-0.22, h:h-0.5, fontSize:10.5, color:C.textMid, margin:0 });
}

// ═══════════════════════════════════════════════════════════════════════════
const pres = new pptxgen();
pres.layout = "LAYOUT_16x9";
pres.title  = "ilinkERP-Fabric Accelerator";

// ── SLIDE 1 ─ Title ──────────────────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.navyDark };

  // Left accent strip (intentional design element)
  s.addShape("rect", { x:0, y:0, w:0.18, h:5.625, fill:{color:C.mint}, line:{color:C.mint} });
  // Decorative circle
  s.addShape("ellipse", { x:6.2, y:-0.6, w:5.2, h:5.2,
    fill:{color:C.teal,transparency:72}, line:{color:C.teal,transparency:72}
  });

  s.addText("ilinkERP-Fabric", { x:0.55, y:1.3, w:7, h:0.95, fontSize:44, bold:true, color:C.white, fontFace:"Calibri" });
  s.addText("Accelerator",     { x:0.55, y:2.18, w:7, h:0.75, fontSize:36, color:C.mint, fontFace:"Calibri" });
  s.addText("Connect any ERP to Microsoft Fabric in minutes — not months", {
    x:0.55, y:3.05, w:6.8, h:0.55, fontSize:15, color:"CADCFC", fontFace:"Calibri"
  });
  s.addShape("rect", { x:0.55, y:3.75, w:6.8, h:0.04, fill:{color:C.mint,transparency:60}, line:{color:C.mint,transparency:60} });
  s.addText("5-step guided wizard  ·  8 ERP platforms  ·  Microsoft Fabric OneLake  ·  Free AI", {
    x:0.55, y:3.88, w:7, h:0.35, fontSize:11, color:"94A3B8"
  });
  s.addText("ilinkERP Team  ·  2025", { x:0.55, y:5.1, w:4, h:0.32, fontSize:10, color:"94A3B8" });
}

// ── SLIDE 2 ─ Navigation Overview ───────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.offWhite };
  hdr(s, "App Navigation — What's Inside", "Four key sections covering the entire ERP-to-Fabric journey");

  const modules = [
    { icon:"⊕", title:"Dashboard",        desc:"KPIs, pipeline status\n& ingestion summary",      color:C.teal  },
    { icon:"⚙", title:"ERP Source Wizard",desc:"5-step wizard to connect\n& deploy any ERP",       color:"065A82"},
    { icon:"⇄", title:"ERP Comparison",   desc:"Compare table & field\nschemas across platforms",  color:"5B21B6"},
    { icon:"☰", title:"Settings",         desc:"Fabric connections &\nuser management",             color:"0F766E"},
  ];

  modules.forEach((m, i) => {
    const x = 0.4 + i * 2.3;
    s.addShape("rect", { x, y:1.35, w:2.1, h:3.6,
      fill:{color:C.white}, line:{color:C.lightGray,pt:1},
      shadow:{type:"outer",blur:5,offset:2,angle:135,color:"000000",opacity:0.08}
    });
    // Colored header bar
    s.addShape("rect", { x, y:1.35, w:2.1, h:0.85, fill:{color:m.color}, line:{color:m.color} });
    s.addText(m.icon, { x:x+0.08, y:1.42, w:0.55, h:0.65, fontSize:28, color:C.white, margin:0 });
    s.addText(String(i+1), { x:x+1.75, y:1.42, w:0.28, h:0.28, fontSize:16, bold:true, color:"FFFFFF", align:"right", margin:0 });
    s.addText(m.title, { x:x+0.12, y:2.28, w:1.88, h:0.4, fontSize:13, bold:true, color:C.textDark, margin:0 });
    s.addText(m.desc,  { x:x+0.12, y:2.74, w:1.88, h:0.65, fontSize:10.5, color:C.textMid, margin:0 });

    // "Click →" indicator
    s.addShape("rect", { x:x+0.12, y:4.5, w:1.88, h:0.3,
      fill:{color:m.color,transparency:88}, line:{color:m.color,transparency:50}
    });
    s.addText(i === 1 ? "5-step guided wizard" : i === 2 ? "Table + Field views" : i === 3 ? "2 tabs" : "Live metrics", {
      x:x+0.16, y:4.5, w:1.82, h:0.3, fontSize:9, color:m.color, bold:true, valign:"middle", margin:0
    });
  });
}

// ── SLIDE 3 ─ Login ──────────────────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.offWhite };
  hdr(s, "Getting Started — Secure Login", "JWT-based authentication protects your ERP data");

  sc(s, S("01_login.png"), 0.35, 1.25, 5.65, 4.05);

  callout(s, 6.25, 1.32, 3.4, 1.1, "Role-based Access",
    "Admin and Viewer roles with separate permissions");
  callout(s, 6.25, 2.52, 3.4, 1.1, "JWT Security",
    "Tokens expire automatically — no session left open");
  callout(s, 6.25, 3.72, 3.4, 1.1, "Local Auth",
    "No external identity provider needed; bcrypt password hashing");
}

// ── SLIDE 4 ─ Dashboard ──────────────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.offWhite };
  hdr(s, "Dashboard — Your Starting Point", "First page after login — shows ERP status, supported systems, and quick-start guide");

  sc(s, S("02_dashboard.png"), 0.35, 1.25, 9.3, 3.95);

  // Caption below screenshot (no overlap)
  s.addShape("rect", { x:0.35, y:5.28, w:9.3, h:0.2, fill:{color:C.lightGray}, line:{color:C.lightGray} });
  s.addText("After login you land here  →  KPI cards  ·  Supported ERP list  ·  How It Works guide  ·  Start New ERP Connection button", {
    x:0.35, y:5.28, w:9.3, h:0.2, fontSize:9, color:C.textLight, align:"center", valign:"middle", margin:0
  });
}

// ── SLIDE 5 ─ Wizard Step 1: Select ERP ─────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.offWhite };
  hdr(s, "ERP Source Wizard — Step 1: Select ERP", "Choose your ERP platform and modules to begin the 5-step guided setup");

  sc(s, S("03_erp_source.png"), 0.35, 1.25, 6.1, 4.05);

  // Step flow (vertical on right)
  const steps = [
    { n:"1", label:"Select ERP",     active:true  },
    { n:"2", label:"Connect",        active:false },
    { n:"3", label:"Discover Tables",active:false },
    { n:"4", label:"Fabric Setup",   active:false },
    { n:"5", label:"Deploy",         active:false },
  ];
  steps.forEach((st, i) => {
    const y = 1.32 + i * 0.76;
    const bg = st.active ? C.mint : C.lightGray;
    const tx = st.active ? C.navyDark : C.textLight;
    s.addShape("ellipse", { x:6.55, y:y+0.04, w:0.38, h:0.38, fill:{color:bg}, line:{color:bg} });
    s.addText(st.n, { x:6.55, y:y+0.06, w:0.38, h:0.32, fontSize:13, bold:true, color:tx, align:"center", margin:0 });
    s.addText(st.label, { x:7.04, y:y+0.06, w:2.55, h:0.32,
      fontSize: st.active ? 12 : 11, bold: st.active, color: st.active ? C.textDark : C.textLight, margin:0
    });
    if (i < 4) {
      s.addShape("rect", { x:6.72, y:y+0.42, w:0.04, h:0.34, fill:{color:C.lightGray}, line:{color:C.lightGray} });
    }
  });

  s.addShape("rect", { x:6.45, y:5.08, w:3.2, h:0.3,
    fill:{color:C.mint,transparency:18}, line:{color:C.mint}
  });
  s.addText("Select ERP → select modules → click Configure Connection →", {
    x:6.5, y:5.08, w:3.15, h:0.3, fontSize:9.5, bold:true, color:C.navyDark, valign:"middle", margin:0
  });
}

// ── SLIDE 6 ─ Wizard Step 2: Configure Connection ────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.offWhite };
  hdr(s, "Wizard — Step 2: Configure Connection", "Enter your ERP credentials — or skip to use pre-built industry table schemas");

  // Connection form mock (left panel)
  s.addShape("rect", { x:0.35, y:1.25, w:5.55, h:4.08,
    fill:{color:C.white}, line:{color:C.lightGray,pt:1.5},
    shadow:{type:"outer",blur:6,offset:2,angle:135,color:"000000",opacity:0.08}
  });
  // Form title area
  s.addShape("rect", { x:0.35, y:1.25, w:5.55, h:0.65, fill:{color:C.navy}, line:{color:C.navy} });
  s.addText("Step 2 of 5 — Configure Connection", { x:0.5, y:1.32, w:5.2, h:0.5, fontSize:13, bold:true, color:C.white, margin:0 });

  // Form fields
  const fields = [
    { label:"Host / URL", ph:"e.g. erp.company.com" },
    { label:"Port",       ph:"1521" },
    { label:"Database / SID", ph:"PROD" },
    { label:"Username",   ph:"erp_user" },
    { label:"Password",   ph:"••••••••" },
  ];
  fields.forEach((f, i) => {
    const y = 2.05 + i * 0.52;
    s.addText(f.label, { x:0.55, y, w:1.5, h:0.28, fontSize:10, bold:true, color:C.textDark, margin:0 });
    s.addShape("rect", { x:0.55, y:y+0.3, w:5.1, h:0.3, fill:{color:C.offWhite}, line:{color:C.lightGray,pt:1} });
    s.addText(f.ph, { x:0.65, y:y+0.3, w:4.9, h:0.3, fontSize:10, color:C.textLight, valign:"middle", margin:0 });
  });

  // Navigation row with Skip button (highlighted)
  s.addShape("rect", { x:0.35, y:4.88, w:5.55, h:0.45, fill:{color:"F8FAFC"}, line:{color:C.lightGray,pt:1} });
  // Back button
  s.addShape("rect", { x:0.5, y:4.95, w:1.0, h:0.3,
    fill:{color:C.white}, line:{color:C.lightGray,pt:1}
  });
  s.addText("← Back", { x:0.5, y:4.95, w:1.0, h:0.3, fontSize:10, color:C.textMid, align:"center", valign:"middle", margin:0 });
  // Skip button
  s.addShape("rect", { x:2.6, y:4.95, w:1.55, h:0.3,
    fill:{color:C.white}, line:{color:C.textLight,pt:1}
  });
  s.addText("Skip — use standard tables", { x:2.6, y:4.95, w:1.55, h:0.3, fontSize:8.5, color:C.textMid, align:"center", valign:"middle", margin:0 });
  // Next/primary button
  s.addShape("rect", { x:4.25, y:4.95, w:1.5, h:0.3, fill:{color:C.teal}, line:{color:C.teal} });
  s.addText("Discover Tables →", { x:4.25, y:4.95, w:1.5, h:0.3, fontSize:9, bold:true, color:C.white, align:"center", valign:"middle", margin:0 });

  // Right panel: explanation + Skip highlight
  callout(s, 6.2, 1.25, 3.45, 1.35,
    "What to enter",
    "Your ERP's database host, port, schema name, and credentials. These are used to scan the live ERP schema in Step 3.");
  callout(s, 6.2, 2.7, 3.45, 1.38,
    "Don't have credentials?",
    "No problem. Use the Skip button to bypass connection setup and proceed with pre-built industry-standard table schemas for your ERP.", "0891B2");

  // Skip callout highlight box
  s.addShape("rect", { x:6.2, y:4.18, w:3.45, h:1.18,
    fill:{color:C.mint,transparency:12}, line:{color:C.mint,pt:1.5}
  });
  s.addText("NEW — Skip Option", { x:6.35, y:4.25, w:3.2, h:0.3, fontSize:12, bold:true, color:C.navyDark, margin:0 });
  s.addText('Click "Skip — use standard tables" to proceed without ERP credentials. The app uses the industry data dictionary for your chosen ERP and module.', {
    x:6.35, y:4.55, w:3.15, h:0.72, fontSize:10, color:C.textDark, margin:0
  });
}

// ── SLIDE 7 ─ Wizard Steps 3–5 ───────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.offWhite };
  hdr(s, "Wizard — Steps 3 to 5: Discover, Setup & Deploy", "From schema scan to live Delta tables in Microsoft Fabric");

  const cards = [
    { n:"3", title:"Discover Tables", color:C.teal, items:[
      "Scans live ERP schema via JDBC",
      "Matches against industry data dictionary",
      "Flags missing / extra tables",
      "You select which tables to ingest",
    ]},
    { n:"4", title:"Fabric Setup", color:"065A82", items:[
      "Enter OneLake storage account",
      "Set container & schema prefix",
      "Map ERP tables → Delta table names",
      "Configure write mode (overwrite/append)",
    ]},
    { n:"5", title:"Deploy Pipeline", color:"1C4A6E", items:[
      "Creates Fabric notebooks (PySpark)",
      "Builds Synapse pipeline trigger",
      "Runs initial full data load",
      "Schedules recurring sync jobs",
    ]},
  ];

  cards.forEach((c, i) => {
    const x = 0.38 + i * 3.21;
    // Card body
    s.addShape("rect", { x, y:1.25, w:3.0, h:3.9,
      fill:{color:C.white}, line:{color:C.lightGray,pt:1},
      shadow:{type:"outer",blur:5,offset:2,angle:135,color:"000000",opacity:0.08}
    });
    // Colored header
    s.addShape("rect", { x, y:1.25, w:3.0, h:0.72, fill:{color:c.color}, line:{color:c.color} });
    // Colored circle for number
    s.addShape("ellipse", { x:x+0.12, y:1.31, w:0.5, h:0.5, fill:{color:C.mint}, line:{color:C.mint} });
    s.addText(c.n, { x:x+0.12, y:1.33, w:0.5, h:0.42, fontSize:18, bold:true, color:C.navyDark, align:"center", margin:0 });
    s.addText(c.title, { x:x+0.72, y:1.38, w:2.12, h:0.52, fontSize:14, bold:true, color:C.white, valign:"middle", margin:0 });
    // Bullets — start at top of body area
    s.addText(
      c.items.map((t,j) => ({ text:t, options:{ bullet:true, breakLine: j < c.items.length-1 } })),
      { x:x+0.2, y:2.06, w:2.65, h:2.8, fontSize:12, color:C.textMid, valign:"top" }
    );
  });

  // Arrows between cards
  [3.38, 6.59].forEach(ax => {
    s.addShape("rect", { x:ax, y:3.0, w:0.22, h:0.06, fill:{color:C.mint}, line:{color:C.mint} });
    s.addText("›", { x:ax+0.14, y:2.82, w:0.2, h:0.38, fontSize:18, bold:true, color:C.mint, margin:0 });
  });
}

// ── SLIDE 8 ─ ERP Comparison ─────────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.offWhite };
  hdr(s, "ERP Comparison — Field-Level Schema Mapping", "Compare exact field names, table names, and data types across all 8 ERP platforms");

  sc(s, S("07_erp_field_tab.png"), 0.35, 1.25, 9.3, 3.9);

  // Caption below screenshot — clearly separated
  s.addText([
    { text:"Table Comparison", options:{ bold:true, color:C.teal } },
    { text:"  ·  Filter by module (GL, AP, AR, INV…)   ", options:{ color:C.textMid } },
    { text:"Field Comparison", options:{ bold:true, color:"5B21B6" } },
    { text:"  ·  See exact column names per ERP (shown above)   ", options:{ color:C.textMid } },
    { text:"Download CSV / Excel", options:{ bold:true, color:C.teal } },
  ],
    { x:0.35, y:5.22, w:9.3, h:0.3, fontSize:10, valign:"middle", margin:0 }
  );
}

// ── SLIDE 9 ─ Settings ───────────────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.offWhite };
  hdr(s, "Settings — Fabric Connections & User Management", "Configure where data lands in Microsoft Fabric and who has access to the app");

  // Two screenshots side by side
  sc(s, S("08_settings.png"),       0.3,  1.25, 4.68, 3.88);
  sc(s, S("09_settings_users.png"), 5.08, 1.25, 4.68, 3.88);

  // Captions — high contrast dark text on light background
  s.addShape("rect", { x:0.3, y:5.2, w:4.68, h:0.3, fill:{color:C.navy}, line:{color:C.navy} });
  s.addText("Fabric Connections — Bearer Token, Service Principal, OAuth2, Managed Identity", {
    x:0.3, y:5.2, w:4.68, h:0.3, fontSize:9, bold:false, color:C.white, align:"center", valign:"middle", margin:0
  });

  s.addShape("rect", { x:5.08, y:5.2, w:4.68, h:0.3, fill:{color:C.navy}, line:{color:C.navy} });
  s.addText("User Management — Admin & Viewer roles, add/edit/delete users", {
    x:5.08, y:5.2, w:4.68, h:0.3, fontSize:9, color:C.white, align:"center", valign:"middle", margin:0
  });
}

// ── SLIDE 10 ─ Summary ───────────────────────────────────────────────────────
{
  const s = pres.addSlide();
  s.background = { color: C.navyDark };

  // Deco circle
  s.addShape("ellipse", { x:6.5, y:-0.4, w:4.5, h:4.5, fill:{color:C.teal,transparency:75}, line:{color:C.teal,transparency:75} });
  // Left strip
  s.addShape("rect", { x:0, y:0, w:0.18, h:5.625, fill:{color:C.mint}, line:{color:C.mint} });

  s.addText("Complete ERP → Fabric Journey in One App", {
    x:0.55, y:0.5, w:8.5, h:0.75, fontSize:26, bold:true, color:C.white, fontFace:"Calibri"
  });

  const points = [
    "Login with role-based JWT auth (Admin / Viewer)",
    "Dashboard — live pipeline status and supported ERP list",
    "5-step wizard — select ERP, connect (or skip), discover, configure Fabric, deploy",
    "ERP Comparison — table & field mapping across 8 platforms, export to Excel",
    "Settings — Fabric auth methods + user management",
  ];

  points.forEach((pt, i) => {
    const y = 1.45 + i * 0.72;
    s.addShape("ellipse", { x:0.55, y:y+0.05, w:0.3, h:0.3, fill:{color:C.mint}, line:{color:C.mint} });
    s.addText(String(i+1), { x:0.55, y:y+0.06, w:0.3, h:0.28, fontSize:11, bold:true, color:C.navyDark, align:"center", margin:0 });
    s.addText(pt, { x:1.0, y, w:8, h:0.42, fontSize:14, color:C.white, valign:"middle", margin:0 });
  });

  // Footer — full width
  s.addShape("rect", { x:0, y:5.15, w:10, h:0.45, fill:{color:C.teal,transparency:45}, line:{color:C.teal,transparency:45} });
  s.addText("ilinkERP-Fabric Accelerator  ·  Powered by Microsoft Fabric  ·  Questions?", {
    x:0, y:5.15, w:10, h:0.45, fontSize:11, color:C.white, align:"center", valign:"middle", margin:0
  });
}

// ── Save ──────────────────────────────────────────────────────────────────────
pres.writeFile({ fileName: OUT }).then(() => {
  const kb = Math.round(fs.statSync(OUT).size / 1024);
  console.log(`✓ Saved: ${OUT}`);
  console.log(`  Size: ${kb} KB  |  Slides: 10`);
}).catch(err => {
  console.error("Error:", err.message);
  process.exit(1);
});
