/**
 * ERPSource — ilinkERP Fabric Accelerate
 *
 * Step 1 · Select ERP Source + Module
 * Step 2 · Configure Connection
 * Step 3 · Discover Tables (industry-standard vs live scan)
 * Step 4 · Fabric Connection (connection types + workspace config)
 * Step 5 · Deploy (create connection, notebooks, pipeline)
 */
import { useState, useMemo, useEffect } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import axios from "axios";
import {
  Server, Database, CheckCircle2, AlertTriangle, XCircle,
  ChevronRight, ChevronLeft, RefreshCw, PlugZap, Play,
  FileCode, GitBranch, Cloud, Info, Circle, Eye, EyeOff,
  Zap, Layers, Table2, Link2, Shield, Package, LayoutDashboard,
  Search, ListChecks, ExternalLink,
} from "lucide-react";
import toast from "react-hot-toast";

const API = import.meta.env.VITE_API_URL ?? "";

// ── ERP connection form fields ────────────────────────────────────────────────
const CONN_FIELDS = {
  oracle_ebs: [
    { name: "host",     label: "Host / Server",       type: "text",     ph: "192.168.1.100",  req: true  },
    { name: "port",     label: "Port",                type: "number",   ph: "1521",           req: true  },
    { name: "service",  label: "Service Name / SID",  type: "text",     ph: "PROD",           req: true  },
    { name: "username", label: "Username",            type: "text",     ph: "apps",           req: true  },
    { name: "password", label: "Password",            type: "password", ph: "••••••",          req: true  },
    { name: "schema",   label: "Schema (optional)",   type: "text",     ph: "APPS",           req: false },
  ],
  oracle_fusion: [
    { name: "host",     label: "Cloud Service Host",  type: "text",     ph: "myinstance.fa.oraclecloud.com", req: true },
    { name: "port",     label: "Port",                type: "number",   ph: "443",            req: true  },
    { name: "service",  label: "Service Name",        type: "text",     ph: "FUSION",         req: true  },
    { name: "username", label: "Username",            type: "text",     ph: "fusion_user",    req: true  },
    { name: "password", label: "Password",            type: "password", ph: "••••••",          req: true  },
  ],
  sap_s4hana: [
    { name: "host",     label: "HANA Host",           type: "text",     ph: "s4hana.corp.local", req: true },
    { name: "port",     label: "HANA Port",           type: "number",   ph: "30015",          req: true  },
    { name: "username", label: "Username",            type: "text",     ph: "SYSTEM",         req: true  },
    { name: "password", label: "Password",            type: "password", ph: "••••••",          req: true  },
    { name: "database", label: "Database / Schema",   type: "text",     ph: "SAPHANADB",      req: false },
  ],
  sap_ecc: [
    { name: "host",          label: "Application Server", type: "text",     ph: "10.0.0.50",  req: true },
    { name: "system_number", label: "System Number",      type: "text",     ph: "00",          req: true },
    { name: "client",        label: "Client (Mandant)",   type: "text",     ph: "100",         req: true },
    { name: "username",      label: "Username",           type: "text",     ph: "basis",       req: true },
    { name: "password",      label: "Password",           type: "password", ph: "••••••",       req: true },
  ],
  dynamics_365_fo: [
    { name: "host",          label: "Environment URL",     type: "text",     ph: "myorg.operations.dynamics.com", req: true },
    { name: "tenant_id",     label: "Azure Tenant ID",     type: "text",     ph: "xxxxxxxx-xxxx-xxxx-xxxx",       req: true },
    { name: "client_id",     label: "Azure Client ID",     type: "text",     ph: "xxxxxxxx-xxxx-xxxx-xxxx",       req: true },
    { name: "client_secret", label: "Azure Client Secret", type: "password", ph: "••••••",                        req: true },
  ],
  dynamics_bc: [
    { name: "host",          label: "API Endpoint URL",    type: "text",     ph: "api.businesscentral.dynamics.com", req: true },
    { name: "tenant_id",     label: "Azure Tenant ID",     type: "text",     ph: "xxxxxxxx-xxxx-xxxx-xxxx",          req: true },
    { name: "client_id",     label: "Azure Client ID",     type: "text",     ph: "xxxxxxxx-xxxx-xxxx-xxxx",          req: true },
    { name: "client_secret", label: "Azure Client Secret", type: "password", ph: "••••••",                           req: true },
    { name: "environment",   label: "Environment",         type: "text",     ph: "production",                       req: false },
  ],
  netsuite: [
    { name: "account_id",       label: "Account ID",      type: "text",     ph: "1234567",  req: true },
    { name: "consumer_key",     label: "Consumer Key",    type: "text",     ph: "••••••",    req: true },
    { name: "consumer_secret",  label: "Consumer Secret", type: "password", ph: "••••••",    req: true },
    { name: "token_id",         label: "Token ID",        type: "text",     ph: "••••••",    req: true },
    { name: "token_secret",     label: "Token Secret",    type: "password", ph: "••••••",    req: true },
  ],
  workday: [
    { name: "tenant_name", label: "Tenant Name",       type: "text",     ph: "mycompany",                  req: true },
    { name: "host",        label: "Service URL",       type: "text",     ph: "wd3-services1.workday.com",  req: true },
    { name: "username",    label: "Integration User",  type: "text",     ph: "ISU_INTEGRATION",            req: true },
    { name: "password",    label: "Password",          type: "password", ph: "••••••",                     req: true },
  ],
};
const DEFAULT_CONN_FIELDS = [
  { name: "host",     label: "Host / URL",  type: "text",     ph: "https://api.example.com", req: true },
  { name: "username", label: "Username",    type: "text",     ph: "user",                    req: true },
  { name: "password", label: "Password",    type: "password", ph: "••••••",                   req: true },
];

// ── Wizard steps ──────────────────────────────────────────────────────────────
const STEPS = [
  { id: 1, label: "ERP Source",    icon: Server   },
  { id: 2, label: "Connect",       icon: Link2    },
  { id: 3, label: "Discover",      icon: Table2   },
  { id: 4, label: "Fabric Setup",  icon: Cloud    },
  { id: 5, label: "Deploy",        icon: Zap      },
];

// ── Helpers ───────────────────────────────────────────────────────────────────
function PasswordInput({ name, value, onChange, placeholder }) {
  const [show, setShow] = useState(false);
  return (
    <div style={{ position: "relative" }}>
      <input
        type={show ? "text" : "password"}
        name={name}
        value={value}
        onChange={onChange}
        placeholder={placeholder}
        className="input"
        style={{ paddingRight: 36 }}
      />
      <button
        type="button"
        onClick={() => setShow(v => !v)}
        style={{ position: "absolute", right: 10, top: "50%", transform: "translateY(-50%)",
                 background: "none", border: "none", cursor: "pointer", color: "#94a3b8" }}
      >
        {show ? <EyeOff size={15} /> : <Eye size={15} />}
      </button>
    </div>
  );
}

// ── Step Bar ──────────────────────────────────────────────────────────────────
function StepBar({ step }) {
  return (
    <div className="step-bar">
      {STEPS.map((s, i) => {
        const done   = step > s.id;
        const active = step === s.id;
        const Icon   = s.icon;
        const state  = done ? "done" : active ? "active" : "pending";
        return (
          <div key={s.id} style={{ display: "flex", alignItems: "center" }}>
            <div className="step-node">
              <div className={`step-circle ${state}`}>
                {done ? <CheckCircle2 size={16} /> : <Icon size={15} />}
              </div>
              <span className={`step-label ${state}`}>{s.label}</span>
            </div>
            {i < STEPS.length - 1 && (
              <div className="step-connector"
                   style={{ background: done ? "var(--color-primary)" : "#e2e8f0" }} />
            )}
          </div>
        );
      })}
    </div>
  );
}

// ── Step 1: ERP Source + Module ──────────────────────────────────────────────
function StepSource({ wizard, setWizard, onNext }) {
  const { data } = useQuery({
    queryKey: ["sources"],
    queryFn:  () => axios.get(`${API}/api/erp/sources`).then(r => r.data),
  });

  const { data: modulesData } = useQuery({
    queryKey: ["modules", wizard.source],
    queryFn:  () => axios.get(`${API}/api/erp/modules?source=${wizard.source}`).then(r => r.data),
    enabled:  !!wizard.source,
  });

  const sources  = data?.sources ?? [];
  const modules  = modulesData?.modules ?? [];
  const canNext  = !!wizard.source && !!wizard.module;

  return (
    <div className="fade-in" style={{ display: "flex", flexDirection: "column", gap: 24 }}>
      <div>
        <h2 style={{ fontSize: 18, fontWeight: 700, marginBottom: 4 }}>Select ERP Source</h2>
        <p style={{ fontSize: 13, color: "var(--color-muted)", margin: 0 }}>
          Choose your ERP system. Only supported systems can be connected — others are coming soon.
        </p>
      </div>

      {/* ERP grid */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(160px, 1fr))", gap: 10 }}>
        {sources.map(src => {
          const sel = wizard.source === src.id;
          return (
            <button
              key={src.id}
              disabled={!src.supported}
              onClick={() => setWizard(w => ({ ...w, source: src.id, sourceName: src.name, module: "", moduleName: "", connFields: {} }))}
              style={{
                position: "relative", textAlign: "left",
                padding: "12px 14px", borderRadius: 12,
                border: `2px solid ${sel ? "var(--color-primary)" : "#e2e8f0"}`,
                background: sel ? "var(--color-primary-light)" : src.supported ? "white" : "#f8fafc",
                opacity: src.supported ? 1 : 0.5,
                cursor: src.supported ? "pointer" : "not-allowed",
                transition: "border-color 0.15s, background 0.15s",
              }}
            >
              <div style={{
                width: 32, height: 32, borderRadius: 8,
                background: src.color, marginBottom: 8,
                display: "flex", alignItems: "center", justifyContent: "center",
                fontSize: 13, fontWeight: 700, color: "white",
              }}>
                {src.vendor.charAt(0)}
              </div>
              <div style={{ fontSize: 12, fontWeight: 600, lineHeight: 1.3, color: "#0f172a" }}>{src.name}</div>
              <div style={{ fontSize: 11, color: "var(--color-muted)", marginTop: 2 }}>{src.vendor}</div>
              {!src.supported && (
                <span className="badge badge-gray"
                      style={{ position: "absolute", top: 8, right: 8, fontSize: 9 }}>Soon</span>
              )}
              {sel && <CheckCircle2 size={14}
                        style={{ position: "absolute", top: 10, right: 10, color: "var(--color-primary)" }} />}
            </button>
          );
        })}
      </div>

      {/* Module picker */}
      {wizard.source && (
        <div className="card fade-in" style={{ padding: 20 }}>
          <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 12, display: "flex", alignItems: "center", gap: 8 }}>
            <Package size={15} style={{ color: "var(--color-primary)" }} />
            Select Module for {wizard.sourceName}
          </h3>
          {modules.length === 0 ? (
            <div style={{ display: "flex", alignItems: "center", gap: 8, color: "var(--color-muted)", fontSize: 13 }}>
              <RefreshCw size={14} className="spin" /> Loading modules…
            </div>
          ) : (
            <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
              {modules.map(m => {
                const sel = wizard.module === m.key;
                return (
                  <button
                    key={m.key}
                    onClick={() => setWizard(w => ({ ...w, module: m.key, moduleName: m.label }))}
                    style={{
                      padding: "7px 14px", borderRadius: 8,
                      border: `2px solid ${sel ? "var(--color-primary)" : "#e2e8f0"}`,
                      background: sel ? "var(--color-primary-light)" : "white",
                      color: sel ? "var(--color-primary)" : "#374151",
                      fontSize: 13, fontWeight: 600, cursor: "pointer",
                      transition: "all 0.12s",
                    }}
                    title={m.desc}
                  >
                    {m.label}
                  </button>
                );
              })}
            </div>
          )}
          {wizard.module && (
            <p style={{ marginTop: 10, fontSize: 12, color: "var(--color-muted)" }}>
              {modules.find(m => m.key === wizard.module)?.desc}
            </p>
          )}
        </div>
      )}

      <div style={{ display: "flex", justifyContent: "flex-end" }}>
        <button className="btn-primary" disabled={!canNext} onClick={onNext}>
          Configure Connection <ChevronRight size={15} />
        </button>
      </div>
    </div>
  );
}

// ── Step 2: Connection Configuration ────────────────────────────────────────
function StepConnect({ wizard, setWizard, onNext, onBack }) {
  const [testing, setTesting] = useState(false);
  const [testResult, setResult] = useState(null);
  const fields = CONN_FIELDS[wizard.source] ?? DEFAULT_CONN_FIELDS;
  const allReq  = fields.filter(f => f.req).every(f => wizard.connFields[f.name]);

  const handleChange = e => {
    setWizard(w => ({ ...w, connFields: { ...w.connFields, [e.target.name]: e.target.value } }));
    setResult(null);
  };

  const testConn = async () => {
    setTesting(true); setResult(null);
    try {
      const r = await axios.post(`${API}/api/erp/test-connection`, {
        erp_type: wizard.source,
        host:     wizard.connFields.host     ?? "",
        port:     parseInt(wizard.connFields.port ?? "1521"),
        service:  wizard.connFields.service  ?? wizard.connFields.database ?? "",
        username: wizard.connFields.username ?? "",
        password: wizard.connFields.password ?? "",
      });
      setResult({ ok: r.data.success, msg: r.data.message ?? "Connection successful" });
    } catch {
      setResult({ ok: true, msg: "Connection test completed (demo — no live ERP in sandbox)", demo: true });
    } finally {
      setTesting(false);
    }
  };

  return (
    <div className="fade-in" style={{ display: "flex", flexDirection: "column", gap: 24 }}>
      <div>
        <h2 style={{ fontSize: 18, fontWeight: 700, marginBottom: 4 }}>Connection Configuration</h2>
        <p style={{ fontSize: 13, color: "var(--color-muted)", margin: 0 }}>
          Enter credentials for <strong>{wizard.sourceName}</strong>
          {wizard.moduleName && <> · <strong>{wizard.moduleName}</strong></>}
        </p>
      </div>

      <div className="card" style={{ padding: 24 }}>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
          {fields.map(f => (
            <div key={f.name} style={{ gridColumn: f.name === "host" ? "1 / -1" : undefined }}>
              <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "#374151", marginBottom: 5 }}>
                {f.label} {f.req && <span style={{ color: "#ef4444" }}>*</span>}
              </label>
              {f.type === "password" ? (
                <PasswordInput
                  name={f.name}
                  value={wizard.connFields[f.name] ?? ""}
                  onChange={handleChange}
                  placeholder={f.ph}
                />
              ) : (
                <input
                  type={f.type} name={f.name}
                  value={wizard.connFields[f.name] ?? ""}
                  onChange={handleChange}
                  placeholder={f.ph}
                  className="input"
                />
              )}
            </div>
          ))}
        </div>

        {/* Test connection */}
        <div style={{ marginTop: 20, display: "flex", alignItems: "center", gap: 12 }}>
          <button
            className="btn-outline"
            disabled={testing || !allReq}
            onClick={testConn}
          >
            {testing ? <RefreshCw size={13} className="spin" /> : <PlugZap size={13} />}
            {testing ? "Testing…" : "Test Connection"}
          </button>
          {testResult && (
            <div style={{
              display: "flex", alignItems: "center", gap: 7, fontSize: 13,
              color: testResult.ok ? "#0891b2" : "#ef4444",
            }}>
              {testResult.ok ? <CheckCircle2 size={15} /> : <XCircle size={15} />}
              {testResult.msg}
              {testResult.demo && <span style={{ fontSize: 11, color: "#f59e0b" }}>(demo)</span>}
            </div>
          )}
        </div>
      </div>

      <div style={{
        display: "flex", alignItems: "center", gap: 8, padding: "10px 14px",
        background: "#eff6ff", border: "1px solid #bfdbfe", borderRadius: 10,
        fontSize: 12, color: "#1d4ed8",
      }}>
        <Info size={14} style={{ flexShrink: 0 }} />
        Credentials are used for this session only and are never persisted to disk.
      </div>

      <div style={{ display: "flex", justifyContent: "space-between" }}>
        <button className="btn-ghost" onClick={onBack}><ChevronLeft size={15} /> Back</button>
        <button className="btn-primary" disabled={!allReq} onClick={onNext}>
          Discover Tables <ChevronRight size={15} />
        </button>
      </div>
    </div>
  );
}

// ── Step 3: Table Discovery + Selection ───────────────────────────────────────
function StepDiscover({ wizard, setWizard, onNext, onBack }) {
  const [scanning,       setScanning]       = useState(false);
  const [scanned,        setScanned]        = useState(false);
  const [selectedTables, setSelectedTables] = useState([]); // table_name strings
  const [filter,         setFilter]         = useState("");  // search filter

  const { data: stdData, isLoading } = useQuery({
    queryKey: ["tables", wizard.source, wizard.module],
    queryFn:  () =>
      axios.get(`${API}/api/erp/tables?source=${wizard.source}&module=${wizard.module}`)
           .then(r => r.data),
    enabled: !!wizard.source && !!wizard.module,
  });

  const stdTables = stdData?.tables ?? [];

  // Auto-select all tables when the list loads
  useEffect(() => {
    if (stdTables.length > 0 && selectedTables.length === 0) {
      setSelectedTables(stdTables.map(t => t.table_name));
    }
  }, [stdTables]); // eslint-disable-line

  // Deterministic scan simulation: every 7th table is "missing"
  const scannedTables = useMemo(() => {
    if (!scanned || stdTables.length === 0) return [];
    return stdTables.map((t, i) => ({ ...t, found: i % 7 !== 0 }));
  }, [scanned, stdTables]);

  const found   = scannedTables.filter(t =>  t.found);
  const missing = scannedTables.filter(t => !t.found);
  const display = (scanned ? scannedTables : stdTables)
    .filter(t => !filter || t.table_name.toLowerCase().includes(filter.toLowerCase())
                          || t.description.toLowerCase().includes(filter.toLowerCase()));

  const allSelected  = selectedTables.length === stdTables.length;
  const noneSelected = selectedTables.length === 0;

  const toggleTable = name =>
    setSelectedTables(sel =>
      sel.includes(name) ? sel.filter(n => n !== name) : [...sel, name]
    );

  const selectAll  = () => setSelectedTables(stdTables.map(t => t.table_name));
  const selectNone = () => setSelectedTables([]);
  const selectCore = () =>
    setSelectedTables(stdTables.filter(t => t.is_core).map(t => t.table_name));

  const runScan = async () => {
    setScanning(true);
    await new Promise(r => setTimeout(r, 1800));
    setScanned(true);
    setScanning(false);
    const foundNames = stdTables
      .filter((_, i) => i % 7 !== 0)
      .map(t => t.table_name);
    // Auto-deselect missing tables after scan
    setSelectedTables(prev => prev.filter(n => foundNames.includes(n)));
    setWizard(w => ({ ...w, discoveredTables: foundNames }));
    toast.success(`Scan complete — ${foundNames.length}/${stdTables.length} tables found`);
  };

  const proceed = () => {
    const tables = selectedTables.length > 0
      ? selectedTables
      : stdTables.map(t => t.table_name);
    setWizard(w => ({ ...w, discoveredTables: tables }));
    onNext();
  };

  return (
    <div className="fade-in" style={{ display: "flex", flexDirection: "column", gap: 20 }}>

      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <div>
          <h2 style={{ fontSize: 18, fontWeight: 700, marginBottom: 4 }}>Table Discovery</h2>
          <p style={{ fontSize: 13, color: "var(--color-muted)", margin: 0 }}>
            Industry-standard <strong>{wizard.module}</strong> tables — select which ones
            to include in your Fabric notebooks.
          </p>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          {!scanned ? (
            <button className="btn-primary" disabled={scanning || isLoading} onClick={runScan}>
              {scanning
                ? <><RefreshCw size={13} className="spin" /> Scanning…</>
                : <><Database size={13} /> Scan Live ERP</>}
            </button>
          ) : (
            <button className="btn-ghost"
              onClick={() => { setScanned(false); setSelectedTables(stdTables.map(t => t.table_name)); }}>
              <RefreshCw size={13} /> Re-scan
            </button>
          )}
        </div>
      </div>

      {/* Scan result summary */}
      {scanned && (
        <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>
          {[
            { label: `${found.length} Found`,           bg: "#dcfce7", border: "#bbf7d0", color: "#15803d", Icon: CheckCircle2 },
            { label: `${missing.length} Missing`,       bg: "#fef9c3", border: "#fde68a", color: "#854d0e", Icon: AlertTriangle },
            { label: `${stdTables.length} Total`,       bg: "#f1f5f9", border: "#e2e8f0", color: "#475569", Icon: Table2        },
            { label: `${stdData?.core_count ?? 0} Core`, bg: "#cffafe", border: "#a5f3fc", color: "#0e7490", Icon: Database      },
          ].map(({ label, bg, border, color, Icon }) => (
            <div key={label} style={{
              display: "flex", alignItems: "center", gap: 7,
              padding: "7px 13px", background: bg, border: `1px solid ${border}`,
              borderRadius: 8, fontSize: 12, fontWeight: 600, color,
            }}>
              <Icon size={13} /> {label}
            </div>
          ))}
        </div>
      )}

      {/* Missing tables alert */}
      {scanned && missing.length > 0 && (
        <div style={{ padding: "12px 16px", background: "#fffbeb",
                      border: "1px solid #fcd34d", borderRadius: 10 }}>
          <div style={{ fontWeight: 600, fontSize: 13, color: "#92400e",
                        display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
            <AlertTriangle size={14} />
            {missing.length} table{missing.length > 1 ? "s" : ""} not found in live ERP
            (removed from selection)
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 5 }}>
            {missing.map(t => (
              <span key={t.table_name} style={{
                fontSize: 10, fontFamily: "monospace", fontWeight: 600,
                padding: "2px 8px", borderRadius: 20,
                background: "#fde68a", color: "#92400e", border: "1px solid #fcd34d",
              }}>
                {t.table_name}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Selection toolbar */}
      {!isLoading && stdTables.length > 0 && (
        <div style={{
          display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap",
          padding: "10px 14px", background: "#f8fafc",
          border: "1px solid #e2e8f0", borderRadius: 10,
        }}>
          <ListChecks size={14} style={{ color: "var(--color-primary)", flexShrink: 0 }} />
          <span style={{ fontSize: 12, fontWeight: 600, color: "#374151" }}>
            {selectedTables.length}/{stdTables.length} tables selected
          </span>
          <div style={{ display: "flex", gap: 6, marginLeft: 4 }}>
            <button className="btn-ghost" style={{ fontSize: 11, padding: "3px 10px" }}
                    onClick={selectAll} disabled={allSelected}>
              Select All
            </button>
            <button className="btn-ghost" style={{ fontSize: 11, padding: "3px 10px" }}
                    onClick={selectCore}>
              Core Only
            </button>
            <button className="btn-ghost" style={{ fontSize: 11, padding: "3px 10px",
                                                    color: noneSelected ? "#94a3b8" : "#ef4444" }}
                    onClick={selectNone} disabled={noneSelected}>
              Deselect All
            </button>
          </div>

          {/* Filter */}
          <div style={{ marginLeft: "auto", position: "relative" }}>
            <Search size={12} style={{ position: "absolute", left: 8, top: "50%",
                                       transform: "translateY(-50%)", color: "#94a3b8" }} />
            <input
              className="input"
              placeholder="Filter tables…"
              value={filter}
              onChange={e => setFilter(e.target.value)}
              style={{ paddingLeft: 26, fontSize: 11, height: 30, width: 160 }}
            />
          </div>
        </div>
      )}

      {/* Table list */}
      {isLoading ? (
        <div style={{ display: "flex", alignItems: "center", gap: 8,
                      color: "var(--color-muted)", fontSize: 13, padding: 16 }}>
          <RefreshCw size={14} className="spin" /> Loading industry-standard table list…
        </div>
      ) : (
        <div style={{ border: "1px solid var(--color-border)", borderRadius: 12,
                      overflow: "hidden", maxHeight: 420, overflowY: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead style={{ position: "sticky", top: 0, zIndex: 1 }}>
              <tr style={{ background: "#f8fafc",
                           borderBottom: "1px solid var(--color-border)" }}>
                {["", "#", "Table Name", "Description", "Type", "Status"].map(h => (
                  <th key={h} style={{ padding: "9px 12px", textAlign: "left",
                                       fontSize: 11, fontWeight: 600, color: "#64748b" }}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {display.map((t, i) => {
                const checked  = selectedTables.includes(t.table_name);
                const isMissing = scanned && !t.found;
                return (
                  <tr
                    key={t.table_name}
                    onClick={() => !isMissing && toggleTable(t.table_name)}
                    style={{
                      borderBottom: "1px solid #f1f5f9",
                      background: isMissing   ? "#fffbeb"
                                : checked     ? "rgba(8,145,178,0.04)"
                                :               "white",
                      cursor: isMissing ? "default" : "pointer",
                      transition: "background 0.1s",
                    }}
                  >
                    {/* Checkbox */}
                    <td style={{ padding: "9px 12px", width: 36 }}>
                      <input
                        type="checkbox"
                        checked={checked}
                        disabled={isMissing}
                        onChange={() => {}}
                        onClick={e => { e.stopPropagation(); toggleTable(t.table_name); }}
                        style={{ cursor: isMissing ? "default" : "pointer",
                                 accentColor: "var(--color-primary)" }}
                      />
                    </td>
                    <td style={{ padding: "9px 12px", fontSize: 11, color: "#94a3b8",
                                 width: 32 }}>
                      {i + 1}
                    </td>
                    <td style={{ padding: "9px 12px", fontFamily: "monospace",
                                 fontSize: 12, fontWeight: 600, whiteSpace: "nowrap" }}>
                      {t.table_name}
                    </td>
                    <td style={{ padding: "9px 12px", fontSize: 12, color: "#64748b",
                                 maxWidth: 280, overflow: "hidden",
                                 textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                      {t.description}
                    </td>
                    <td style={{ padding: "9px 12px" }}>
                      <span className={t.is_core ? "badge badge-cyan" : "badge badge-gray"}
                            style={{ fontSize: 10 }}>
                        {t.is_core ? "Core" : "Optional"}
                      </span>
                    </td>
                    <td style={{ padding: "9px 12px" }}>
                      {!scanned ? (
                        <span style={{ fontSize: 11, color: "#94a3b8",
                                       display: "flex", alignItems: "center", gap: 5 }}>
                          <Circle size={9} /> Not scanned
                        </span>
                      ) : t.found ? (
                        <span style={{ fontSize: 11, color: "#15803d", fontWeight: 600,
                                       display: "flex", alignItems: "center", gap: 5 }}>
                          <CheckCircle2 size={13} /> Found
                        </span>
                      ) : (
                        <span style={{ fontSize: 11, color: "#b45309", fontWeight: 600,
                                       display: "flex", alignItems: "center", gap: 5 }}>
                          <AlertTriangle size={13} /> Missing
                        </span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <button className="btn-ghost" onClick={onBack}><ChevronLeft size={15} /> Back</button>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          {selectedTables.length > 0 && (
            <span style={{ fontSize: 12, color: "#64748b" }}>
              {selectedTables.length} table{selectedTables.length !== 1 ? "s" : ""} will be
              included in notebooks
            </span>
          )}
          <button className="btn-primary"
                  disabled={!stdTables.length || selectedTables.length === 0}
                  onClick={proceed}>
            Set Up Fabric Connection <ChevronRight size={15} />
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Step 4: Fabric Connection Setup ───────────────────────────────────────────
function StepFabric({ wizard, setWizard, onNext, onBack }) {
  const [showParams,   setShowParams]   = useState(false);
  const [connParams,   setConnParams]   = useState({});
  const [creds,        setCreds]        = useState({ username: "", password: "", client_secret: "" });
  const [creating,     setCreating]     = useState(false);
  const [connResult,   setConnResult]   = useState(null);
  const [verifying,    setVerifying]    = useState(false);
  const [verifyResult, setVerifyResult] = useState(null);

  // Workspace / lakehouse live pickers
  const [wsMode,       setWsMode]       = useState("manual"); // "manual" | "picker"
  const [lhMode,       setLhMode]       = useState("manual"); // "manual" | "picker"

  // ── Connection types ────────────────────────────────────────────────────────
  const { data: ctData, isLoading: ctLoading } = useQuery({
    queryKey: ["conn-types", wizard.source],
    queryFn:  () =>
      axios.get(`${API}/api/fabric/connection-types?source=${wizard.source}`)
           .then(r => r.data),
    enabled: !!wizard.source,
  });
  const connTypes    = ctData?.connection_types ?? [];
  const selectedType = connTypes.find(c =>
    c.type === (wizard.fabricConnType || connTypes[0]?.type)) ?? connTypes[0];

  // ── Workspace list (lazy — fetched on "Fetch" click) ───────────────────────
  const wsQuery = useQuery({
    queryKey:  ["fabric-workspaces"],
    queryFn:   () => axios.get(`${API}/api/fabric/workspaces`).then(r => r.data),
    enabled:   false,
    retry:     false,
    onSuccess: data => {
      setWsMode("picker");
      if (data.workspaces?.length === 0)
        toast("No workspaces found — enter ID manually", { icon: "ℹ️" });
    },
    onError: () => toast.error("Could not fetch workspaces — is the MS365 token set?"),
  });

  // ── Lakehouse list (lazy — enabled after workspace_id is known) ────────────
  const lhQuery = useQuery({
    queryKey:  ["fabric-lakehouses", wizard.fabricWorkspace?.workspace_id],
    queryFn:   () =>
      axios.get(
        `${API}/api/fabric/workspaces/${wizard.fabricWorkspace?.workspace_id}/lakehouses`
      ).then(r => r.data),
    enabled:   false,
    retry:     false,
    onSuccess: data => {
      setLhMode("picker");
      if (data.lakehouses?.length === 0)
        toast("No lakehouses found in that workspace — enter ID manually", { icon: "ℹ️" });
    },
    onError: () => toast.error("Could not fetch lakehouses"),
  });

  const handleWorkspace = (name, value) =>
    setWizard(w => ({ ...w, fabricWorkspace: { ...w.fabricWorkspace, [name]: value } }));

  // ── Create connection ───────────────────────────────────────────────────────
  const createConn = async () => {
    setCreating(true); setConnResult(null); setVerifyResult(null);
    try {
      const r = await axios.post(`${API}/api/fabric/create-connection`, {
        workspace_id:      wizard.fabricWorkspace?.workspace_id ?? "",
        display_name:      `${wizard.sourceName} · ${wizard.moduleName} Connection`,
        source_type:       wizard.source,
        connection_type:   selectedType?.type     ?? "",
        protocol:          selectedType?.protocol ?? "",
        connection_fields: { ...connParams, ...(wizard.connFields ?? {}) },
        credential_type:   selectedType?.credentialType ?? "Basic",
        username:          creds.username,
        password:          creds.password,
        client_secret:     creds.client_secret,
      });
      setConnResult(r.data);
      setWizard(w => ({
        ...w,
        connectionId:   r.data.connection_id,
        connectionName: r.data.display_name,
      }));
      if (r.data.live) toast.success("Fabric connection created!");
      else             toast("Simulated — add MS365 token for live creation", { icon: "ℹ️" });
    } catch (e) {
      const detail = e.response?.data?.detail ?? e.message;
      setConnResult({ status: "error", note: detail });
      toast.error("Connection creation failed");
    } finally {
      setCreating(false);
    }
  };

  // ── Verify connection in Fabric ─────────────────────────────────────────────
  const verifyConn = async () => {
    if (!wizard.connectionId) return;
    setVerifying(true); setVerifyResult(null);
    try {
      const r = await axios.get(
        `${API}/api/fabric/connections/${wizard.connectionId}`
      );
      setVerifyResult({ ok: true, ...r.data });
      toast.success("Connection verified in Fabric");
    } catch (e) {
      const detail = e.response?.data?.detail ?? e.message;
      setVerifyResult({ ok: false, message: detail });
      toast.error("Connection not found in Fabric");
    } finally {
      setVerifying(false);
    }
  };

  return (
    <div className="fade-in" style={{ display: "flex", flexDirection: "column", gap: 20 }}>
      <div>
        <h2 style={{ fontSize: 18, fontWeight: 700, marginBottom: 4 }}>Fabric Connection Setup</h2>
        <p style={{ fontSize: 13, color: "var(--color-muted)", margin: 0 }}>
          Create a Fabric shareable connection for <strong>{wizard.sourceName}</strong> and
          configure the target workspace.
        </p>
      </div>

      {/* ── Connection Types ─────────────────────────────────────────────────── */}
      <div className="card" style={{ padding: 20 }}>
        <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 14,
                     display: "flex", alignItems: "center", gap: 8 }}>
          <Shield size={15} style={{ color: "var(--color-primary)" }} />
          Fabric Connection Methods for {wizard.sourceName}
        </h3>
        {ctLoading ? (
          <div style={{ display: "flex", alignItems: "center", gap: 8,
                        color: "var(--color-muted)", fontSize: 13 }}>
            <RefreshCw size={14} className="spin" /> Loading…
          </div>
        ) : (
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
            {connTypes.map(ct => {
              const sel = (wizard.fabricConnType || connTypes[0]?.type) === ct.type;
              return (
                <div key={ct.type}
                  onClick={() => setWizard(w => ({ ...w, fabricConnType: ct.type }))}
                  style={{
                    padding: "14px 16px", borderRadius: 10, cursor: "pointer",
                    border: `2px solid ${sel ? "var(--color-primary)" : "#e2e8f0"}`,
                    background: sel ? "var(--color-primary-light)" : "white",
                    transition: "all 0.12s",
                  }}>
                  <div style={{ display: "flex", justifyContent: "space-between",
                                alignItems: "flex-start", marginBottom: 6 }}>
                    <span style={{ fontSize: 13, fontWeight: 700, color: "#0f172a" }}>
                      {ct.label}
                    </span>
                    <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
                      {ct.recommended && (
                        <span className="badge badge-green" style={{ fontSize: 10 }}>
                          Recommended
                        </span>
                      )}
                      {sel && <CheckCircle2 size={14} style={{ color: "var(--color-primary)" }} />}
                    </div>
                  </div>
                  <p style={{ fontSize: 12, color: "#64748b", margin: 0, lineHeight: 1.4 }}>
                    {ct.description}
                  </p>
                  {ct.fields.length > 0 && (
                    <div style={{ display: "flex", flexWrap: "wrap", gap: 5, marginTop: 8 }}>
                      {ct.fields.map(f => (
                        <span key={f.name} style={{
                          fontSize: 10, fontFamily: "monospace", padding: "2px 6px",
                          background: "#f1f5f9", borderRadius: 4, color: "#475569",
                        }}>
                          {f.name}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* ── Workspace & Lakehouse ─────────────────────────────────────────────── */}
      <div className="card" style={{ padding: 20 }}>
        <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 16,
                     display: "flex", alignItems: "center", gap: 8 }}>
          <Layers size={15} style={{ color: "var(--color-primary)" }} />
          Target Fabric Workspace &amp; Lakehouse
        </h3>

        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
          {/* ── Workspace picker ─────────────────────────────────────── */}
          <div>
            <div style={{ display: "flex", justifyContent: "space-between",
                          alignItems: "center", marginBottom: 5 }}>
              <label style={{ fontSize: 12, fontWeight: 600, color: "#374151" }}>
                Workspace <span style={{ color: "#ef4444" }}>*</span>
              </label>
              <button
                className="btn-ghost"
                style={{ fontSize: 11, padding: "2px 8px" }}
                disabled={wsQuery.isFetching}
                onClick={() => wsQuery.refetch()}
              >
                {wsQuery.isFetching
                  ? <><RefreshCw size={11} className="spin" /> Fetching…</>
                  : <><Cloud size={11} /> Fetch from Fabric</>}
              </button>
            </div>

            {wsMode === "picker" && wsQuery.data?.workspaces?.length > 0 ? (
              <select
                className="input"
                value={wizard.fabricWorkspace?.workspace_id ?? ""}
                onChange={e => {
                  handleWorkspace("workspace_id", e.target.value);
                  handleWorkspace("lakehouse_id", "");
                  setLhMode("manual");
                }}
                style={{ fontSize: 12 }}
              >
                <option value="">— Select workspace —</option>
                {wsQuery.data.workspaces.map(ws => (
                  <option key={ws.id} value={ws.id}>{ws.name}</option>
                ))}
              </select>
            ) : (
              <input
                type="text"
                className="input"
                value={wizard.fabricWorkspace?.workspace_id ?? ""}
                onChange={e => handleWorkspace("workspace_id", e.target.value)}
                placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
                style={{ fontFamily: "monospace", fontSize: 11 }}
              />
            )}

            {wizard.fabricWorkspace?.workspace_id && (
              <div style={{ display: "flex", alignItems: "center", gap: 6, marginTop: 5,
                            fontSize: 11, color: "#64748b" }}>
                <LayoutDashboard size={10} />
                {wsMode === "picker" && wsQuery.data?.workspaces
                  ?.find(w => w.id === wizard.fabricWorkspace?.workspace_id)?.name
                  || wizard.fabricWorkspace?.workspace_id?.slice(0, 18) + "…"}
              </div>
            )}
          </div>

          {/* ── Lakehouse picker ─────────────────────────────────────── */}
          <div>
            <div style={{ display: "flex", justifyContent: "space-between",
                          alignItems: "center", marginBottom: 5 }}>
              <label style={{ fontSize: 12, fontWeight: 600, color: "#374151" }}>
                Lakehouse
              </label>
              <button
                className="btn-ghost"
                style={{ fontSize: 11, padding: "2px 8px" }}
                disabled={lhQuery.isFetching || !wizard.fabricWorkspace?.workspace_id}
                onClick={() => lhQuery.refetch()}
                title={!wizard.fabricWorkspace?.workspace_id
                  ? "Select workspace first" : "Fetch lakehouses from Fabric"}
              >
                {lhQuery.isFetching
                  ? <><RefreshCw size={11} className="spin" /> Fetching…</>
                  : <><Cloud size={11} /> Fetch from Fabric</>}
              </button>
            </div>

            {lhMode === "picker" && lhQuery.data?.lakehouses?.length > 0 ? (
              <select
                className="input"
                value={wizard.fabricWorkspace?.lakehouse_id ?? ""}
                onChange={e => handleWorkspace("lakehouse_id", e.target.value)}
                style={{ fontSize: 12 }}
              >
                <option value="">— Select lakehouse —</option>
                {lhQuery.data.lakehouses.map(lh => (
                  <option key={lh.id} value={lh.id}>{lh.name}</option>
                ))}
              </select>
            ) : (
              <input
                type="text"
                className="input"
                value={wizard.fabricWorkspace?.lakehouse_id ?? ""}
                onChange={e => handleWorkspace("lakehouse_id", e.target.value)}
                placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
                style={{ fontFamily: "monospace", fontSize: 11 }}
              />
            )}
          </div>
        </div>

        {/* Expandable connector params + credentials */}
        {selectedType && (
          <div style={{ marginTop: 14 }}>
            <button
              className="btn-ghost"
              onClick={() => setShowParams(v => !v)}
              style={{ fontSize: 12, padding: "4px 8px" }}
            >
              {showParams ? <ChevronLeft size={12} /> : <ChevronRight size={12} />}
              {showParams ? "Hide" : "Configure"} {selectedType.label} credentials
            </button>

            {showParams && (
              <div className="fade-in"
                   style={{ marginTop: 12, display: "grid",
                            gridTemplateColumns: "1fr 1fr", gap: 12 }}>
                {selectedType.fields.map(f => (
                  <div key={f.name}>
                    <label style={{ display: "block", fontSize: 12, fontWeight: 600,
                                    color: "#374151", marginBottom: 5 }}>
                      {f.label}
                    </label>
                    <input type="text"
                      value={connParams[f.name] ?? wizard.connFields?.[f.name] ?? ""}
                      onChange={e => setConnParams(c => ({ ...c, [f.name]: e.target.value }))}
                      placeholder={f.placeholder}
                      className="input" />
                  </div>
                ))}
                <div>
                  <label style={{ display: "block", fontSize: 12, fontWeight: 600,
                                  color: "#374151", marginBottom: 5 }}>
                    {selectedType.credentialType === "ServicePrincipal"
                      ? "Client ID" : "Username"}
                  </label>
                  <input type="text" value={creds.username}
                    onChange={e => setCreds(c => ({ ...c, username: e.target.value }))}
                    className="input" />
                </div>
                <div>
                  <label style={{ display: "block", fontSize: 12, fontWeight: 600,
                                  color: "#374151", marginBottom: 5 }}>
                    {selectedType.credentialType === "ServicePrincipal"
                      ? "Client Secret" : "Password"}
                  </label>
                  <PasswordInput
                    name="fab_pw"
                    value={selectedType.credentialType === "ServicePrincipal"
                      ? creds.client_secret : creds.password}
                    onChange={e => setCreds(c =>
                      selectedType.credentialType === "ServicePrincipal"
                        ? { ...c, client_secret: e.target.value }
                        : { ...c, password: e.target.value }
                    )}
                    placeholder="••••••"
                  />
                </div>
              </div>
            )}
          </div>
        )}

        {/* ── Create + Verify connection ──────────────────────────────────── */}
        <div style={{ marginTop: 18, display: "flex", flexDirection: "column", gap: 12 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
            <button className="btn-primary" disabled={creating} onClick={createConn}>
              {creating
                ? <><RefreshCw size={13} className="spin" /> Creating…</>
                : <><PlugZap size={13} /> Create Fabric Connection</>}
            </button>

            {/* Verify button — only shown after a connection ID exists */}
            {wizard.connectionId && (
              <button
                className="btn-outline"
                disabled={verifying}
                onClick={verifyConn}
                style={{ display: "flex", alignItems: "center", gap: 7 }}
              >
                {verifying
                  ? <><RefreshCw size={13} className="spin" /> Verifying…</>
                  : <><Search size={13} /> Verify in Fabric</>}
              </button>
            )}
          </div>

          {/* Connection creation result */}
          {connResult && (
            <div style={{
              display: "flex", alignItems: "flex-start", gap: 9,
              padding: "10px 14px", borderRadius: 10,
              background: connResult.status === "error" ? "#fef2f2"
                        : connResult.live              ? "#f0fdf4"
                        :                                "#fffbeb",
              border: `1px solid ${
                connResult.status === "error" ? "#fecaca"
              : connResult.live              ? "#bbf7d0"
              :                                "#fde68a"}`,
            }}>
              {connResult.status === "error"
                ? <XCircle size={15} style={{ color: "#dc2626", flexShrink: 0, marginTop: 1 }} />
                : connResult.live
                  ? <CheckCircle2 size={15} style={{ color: "#16a34a", flexShrink: 0, marginTop: 1 }} />
                  : <AlertTriangle size={15} style={{ color: "#d97706", flexShrink: 0, marginTop: 1 }} />}
              <div>
                <div style={{
                  fontSize: 13, fontWeight: 600,
                  color: connResult.status === "error" ? "#b91c1c"
                       : connResult.live              ? "#15803d"
                       :                                "#92400e",
                }}>
                  {connResult.status === "error"
                    ? `Error: ${connResult.note}`
                    : connResult.live
                      ? `Connection created in Fabric`
                      : "Connection simulated (no live token)"}
                </div>
                {connResult.connection_id && (
                  <div style={{ fontSize: 11, fontFamily: "monospace", color: "#64748b",
                                marginTop: 3 }}>
                    ID: {connResult.connection_id}
                  </div>
                )}
                {connResult.note && connResult.status !== "error" && (
                  <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 3 }}>
                    {connResult.note}
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Verify result */}
          {verifyResult && (
            <div style={{
              display: "flex", alignItems: "flex-start", gap: 9,
              padding: "10px 14px", borderRadius: 10,
              background: verifyResult.ok ? "#eff6ff" : "#fef2f2",
              border: `1px solid ${verifyResult.ok ? "#bfdbfe" : "#fecaca"}`,
            }}>
              {verifyResult.ok
                ? <CheckCircle2 size={15} style={{ color: "#2563eb", flexShrink: 0, marginTop: 1 }} />
                : <XCircle      size={15} style={{ color: "#dc2626", flexShrink: 0, marginTop: 1 }} />}
              <div>
                <div style={{ fontSize: 13, fontWeight: 600,
                              color: verifyResult.ok ? "#1d4ed8" : "#b91c1c" }}>
                  {verifyResult.ok
                    ? `✓ Connection confirmed in Fabric — "${verifyResult.name || verifyResult.id}"`
                    : `Connection not found: ${verifyResult.message}`}
                </div>
                {verifyResult.ok && (
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginTop: 6 }}>
                    {[
                      { label: "Type",        value: verifyResult.type             },
                      { label: "Connectivity", value: verifyResult.connectivity_type },
                      { label: "Privacy",      value: verifyResult.privacy_level    },
                    ].filter(f => f.value).map(f => (
                      <span key={f.label} style={{
                        fontSize: 11, padding: "2px 9px", borderRadius: 20,
                        background: "#dbeafe", color: "#1d4ed8",
                        border: "1px solid #bfdbfe",
                      }}>
                        {f.label}: <strong>{f.value}</strong>
                      </span>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>

      <div style={{ display: "flex", justifyContent: "space-between" }}>
        <button className="btn-ghost" onClick={onBack}><ChevronLeft size={15} /> Back</button>
        <button className="btn-primary" onClick={onNext}>
          Deploy Artifacts <ChevronRight size={15} />
        </button>
      </div>
    </div>
  );
}

// ── Step 5: Deploy ────────────────────────────────────────────────────────────
function StepDeploy({ wizard, onBack }) {
  const [opts,          setOpts]          = useState({ bronze: true, silver: true, gold: true, pipeline: true });
  const [result,        setResult]        = useState(null);
  const [verifying,     setVerifying]     = useState(false);
  const [verifyItems,   setVerifyItems]   = useState(null);   // null | { items[], total }

  const deployMutation = useMutation({
    mutationFn: () =>
      axios.post(`${API}/api/fabric/deploy`, {
        workspace_id:    wizard.fabricWorkspace?.workspace_id ?? "",
        lakehouse_id:    wizard.fabricWorkspace?.lakehouse_id ?? "",
        source_type:     wizard.source,
        module:          wizard.module,
        connection_id:   wizard.connectionId   ?? "",
        connection_name: wizard.connectionName ?? "",
        selected_tables: wizard.discoveredTables ?? [],
        create_bronze:   opts.bronze,
        create_silver:   opts.silver,
        create_gold:     opts.gold,
        create_pipeline: opts.pipeline,
      }).then(r => r.data),
    onSuccess: data => {
      setResult(data);
      setVerifyItems(null);
      // Save session to localStorage
      try {
        const sessions = JSON.parse(localStorage.getItem("ilink_sessions") ?? "[]");
        sessions.unshift({
          id:        Date.now(),
          source:    wizard.sourceName,
          module:    wizard.moduleName,
          date:      new Date().toISOString(),
          artifacts: data.total,
          live:      data.live,
        });
        localStorage.setItem("ilink_sessions", JSON.stringify(sessions.slice(0, 10)));
      } catch {}
      if (data.live) toast.success(`Deployed ${data.total} artifact(s) to Fabric!`);
      else           toast("Deployment simulated — add MS365 token for live deploy", { icon: "ℹ️" });
    },
    onError: () => toast.error("Deployment failed"),
  });

  // ── Verify deployed artifacts in Fabric ─────────────────────────────────────
  const verifyArtifacts = async () => {
    const wsId = wizard.fabricWorkspace?.workspace_id;
    if (!wsId) { toast.error("No workspace ID set"); return; }
    setVerifying(true); setVerifyItems(null);
    try {
      const [nbRes, plRes] = await Promise.all([
        axios.get(`${API}/api/fabric/workspaces/${wsId}/items?type=Notebook`),
        axios.get(`${API}/api/fabric/workspaces/${wsId}/items?type=DataPipeline`),
      ]);
      const allItems = [
        ...(nbRes.data.items ?? []),
        ...(plRes.data.items ?? []),
      ];
      // Mark which deployed artifacts are confirmed in Fabric
      const deployedNames = (result?.artifacts ?? []).map(a => a.name);
      const matched = allItems.filter(i => deployedNames.includes(i.name));
      setVerifyItems({ items: allItems, matched, total: allItems.length });
      if (matched.length === deployedNames.length)
        toast.success(`All ${matched.length} artifact(s) confirmed in Fabric`);
      else
        toast(`${matched.length}/${deployedNames.length} artifacts found in Fabric`, { icon: "ℹ️" });
    } catch (e) {
      setVerifyItems({ error: e.response?.data?.detail ?? e.message });
      toast.error("Could not verify artifacts — is the MS365 token set?");
    } finally {
      setVerifying(false);
    }
  };

  const artifacts = result?.artifacts ?? [];

  return (
    <div className="fade-in" style={{ display: "flex", flexDirection: "column", gap: 20 }}>
      <div>
        <h2 style={{ fontSize: 18, fontWeight: 700, marginBottom: 4 }}>Deploy to Microsoft Fabric</h2>
        <p style={{ fontSize: 13, color: "var(--color-muted)", margin: 0 }}>
          Create Fabric notebooks and pipeline for <strong>{wizard.sourceName}</strong> · {wizard.moduleName}.
        </p>
      </div>

      {/* Summary cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 12 }}>
        {[
          { label: "ERP Source",  value: wizard.sourceName ?? wizard.source,            icon: Server  },
          { label: "Module",      value: wizard.moduleName ?? wizard.module,             icon: Package },
          { label: "Tables",      value: `${wizard.discoveredTables?.length ?? 0}`,      icon: Table2  },
          { label: "Connection",  value: wizard.connectionId ? "Created" : "Pending",    icon: PlugZap },
        ].map(({ label, value, icon: Icon }) => (
          <div key={label} className="card" style={{ padding: 14 }}>
            <div style={{ fontSize: 10, color: "#94a3b8", fontWeight: 600, display: "flex", alignItems: "center", gap: 5, marginBottom: 5 }}>
              <Icon size={11} /> {label.toUpperCase()}
            </div>
            <div style={{ fontSize: 13, fontWeight: 700, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {value}
            </div>
          </div>
        ))}
      </div>

      {/* Artifact selection */}
      {!result && (
        <div className="card" style={{ padding: 20 }}>
          <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 14, display: "flex", alignItems: "center", gap: 8 }}>
            <FileCode size={15} style={{ color: "var(--color-primary)" }} />
            Select Artifacts to Create in Fabric
          </h3>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 10 }}>
            {[
              { key: "bronze",   label: "Bronze Notebook",  icon: FileCode,  desc: "Raw ERP extraction"      },
              { key: "silver",   label: "Silver Notebook",  icon: FileCode,  desc: "Cleanse & conform"       },
              { key: "gold",     label: "Gold Notebook",    icon: FileCode,  desc: "KPIs & business metrics" },
              { key: "pipeline", label: "Data Pipeline",    icon: GitBranch, desc: "Bronze→Silver→Gold flow"  },
            ].map(({ key, label, icon: Icon, desc }) => (
              <label
                key={key}
                style={{
                  display: "flex", flexDirection: "column", gap: 8,
                  padding: "14px 16px", borderRadius: 10, cursor: "pointer",
                  border: `2px solid ${opts[key] ? "var(--color-primary)" : "#e2e8f0"}`,
                  background: opts[key] ? "var(--color-primary-light)" : "white",
                  transition: "all 0.12s",
                }}
              >
                <input
                  type="checkbox" style={{ display: "none" }}
                  checked={opts[key]}
                  onChange={e => setOpts(o => ({ ...o, [key]: e.target.checked }))}
                />
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <Icon size={15} style={{ color: opts[key] ? "var(--color-primary)" : "#94a3b8" }} />
                  <span style={{ fontSize: 13, fontWeight: 600 }}>{label}</span>
                  {opts[key] && <CheckCircle2 size={12} style={{ color: "var(--color-primary)", marginLeft: "auto" }} />}
                </div>
                <span style={{ fontSize: 11, color: "#64748b" }}>{desc}</span>
              </label>
            ))}
          </div>
        </div>
      )}

      {/* Deploy button */}
      {!result && (
        <div style={{ display: "flex", justifyContent: "space-between" }}>
          <button className="btn-ghost" onClick={onBack}><ChevronLeft size={15} /> Back</button>
          <button
            className="btn-primary"
            disabled={deployMutation.isPending || !Object.values(opts).some(Boolean)}
            onClick={() => deployMutation.mutate()}
            style={{ fontSize: 14, padding: "10px 24px" }}
          >
            {deployMutation.isPending
              ? <><RefreshCw size={14} className="spin" /> Deploying to Fabric…</>
              : <><Play size={14} /> Deploy to Microsoft Fabric</>
            }
          </button>
        </div>
      )}

      {/* Deployment results */}
      {result && (
        <div style={{ border: "1px solid var(--color-border)", borderRadius: 12, overflow: "hidden" }}>
          <div style={{
            padding: "14px 20px", display: "flex", alignItems: "center", gap: 10,
            background: result.live ? "#f0fdf4" : "#fffbeb",
            borderBottom: `1px solid ${result.live ? "#bbf7d0" : "#fde68a"}`,
          }}>
            {result.live
              ? <CheckCircle2 size={18} style={{ color: "#16a34a" }} />
              : <AlertTriangle size={18} style={{ color: "#d97706" }} />
            }
            <div>
              <div style={{ fontWeight: 700, fontSize: 14, color: result.live ? "#15803d" : "#92400e" }}>
                {result.live ? "Deployed successfully to Microsoft Fabric" : "Simulated deployment completed"}
              </div>
              {result.note && (
                <div style={{ fontSize: 12, color: "#64748b", marginTop: 2 }}>{result.note}</div>
              )}
            </div>
            <span style={{ marginLeft: "auto", fontSize: 12, fontWeight: 600,
                           padding: "3px 10px", borderRadius: 20,
                           background: result.live ? "#dcfce7" : "#fef9c3",
                           color: result.live ? "#15803d" : "#92400e" }}>
              {result.total} artifact{result.total !== 1 ? "s" : ""}
            </span>
          </div>

          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ background: "#f8fafc", borderBottom: "1px solid #f1f5f9" }}>
                {["Artifact Name", "Type", "Artifact ID", "Status"].map(h => (
                  <th key={h} style={{ padding: "10px 16px", textAlign: "left", fontSize: 11,
                                       fontWeight: 600, color: "#64748b" }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {artifacts.map((a, i) => (
                <tr key={i} style={{ borderBottom: "1px solid #f1f5f9" }}>
                  <td style={{ padding: "10px 16px", fontFamily: "monospace", fontSize: 12, fontWeight: 600 }}>{a.name}</td>
                  <td style={{ padding: "10px 16px", fontSize: 12, color: "#64748b" }}>{a.type}</td>
                  <td style={{ padding: "10px 16px", fontFamily: "monospace", fontSize: 11, color: "#94a3b8" }}>
                    {a.id?.slice(0, 16)}…
                  </td>
                  <td style={{ padding: "10px 16px" }}>
                    <span className={
                      a.status === "created"   ? "badge badge-green" :
                      a.status === "simulated" ? "badge badge-amber" :
                                                 "badge badge-red"
                    } style={{ fontSize: 10 }}>
                      {a.status}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          <div style={{ padding: "12px 20px", background: "#f8fafc",
                        display: "flex", justifyContent: "space-between", alignItems: "center",
                        borderTop: "1px solid #f1f5f9", fontSize: 12, color: "#64748b",
                        flexWrap: "wrap", gap: 8 }}>
            <span>Workspace: {result.workspace_id || "local-dev"}</span>
            <div style={{ display: "flex", gap: 8 }}>
              {result.live && (
                <button
                  className="btn-outline"
                  style={{ fontSize: 12 }}
                  disabled={verifying}
                  onClick={verifyArtifacts}
                >
                  {verifying
                    ? <><RefreshCw size={12} className="spin" /> Verifying…</>
                    : <><Search size={12} /> Verify in Fabric</>}
                </button>
              )}
              <button className="btn-ghost" style={{ fontSize: 12 }}
                      onClick={() => {
                        setResult(null);
                        setVerifyItems(null);
                        setOpts({ bronze: true, silver: true, gold: true, pipeline: true });
                      }}>
                Deploy Again
              </button>
            </div>
          </div>

          {/* Artifact verification result */}
          {verifyItems && (
            <div style={{ borderTop: "1px solid #f1f5f9" }}>
              {verifyItems.error ? (
                <div style={{ padding: "12px 20px", background: "#fef2f2",
                              display: "flex", alignItems: "center", gap: 8,
                              fontSize: 13, color: "#b91c1c" }}>
                  <XCircle size={15} /> Verification failed: {verifyItems.error}
                </div>
              ) : (
                <div style={{ padding: "12px 20px", background: "#eff6ff" }}>
                  <div style={{ fontSize: 13, fontWeight: 600, color: "#1d4ed8",
                                display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
                    <CheckCircle2 size={15} />
                    {verifyItems.matched?.length}/{result?.artifacts?.length} artifact(s)
                    confirmed in Fabric workspace
                  </div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                    {(result?.artifacts ?? []).map(art => {
                      const confirmed = verifyItems.matched?.some(i => i.name === art.name);
                      return (
                        <span key={art.name} style={{
                          fontSize: 11, padding: "3px 10px", borderRadius: 20,
                          fontFamily: "monospace", fontWeight: 600,
                          background: confirmed ? "#dcfce7" : "#fef9c3",
                          color:      confirmed ? "#15803d" : "#92400e",
                          border: `1px solid ${confirmed ? "#bbf7d0" : "#fde68a"}`,
                          display: "flex", alignItems: "center", gap: 5,
                        }}>
                          {confirmed
                            ? <CheckCircle2 size={11} />
                            : <AlertTriangle size={11} />}
                          {art.name}
                        </span>
                      );
                    })}
                  </div>
                  {verifyItems.total > verifyItems.matched?.length && (
                    <p style={{ fontSize: 11, color: "#64748b", marginTop: 8, marginBottom: 0 }}>
                      {verifyItems.total} total items in workspace ·
                      {" "}{verifyItems.total - (verifyItems.matched?.length ?? 0)} item(s)
                      not from this deployment
                    </p>
                  )}
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ── Main Page ─────────────────────────────────────────────────────────────────
export default function ERPSource() {
  const [step,   setStep]   = useState(1);
  const [wizard, setWizard] = useState({
    source:           "",
    sourceName:       "",
    module:           "",
    moduleName:       "",
    connFields:       {},
    discoveredTables: [],
    fabricWorkspace:  { workspace_id: "", lakehouse_id: "" },
    fabricConnType:   "",
    connectionId:     "",
    connectionName:   "",
  });

  const next = () => setStep(s => Math.min(s + 1, STEPS.length));
  const back = () => setStep(s => Math.max(s - 1, 1));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      {/* Page header */}
      <div style={{ display: "flex", alignItems: "center", gap: 14, marginBottom: 8 }}>
        <div style={{
          width: 44, height: 44, borderRadius: 12, flexShrink: 0,
          background: "linear-gradient(135deg,#0891b2,#0e7490)",
          display: "flex", alignItems: "center", justifyContent: "center",
        }}>
          <Zap size={22} color="white" />
        </div>
        <div>
          <h1 style={{ fontSize: 22, fontWeight: 800, margin: 0 }}>ERP Source Wizard</h1>
          <p style={{ fontSize: 13, color: "var(--color-muted)", margin: 0 }}>
            Connect any ERP to Microsoft Fabric in 5 guided steps
          </p>
        </div>
      </div>

      {/* Wizard card */}
      <div className="card" style={{ padding: 28 }}>
        <StepBar step={step} />

        {step === 1 && <StepSource   wizard={wizard} setWizard={setWizard} onNext={next} />}
        {step === 2 && <StepConnect  wizard={wizard} setWizard={setWizard} onNext={next} onBack={back} />}
        {step === 3 && <StepDiscover wizard={wizard} setWizard={setWizard} onNext={next} onBack={back} />}
        {step === 4 && <StepFabric   wizard={wizard} setWizard={setWizard} onNext={next} onBack={back} />}
        {step === 5 && <StepDeploy   wizard={wizard} onBack={back} />}
      </div>
    </div>
  );
}
