/**
 * ERPSource — ilinkERP Fabric Accelerate
 *
 * Step 1 · Select ERP Source + Module
 * Step 2 · Configure Connection
 * Step 3 · Discover Tables (industry-standard vs live scan)
 * Step 4 · Fabric Connection (connection types + workspace config)
 * Step 5 · Deploy (create connection, notebooks, pipeline)
 */
import { useState, useMemo, useEffect, useRef } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import axios from "axios";
import {
  Server, Database, CheckCircle2, AlertTriangle, XCircle,
  ChevronRight, ChevronLeft, RefreshCw, PlugZap, Play,
  FileCode, GitBranch, Cloud, Info, Circle, Eye, EyeOff,
  Zap, Layers, Table2, Link2, Shield, Package, LayoutDashboard,
  Search, ListChecks, ExternalLink, Key, Monitor, User,
  Plus, Star, Wifi, WifiOff, Copy, Code, Terminal, ChevronDown, ChevronUp,
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

  const { data: savedCfgs } = useQuery({
    queryKey: ["erp-configs"],
    queryFn:  () => axios.get(`${API}/api/erp/configs`).then(r => r.data),
  });
  const savedList = savedCfgs?.configs ?? [];

  const [showSaved, setShowSaved] = useState(false);

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

      {/* Previously used ERP configs */}
      {savedList.length > 0 && (
        <div className="card" style={{ padding: 14 }}>
          <button
            className="btn-ghost"
            style={{ fontSize: 12, padding: "3px 8px", display: "flex", alignItems: "center", gap: 6 }}
            onClick={() => setShowSaved(v => !v)}>
            <Star size={12} style={{ color: "#d97706" }} />
            Previously used ({savedList.length})
            {showSaved ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
          </button>
          {showSaved && (
            <div className="fade-in" style={{ marginTop: 10, display: "flex", flexWrap: "wrap", gap: 8 }}>
              {savedList.map(cfg => (
                <button
                  key={cfg.name}
                  onClick={() => setWizard(w => ({
                    ...w,
                    source:     cfg.source,
                    sourceName: cfg.sourceName || cfg.source,
                    module:     cfg.module,
                    moduleName: cfg.moduleName || cfg.module,
                    connFields: {
                      host:     cfg.host     || "",
                      port:     cfg.port     || "",
                      service:  cfg.service  || "",
                      username: cfg.username || "",
                    },
                  }))}
                  style={{
                    padding: "6px 12px", borderRadius: 8, fontSize: 12, fontWeight: 600,
                    border: "1px solid #e2e8f0", background: "white", cursor: "pointer",
                    display: "flex", alignItems: "center", gap: 6,
                    transition: "all 0.12s",
                  }}>
                  <Database size={11} style={{ color: "var(--color-primary)" }} />
                  {cfg.sourceName || cfg.source}
                  {cfg.moduleName && <span style={{ color: "#94a3b8" }}>· {cfg.moduleName}</span>}
                  {cfg.host && <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b" }}>({cfg.host})</span>}
                </button>
              ))}
            </div>
          )}
        </div>
      )}

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
        erp_type:      wizard.source,
        host:          wizard.connFields.host          ?? "",
        port:          parseInt(wizard.connFields.port ?? "1521"),
        service:       wizard.connFields.service       ?? wizard.connFields.database ?? "",
        username:      wizard.connFields.username      ?? "",
        password:      wizard.connFields.password      ?? "",
        // Dynamics 365 fields
        tenant_id:     wizard.connFields.tenant_id     ?? "",
        client_id:     wizard.connFields.client_id     ?? "",
        client_secret: wizard.connFields.client_secret ?? "",
        // SAP fields
        system_number: wizard.connFields.system_number ?? "",
        client:        wizard.connFields.client        ?? "",
      });
      const d = r.data;
      setResult({
        ok:        d.success,
        msg:       d.message ?? (d.success ? "Connection successful" : "Connection failed"),
        simulated: d.simulated ?? false,
      });
      if (d.success) {
        // Auto-save config (no password)
        try {
          await axios.post(`${API}/api/erp/configure`, {
            name:       `${wizard.source}_${wizard.connFields.host ?? "conn"}`,
            source:     wizard.source,
            sourceName: wizard.sourceName,
            module:     wizard.module,
            moduleName: wizard.moduleName,
            host:       wizard.connFields.host     ?? "",
            port:       wizard.connFields.port     ?? "",
            service:    wizard.connFields.service  ?? wizard.connFields.database ?? "",
            username:   wizard.connFields.username ?? "",
            // password intentionally omitted
          });
        } catch (_) { /* non-fatal */ }
      }
    } catch (err) {
      setResult({
        ok:  false,
        msg: err.response?.data?.detail ?? err.message ?? "Connection test failed",
      });
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
              {testResult.simulated && (
                <span style={{ fontSize: 11, color: "#f59e0b", fontWeight: 600 }}>
                  (credentials stored)
                </span>
              )}
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
        Connection details (except password) are saved for quick reuse. Passwords are never stored.
      </div>

      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <button className="btn-ghost" onClick={onBack}><ChevronLeft size={15} /> Back</button>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <button
            className="btn-ghost"
            onClick={onNext}
            style={{ fontSize: 13, color: "#64748b" }}
            title="Skip connection — use standard industry tables for this ERP/module"
          >
            Skip — use standard tables
          </button>
          <button className="btn-primary" disabled={!allReq} onClick={onNext}>
            Discover Tables <ChevronRight size={15} />
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Step 3: Table Discovery + Selection ───────────────────────────────────────
function StepDiscover({ wizard, setWizard, onNext, onBack }) {
  const [scanning,       setScanning]       = useState(false);
  const [scanned,        setScanned]        = useState(false);
  const [scanResult,     setScanResult]     = useState(null);  // { found:[], missing:[], method, note, error }
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

  // Merge scan results into the standard table list
  const scannedTables = useMemo(() => {
    if (!scanned || !scanResult || stdTables.length === 0) return [];
    const foundSet = new Set((scanResult.found ?? []).map(n => n.toUpperCase()));
    return stdTables.map(t => ({ ...t, found: foundSet.has(t.table_name.toUpperCase()) }));
  }, [scanned, scanResult, stdTables]);

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
    try {
      const r = await axios.post(`${API}/api/erp/scan-tables`, {
        erp_type: wizard.source,
        host:     wizard.connFields?.host     ?? "",
        port:     parseInt(wizard.connFields?.port ?? "1521"),
        service:  wizard.connFields?.service  ?? wizard.connFields?.database ?? "",
        username: wizard.connFields?.username ?? "",
        password: wizard.connFields?.password ?? "",
        schema:   wizard.connFields?.schema   ?? "",
        tables:   stdTables.map(t => t.table_name),
      });
      const data = r.data;
      setScanResult(data);
      setScanned(true);

      if (data.method === "error") {
        // Scan failed — keep all tables selected, show error
        toast.error(`Scan error: ${data.error}`);
      } else if (data.method === "assumed") {
        // Not a live scan — all tables assumed present
        toast(`${data.note ?? "All tables assumed present (live scan unavailable)"}`, { icon: "ℹ️" });
        setWizard(w => ({ ...w, discoveredTables: data.found }));
      } else {
        // Live scan — auto-deselect missing tables
        const foundSet = new Set((data.found ?? []).map(n => n.toUpperCase()));
        setSelectedTables(prev => prev.filter(n => foundSet.has(n.toUpperCase())));
        setWizard(w => ({ ...w, discoveredTables: data.found }));
        toast.success(`Scan complete — ${data.found.length}/${stdTables.length} tables found in database`);
      }
    } catch (err) {
      toast.error(err.response?.data?.detail ?? "Table scan failed");
    } finally {
      setScanning(false);
    }
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
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          {scanned && scanResult?.method && (
            <span style={{
              fontSize: 10, fontWeight: 600, padding: "3px 8px", borderRadius: 20,
              background: scanResult.method === "live_oracle" || scanResult.method === "live_hana"
                ? "#dcfce7" : scanResult.method === "error" ? "#fef2f2" : "#fef9c3",
              color:      scanResult.method === "live_oracle" || scanResult.method === "live_hana"
                ? "#15803d" : scanResult.method === "error" ? "#b91c1c" : "#92400e",
              border:     "1px solid",
              borderColor: scanResult.method === "live_oracle" || scanResult.method === "live_hana"
                ? "#bbf7d0" : scanResult.method === "error" ? "#fecaca" : "#fde68a",
            }}>
              {scanResult.method === "live_oracle" ? "🔌 Live Oracle Scan"
               : scanResult.method === "live_hana"   ? "🔌 Live HANA Scan"
               : scanResult.method === "error"        ? "⚠ Scan Error"
               : "ℹ Assumed Present"}
            </span>
          )}
          {!scanned ? (
            <button className="btn-primary" disabled={scanning || isLoading} onClick={runScan}>
              {scanning
                ? <><RefreshCw size={13} className="spin" /> Scanning database…</>
                : <><Database size={13} /> Scan Live ERP</>}
            </button>
          ) : (
            <button className="btn-ghost"
              onClick={() => {
                setScanned(false);
                setScanResult(null);
                setSelectedTables(stdTables.map(t => t.table_name));
              }}>
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

      {/* Assumed-present note */}
      {scanned && scanResult?.method === "assumed" && (
        <div style={{ padding: "12px 16px", background: "#fffbeb",
                      border: "1px solid #fde68a", borderRadius: 10,
                      fontSize: 12, color: "#92400e",
                      display: "flex", alignItems: "flex-start", gap: 8 }}>
          <Info size={14} style={{ flexShrink: 0, marginTop: 1 }} />
          <span>{scanResult.note ?? "All tables assumed present — live scan unavailable for this ERP type."}</span>
        </div>
      )}

      {/* Missing tables alert */}
      {scanned && missing.length > 0 && scanResult?.method !== "assumed" && (
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
// ── Inline auth method mini-form (used inside StepFabric) ────────────────────
const QUICK_METHODS = [
  { id: "bearer",           label: "Bearer Token",      Icon: Key     },
  { id: "service_principal",label: "Service Principal", Icon: Server  },
  { id: "device_code",      label: "Device Code",       Icon: Monitor },
  { id: "managed_identity", label: "Managed Identity",  Icon: Cloud   },
  { id: "username_password",label: "User/Password",     Icon: User    },
];

function QuickConnectPanel({ onSaved }) {
  const qc = useQueryClient();
  const [method,    setMethod]    = useState("bearer");
  const [name,      setName]      = useState("");
  const [wsId,      setWsId]      = useState("");
  const [token,     setToken]     = useState("");
  const [showTok,   setShowTok]   = useState(false);
  const [spTenant,  setSpTenant]  = useState(""); const [spCid, setSpCid] = useState(""); const [spSec, setSpSec] = useState("");
  const [dcTenant,  setDcTenant]  = useState(""); const [dcCid, setDcCid] = useState("");
  const [dcSession, setDcSession] = useState(null);
  const [dcPolling, setDcPolling] = useState(false);
  const dcRef = useRef(null);
  const [upTenant,  setUpTenant]  = useState(""); const [upCid, setUpCid] = useState("");
  const [upUser,    setUpUser]    = useState(""); const [upPwd, setUpPwd] = useState("");
  const [busy,      setBusy]      = useState(false);

  const reset = () => {
    setName(""); setWsId(""); setToken("");
    setSpTenant(""); setSpCid(""); setSpSec("");
    setDcTenant(""); setDcCid(""); setDcSession(null);
    setUpTenant(""); setUpCid(""); setUpUser(""); setUpPwd("");
  };

  const onOk = (d) => {
    qc.invalidateQueries(["fabric-connections"]);
    reset();
    toast.success(`"${d.name}" saved — ${d.test_result === "valid" ? "token valid ✓" : "token untested"}`);
    onSaved?.(d);
  };
  const onErr = (e) => toast.error(e.response?.data?.detail ?? "Auth failed");

  const go = async () => {
    setBusy(true);
    try {
      let res;
      if (method === "bearer") {
        res = await axios.post(`${API}/api/fabric/fabric-connections`,
          { name, token, workspace_id: wsId });
      } else if (method === "service_principal") {
        res = await axios.post(`${API}/api/fabric/auth/service-principal`,
          { name, tenant_id: spTenant, client_id: spCid, client_secret: spSec, workspace_id: wsId });
      } else if (method === "device_code") {
        const s = await axios.post(`${API}/api/fabric/auth/device-code/start`,
          { name, tenant_id: dcTenant, client_id: dcCid, workspace_id: wsId });
        setDcSession(s.data);
        setBusy(false);
        // poll
        setDcPolling(true);
        dcRef.current = setInterval(async () => {
          try {
            const p = await axios.post(`${API}/api/fabric/auth/device-code/poll`,
              { session_id: s.data.session_id, name, workspace_id: wsId });
            if (p.data.status !== "pending") {
              clearInterval(dcRef.current); setDcPolling(false); setDcSession(null);
              onOk(p.data);
            }
          } catch (e2) {
            clearInterval(dcRef.current); setDcPolling(false); setDcSession(null);
            onErr(e2);
          }
        }, (s.data.interval ?? 5) * 1000);
        return;
      } else if (method === "managed_identity") {
        res = await axios.post(`${API}/api/fabric/auth/managed-identity`,
          { name: name || "Managed Identity", workspace_id: wsId });
      } else if (method === "username_password") {
        res = await axios.post(`${API}/api/fabric/auth/username-password`,
          { name, tenant_id: upTenant, client_id: upCid, username: upUser, password: upPwd, workspace_id: wsId });
      }
      onOk(res.data);
    } catch (e) { onErr(e); }
    finally { setBusy(false); }
  };

  const valid = () => {
    if (!name && method !== "managed_identity") return false;
    if (method === "bearer")            return !!token;
    if (method === "service_principal") return !!(spTenant && spCid && spSec);
    if (method === "device_code")       return !!(dcTenant && dcCid);
    if (method === "managed_identity")  return true;
    if (method === "username_password") return !!(upTenant && upCid && upUser && upPwd);
  };

  const inp = (val, set, ph, pw) => (
    <div style={{ position: "relative" }}>
      <input className="input" type="text" value={val} onChange={e => set(e.target.value)}
        placeholder={ph} style={{ fontSize: 11, fontFamily: "monospace" }} />
    </div>
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      {/* Method tabs */}
      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
        {QUICK_METHODS.map(({ id, label, Icon }) => {
          const sel = method === id;
          return (
            <button key={id}
              onClick={() => { setMethod(id); setDcSession(null); if (dcRef.current) clearInterval(dcRef.current); setDcPolling(false); }}
              style={{
                display: "flex", alignItems: "center", gap: 5,
                padding: "5px 12px", borderRadius: 20, fontSize: 11, fontWeight: 600,
                cursor: "pointer", transition: "all 0.12s",
                border: `1px solid ${sel ? "var(--color-primary)" : "#e2e8f0"}`,
                background: sel ? "var(--color-primary)" : "white",
                color: sel ? "white" : "#64748b",
              }}>
              <Icon size={11} /> {label}
            </button>
          );
        })}
      </div>

      {/* Common: name + workspace */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
        {method !== "managed_identity" && (
          <div>
            <label style={{ fontSize: 11, fontWeight: 600, color: "#374151", display: "block", marginBottom: 3 }}>
              Connection Name *
            </label>
            <input className="input" value={name} onChange={e => setName(e.target.value)}
              placeholder="e.g. Fabric Prod" style={{ fontSize: 12 }} />
          </div>
        )}
        <div>
          <label style={{ fontSize: 11, fontWeight: 600, color: "#374151", display: "block", marginBottom: 3 }}>
            Workspace ID <span style={{ fontWeight: 400, color: "#94a3b8" }}>(optional)</span>
          </label>
          <input className="input" value={wsId} onChange={e => setWsId(e.target.value)}
            placeholder="xxxxxxxx-xxxx-xxxx…" style={{ fontSize: 11, fontFamily: "monospace" }} />
        </div>
      </div>

      {/* Method fields */}
      {method === "bearer" && (
        <div>
          <label style={{ fontSize: 11, fontWeight: 600, color: "#374151", display: "block", marginBottom: 3 }}>
            Bearer Token *
          </label>
          <div style={{ position: "relative" }}>
            <input className="input" type={showTok ? "text" : "password"} value={token}
              onChange={e => setToken(e.target.value)}
              placeholder="eyJ0eXAiOiJKV1Qi…"
              style={{ fontSize: 11, fontFamily: "monospace", paddingRight: 36 }} />
            <button type="button" onClick={() => setShowTok(p => !p)}
              style={{ position: "absolute", right: 8, top: "50%", transform: "translateY(-50%)",
                       background: "none", border: "none", cursor: "pointer", color: "#64748b" }}>
              {showTok ? <EyeOff size={13} /> : <Eye size={13} />}
            </button>
          </div>
        </div>
      )}

      {method === "service_principal" && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
          <div>
            <label style={{ fontSize: 11, fontWeight: 600, color: "#374151", display: "block", marginBottom: 3 }}>Tenant ID *</label>
            {inp(spTenant, setSpTenant, "xxxxxxxx-xxxx-xxxx")}
          </div>
          <div>
            <label style={{ fontSize: 11, fontWeight: 600, color: "#374151", display: "block", marginBottom: 3 }}>Client ID *</label>
            {inp(spCid, setSpCid, "xxxxxxxx-xxxx-xxxx")}
          </div>
          <div style={{ gridColumn: "1/-1" }}>
            <label style={{ fontSize: 11, fontWeight: 600, color: "#374151", display: "block", marginBottom: 3 }}>Client Secret *</label>
            <input className="input" type="password" value={spSec} onChange={e => setSpSec(e.target.value)}
              placeholder="your-client-secret" style={{ fontSize: 12 }} />
          </div>
        </div>
      )}

      {method === "device_code" && !dcSession && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
          <div>
            <label style={{ fontSize: 11, fontWeight: 600, color: "#374151", display: "block", marginBottom: 3 }}>Tenant ID *</label>
            {inp(dcTenant, setDcTenant, "xxxxxxxx-xxxx-xxxx")}
          </div>
          <div>
            <label style={{ fontSize: 11, fontWeight: 600, color: "#374151", display: "block", marginBottom: 3 }}>Client ID *</label>
            {inp(dcCid, setDcCid, "xxxxxxxx-xxxx-xxxx")}
          </div>
        </div>
      )}

      {method === "device_code" && dcSession && (
        <div style={{ padding: "12px 14px", borderRadius: 10, background: "#f0fdf4",
                      border: "2px solid #bbf7d0" }}>
          <div style={{ fontWeight: 700, fontSize: 12, color: "#15803d", marginBottom: 6 }}>
            Open browser → authenticate
          </div>
          <a href={dcSession.verification_uri} target="_blank" rel="noreferrer"
            style={{ fontSize: 12, color: "#1d4ed8", fontWeight: 600 }}>
            {dcSession.verification_uri} <ExternalLink size={11} style={{ verticalAlign: "middle" }} />
          </a>
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginTop: 8 }}>
            <span style={{ padding: "6px 14px", borderRadius: 6, background: "#dcfce7",
                           fontWeight: 800, fontSize: 18, letterSpacing: "0.12em",
                           fontFamily: "monospace", color: "#15803d" }}>
              {dcSession.user_code}
            </span>
            <button className="btn-ghost" style={{ fontSize: 11 }}
              onClick={() => { navigator.clipboard.writeText(dcSession.user_code); toast("Copied"); }}>
              <Copy size={11} /> Copy
            </button>
          </div>
          {dcPolling && (
            <div style={{ marginTop: 8, fontSize: 11, color: "#64748b",
                          display: "flex", alignItems: "center", gap: 6 }}>
              <RefreshCw size={11} className="spin" /> Waiting for authentication…
            </div>
          )}
        </div>
      )}

      {method === "username_password" && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
          <div>
            <label style={{ fontSize: 11, fontWeight: 600, color: "#374151", display: "block", marginBottom: 3 }}>Tenant ID *</label>
            {inp(upTenant, setUpTenant, "xxxxxxxx-xxxx-xxxx")}
          </div>
          <div>
            <label style={{ fontSize: 11, fontWeight: 600, color: "#374151", display: "block", marginBottom: 3 }}>Client ID *</label>
            {inp(upCid, setUpCid, "xxxxxxxx-xxxx-xxxx")}
          </div>
          <div>
            <label style={{ fontSize: 11, fontWeight: 600, color: "#374151", display: "block", marginBottom: 3 }}>Username *</label>
            <input className="input" value={upUser} onChange={e => setUpUser(e.target.value)}
              placeholder="user@contoso.com" style={{ fontSize: 12 }} />
          </div>
          <div>
            <label style={{ fontSize: 11, fontWeight: 600, color: "#374151", display: "block", marginBottom: 3 }}>Password *</label>
            <input className="input" type="password" value={upPwd} onChange={e => setUpPwd(e.target.value)}
              placeholder="••••••" style={{ fontSize: 12 }} />
          </div>
        </div>
      )}

      {method === "managed_identity" && (
        <div style={{ fontSize: 12, color: "#64748b", padding: "8px 12px",
                      background: "#fef9c3", borderRadius: 8, border: "1px solid #fde68a" }}>
          ⚠️ Managed Identity only works on Azure VMs / containers with an identity assigned.
          The backend will call the IMDS endpoint automatically.
        </div>
      )}

      {!(method === "device_code" && dcSession) && (
        <button className="btn-primary"
          style={{ alignSelf: "flex-start", fontSize: 12 }}
          disabled={!valid() || busy}
          onClick={go}>
          {busy
            ? <><RefreshCw size={12} className="spin" /> Authenticating…</>
            : method === "device_code"
              ? <><Monitor size={12} /> Start Device Code</>
              : <><Plus size={12} /> Add Connection</>
          }
        </button>
      )}
    </div>
  );
}

function StepFabric({ wizard, setWizard, onNext, onBack }) {
  const qc = useQueryClient();
  const [showParams,     setShowParams]     = useState(false);
  const [showSavedConns, setShowSavedConns] = useState(false);
  const [connParams,   setConnParams]   = useState({});
  const [creds,        setCreds]        = useState({ username: "", password: "", client_secret: "" });
  const [creating,     setCreating]     = useState(false);
  const [connResult,   setConnResult]   = useState(null);
  const [verifying,    setVerifying]    = useState(false);
  const [verifyResult, setVerifyResult] = useState(null);
  const [showQuickConn, setShowQuickConn] = useState(false);

  // Workspace / lakehouse live pickers
  const [wsMode,       setWsMode]       = useState("manual"); // "manual" | "picker"
  const [lhMode,       setLhMode]       = useState("manual"); // "manual" | "picker"

  // ── Saved Fabric connections (for active-connection banner + selector) ────────
  const { data: connsData, refetch: refetchConns } = useQuery({
    queryKey: ["fabric-connections"],
    queryFn:  () => axios.get(`${API}/api/fabric/fabric-connections`).then(r => r.data),
    refetchOnWindowFocus: false,
  });
  const savedConns = connsData?.connections ?? [];
  const activeConn = savedConns.find(c => c.id === connsData?.active_id) ?? null;

  // ── Previously saved ERP→Fabric connections ──────────────────────────────
  const { data: savedConnData } = useQuery({
    queryKey: ["saved-connections"],
    queryFn:  () => axios.get(`${API}/api/fabric/saved-connections`).then(r => r.data),
    refetchOnWindowFocus: false,
  });
  const savedConnList = savedConnData?.connections ?? [];

  // Auto-expand QuickConnectPanel when there are no saved connections
  useEffect(() => {
    if (connsData && savedConns.length === 0) {
      setShowQuickConn(true);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connsData?.count]);

  // Auto-fill workspace_id from active connection when it has one
  useEffect(() => {
    if (activeConn?.workspace_id && !wizard.fabricWorkspace?.workspace_id) {
      setWizard(w => ({
        ...w,
        fabricWorkspace: { ...w.fabricWorkspace, workspace_id: activeConn.workspace_id },
      }));
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeConn?.id]);

  const activateConn = async (cid) => {
    try {
      await axios.post(`${API}/api/fabric/fabric-connections/${cid}/activate`);
      refetchConns();
      toast.success("Connection activated");
    } catch { toast.error("Could not activate"); }
  };

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

  // ── Workspace list (auto-fetched when active Fabric connection is valid) ─────
  const wsQuery = useQuery({
    queryKey:  ["fabric-workspaces"],
    queryFn:   () => axios.get(`${API}/api/fabric/workspaces`).then(r => r.data),
    enabled:   activeConn?.status === "valid",
    retry:     false,
    staleTime: 5 * 60 * 1000,
    onSuccess: data => {
      setWsMode("picker");
      if (data.workspaces?.length === 0)
        toast("No workspaces found — enter ID manually", { icon: "ℹ️" });
    },
    onError: () => toast.error("Could not fetch workspaces — is the MS365 token set?"),
  });

  // Switch to picker mode automatically when workspace data arrives
  useEffect(() => {
    if (wsQuery.data?.workspaces?.length > 0) setWsMode("picker");
  }, [wsQuery.data]);

  // ── Lakehouse list (auto-fetched when workspace is selected + connection valid)
  const lhQuery = useQuery({
    queryKey:  ["fabric-lakehouses", wizard.fabricWorkspace?.workspace_id],
    queryFn:   () =>
      axios.get(
        `${API}/api/fabric/workspaces/${wizard.fabricWorkspace?.workspace_id}/lakehouses`
      ).then(r => r.data),
    enabled:   !!wizard.fabricWorkspace?.workspace_id && activeConn?.status === "valid",
    retry:     false,
    staleTime: 5 * 60 * 1000,
    onSuccess: data => {
      setLhMode("picker");
      if (data.lakehouses?.length === 0)
        toast("No lakehouses found in that workspace — enter ID manually", { icon: "ℹ️" });
    },
    onError: () => toast.error("Could not fetch lakehouses"),
  });

  // Switch to picker mode automatically when lakehouse data arrives
  useEffect(() => {
    if (lhQuery.data?.lakehouses?.length > 0) setLhMode("picker");
  }, [lhQuery.data]);

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
          Authenticate with Microsoft Fabric, then create a shareable connection for{" "}
          <strong>{wizard.sourceName}</strong> and configure the target workspace.
        </p>
      </div>

      {/* ── Active Fabric Connection banner ──────────────────────────────────── */}
      <div id="fabric-auth-card" className="card" style={{ padding: 18 }}>
        <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 10,
                     display: "flex", alignItems: "center", gap: 8 }}>
          <Wifi size={15} style={{ color: "var(--color-primary)" }} />
          Fabric Authentication
          {activeConn?.status === "valid" && (
            <span style={{ marginLeft: "auto", fontSize: 10, fontWeight: 700, padding: "2px 10px",
                           borderRadius: 20, background: "#dcfce7", color: "#15803d" }}>
              LIVE
            </span>
          )}
        </h3>

        {/* Active connection display */}
        {activeConn ? (
          <div style={{
            padding: "10px 14px", borderRadius: 10,
            background: activeConn.status === "valid" ? "#f0fdf4" : "#fffbeb",
            border: `1px solid ${activeConn.status === "valid" ? "#bbf7d0" : "#fde68a"}`,
            marginBottom: 12,
          }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
              {activeConn.status === "valid"
                ? <CheckCircle2 size={15} style={{ color: "#16a34a", flexShrink: 0 }} />
                : <AlertTriangle size={15} style={{ color: "#d97706", flexShrink: 0 }} />}
              <div style={{ flex: 1 }}>
                <span style={{ fontWeight: 700, fontSize: 13, color: "#0f172a" }}>
                  {activeConn.name}
                </span>
                <span style={{ fontSize: 11, color: "#64748b", marginLeft: 8,
                               fontFamily: "monospace" }}>
                  {activeConn.token_masked}
                </span>
              </div>
              <span style={{ fontSize: 10, color: activeConn.status === "valid" ? "#15803d" : "#92400e",
                             background: activeConn.status === "valid" ? "#dcfce7" : "#fef9c3",
                             padding: "2px 8px", borderRadius: 20, fontWeight: 700 }}>
                {activeConn.status === "valid" ? "Valid" : "Untested"}
              </span>
            </div>
          </div>
        ) : (
          <div style={{ padding: "10px 14px", borderRadius: 10, marginBottom: 12,
                        background: "#fef2f2", border: "1px solid #fecaca",
                        display: "flex", alignItems: "center", gap: 8 }}>
            <WifiOff size={14} style={{ color: "#dc2626", flexShrink: 0 }} />
            <span style={{ fontSize: 12, color: "#b91c1c" }}>
              No active Fabric connection — operations will run in <strong>simulation mode</strong>.
            </span>
          </div>
        )}

        {/* Saved connections selector */}
        {savedConns.length > 0 && (
          <div style={{ marginBottom: 12 }}>
            <label style={{ fontSize: 11, fontWeight: 600, color: "#374151",
                            display: "block", marginBottom: 5 }}>
              Switch Active Connection
            </label>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
              {savedConns.map(c => (
                <button key={c.id}
                  onClick={() => activateConn(c.id)}
                  style={{
                    display: "flex", alignItems: "center", gap: 5,
                    padding: "4px 12px", borderRadius: 20, fontSize: 11, fontWeight: 600,
                    cursor: "pointer", transition: "all 0.1s",
                    border: `1px solid ${c.id === connsData?.active_id ? "var(--color-primary)" : "#e2e8f0"}`,
                    background: c.id === connsData?.active_id ? "var(--color-primary)" : "white",
                    color: c.id === connsData?.active_id ? "white" : "#374151",
                  }}>
                  {c.id === connsData?.active_id && <Star size={10} />}
                  {c.name}
                  {c.status === "valid" && <Wifi size={9} style={{ color: c.id === connsData?.active_id ? "white" : "#22c55e" }} />}
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Collapsible quick-connect panel */}
        <button
          className="btn-outline"
          style={{ fontSize: 12, padding: "6px 14px", display: "flex", alignItems: "center", gap: 6 }}
          onClick={() => setShowQuickConn(p => !p)}>
          <Plus size={13} />
          {showQuickConn ? "Hide" : "Add New"} Fabric Connection
          <ChevronRight size={12}
            style={{ transform: showQuickConn ? "rotate(90deg)" : "none", transition: "transform 0.2s" }} />
        </button>

        {showQuickConn && (
          <div className="fade-in" style={{ marginTop: 14, padding: "14px 16px",
                                            border: "1px solid #e2e8f0", borderRadius: 10,
                                            background: "#fafbfc" }}>
            <QuickConnectPanel onSaved={(d) => {
              refetchConns();
              setShowQuickConn(false);
              // Auto-fill workspace if returned
              if (d.workspaces?.length > 0 && !wizard.fabricWorkspace?.workspace_id) {
                setWizard(w => ({
                  ...w,
                  fabricWorkspace: { ...w.fabricWorkspace, workspace_id: d.workspaces[0].id },
                }));
              }
            }} />
          </div>
        )}
      </div>

      {/* ── Previously saved ERP→Fabric connections ──────────────────────────── */}
      {savedConnList.length > 0 && (
        <div className="card" style={{ padding: 16 }}>
          <button
            className="btn-ghost"
            style={{ fontSize: 12, padding: "3px 8px", display: "flex", alignItems: "center", gap: 6 }}
            onClick={() => setShowSavedConns(v => !v)}>
            <Star size={12} style={{ color: "#d97706" }} />
            Previously saved connections ({savedConnList.length})
            {showSavedConns ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
          </button>
          {showSavedConns && (
            <div className="fade-in" style={{ marginTop: 10, display: "flex", flexDirection: "column", gap: 6 }}>
              {savedConnList.map(sc => (
                <button
                  key={sc.id}
                  onClick={() => {
                    setWizard(w => ({
                      ...w,
                      fabricWorkspace: {
                        ...w.fabricWorkspace,
                        workspace_id: sc.workspace_id || w.fabricWorkspace?.workspace_id || "",
                        lakehouse_id: sc.lakehouse_id || w.fabricWorkspace?.lakehouse_id || "",
                      },
                      connectionId:   sc.id,
                      connectionName: sc.display_name,
                    }));
                    toast(`Loaded: ${sc.display_name}`, { icon: "✓" });
                    setShowSavedConns(false);
                  }}
                  style={{
                    padding: "8px 12px", borderRadius: 8, fontSize: 12, fontWeight: 600,
                    border: "1px solid #e2e8f0", background: "white", cursor: "pointer",
                    display: "flex", alignItems: "center", gap: 8, textAlign: "left",
                    transition: "all 0.12s",
                  }}>
                  {sc.live
                    ? <CheckCircle2 size={12} style={{ color: "#16a34a", flexShrink: 0 }} />
                    : <Database     size={12} style={{ color: "#64748b", flexShrink: 0 }} />}
                  <span style={{ flex: 1 }}>{sc.display_name}</span>
                  <span style={{ fontSize: 10, fontFamily: "monospace", color: "#94a3b8" }}>
                    {sc.connection_type}
                  </span>
                  {sc.live
                    ? <span className="badge badge-green" style={{ fontSize: 9 }}>live</span>
                    : <span className="badge badge-gray"  style={{ fontSize: 9 }}>local</span>}
                </button>
              ))}
            </div>
          )}
        </div>
      )}

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
            <button className="btn-primary" disabled={creating} onClick={createConn}
              style={!activeConn ? { background: "#d97706", borderColor: "#d97706" } : {}}>
              {creating
                ? <><RefreshCw size={13} className="spin" /> Creating…</>
                : <><PlugZap size={13} /> Create Fabric Connection</>}
            </button>
            {!activeConn && (
              <span style={{ fontSize: 11, color: "#92400e", background: "#fef9c3",
                             border: "1px solid #fde68a", borderRadius: 20,
                             padding: "3px 10px", display: "flex", alignItems: "center", gap: 4 }}>
                <AlertTriangle size={11} /> No token — will simulate
              </span>
            )}

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
          {connResult && (() => {
            const isError   = connResult.status === "error";
            const isLive    = connResult.live === true;
            const isStored  = connResult.stored === true && !isLive;
            const isSimOnly = !isLive && !isStored && !isError;
            const bg      = isError ? "#fef2f2" : isLive ? "#f0fdf4" : isStored ? "#eff6ff" : "#fffbeb";
            const border_ = isError ? "#fecaca" : isLive ? "#bbf7d0" : isStored ? "#bfdbfe" : "#fde68a";
            const textClr = isError ? "#b91c1c" : isLive ? "#15803d" : isStored ? "#1d4ed8" : "#92400e";
            const label   = isError   ? `Error: ${connResult.note}`
                          : isLive    ? "✓ Fabric managed connection created"
                          : isStored  ? "✓ Connection stored locally — JDBC at runtime"
                          :             "Connection simulated (no live token)";
            const Icon = isError ? XCircle : (isLive || isStored) ? CheckCircle2 : AlertTriangle;
            const iconClr = isError ? "#dc2626" : isLive ? "#16a34a" : isStored ? "#2563eb" : "#d97706";
            return (
              <div style={{
                display: "flex", alignItems: "flex-start", gap: 9,
                padding: "10px 14px", borderRadius: 10,
                background: bg, border: `1px solid ${border_}`,
              }}>
                <Icon size={15} style={{ color: iconClr, flexShrink: 0, marginTop: 1 }} />
                <div>
                  <div style={{ fontSize: 13, fontWeight: 600, color: textClr }}>
                    {label}
                  </div>
                  {connResult.connection_id && (
                    <div style={{ fontSize: 11, fontFamily: "monospace", color: "#64748b", marginTop: 3 }}>
                      ID: {connResult.connection_id}
                    </div>
                  )}
                  {connResult.note && !isError && (
                    <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 3 }}>
                      {connResult.note}
                    </div>
                  )}
                  {/* Action button — only for fully simulated (no JDBC details) */}
                  {isSimOnly && (
                    <button
                      className="btn-outline"
                      style={{ marginTop: 10, fontSize: 11, padding: "5px 12px",
                               display: "flex", alignItems: "center", gap: 5,
                               borderColor: "#d97706", color: "#92400e" }}
                      onClick={() => {
                        setShowQuickConn(true);
                        document.getElementById("fabric-auth-card")?.scrollIntoView({ behavior: "smooth" });
                      }}>
                      <Key size={11} /> Add Fabric Token to enable live mode
                    </button>
                  )}
                </div>
              </div>
            );
          })()}

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

// ── Dimensional modeling suggestions (Silver layer) ──────────────────────────
// relationship: "1:1" one source → one target
//              "N:1" many source tables (additionalSources) → one target (JOIN/MERGE)
//              "1:N" one source → many targets (additionalTargets split/filter)
const DIM_SUGGESTIONS = {
  finance: [
    { sourceTable: "GL_JE_LINES",               additionalSources: ["GL_JE_HEADERS"],                    relationship: "N:1", targetName: "fact_gl_transactions",    type: "fact", businessName: "GL Transactions",           description: "Journal lines merged with headers — amounts, accounts, periods, ledger" },
    { sourceTable: "GL_CODE_COMBINATIONS",       relationship: "1:1",                                                          targetName: "dim_account",             type: "dim",  businessName: "Chart of Accounts",         description: "Account codes, segments, natural accounts and descriptions" },
    { sourceTable: "GL_PERIODS",                 relationship: "1:1",                                                          targetName: "dim_period",              type: "dim",  businessName: "Accounting Periods",        description: "Fiscal calendar: year, quarter, month, period number" },
    { sourceTable: "FND_CURRENCIES",             relationship: "1:1",                                                          targetName: "dim_currency",            type: "dim",  businessName: "Currencies",                description: "Currency codes, names and exchange rate metadata" },
    { sourceTable: "FND_FLEX_VALUES",            relationship: "1:N", additionalTargets: ["dim_cost_center_hierarchy"],        targetName: "dim_cost_center",         type: "dim",  businessName: "Cost Centers",              description: "Cost center master → dim_cost_center + dim_cost_center_hierarchy" },
    { sourceTable: "GL_LEDGERS",                 relationship: "1:1",                                                          targetName: "dim_ledger",              type: "dim",  businessName: "Ledgers / Business Units",  description: "Ledger master with currency, chart of accounts, calendar" },
  ],
  order_management: [
    { sourceTable: "OE_ORDER_LINES_ALL",         additionalSources: ["OE_ORDER_HEADERS_ALL"],              relationship: "N:1", targetName: "fact_order_lines",        type: "fact", businessName: "Order Lines",               description: "Order lines merged with header — product, qty, price, status" },
    { sourceTable: "OE_ORDER_HEADERS_ALL",       relationship: "1:N", additionalTargets: ["fact_order_holds", "fact_order_payments"], targetName: "fact_sales_orders", type: "fact", businessName: "Sales Orders",           description: "Order headers → fact_sales_orders + fact_order_holds + fact_order_payments" },
    { sourceTable: "HZ_CUST_ACCOUNTS",           additionalSources: ["HZ_PARTIES","HZ_LOCATIONS"],         relationship: "N:1", targetName: "dim_customer",            type: "dim",  businessName: "Customers",                 description: "Customer accounts merged with party and location master" },
    { sourceTable: "MTL_SYSTEM_ITEMS_B",         additionalSources: ["MTL_SYSTEM_ITEMS_TL"],               relationship: "N:1", targetName: "dim_product",             type: "dim",  businessName: "Products / Items",          description: "Item master base + translations merged into unified product dimension" },
    { sourceTable: "RA_TERRITORIES",             relationship: "1:1",                                                          targetName: "dim_territory",           type: "dim",  businessName: "Sales Territories",         description: "Geographic and sales territory hierarchy" },
    { sourceTable: "QP_LIST_HEADERS_B",          relationship: "1:N", additionalTargets: ["dim_price_list_lines"],             targetName: "dim_price_list",          type: "dim",  businessName: "Price Lists",               description: "Price list header → dim_price_list + dim_price_list_lines" },
  ],
  accounts_payable: [
    { sourceTable: "AP_INVOICE_LINES_ALL",       additionalSources: ["AP_INVOICES_ALL"],                  relationship: "N:1", targetName: "fact_ap_invoice_lines",   type: "fact", businessName: "AP Invoice Lines",          description: "Invoice lines merged with header — GL distributions, tax, amounts" },
    { sourceTable: "AP_INVOICES_ALL",            relationship: "1:N", additionalTargets: ["fact_ap_invoice_holds"],            targetName: "fact_ap_invoices",        type: "fact", businessName: "AP Invoices",               description: "Supplier invoices → fact_ap_invoices + fact_ap_invoice_holds" },
    { sourceTable: "AP_PAYMENTS_ALL",            relationship: "1:1",                                                          targetName: "fact_ap_payments",        type: "fact", businessName: "AP Payments",               description: "Payment runs and remittance transactions" },
    { sourceTable: "AP_SUPPLIERS",               additionalSources: ["AP_SUPPLIER_SITES_ALL"],             relationship: "N:1", targetName: "dim_supplier",            type: "dim",  businessName: "Suppliers",                 description: "Supplier master merged with site/address details" },
    { sourceTable: "AP_TERMS",                   relationship: "1:1",                                                          targetName: "dim_payment_terms",       type: "dim",  businessName: "Payment Terms",             description: "Payment terms: due date rules, discount windows" },
  ],
  accounts_receivable: [
    { sourceTable: "RA_CUSTOMER_TRX_ALL",        relationship: "1:N", additionalTargets: ["fact_ar_credit_memos"],             targetName: "fact_ar_invoices",        type: "fact", businessName: "AR Invoices",               description: "Customer invoices → fact_ar_invoices + fact_ar_credit_memos" },
    { sourceTable: "AR_CASH_RECEIPTS_ALL",       additionalSources: ["AR_RECEIVABLE_APPLICATIONS_ALL"],   relationship: "N:1", targetName: "fact_ar_receipts",        type: "fact", businessName: "AR Receipts",               description: "Cash receipts merged with invoice application details" },
    { sourceTable: "HZ_CUST_ACCOUNTS",           additionalSources: ["HZ_PARTIES","HZ_LOCATIONS"],        relationship: "N:1", targetName: "dim_customer",            type: "dim",  businessName: "Customers",                 description: "Customer accounts merged with party master and location" },
    { sourceTable: "AR_AGING_BUCKET_LINES_B",    relationship: "1:1",                                                          targetName: "dim_aging_bucket",        type: "dim",  businessName: "AR Aging Buckets",          description: "Aging bucket definitions (0–30, 31–60, 61–90, 90+ days)" },
  ],
  procurement: [
    { sourceTable: "PO_LINES_ALL",               additionalSources: ["PO_HEADERS_ALL"],                   relationship: "N:1", targetName: "fact_po_lines",           type: "fact", businessName: "PO Lines",                  description: "PO lines merged with header — item, qty, price, supplier" },
    { sourceTable: "PO_HEADERS_ALL",             relationship: "1:N", additionalTargets: ["fact_po_releases"],                 targetName: "fact_purchase_orders",    type: "fact", businessName: "Purchase Orders",           description: "PO headers → fact_purchase_orders + fact_po_releases" },
    { sourceTable: "RCV_TRANSACTIONS",           additionalSources: ["RCV_SHIPMENT_HEADERS"],              relationship: "N:1", targetName: "fact_goods_receipts",     type: "fact", businessName: "Goods Receipts",            description: "GR transactions merged with shipment header context" },
    { sourceTable: "AP_SUPPLIERS",               additionalSources: ["AP_SUPPLIER_SITES_ALL"],             relationship: "N:1", targetName: "dim_supplier",            type: "dim",  businessName: "Suppliers",                 description: "Supplier master merged with site details" },
    { sourceTable: "MTL_SYSTEM_ITEMS_B",         relationship: "1:1",                                                          targetName: "dim_item",                type: "dim",  businessName: "Items",                     description: "Item master with purchasing and inventory attributes" },
    { sourceTable: "PO_AGENTS",                  relationship: "1:1",                                                          targetName: "dim_buyer",               type: "dim",  businessName: "Buyers / Purchasers",       description: "Buyer master for procurement analytics" },
  ],
  inventory: [
    { sourceTable: "MTL_TRANSACTION_ACCOUNTS",   additionalSources: ["MTL_MATERIAL_TRANSACTIONS"],         relationship: "N:1", targetName: "fact_inventory_movements",type: "fact", businessName: "Inventory Movements",       description: "Inventory accounting lines merged with material transaction header" },
    { sourceTable: "MTL_ONHAND_QUANTITIES_DETAIL",relationship: "1:1",                                                         targetName: "fact_inventory_snapshot", type: "fact", businessName: "Inventory Snapshot",        description: "On-hand quantities by item, subinventory, locator and lot" },
    { sourceTable: "MTL_SYSTEM_ITEMS_B",         additionalSources: ["MTL_SYSTEM_ITEMS_TL"],               relationship: "N:1", targetName: "dim_item",                type: "dim",  businessName: "Items / Products",          description: "Item master merged with translations" },
    { sourceTable: "ORG_ORGANIZATION_DEFINITIONS",additionalSources: ["HR_LOCATIONS"],                     relationship: "N:1", targetName: "dim_warehouse",           type: "dim",  businessName: "Warehouses / Orgs",         description: "Inventory organizations merged with HR location data" },
    { sourceTable: "MTL_ITEM_CATEGORIES",        additionalSources: ["MTL_CATEGORIES_B"],                  relationship: "N:1", targetName: "dim_item_category",       type: "dim",  businessName: "Item Categories",           description: "Item categories merged with category set master" },
    { sourceTable: "MTL_LOT_NUMBERS",            relationship: "1:1",                                                          targetName: "dim_lot",                 type: "dim",  businessName: "Lots / Batches",            description: "Lot/batch master for traceability and expiry tracking" },
  ],
  human_resources: [
    { sourceTable: "PER_ALL_PEOPLE_F",           additionalSources: ["PER_ALL_ASSIGNMENTS_F","PER_ADDRESSES"], relationship: "N:1", targetName: "dim_employee",        type: "dim",  businessName: "Employees",                 description: "Employee personal + current assignment + address merged" },
    { sourceTable: "PER_ALL_ASSIGNMENTS_F",      additionalSources: ["PER_JOBS","PER_GRADES"],              relationship: "N:1", targetName: "fact_assignments",        type: "fact", businessName: "Employee Assignments",      description: "Assignments merged with job and grade master" },
    { sourceTable: "PER_ABSENCE_ATTENDANCES",    relationship: "1:1",                                                          targetName: "fact_absence",            type: "fact", businessName: "Absence Records",           description: "Leave and absence events with duration and type" },
    { sourceTable: "HR_ALL_ORGANIZATION_UNITS",  additionalSources: ["HR_ORGANIZATION_INFORMATION"],       relationship: "N:1", targetName: "dim_department",          type: "dim",  businessName: "Departments / Org Units",   description: "Organization units merged with classification details" },
    { sourceTable: "PER_JOBS",                   relationship: "1:1",                                                          targetName: "dim_job",                 type: "dim",  businessName: "Jobs / Roles",              description: "Job definitions, families and functions" },
    { sourceTable: "PAY_PAYROLL_ACTIONS",        additionalSources: ["PAY_ASSIGNMENT_ACTIONS","PAY_RUN_RESULTS"], relationship: "N:1", targetName: "fact_payroll_runs", type: "fact", businessName: "Payroll Runs",              description: "Payroll run results merged with assignment actions" },
  ],
};

// ── Gold KPI suggestions (Gold layer) ─────────────────────────────────────────
const GOLD_KPI_SUGGESTIONS = {
  finance:            [
    { targetTable: "fact_financial_performance_monthly", businessName: "Monthly P&L Summary",        description: "Revenue, expenses, net income by cost center and period",   granularity: "Monthly" },
    { targetTable: "fact_account_balance_ytd",           businessName: "YTD Account Balances",        description: "Year-to-date running balance by account, entity, currency",  granularity: "Monthly" },
    { targetTable: "fact_cash_flow_monthly",             businessName: "Monthly Cash Flow",            description: "Operating, investing, financing cash flow by period",        granularity: "Monthly" },
  ],
  order_management:   [
    { targetTable: "fact_sales_performance_monthly",     businessName: "Monthly Sales Performance",   description: "Revenue, orders, units sold by customer, product, territory", granularity: "Monthly" },
    { targetTable: "fact_order_fulfillment_kpi",         businessName: "Order Fulfillment KPIs",      description: "On-time delivery %, fill rate, lead time by warehouse",       granularity: "Weekly"  },
    { targetTable: "fact_customer_revenue_monthly",      businessName: "Customer Revenue Monthly",    description: "Invoiced revenue by customer and product line per month",     granularity: "Monthly" },
  ],
  accounts_payable:   [
    { targetTable: "fact_ap_aging_monthly",              businessName: "AP Aging Monthly",            description: "Outstanding AP by supplier, age bucket (0-30, 31-60, 60+)",  granularity: "Monthly" },
    { targetTable: "fact_spend_analysis_monthly",        businessName: "Monthly Spend Analysis",      description: "Total spend by supplier, category, cost center per month",    granularity: "Monthly" },
    { targetTable: "fact_payment_performance_monthly",   businessName: "Payment Performance (DPO)",   description: "On-time payment %, Days Payable Outstanding by supplier",     granularity: "Monthly" },
  ],
  accounts_receivable:[
    { targetTable: "fact_ar_aging_monthly",              businessName: "AR Aging Monthly",            description: "Outstanding AR by customer, age bucket per month",            granularity: "Monthly" },
    { targetTable: "fact_revenue_by_customer_monthly",   businessName: "Monthly Revenue by Customer", description: "Invoiced revenue by customer, product category per month",    granularity: "Monthly" },
    { targetTable: "fact_dso_monthly",                   businessName: "DSO (Days Sales Outstanding)", description: "DSO trend by business unit per month",                       granularity: "Monthly" },
  ],
  procurement:        [
    { targetTable: "fact_po_spend_monthly",              businessName: "Monthly PO Spend",            description: "Committed spend by supplier, category, business unit",        granularity: "Monthly" },
    { targetTable: "fact_supplier_performance_monthly",  businessName: "Supplier Performance",        description: "On-time delivery, defect rate, spend by supplier monthly",    granularity: "Monthly" },
  ],
  inventory:          [
    { targetTable: "fact_inventory_kpi_daily",           businessName: "Daily Inventory KPIs",        description: "Stock on hand value, turnover, days on hand by item",         granularity: "Daily"   },
    { targetTable: "fact_stock_aging_weekly",            businessName: "Stock Aging Analysis",        description: "Aging buckets (0-30, 31-60, 90+ days) by item, warehouse",    granularity: "Weekly"  },
    { targetTable: "fact_inventory_accuracy_monthly",    businessName: "Inventory Accuracy",          description: "Cycle count accuracy %, variance by warehouse",               granularity: "Monthly" },
  ],
  human_resources:    [
    { targetTable: "fact_headcount_monthly",             businessName: "Monthly Headcount",           description: "Headcount by department, job, location, employment type",     granularity: "Monthly" },
    { targetTable: "fact_attrition_quarterly",           businessName: "Quarterly Attrition Rate",    description: "Voluntary/involuntary attrition rate by department",           granularity: "Quarterly"},
    { targetTable: "fact_payroll_summary_monthly",       businessName: "Monthly Payroll Summary",     description: "Payroll cost by department, grade, location per month",        granularity: "Monthly" },
  ],
  generic:            [
    { targetTable: "fact_summary_monthly",               businessName: "Monthly Summary Metrics",     description: "Aggregated KPIs by dimension and month",                      granularity: "Monthly" },
    { targetTable: "fact_performance_kpi",               businessName: "Performance KPIs",            description: "Key performance indicators by business dimension",             granularity: "Daily"   },
  ],
};

// ── Industry-standard ERP dimensional model: per-source, per-module ──────────
// N:1  = multiple source tables JOIN → one target  (additionalSources lists the extra tables)
// 1:N  = one source table SPLITS → multiple targets (additionalTargets lists the extra tables)
// 1:1  = direct 1-to-1 mapping
//
// Sources compiled from: Oracle EBS Data Model Guide, SAP Community / ABAP Dictionary,
// Microsoft Dynamics 365 F&O Entity Catalog, NetSuite SuiteAnalytics schema,
// Workday Report Designer canonical fields, and Microsoft Fabric medallion architecture patterns.
const DIM_SUGGESTIONS_BY_ERP = {
  sap_s4hana: {
    finance: [
      { sourceTable: "BSEG",  additionalSources: ["BKPF"],           relationship: "N:1", targetName: "fact_gl_transactions",    type: "fact", businessName: "GL Transactions",          description: "BSEG (line items) merged with BKPF (document header) via MANDT+BUKRS+BELNR+GJAHR" },
      { sourceTable: "SKAT",  additionalSources: ["SKA1"],            relationship: "N:1", targetName: "dim_account",             type: "dim",  businessName: "GL Accounts",              description: "G/L account descriptions (SKAT) merged with chart of accounts master (SKA1)" },
      { sourceTable: "T009B", relationship: "1:1",                                         targetName: "dim_period",              type: "dim",  businessName: "Fiscal Periods",           description: "SAP fiscal year variant periods (T009B)" },
      { sourceTable: "TCURR", relationship: "1:1",                                         targetName: "dim_currency_rate",       type: "dim",  businessName: "Currency Rates",           description: "Exchange rates by currency pair and date" },
      { sourceTable: "BSID",  additionalSources: ["BSAD"],            relationship: "N:1", targetName: "fact_ar_line_items",      type: "fact", businessName: "AR Line Items (Open+Cleared)", description: "AR open items (BSID) merged with cleared items (BSAD) for full AR aging" },
      { sourceTable: "BSIK",  additionalSources: ["BSAK"],            relationship: "N:1", targetName: "fact_ap_line_items",      type: "fact", businessName: "AP Line Items (Open+Cleared)", description: "AP open (BSIK) + cleared (BSAK) for full payable aging" },
    ],
    order_management: [
      { sourceTable: "VBAP",  additionalSources: ["VBAK"],            relationship: "N:1", targetName: "fact_order_lines",        type: "fact", businessName: "Sales Order Lines",        description: "VBAP (items) merged with VBAK (header) via VBELN — qty, price, material" },
      { sourceTable: "VBAK",  relationship: "1:N", additionalTargets: ["fact_sales_order_partners","fact_sales_order_conditions"], targetName: "fact_sales_orders", type: "fact", businessName: "Sales Orders", description: "VBAK → fact_sales_orders + partner functions + pricing conditions" },
      { sourceTable: "KNA1",  additionalSources: ["KNVV","KNVP"],     relationship: "N:1", targetName: "dim_customer",            type: "dim",  businessName: "Customers",                description: "General customer (KNA1) merged with sales area data (KNVV) and partner fns (KNVP)" },
      { sourceTable: "MARA",  additionalSources: ["MARC","MAKT"],     relationship: "N:1", targetName: "dim_product",             type: "dim",  businessName: "Products / Materials",     description: "Material master (MARA) + plant extension (MARC) + description (MAKT)" },
      { sourceTable: "TVKOT", relationship: "1:1",                                         targetName: "dim_sales_org",           type: "dim",  businessName: "Sales Organizations",      description: "Sales organization master — company, region, division" },
    ],
    accounts_payable: [
      { sourceTable: "RBKP",  additionalSources: ["RSEG"],            relationship: "N:1", targetName: "fact_ap_invoices",        type: "fact", businessName: "AP Invoice Documents",     description: "Invoice header (RBKP) merged with line items (RSEG) — LIV posting" },
      { sourceTable: "PAYR",  relationship: "1:1",                                         targetName: "fact_ap_payments",        type: "fact", businessName: "AP Payments",              description: "Payment medium data (PAYR) — check, ACH runs" },
      { sourceTable: "LFA1",  additionalSources: ["LFB1","LFM1"],     relationship: "N:1", targetName: "dim_supplier",            type: "dim",  businessName: "Suppliers / Vendors",      description: "Vendor general (LFA1) + company extension (LFB1) + purchasing org (LFM1)" },
      { sourceTable: "T052",  relationship: "1:1",                                         targetName: "dim_payment_terms",       type: "dim",  businessName: "Payment Terms",            description: "SAP payment term keys and due date rules (T052)" },
    ],
    procurement: [
      { sourceTable: "EKPO",  additionalSources: ["EKKO"],            relationship: "N:1", targetName: "fact_po_lines",           type: "fact", businessName: "PO Line Items",            description: "EKPO (PO items) merged with EKKO (header) via EBELN — item, qty, price" },
      { sourceTable: "EKKO",  relationship: "1:N", additionalTargets: ["fact_po_account_assignments"],                           targetName: "fact_purchase_orders", type: "fact", businessName: "Purchase Orders", description: "EKKO → fact_purchase_orders + account assignment lines (EKKN)" },
      { sourceTable: "MSEG",  additionalSources: ["MKPF"],            relationship: "N:1", targetName: "fact_goods_receipts",     type: "fact", businessName: "Goods Receipts / GI",      description: "MSEG (material document items) merged with MKPF (header) via MBLNR+MJAHR" },
      { sourceTable: "LFA1",  additionalSources: ["LFB1","LFM1"],     relationship: "N:1", targetName: "dim_supplier",            type: "dim",  businessName: "Suppliers / Vendors",      description: "Vendor master: general + company-code + purchasing org" },
      { sourceTable: "MARA",  additionalSources: ["MARC"],            relationship: "N:1", targetName: "dim_item",                type: "dim",  businessName: "Materials / Items",        description: "Material master (MARA) merged with plant data (MARC)" },
    ],
    inventory: [
      { sourceTable: "MSEG",  additionalSources: ["MKPF"],            relationship: "N:1", targetName: "fact_inventory_movements",type: "fact", businessName: "Inventory Movements",      description: "MSEG items + MKPF header — all material document transactions" },
      { sourceTable: "MARD",  relationship: "1:1",                                         targetName: "fact_inventory_snapshot", type: "fact", businessName: "Stock Snapshot (by Plant)", description: "Unrestricted, in-QI, restricted stock by material/plant/storage location" },
      { sourceTable: "MARA",  additionalSources: ["MARC","MAKT","MBEW"], relationship: "N:1", targetName: "dim_item",             type: "dim",  businessName: "Materials",                description: "Material master + plant extension + description + valuation class" },
      { sourceTable: "T001W", relationship: "1:1",                                         targetName: "dim_plant",               type: "dim",  businessName: "Plants / Warehouses",      description: "Plant master (T001W) — distribution center, factory" },
    ],
    human_resources: [
      { sourceTable: "PA0001",additionalSources: ["PA0002","PA0006","PA0007"], relationship: "N:1", targetName: "dim_employee", type: "dim",  businessName: "Employees",                description: "Org. assignment (PA0001) + personal data (PA0002) + addresses (PA0006) + planned work time (PA0007)" },
      { sourceTable: "PA0001",relationship: "1:N", additionalTargets: ["fact_employee_compensation","fact_employee_actions"],    targetName: "fact_assignments", type: "fact", businessName: "Employee Assignments", description: "PA0001 org data → fact_assignments + compensation (PA0008) + actions (PA0000)" },
      { sourceTable: "PA2001",relationship: "1:1",                                         targetName: "fact_absence",            type: "fact", businessName: "Absences",                 description: "PA2001 absence/attendance records with hours, type, cost center" },
      { sourceTable: "HRP1000",additionalSources: ["HRP1001"],        relationship: "N:1", targetName: "dim_department",          type: "dim",  businessName: "Org Units / Cost Centers", description: "HRP1000 (org objects) + HRP1001 (relationships) — org hierarchy" },
    ],
  },

  dynamics_365_fo: {
    finance: [
      { sourceTable: "GeneralJournalAccountEntry", additionalSources: ["GeneralJournalEntry"], relationship: "N:1", targetName: "fact_gl_transactions", type: "fact", businessName: "GL Transactions", description: "Account entries merged with journal entry header via RecId — amounts, accounts, dimensions" },
      { sourceTable: "MainAccount",              relationship: "1:1",                                         targetName: "dim_account",             type: "dim",  businessName: "Chart of Accounts",        description: "Main account master — account number, type, category" },
      { sourceTable: "FiscalCalendarPeriod",     additionalSources: ["FiscalCalendarYear"],  relationship: "N:1", targetName: "dim_period",            type: "dim",  businessName: "Fiscal Periods",           description: "Fiscal periods merged with fiscal year via FiscalCalendar entity" },
      { sourceTable: "CustTrans",               additionalSources: ["CustTransOpen"],       relationship: "N:1", targetName: "fact_ar_transactions",   type: "fact", businessName: "AR Transactions",          description: "CustTrans (all) merged with CustTransOpen (outstanding) for aging" },
      { sourceTable: "VendTrans",               additionalSources: ["VendTransOpen"],       relationship: "N:1", targetName: "fact_ap_transactions",   type: "fact", businessName: "AP Transactions",          description: "VendTrans (all) merged with VendTransOpen (outstanding) for DPO/aging" },
    ],
    order_management: [
      { sourceTable: "SalesLine",               additionalSources: ["SalesTable"],          relationship: "N:1", targetName: "fact_order_lines",       type: "fact", businessName: "Sales Order Lines",        description: "SalesLine items merged with SalesTable header via SalesId" },
      { sourceTable: "SalesTable",              relationship: "1:N", additionalTargets: ["fact_sales_order_charges"],                                  targetName: "fact_sales_orders", type: "fact", businessName: "Sales Orders",     description: "SalesTable → fact_sales_orders + misc charges (MarkupTrans)" },
      { sourceTable: "CustTable",              additionalSources: ["DirPartyTable","LogisticsPostalAddress"], relationship: "N:1", targetName: "dim_customer", type: "dim", businessName: "Customers",           description: "CustTable merged with party (DirPartyTable) and address data" },
      { sourceTable: "EcoResProduct",          additionalSources: ["EcoResProductTranslation","InventTable"], relationship: "N:1", targetName: "dim_product", type: "dim", businessName: "Products",             description: "Product master merged with translations and inventory settings" },
    ],
    accounts_payable: [
      { sourceTable: "VendInvoiceTrans",        additionalSources: ["VendInvoiceInfoTable"], relationship: "N:1", targetName: "fact_ap_invoice_lines",  type: "fact", businessName: "AP Invoice Lines",         description: "Invoice line transactions merged with invoice header" },
      { sourceTable: "VendTable",              additionalSources: ["DirPartyTable","LogisticsPostalAddress"], relationship: "N:1", targetName: "dim_supplier", type: "dim", businessName: "Suppliers / Vendors",  description: "VendTable + party master + address merged" },
    ],
    procurement: [
      { sourceTable: "PurchLine",               additionalSources: ["PurchTable"],          relationship: "N:1", targetName: "fact_po_lines",           type: "fact", businessName: "PO Lines",                 description: "PurchLine items merged with PurchTable header via PurchId" },
      { sourceTable: "PurchTable",              relationship: "1:N", additionalTargets: ["fact_po_charges"],                                            targetName: "fact_purchase_orders", type: "fact", businessName: "Purchase Orders", description: "PurchTable → fact_purchase_orders + charges" },
      { sourceTable: "InventTrans",             relationship: "1:1",                                         targetName: "fact_goods_receipts",     type: "fact", businessName: "Goods Receipts",           description: "InventTrans receipts filtered by TransType = Purchase" },
      { sourceTable: "VendTable",              additionalSources: ["DirPartyTable"],        relationship: "N:1", targetName: "dim_supplier",            type: "dim",  businessName: "Suppliers",                description: "Vendor master merged with party table" },
    ],
    inventory: [
      { sourceTable: "InventTrans",             relationship: "1:1",                                         targetName: "fact_inventory_movements", type: "fact", businessName: "Inventory Movements",      description: "All inventory transactions — receipts, issues, transfers, adjustments" },
      { sourceTable: "InventSum",               relationship: "1:1",                                         targetName: "fact_inventory_snapshot",  type: "fact", businessName: "Inventory Snapshot",       description: "On-hand qty summary by item and warehouse" },
      { sourceTable: "InventTable",            additionalSources: ["EcoResProduct","InventItemGroupItem"], relationship: "N:1", targetName: "dim_item", type: "dim", businessName: "Items / Products",       description: "InventTable merged with product master and item group" },
      { sourceTable: "InventLocation",          relationship: "1:1",                                         targetName: "dim_warehouse",           type: "dim",  businessName: "Warehouses",               description: "InventLocation master — warehouse, zone, bay" },
    ],
    human_resources: [
      { sourceTable: "HcmWorker",              additionalSources: ["DirPersonName","HcmEmployment","HcmPosition"], relationship: "N:1", targetName: "dim_employee", type: "dim", businessName: "Employees",          description: "Worker merged with name, employment terms, and position details" },
      { sourceTable: "HcmEmployment",          relationship: "1:N", additionalTargets: ["fact_employee_compensation"],                                 targetName: "fact_assignments", type: "fact", businessName: "Employee Assignments", description: "HcmEmployment → assignments + compensation (HcmPositionWorkerAssignment)" },
      { sourceTable: "HcmAbsenceCode",         relationship: "1:1",                                         targetName: "dim_absence_code",         type: "dim",  businessName: "Leave Types",              description: "Absence type codes — sick, vacation, FMLA, etc." },
      { sourceTable: "HcmDepartment",          relationship: "1:1",                                         targetName: "dim_department",           type: "dim",  businessName: "Departments",              description: "Department master — cost center, manager, hierarchy" },
    ],
  },

  netsuite: {
    finance: [
      { sourceTable: "TRANSACTION",            additionalSources: ["TRANSACTION_LINES","ACCOUNTS"],          relationship: "N:1", targetName: "fact_gl_transactions", type: "fact", businessName: "GL Transactions", description: "Transaction lines merged with header and account — journals, invoices, bills" },
      { sourceTable: "ACCOUNTS",               relationship: "1:1",                                         targetName: "dim_account",             type: "dim",  businessName: "GL Accounts",              description: "NetSuite chart of accounts with type, sub-type, and category" },
      { sourceTable: "ACCOUNTING_PERIODS",     relationship: "1:1",                                         targetName: "dim_period",              type: "dim",  businessName: "Accounting Periods",       description: "Fiscal periods with start/end dates, year, quarter" },
      { sourceTable: "CURRENCIES",             relationship: "1:1",                                         targetName: "dim_currency",            type: "dim",  businessName: "Currencies",               description: "Currency codes and exchange rate metadata" },
    ],
    order_management: [
      { sourceTable: "TRANSACTION_LINES",      additionalSources: ["TRANSACTION","ITEMS"],                  relationship: "N:1", targetName: "fact_order_lines",       type: "fact", businessName: "Sales Order Lines",        description: "Transaction lines (type=SalesOrder) merged with header and item" },
      { sourceTable: "TRANSACTION",            relationship: "1:N", additionalTargets: ["fact_sales_invoices"],                                    targetName: "fact_sales_orders",      type: "fact", businessName: "Sales Orders",             description: "Sales order transactions → fact_sales_orders + invoiced amounts" },
      { sourceTable: "CUSTOMERS",              additionalSources: ["CONTACTS","ADDRESSES"],                 relationship: "N:1", targetName: "dim_customer",            type: "dim",  businessName: "Customers",                description: "Customer master merged with primary contact and billing address" },
      { sourceTable: "ITEMS",                  additionalSources: ["ITEM_LOCATIONS"],                       relationship: "N:1", targetName: "dim_product",             type: "dim",  businessName: "Products / Items",         description: "Item master merged with location-level pricing and inventory attributes" },
    ],
    accounts_payable: [
      { sourceTable: "TRANSACTION_LINES",      additionalSources: ["TRANSACTION","VENDORS"],                relationship: "N:1", targetName: "fact_ap_invoice_lines",  type: "fact", businessName: "AP Invoice Lines",         description: "Bill lines merged with header and vendor — for spend analysis" },
      { sourceTable: "VENDORS",                additionalSources: ["ADDRESSES"],                            relationship: "N:1", targetName: "dim_supplier",            type: "dim",  businessName: "Suppliers / Vendors",      description: "Vendor master merged with primary remittance address" },
    ],
    procurement: [
      { sourceTable: "TRANSACTION_LINES",      additionalSources: ["TRANSACTION"],                         relationship: "N:1", targetName: "fact_po_lines",           type: "fact", businessName: "PO Lines",                 description: "Purchase order lines merged with header (type=PurchaseOrder)" },
      { sourceTable: "VENDORS",                additionalSources: ["ADDRESSES"],                            relationship: "N:1", targetName: "dim_supplier",            type: "dim",  businessName: "Suppliers",                description: "Vendor master merged with address" },
      { sourceTable: "ITEMS",                  relationship: "1:1",                                         targetName: "dim_item",                type: "dim",  businessName: "Items",                    description: "Purchaseable item master with GL impact accounts" },
    ],
    inventory: [
      { sourceTable: "INVENTORY_BALANCE",      relationship: "1:1",                                         targetName: "fact_inventory_snapshot",  type: "fact", businessName: "Inventory Snapshot",       description: "On-hand quantity and value by item and location" },
      { sourceTable: "ITEMS",                  additionalSources: ["ITEM_LOCATIONS"],                       relationship: "N:1", targetName: "dim_item",                type: "dim",  businessName: "Items",                    description: "Item master merged with bin/location reorder settings" },
      { sourceTable: "LOCATIONS",              relationship: "1:1",                                         targetName: "dim_warehouse",           type: "dim",  businessName: "Locations / Warehouses",   description: "Inventory location hierarchy" },
    ],
    human_resources: [
      { sourceTable: "EMPLOYEES",              additionalSources: ["CONTACTS","JOB_CODES"],                 relationship: "N:1", targetName: "dim_employee",            type: "dim",  businessName: "Employees",                description: "Employee master merged with contact info and job classification" },
      { sourceTable: "DEPARTMENTS",            relationship: "1:1",                                         targetName: "dim_department",          type: "dim",  businessName: "Departments",              description: "Department master with budget owner" },
    ],
  },

  workday: {
    finance: [
      { sourceTable: "Ledger_Account_Line",    additionalSources: ["Journal","Accounting_Period"],          relationship: "N:1", targetName: "fact_gl_transactions",    type: "fact", businessName: "GL Transactions",          description: "Ledger account lines merged with journal header and period" },
      { sourceTable: "Ledger_Account",         relationship: "1:1",                                         targetName: "dim_account",             type: "dim",  businessName: "Ledger Accounts",          description: "Workday ledger account with account type and summary level" },
      { sourceTable: "Fiscal_Time_Period",     relationship: "1:1",                                         targetName: "dim_period",              type: "dim",  businessName: "Fiscal Periods",           description: "Fiscal periods by company calendar" },
      { sourceTable: "Currency_Rate",          relationship: "1:1",                                         targetName: "dim_currency_rate",       type: "dim",  businessName: "Exchange Rates",           description: "Daily currency conversion rates" },
    ],
    accounts_payable: [
      { sourceTable: "Supplier_Invoice_Line",  additionalSources: ["Supplier_Invoice"],                    relationship: "N:1", targetName: "fact_ap_invoice_lines",  type: "fact", businessName: "AP Invoice Lines",         description: "Invoice lines merged with invoice header — amounts, spend categories" },
      { sourceTable: "Supplier",               additionalSources: ["Supplier_Contact"],                    relationship: "N:1", targetName: "dim_supplier",            type: "dim",  businessName: "Suppliers",                description: "Supplier master merged with primary contact" },
      { sourceTable: "Payment_Term",           relationship: "1:1",                                         targetName: "dim_payment_terms",       type: "dim",  businessName: "Payment Terms",            description: "Workday payment term definitions" },
    ],
    procurement: [
      { sourceTable: "Purchase_Order_Line",    additionalSources: ["Purchase_Order"],                      relationship: "N:1", targetName: "fact_po_lines",           type: "fact", businessName: "PO Lines",                 description: "PO lines merged with PO header — spend category, quantity, price" },
      { sourceTable: "Supplier",               relationship: "1:1",                                         targetName: "dim_supplier",            type: "dim",  businessName: "Suppliers",                description: "Supplier master" },
      { sourceTable: "Spend_Category",         relationship: "1:1",                                         targetName: "dim_spend_category",      type: "dim",  businessName: "Spend Categories",         description: "Workday spend category hierarchy for procurement analytics" },
    ],
    human_resources: [
      { sourceTable: "Worker",                 additionalSources: ["Worker_Profile","Employment_Data","Position"], relationship: "N:1", targetName: "dim_employee", type: "dim",  businessName: "Employees / Workers",      description: "Worker merged with profile, employment terms and current position" },
      { sourceTable: "Worker",                 relationship: "1:N", additionalTargets: ["fact_compensation_history","fact_employee_actions"],   targetName: "fact_assignments", type: "fact", businessName: "Assignments",       description: "Worker → assignments + compensation changes + HR actions over time" },
      { sourceTable: "Absence_Input",          relationship: "1:1",                                         targetName: "fact_absence",            type: "fact", businessName: "Absence / Leave",          description: "Leave and absence requests with approved hours and type" },
      { sourceTable: "Cost_Center",            relationship: "1:1",                                         targetName: "dim_department",          type: "dim",  businessName: "Cost Centers / Depts",     description: "Cost center hierarchy with manager and budget" },
      { sourceTable: "Pay_Component",          additionalSources: ["Payroll_Result"],                      relationship: "N:1", targetName: "fact_payroll_runs",       type: "fact", businessName: "Payroll Runs",             description: "Payroll pay components merged with result header" },
    ],
  },
};

function _guessTableType(t) {
  const n = t.toLowerCase();
  if (n.startsWith("fact_") || n.endsWith("_all") || n.includes("_transaction") ||
      n.includes("_lines") || n.includes("_history") || n.includes("_je_")) return "fact";
  return "dim";
}
function _guessTargetName(t) {
  const type = _guessTableType(t);
  const cleaned = t.toLowerCase()
    .replace(/_all$/,"").replace(/_b$/,"").replace(/_f$/,"").replace(/_tl$/,"")
    .replace(/^oe_/,"").replace(/^hz_/,"").replace(/^mtl_/,"")
    .replace(/^per_/,"").replace(/^hr_/,"").replace(/^gl_/,"")
    .replace(/^ap_/,"").replace(/^ar_/,"").replace(/^po_/,"")
    .replace(/^ra_/,"").replace(/^fnd_/,"").replace(/^org_/,"");
  return (type === "fact" ? "fact_" : "dim_") + (cleaned || t.toLowerCase());
}
function _humanize(t) {
  return t.replace(/_all$/i,"").replace(/_b$/i,"").replace(/_f$/i,"")
    .replace(/_/g," ").split(" ").map(w => w.charAt(0).toUpperCase()+w.slice(1).toLowerCase()).join(" ");
}

// ── Step 5: Deploy ────────────────────────────────────────────────────────────
function StepDeploy({ wizard, onBack }) {
  // ── Core state ────────────────────────────────────────────────────────────
  const [opts,        setOpts]        = useState({ bronze: true, silver: true, gold: true, pipeline: true });
  const [result,      setResult]      = useState(null);
  const [verifying,   setVerifying]   = useState(false);
  const [verifyItems, setVerifyItems] = useState(null);

  // ── Sub-step navigation: 1=Bronze 2=Silver 3=Gold 4=Deploy ────────────────
  const [deploySubStep, setDeploySubStep] = useState(1);

  // ── Bronze ERL — Extract configuration ────────────────────────────────────
  const [bronzeErl, setBronzeErl] = useState({
    loadMode:     "full",      // "full" | "incremental"
    watermarkCol: "",          // e.g. LAST_UPDATE_DATE
    parallelism:  4,
    compression:  "snappy",    // "snappy" | "gzip" | "none"
    partitionBy:  "",          // optional partition column
  });

  // ── Silver ERL — Refine + Dimensional Modeling ────────────────────────────
  const [silverErl, setSilverErl] = useState({
    dedup:        true,
    nullHandling: "keep",      // "keep" | "drop_rows" | "fill_default"
    castTypes:    true,
    surrogateKey: false,
    scdType:      "type1",     // "type1" | "type2"
  });
  const [dimMappings,    setDimMappings]    = useState([]);
  const [dimExpanded,    setDimExpanded]    = useState(null);

  // ── Gold ERL — Aggregate / KPI configuration ──────────────────────────────
  const [goldErl, setGoldErl] = useState({
    granularity:    "monthly", // "daily" | "weekly" | "monthly" | "quarterly"
    addCalendarDim: true,
  });
  const [goldKpis,       setGoldKpis]       = useState([]);
  const [kpiExpanded,    setKpiExpanded]    = useState(null);

  // Notebook kernel per layer: "pyspark" (default) or "sql"
  const [notebookTypes, setNotebookTypes] = useState({ bronze: "pyspark", silver: "pyspark", gold: "pyspark" });

  // Code preview & editor
  const [codePreview,    setCodePreview]    = useState({});   // raw from backend
  const [customCode,     setCustomCode]     = useState({});   // user-edited overrides
  const [showPreview,    setShowPreview]    = useState(false);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [activeCodeTab,  setActiveCodeTab]  = useState("bronze");

  // Custom SQL per table per layer
  const [sqlConfig,       setSqlConfig]       = useState({});  // { table: { bronze, silver, gold } }
  const [sqlExpandedTable, setSqlExpandedTable] = useState(null);
  const [sqlActiveLayer,   setSqlActiveLayer]   = useState("bronze");

  // Industry-standard SQL templates fetched from backend
  const [standardSql,    setStandardSql]    = useState({ silver: {}, gold: {} });
  const [standardLoaded, setStandardLoaded] = useState(false);

  // Per-table kernel type overrides
  const [tableKernelTypes, setTableKernelTypes] = useState({});

  // Error expansion in results table
  const [expandedErrors, setExpandedErrors] = useState(new Set());

  // Per-layer deploy results & loading
  const [layerResults,  setLayerResults]  = useState({});
  const [layerDeploying,setLayerDeploying]= useState({ bronze: false, silver: false, gold: false });

  const tables = wizard.discoveredTables ?? [];

  // ── Initialize dimensional model mappings from suggestions ────────────────
  useEffect(() => {
    if (tables.length === 0 || dimMappings.length > 0) return;
    const modKey = (wizard.module ?? "").toLowerCase().replace(/[\s\-]+/g, "_");
    const srcKey = (wizard.source ?? "").toLowerCase();

    // Priority: 1) ERP-specific suggestions  2) generic (Oracle-EBS-aligned) suggestions
    const erpSpecific = DIM_SUGGESTIONS_BY_ERP[srcKey]?.[modKey];
    const suggs = erpSpecific?.length > 0 ? erpSpecific : DIM_SUGGESTIONS[modKey];

    const normalise = (tbl) => tbl.toUpperCase().replace(/_ALL$/, "").replace(/_B$/, "");

    if (suggs?.length > 0) {
      // Match suggestions against discovered tables (primary source table, partial match)
      const matched = suggs.filter(s =>
        tables.some(t => t.toUpperCase().includes(normalise(s.sourceTable)))
      );
      const matchedSrcSet = new Set(
        matched.flatMap(s => [s.sourceTable, ...(s.additionalSources ?? [])].map(normalise))
      );
      const unmatched = tables
        .filter(t => !matchedSrcSet.has(normalise(t)))
        .map((t, i) => ({
          id: `gen-${i}`, sourceTable: t,
          targetName: _guessTargetName(t), type: _guessTableType(t),
          businessName: _humanize(t), description: "",
          relationship: "1:1", additionalSources: [], additionalTargets: [], enabled: true,
        }));
      setDimMappings([
        ...matched.map((s, i) => ({
          ...s,
          enabled: true,
          id: `sug-${i}`,
          relationship:       s.relationship       ?? "1:1",
          additionalSources:  s.additionalSources  ?? [],
          additionalTargets:  s.additionalTargets  ?? [],
        })),
        ...unmatched,
      ]);
    } else {
      setDimMappings(tables.map((t, i) => ({
        id: `gen-${i}`, sourceTable: t, targetName: _guessTargetName(t),
        type: _guessTableType(t), businessName: _humanize(t), description: "",
        relationship: "1:1", additionalSources: [], additionalTargets: [], enabled: true,
      })));
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tables.join(","), wizard.module, wizard.source]);

  // ── Initialize Gold KPI suggestions ──────────────────────────────────────
  useEffect(() => {
    if (goldKpis.length > 0 || !wizard.module) return;
    const modKey = (wizard.module ?? "").toLowerCase().replace(/[\s\-]+/g, "_");
    const suggs  = GOLD_KPI_SUGGESTIONS[modKey] ?? GOLD_KPI_SUGGESTIONS.generic;
    setGoldKpis(suggs.map((k, i) => ({ ...k, id: `kpi-${i}`, enabled: true })));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [wizard.module]);

  // ── Derived SQL dicts (for deploy & preview payloads) ────────────────────
  const custom_sql = Object.fromEntries(
    Object.entries(sqlConfig).filter(([, v]) => v.bronze?.trim()).map(([t, v]) => [t, v.bronze])
  );
  const silver_sql = Object.fromEntries(
    Object.entries(sqlConfig).filter(([, v]) => v.silver?.trim()).map(([t, v]) => [t, v.silver])
  );
  const gold_sql = Object.fromEntries(
    Object.entries(sqlConfig).filter(([, v]) => v.gold?.trim()).map(([t, v]) => [t, v.gold])
  );
  const custom_notebook_code = Object.fromEntries(
    Object.entries(customCode).filter(([k, v]) => k !== "pipeline" && v?.trim())
  );
  const custom_pipeline_json = customCode.pipeline?.trim() ?? "";

  // ── Fetch industry-standard SQL templates on source/module change ─────────
  useEffect(() => {
    if (!wizard.source || !wizard.module) return;
    axios.get(`${API}/api/fabric/standard-sql?source_type=${wizard.source}&module=${wizard.module}`)
      .then(r => {
        if (r.data.has_standard) {
          setStandardSql({ silver: r.data.silver ?? {}, gold: r.data.gold ?? {} });
          // Pre-populate sqlConfig for tables that don't have custom SQL yet
          setSqlConfig(prev => {
            const next = { ...prev };
            Object.entries(r.data.silver ?? {}).forEach(([tbl, sql]) => {
              if (!next[tbl]?.silver) {
                next[tbl] = { ...next[tbl], silver: sql };
              }
            });
            Object.entries(r.data.gold ?? {}).forEach(([tbl, sql]) => {
              if (!next[tbl]?.gold) {
                next[tbl] = { ...next[tbl], gold: sql };
              }
            });
            return next;
          });
          setStandardLoaded(true);
        }
      })
      .catch(() => {});
  }, [wizard.source, wizard.module]);

  // ── Deploy mutation ───────────────────────────────────────────────────────
  const deployMutation = useMutation({
    mutationFn: () =>
      axios.post(`${API}/api/fabric/deploy`, {
        workspace_id:         wizard.fabricWorkspace?.workspace_id ?? "",
        lakehouse_id:         wizard.fabricWorkspace?.lakehouse_id ?? "",
        source_type:          wizard.source,
        module:               wizard.module,
        connection_id:        wizard.connectionId   ?? "",
        connection_name:      wizard.connectionName ?? "",
        selected_tables:      tables,
        create_bronze:        opts.bronze,
        create_silver:        opts.silver,
        create_gold:          opts.gold,
        create_pipeline:      opts.pipeline,
        custom_sql,
        silver_sql,
        gold_sql,
        notebook_types:       notebookTypes,
        table_kernel_types:   tableKernelTypes,
        custom_notebook_code,
        custom_pipeline_json,
      }).then(r => r.data),
    onSuccess: data => {
      setResult(data);
      setVerifyItems(null);
      setExpandedErrors(new Set());
      try {
        const sessions = JSON.parse(localStorage.getItem("ilink_sessions") ?? "[]");
        sessions.unshift({
          id: Date.now(), source: wizard.sourceName, module: wizard.moduleName,
          date: new Date().toISOString(), artifacts: data.total, live: data.live,
        });
        localStorage.setItem("ilink_sessions", JSON.stringify(sessions.slice(0, 10)));
      } catch {}
      if (data.live) toast.success(`Deployed ${data.total} artifact(s) to Fabric!`);
      else           toast("Deployment simulated — add MS365 token for live deploy", { icon: "ℹ️" });
    },
    onError: (err) => {
      const detail = err.response?.data?.detail ?? err.message ?? "Deployment failed";
      toast.error(detail);
    },
  });

  // ── Per-layer deploy (Bronze / Silver / Gold independently) ──────────────
  const deployLayer = async (layer) => {
    setLayerDeploying(p => ({ ...p, [layer]: true }));
    try {
      const r = await axios.post(`${API}/api/fabric/deploy`, {
        workspace_id:         wizard.fabricWorkspace?.workspace_id ?? "",
        lakehouse_id:         wizard.fabricWorkspace?.lakehouse_id ?? "",
        source_type:          wizard.source,
        module:               wizard.module,
        connection_id:        wizard.connectionId   ?? "",
        connection_name:      wizard.connectionName ?? "",
        selected_tables:      tables,
        create_bronze:        layer === "bronze",
        create_silver:        layer === "silver",
        create_gold:          layer === "gold",
        create_pipeline:      false,
        custom_sql,
        silver_sql,
        gold_sql,
        notebook_types:       notebookTypes,
        table_kernel_types:   tableKernelTypes,
        custom_notebook_code,
        custom_pipeline_json: "",
      });
      setLayerResults(p => ({ ...p, [layer]: r.data }));
      const cap = layer.charAt(0).toUpperCase() + layer.slice(1);
      if (r.data.live) toast.success(`${cap} layer deployed to Fabric — ${r.data.total} artifact(s)`);
      else             toast(`${cap} deployment simulated (no live Fabric token)`, { icon: "ℹ️" });
    } catch (e) {
      toast.error(e.response?.data?.detail ?? `${layer} deployment failed`);
      setLayerResults(p => ({ ...p, [layer]: { error: true } }));
    } finally {
      setLayerDeploying(p => ({ ...p, [layer]: false }));
    }
  };

  // ── Fetch code preview from backend ──────────────────────────────────────
  const fetchPreview = async () => {
    setPreviewLoading(true);
    try {
      const resp = await axios.post(`${API}/api/fabric/preview-notebooks`, {
        source_type:     wizard.source,
        module:          wizard.module,
        selected_tables: tables,
        create_bronze:   opts.bronze,
        create_silver:   opts.silver,
        create_gold:     opts.gold,
        create_pipeline: opts.pipeline,
        custom_sql,
        silver_sql,
        gold_sql,
        notebook_types:     notebookTypes,
        table_kernel_types: tableKernelTypes,
      });
      const layers = resp.data.layers ?? {};
      setCodePreview(layers);
      // Reset user edits when re-generating
      setCustomCode({});
      setShowPreview(true);
      const available = ["bronze", "silver", "gold", "pipeline"].filter(k => layers[k]);
      if (available.length > 0) setActiveCodeTab(available[0]);
    } catch (e) {
      toast.error("Failed to generate code preview");
    } finally {
      setPreviewLoading(false);
    }
  };

  // ── Verify deployed artifacts ─────────────────────────────────────────────
  const verifyArtifacts = async () => {
    const wsId = wizard.fabricWorkspace?.workspace_id;
    if (!wsId) { toast.error("No workspace ID set"); return; }
    setVerifying(true); setVerifyItems(null);
    try {
      const [nbRes, plRes] = await Promise.all([
        axios.get(`${API}/api/fabric/workspaces/${wsId}/items?type=Notebook`),
        axios.get(`${API}/api/fabric/workspaces/${wsId}/items?type=DataPipeline`),
      ]);
      const allItems = [...(nbRes.data.items ?? []), ...(plRes.data.items ?? [])];
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

  // ── SQL layer placeholder ─────────────────────────────────────────────────
  const sqlPlaceholder = (table, layer) => {
    if (layer === "bronze") return `SELECT * FROM ${table}`;
    if (layer === "silver") return `SELECT DISTINCT *\nFROM bronze.${table.toLowerCase()}\nWHERE 1=1  -- add filters`;
    return `SELECT date_trunc('month', created_date) AS month,\n       COUNT(*) AS total\nFROM silver.${table.toLowerCase()}\nGROUP BY 1`;
  };

  // ── SQL configured badge helper ───────────────────────────────────────────
  const tableHasSql = (t) =>
    ["bronze", "silver", "gold"].some(l => sqlConfig[t]?.[l]?.trim());

  const sqlConfiguredCount = Object.keys(sqlConfig).filter(tableHasSql).length;

  // ── Toggle error expansion ────────────────────────────────────────────────
  const toggleError = (i) => setExpandedErrors(prev => {
    const n = new Set(prev);
    n.has(i) ? n.delete(i) : n.add(i);
    return n;
  });

  // ── Active code tab value (custom override > fetched preview) ─────────────
  const activeCode = customCode[activeCodeTab] ?? codePreview[activeCodeTab] ?? "";
  const isCustomised = !!customCode[activeCodeTab]?.trim();

  // ── Layer badge color helper ──────────────────────────────────────────────
  const layerColor = { bronze: "#92400e", silver: "#1d4ed8", gold: "#854d0e", pipeline: "#5b21b6" };
  const layerBg    = { bronze: "#fef3c7", silver: "#eff6ff", gold: "#fef9c3", pipeline: "#f5f3ff" };

  return (
    <div className="fade-in" style={{ display: "flex", flexDirection: "column", gap: 20 }}>
      {/* ── Header ── */}
      <div>
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 4 }}>
          <h2 style={{ fontSize: 18, fontWeight: 700, margin: 0 }}>Deploy to Microsoft Fabric</h2>
          {standardLoaded && (
            <span style={{
              fontSize: 10, fontWeight: 700, padding: "2px 10px", borderRadius: 20,
              background: "#f0fdf4", color: "#15803d", border: "1px solid #bbf7d0",
              display: "flex", alignItems: "center", gap: 4,
            }}>
              <CheckCircle2 size={10} /> Standard SQL loaded
            </span>
          )}
        </div>
        <p style={{ fontSize: 13, color: "var(--color-muted)", margin: 0 }}>
          Create Fabric notebooks and pipeline for <strong>{wizard.sourceName}</strong> · {wizard.moduleName}.
        </p>
      </div>

      {/* ── Summary cards ── */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(160px,1fr))", gap: 10 }}>
        {[
          { label: "ERP Source", value: wizard.sourceName ?? wizard.source,         icon: Server  },
          { label: "Module",     value: wizard.moduleName ?? wizard.module,          icon: Package },
          { label: "Tables",     value: `${tables.length}`,                          icon: Table2  },
          { label: "Connection", value: wizard.connectionId ? "Created" : "Pending", icon: PlugZap },
        ].map(({ label, value, icon: Icon }) => (
          <div key={label} className="card" style={{ padding: 14 }}>
            <div style={{ fontSize: 10, color: "#94a3b8", fontWeight: 600,
                          display: "flex", alignItems: "center", gap: 5, marginBottom: 5 }}>
              <Icon size={11} /> {label.toUpperCase()}
            </div>
            <div style={{ fontSize: 13, fontWeight: 700, overflow: "hidden",
                          textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {value}
            </div>
          </div>
        ))}
      </div>

      {!result && (
        <>
          {/* ── Deploy Sub-step Navigation Bar ── */}
          <div style={{
            display: "flex", alignItems: "center", gap: 0,
            background: "#f8fafc", border: "1px solid #e2e8f0",
            borderRadius: 10, padding: "6px 8px",
          }}>
            {[
              { id: 1, label: "Bronze",  sublabel: "Extract",          color: "#92400e", bg: "#fef3c7", emoji: "🥉" },
              { id: 2, label: "Silver",  sublabel: "Refine",            color: "#1d4ed8", bg: "#eff6ff", emoji: "🥈" },
              { id: 3, label: "Gold",    sublabel: "Load / Aggregate",  color: "#854d0e", bg: "#fef9c3", emoji: "🥇" },
              { id: 4, label: "Deploy",  sublabel: "Publish to Fabric", color: "#5b21b6", bg: "#f5f3ff", emoji: "🚀" },
            ].map((s, i) => {
              const done   = deploySubStep > s.id;
              const active = deploySubStep === s.id;
              return (
                <div key={s.id} style={{ display: "flex", alignItems: "center", flex: 1 }}>
                  <button
                    onClick={() => setDeploySubStep(s.id)}
                    style={{
                      flex: 1, display: "flex", alignItems: "center", gap: 8,
                      padding: "8px 12px", borderRadius: 8, cursor: "pointer",
                      border: active ? `2px solid ${s.color}` : "2px solid transparent",
                      background: active ? s.bg : done ? "#f0fdf4" : "transparent",
                      transition: "all 0.12s",
                    }}
                  >
                    <span style={{ fontSize: 16 }}>{done ? "✅" : s.emoji}</span>
                    <div>
                      <div style={{ fontSize: 12, fontWeight: 700, color: active ? s.color : done ? "#15803d" : "#64748b" }}>
                        {s.label}{done ? " ✓" : ""}
                      </div>
                      <div style={{ fontSize: 10, color: "#94a3b8" }}>{s.sublabel}</div>
                    </div>
                  </button>
                  {i < 3 && <ChevronRight size={14} style={{ color: "#cbd5e1", flexShrink: 0 }} />}
                </div>
              );
            })}
          </div>

          {/* ══════════════════════════════════════════════════════════════════ */}
          {/* SUB-STEP 1 — Bronze: Extract Configuration                        */}
          {/* ══════════════════════════════════════════════════════════════════ */}
          {deploySubStep === 1 && (
            <>
              <div className="card" style={{ padding: 20, borderLeft: "4px solid #d97706" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 14, marginBottom: 16 }}>
                  <h3 style={{ fontSize: 14, fontWeight: 700, margin: 0, flex: 1, display: "flex", alignItems: "center", gap: 8 }}>
                    <span style={{ fontSize: 18 }}>🥉</span>
                    Bronze Notebook — ERL: <span style={{ color: "#d97706" }}>Extract</span>
                    <span style={{ fontSize: 10, fontWeight: 400, color: "#94a3b8", marginLeft: 4 }}>Raw ERP → Delta (bronze schema)</span>
                  </h3>
                  <label style={{ display: "flex", alignItems: "center", gap: 6, cursor: "pointer", fontSize: 13 }}>
                    <input type="checkbox" checked={opts.bronze} onChange={e => setOpts(o => ({ ...o, bronze: e.target.checked }))} />
                    Enable Bronze
                  </label>
                  {opts.bronze && (
                    <div style={{ display: "flex", gap: 4 }}>
                      {[{ val: "pyspark", lbl: "PySpark", ico: Terminal }, { val: "sql", lbl: "SparkSQL", ico: Code }].map(({ val, lbl, ico: KIcon }) => (
                        <button key={val} onClick={() => setNotebookTypes(p => ({ ...p, bronze: val }))}
                          style={{ padding: "4px 10px", fontSize: 11, fontWeight: 600, borderRadius: 6, cursor: "pointer", border: "1px solid", display: "flex", alignItems: "center", gap: 3, background: notebookTypes.bronze === val ? "#1e40af" : "white", color: notebookTypes.bronze === val ? "white" : "#64748b", borderColor: notebookTypes.bronze === val ? "#1e40af" : "#cbd5e1" }}>
                          <KIcon size={9} /> {lbl}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
                {opts.bronze && (
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
                    <div>
                      <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "#374151", marginBottom: 6 }}>Extract Mode</label>
                      <div style={{ display: "flex", gap: 8 }}>
                        {[
                          { val: "full",        label: "Full Refresh",  desc: "Re-extract all rows on each run"      },
                          { val: "incremental", label: "Incremental",   desc: "Extract only new/changed rows"        },
                        ].map(({ val, label, desc }) => (
                          <button key={val} onClick={() => setBronzeErl(e => ({ ...e, loadMode: val }))}
                            style={{ flex: 1, padding: "10px 12px", borderRadius: 8, cursor: "pointer", border: `2px solid ${bronzeErl.loadMode === val ? "#d97706" : "#e2e8f0"}`, background: bronzeErl.loadMode === val ? "#fef3c7" : "white", textAlign: "left" }}>
                            <div style={{ fontSize: 12, fontWeight: 600, color: bronzeErl.loadMode === val ? "#92400e" : "#374151" }}>{label}</div>
                            <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 2 }}>{desc}</div>
                          </button>
                        ))}
                      </div>
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "#374151", marginBottom: 6 }}>
                        Watermark Column {bronzeErl.loadMode === "incremental" && <span style={{ color: "#ef4444" }}>*</span>}
                        <span style={{ fontWeight: 400, color: "#94a3b8", marginLeft: 6, fontSize: 11 }}>(for incremental extraction)</span>
                      </label>
                      <input type="text" className="input" value={bronzeErl.watermarkCol} onChange={e => setBronzeErl(v => ({ ...v, watermarkCol: e.target.value }))} placeholder="e.g. LAST_UPDATE_DATE, UPDATED_AT" style={{ fontFamily: "monospace", fontSize: 12 }} disabled={bronzeErl.loadMode === "full"} />
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "#374151", marginBottom: 6 }}>Parallelism <span style={{ fontWeight: 400, color: "#94a3b8" }}>(concurrent threads)</span></label>
                      <select className="input" value={bronzeErl.parallelism} onChange={e => setBronzeErl(v => ({ ...v, parallelism: Number(e.target.value) }))}>
                        {[1,2,4,8,16].map(n => <option key={n} value={n}>{n} threads</option>)}
                      </select>
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "#374151", marginBottom: 6 }}>Delta Compression</label>
                      <select className="input" value={bronzeErl.compression} onChange={e => setBronzeErl(v => ({ ...v, compression: e.target.value }))}>
                        <option value="snappy">Snappy (default, fast)</option>
                        <option value="gzip">GZip (higher ratio)</option>
                        <option value="none">None (uncompressed)</option>
                      </select>
                    </div>
                    <div style={{ gridColumn: "1 / -1" }}>
                      <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "#374151", marginBottom: 6 }}>Partition By Column <span style={{ fontWeight: 400, color: "#94a3b8" }}>(optional, e.g. ORG_ID, LEDGER_ID)</span></label>
                      <input type="text" className="input" value={bronzeErl.partitionBy} onChange={e => setBronzeErl(v => ({ ...v, partitionBy: e.target.value }))} placeholder="Leave blank to skip partitioning" style={{ fontFamily: "monospace", fontSize: 12 }} />
                    </div>
                  </div>
                )}
              </div>
              {/* ── Deploy Bronze to Fabric ───────────────────────────── */}
              {opts.bronze && (
                <div style={{ display: "flex", alignItems: "center", gap: 14, padding: "14px 18px", background: "#fef3c7", border: "2px solid #fbbf24", borderRadius: 12 }}>
                  <span style={{ fontSize: 22 }}>🥉</span>
                  <div style={{ flex: 1 }}>
                    <div style={{ fontSize: 13, fontWeight: 700, color: "#92400e" }}>Deploy Bronze Layer</div>
                    <div style={{ fontSize: 11, color: "#92400e", marginTop: 2 }}>
                      Extract {tables.length} table(s) from <strong>{wizard.sourceName}</strong> → Delta (bronze schema) — raw ingestion, no transformation
                    </div>
                  </div>
                  {layerResults.bronze && (
                    <span style={{ fontSize: 11, padding: "3px 12px", borderRadius: 20, fontWeight: 700,
                      background: layerResults.bronze.error ? "#fef2f2" : "#dcfce7",
                      color:      layerResults.bronze.error ? "#b91c1c" : "#15803d",
                      border:     `1px solid ${layerResults.bronze.error ? "#fecaca" : "#bbf7d0"}` }}>
                      {layerResults.bronze.error ? "✗ Failed" : `✓ ${layerResults.bronze.total ?? 0} artifact(s)`}
                    </span>
                  )}
                  <button
                    disabled={layerDeploying.bronze}
                    onClick={() => deployLayer("bronze")}
                    style={{ padding: "9px 20px", fontSize: 12, fontWeight: 700, borderRadius: 8, cursor: layerDeploying.bronze ? "not-allowed" : "pointer", border: "2px solid #d97706", background: "#d97706", color: "white", display: "flex", alignItems: "center", gap: 7, flexShrink: 0 }}>
                    {layerDeploying.bronze ? <><RefreshCw size={13} className="spin" /> Deploying…</> : <><Play size={13} /> Deploy Bronze to Fabric</>}
                  </button>
                </div>
              )}

              {tables.length > 0 && opts.bronze && (
                <div className="card" style={{ padding: 20 }}>
                  <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 12, display: "flex", alignItems: "center", gap: 8 }}>
                    <Code size={14} style={{ color: "#d97706" }} /> Bronze SQL Override
                    <span style={{ fontSize: 11, fontWeight: 400, color: "#64748b" }}>(optional — overrides default SELECT *)</span>
                  </h3>
                  <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                    {tables.map(t => (
                      <div key={t} style={{ border: "1px solid", borderColor: sqlConfig[t]?.bronze?.trim() ? "#fbbf24" : "#e2e8f0", borderRadius: 8, overflow: "hidden" }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 12px", cursor: "pointer", background: sqlConfig[t]?.bronze?.trim() ? "#fffbeb" : "white" }} onClick={() => setSqlExpandedTable(sqlExpandedTable === `b-${t}` ? null : `b-${t}`)}>
                          <Table2 size={12} style={{ color: "#d97706" }} />
                          <span style={{ fontSize: 12, fontWeight: 600, fontFamily: "monospace", flex: 1 }}>{t}</span>
                          {sqlConfig[t]?.bronze?.trim() && <span style={{ fontSize: 10, padding: "1px 6px", borderRadius: 10, background: "#fef9c3", color: "#92400e", fontWeight: 600 }}>SQL set</span>}
                          {sqlExpandedTable === `b-${t}` ? <ChevronUp size={12} style={{ color: "#94a3b8" }} /> : <ChevronDown size={12} style={{ color: "#94a3b8" }} />}
                        </div>
                        {sqlExpandedTable === `b-${t}` && (
                          <div style={{ borderTop: "1px solid #e2e8f0", padding: 12 }}>
                            <textarea placeholder={`SELECT * FROM ${t}  -- default JDBC query`} value={sqlConfig[t]?.bronze ?? ""} onChange={e => setSqlConfig(p => ({ ...p, [t]: { ...(p[t] ?? {}), bronze: e.target.value } }))} style={{ width: "100%", height: 80, fontFamily: "monospace", fontSize: 12, padding: 8, borderRadius: 6, border: "1px solid #cbd5e1", resize: "vertical", background: "#f8fafc" }} />
                            <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4 }}>
                              <span style={{ fontSize: 11, color: "#94a3b8" }}>Override JDBC extraction SQL for this table</span>
                              {sqlConfig[t]?.bronze?.trim() && <button onClick={() => setSqlConfig(p => ({ ...p, [t]: { ...p[t], bronze: "" } }))} style={{ fontSize: 11, color: "#b91c1c", background: "none", border: "none", cursor: "pointer" }}>Clear</button>}
                            </div>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </>
          )}

          {/* ══════════════════════════════════════════════════════════════════ */}
          {/* SUB-STEP 2 — Silver: Refine + Dimensional Modeling                */}
          {/* ══════════════════════════════════════════════════════════════════ */}
          {deploySubStep === 2 && (
            <>
              <div className="card" style={{ padding: 20, borderLeft: "4px solid #2563eb" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 14, marginBottom: 16 }}>
                  <h3 style={{ fontSize: 14, fontWeight: 700, margin: 0, flex: 1, display: "flex", alignItems: "center", gap: 8 }}>
                    <span style={{ fontSize: 18 }}>🥈</span>
                    Silver Notebook — ERL: <span style={{ color: "#2563eb" }}>Refine</span>
                    <span style={{ fontSize: 10, fontWeight: 400, color: "#94a3b8", marginLeft: 4 }}>Bronze → Conformed dim/fact (silver schema)</span>
                  </h3>
                  <label style={{ display: "flex", alignItems: "center", gap: 6, cursor: "pointer", fontSize: 13 }}>
                    <input type="checkbox" checked={opts.silver} onChange={e => setOpts(o => ({ ...o, silver: e.target.checked }))} />
                    Enable Silver
                  </label>
                  {opts.silver && (
                    <div style={{ display: "flex", gap: 4 }}>
                      {[{ val: "pyspark", lbl: "PySpark", ico: Terminal }, { val: "sql", lbl: "SparkSQL", ico: Code }].map(({ val, lbl, ico: KIcon }) => (
                        <button key={val} onClick={() => setNotebookTypes(p => ({ ...p, silver: val }))}
                          style={{ padding: "4px 10px", fontSize: 11, fontWeight: 600, borderRadius: 6, cursor: "pointer", border: "1px solid", display: "flex", alignItems: "center", gap: 3, background: notebookTypes.silver === val ? "#1e40af" : "white", color: notebookTypes.silver === val ? "white" : "#64748b", borderColor: notebookTypes.silver === val ? "#1e40af" : "#cbd5e1" }}>
                          <KIcon size={9} /> {lbl}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
                {opts.silver && (
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 14 }}>
                    {[
                      { key: "dedup",       label: "Remove Duplicates",  desc: "Deduplicate rows based on primary key"           },
                      { key: "castTypes",   label: "Auto Type Casting",  desc: "Cast string→date/number where possible"          },
                      { key: "surrogateKey",label: "Surrogate Key",      desc: "Add auto-increment integer surrogate key"        },
                    ].map(({ key, label, desc }) => (
                      <div key={key} style={{ padding: "12px 14px", borderRadius: 8, border: `2px solid ${silverErl[key] ? "#2563eb" : "#e2e8f0"}`, background: silverErl[key] ? "#eff6ff" : "white", cursor: "pointer" }} onClick={() => setSilverErl(v => ({ ...v, [key]: !v[key] }))}>
                        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                          <CheckCircle2 size={14} style={{ color: silverErl[key] ? "#2563eb" : "#cbd5e1" }} />
                          <span style={{ fontSize: 13, fontWeight: 600 }}>{label}</span>
                        </div>
                        <p style={{ fontSize: 11, color: "#64748b", margin: "4px 0 0" }}>{desc}</p>
                      </div>
                    ))}
                    <div>
                      <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "#374151", marginBottom: 6 }}>Null Handling</label>
                      <select className="input" value={silverErl.nullHandling} onChange={e => setSilverErl(v => ({ ...v, nullHandling: e.target.value }))}>
                        <option value="keep">Keep nulls as-is</option>
                        <option value="drop_rows">Drop rows with nulls in key columns</option>
                        <option value="fill_default">Fill nulls with defaults (0 / "Unknown")</option>
                      </select>
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "#374151", marginBottom: 6 }}>SCD Strategy</label>
                      <select className="input" value={silverErl.scdType} onChange={e => setSilverErl(v => ({ ...v, scdType: e.target.value }))}>
                        <option value="type1">Type 1 — Overwrite (no history)</option>
                        <option value="type2">Type 2 — Keep history (effective dates)</option>
                      </select>
                    </div>
                  </div>
                )}
              </div>

              {opts.silver && dimMappings.length > 0 && (
                <div className="card" style={{ padding: 20 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 14 }}>
                    <h3 style={{ fontSize: 14, fontWeight: 700, margin: 0, display: "flex", alignItems: "center", gap: 8, flex: 1 }}>
                      <Layers size={14} style={{ color: "#2563eb" }} />
                      Dimensional Modeling — Target Tables
                      <span style={{ fontSize: 11, fontWeight: 400, color: "#64748b" }}>Industry-standard dim/fact naming</span>
                    </h3>
                    {standardLoaded && <span style={{ fontSize: 10, padding: "2px 8px", borderRadius: 20, background: "#f0fdf4", color: "#15803d", border: "1px solid #bbf7d0", fontWeight: 600, display: "flex", alignItems: "center", gap: 4 }}><CheckCircle2 size={10} /> Standard SQL applied</span>}
                  </div>
                  {/* Header row */}
                  <div style={{ display: "grid", gridTemplateColumns: "26px 1fr 80px 80px 1fr", gap: 6, padding: "5px 10px", background: "#f8fafc", borderRadius: 6, marginBottom: 6, fontSize: 10, fontWeight: 600, color: "#64748b" }}>
                    <span>✓</span>
                    <span>Source Table(s) → Bronze</span>
                    <span>Dim/Fact</span>
                    <span>Cardinality</span>
                    <span>Target Table → Silver / Business Name</span>
                  </div>
                  <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                    {dimMappings.map((m, idx) => {
                      const rel = m.relationship ?? "1:1";
                      const relColor = rel === "N:1" ? "#d97706" : rel === "1:N" ? "#7c3aed" : "#64748b";
                      const relBg    = rel === "N:1" ? "#fef3c7" : rel === "1:N" ? "#f5f3ff" : "#f1f5f9";
                      return (
                        <div key={m.id} style={{ border: "1px solid", borderRadius: 8, overflow: "hidden", borderColor: m.enabled ? (m.type === "fact" ? "#bfdbfe" : "#bbf7d0") : "#e2e8f0", background: m.enabled ? (m.type === "fact" ? "#f0f9ff" : "#f0fdf4") : "#f8fafc", opacity: m.enabled ? 1 : 0.6 }}>
                          {/* Summary row */}
                          <div style={{ display: "grid", gridTemplateColumns: "26px 1fr 80px 80px 1fr", gap: 6, alignItems: "center", padding: "8px 10px", cursor: "pointer" }} onClick={() => setDimExpanded(dimExpanded === m.id ? null : m.id)}>
                            <input type="checkbox" checked={m.enabled} onClick={e => e.stopPropagation()} onChange={e => setDimMappings(dm => dm.map((d, i) => i === idx ? { ...d, enabled: e.target.checked } : d))} />
                            {/* Source column — show count badge if N:1 */}
                            <div style={{ display: "flex", alignItems: "center", gap: 4, overflow: "hidden" }}>
                              <span style={{ fontSize: 11, fontFamily: "monospace", fontWeight: 600, color: "#374151", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{m.sourceTable}</span>
                              {(m.additionalSources ?? []).length > 0 && (
                                <span style={{ flexShrink: 0, fontSize: 9, fontWeight: 700, padding: "1px 5px", borderRadius: 10, background: "#fef3c7", color: "#92400e", border: "1px solid #fde68a" }}>
                                  +{m.additionalSources.length}
                                </span>
                              )}
                            </div>
                            {/* Type selector */}
                            <div onClick={e => e.stopPropagation()}>
                              <select value={m.type}
                                onChange={e => setDimMappings(dm => dm.map((d, i) => i === idx ? { ...d, type: e.target.value, targetName: (e.target.value === "fact" ? "fact_" : "dim_") + m.targetName.replace(/^(fact_|dim_)/, "") } : d))}
                                style={{ fontSize: 10, padding: "2px 4px", borderRadius: 6, border: `1px solid ${m.type === "fact" ? "#93c5fd" : "#86efac"}`, background: m.type === "fact" ? "#dbeafe" : "#dcfce7", color: m.type === "fact" ? "#1d4ed8" : "#15803d", cursor: "pointer", fontWeight: 700, width: "100%" }}>
                                <option value="dim">dim</option><option value="fact">fact</option>
                              </select>
                            </div>
                            {/* Cardinality selector */}
                            <div onClick={e => e.stopPropagation()}>
                              <select value={rel}
                                onChange={e => setDimMappings(dm => dm.map((d, i) => i === idx ? { ...d, relationship: e.target.value } : d))}
                                title="Source-to-Target cardinality: 1:1 direct, N:1 merge multiple sources, 1:N split to multiple targets"
                                style={{ fontSize: 10, padding: "2px 4px", borderRadius: 6, border: `1px solid ${relColor}`, background: relBg, color: relColor, cursor: "pointer", fontWeight: 700, width: "100%" }}>
                                <option value="1:1">1 : 1</option>
                                <option value="N:1">N : 1</option>
                                <option value="1:N">1 : N</option>
                              </select>
                            </div>
                            {/* Target */}
                            <div style={{ display: "flex", flexDirection: "column", gap: 1, overflow: "hidden" }}>
                              <span style={{ fontSize: 11, fontFamily: "monospace", color: m.type === "fact" ? "#1d4ed8" : "#15803d", fontWeight: 600, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{m.targetName}</span>
                              <span style={{ fontSize: 10, color: "#64748b", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{m.businessName}</span>
                            </div>
                          </div>

                          {/* Expanded detail panel */}
                          {dimExpanded === m.id && (
                            <div style={{ borderTop: "1px solid #e2e8f0", padding: "10px 12px", display: "flex", flexDirection: "column", gap: 10 }}>
                              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
                                <div>
                                  <label style={{ display: "block", fontSize: 11, fontWeight: 600, color: "#374151", marginBottom: 4 }}>Target Table Name</label>
                                  <input type="text" className="input" value={m.targetName} onChange={e => setDimMappings(dm => dm.map((d, i) => i === idx ? { ...d, targetName: e.target.value } : d))} style={{ fontFamily: "monospace", fontSize: 11 }} />
                                </div>
                                <div>
                                  <label style={{ display: "block", fontSize: 11, fontWeight: 600, color: "#374151", marginBottom: 4 }}>Business-Friendly Name</label>
                                  <input type="text" className="input" value={m.businessName} onChange={e => setDimMappings(dm => dm.map((d, i) => i === idx ? { ...d, businessName: e.target.value } : d))} />
                                </div>
                                <div style={{ gridColumn: "1 / -1" }}>
                                  <label style={{ display: "block", fontSize: 11, fontWeight: 600, color: "#374151", marginBottom: 4 }}>Description / Transform Notes</label>
                                  <input type="text" className="input" value={m.description ?? ""} onChange={e => setDimMappings(dm => dm.map((d, i) => i === idx ? { ...d, description: e.target.value } : d))} placeholder="e.g. Join condition, filter logic, business rule" />
                                </div>
                              </div>

                              {/* N:1 — additional source tables panel */}
                              {rel === "N:1" && (
                                <div style={{ padding: "10px 12px", background: "#fffbeb", borderRadius: 8, border: "1px dashed #fbbf24" }}>
                                  <div style={{ fontSize: 11, fontWeight: 700, color: "#92400e", marginBottom: 8, display: "flex", alignItems: "center", gap: 6 }}>
                                    🔀 Additional Source Tables (JOINed / MERGEd into <code style={{ fontFamily: "monospace", background: "#fef3c7", padding: "1px 5px", borderRadius: 4 }}>{m.targetName}</code>)
                                  </div>
                                  <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginBottom: 8 }}>
                                    {(m.additionalSources ?? []).map((src, si) => (
                                      <span key={si} style={{ display: "flex", alignItems: "center", gap: 4, padding: "3px 8px", background: "#fef3c7", border: "1px solid #fbbf24", borderRadius: 6, fontSize: 11, fontFamily: "monospace", fontWeight: 600 }}>
                                        {src}
                                        <button onClick={() => setDimMappings(dm => dm.map((d, i) => i === idx ? { ...d, additionalSources: (d.additionalSources ?? []).filter((_, j) => j !== si) } : d))}
                                          style={{ background: "none", border: "none", cursor: "pointer", color: "#d97706", fontSize: 14, padding: 0, lineHeight: 1 }}>×</button>
                                      </span>
                                    ))}
                                    <input placeholder="+ JOIN table (Enter to add)" onKeyDown={e => { if (e.key === "Enter" && e.target.value.trim()) { const v = e.target.value.trim(); setDimMappings(dm => dm.map((d, i) => i === idx ? { ...d, additionalSources: [...(d.additionalSources ?? []), v] } : d)); e.target.value = ""; } }}
                                      style={{ fontSize: 11, fontFamily: "monospace", padding: "3px 8px", border: "1px dashed #fbbf24", borderRadius: 6, background: "white", outline: "none", minWidth: 200 }} />
                                  </div>
                                  <p style={{ fontSize: 10, color: "#92400e", margin: 0 }}>Press Enter to add. Use standard ERP join keys (e.g. HEADER_ID, ORG_ID). Join logic auto-generated in the notebook.</p>
                                </div>
                              )}

                              {/* 1:N — additional target tables panel */}
                              {rel === "1:N" && (
                                <div style={{ padding: "10px 12px", background: "#f5f3ff", borderRadius: 8, border: "1px dashed #c4b5fd" }}>
                                  <div style={{ fontSize: 11, fontWeight: 700, color: "#6d28d9", marginBottom: 8, display: "flex", alignItems: "center", gap: 6 }}>
                                    🔀 Additional Target Tables (derived from <code style={{ fontFamily: "monospace", background: "#ede9fe", padding: "1px 5px", borderRadius: 4 }}>{m.sourceTable}</code>)
                                  </div>
                                  <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginBottom: 8 }}>
                                    {(m.additionalTargets ?? []).map((tgt, ti) => {
                                      const tgtName = typeof tgt === "string" ? tgt : tgt.targetName;
                                      return (
                                        <span key={ti} style={{ display: "flex", alignItems: "center", gap: 4, padding: "3px 8px", background: "#ede9fe", border: "1px solid #c4b5fd", borderRadius: 6, fontSize: 11, fontFamily: "monospace", fontWeight: 600, color: "#6d28d9" }}>
                                          {tgtName}
                                          <button onClick={() => setDimMappings(dm => dm.map((d, i) => i === idx ? { ...d, additionalTargets: (d.additionalTargets ?? []).filter((_, j) => j !== ti) } : d))}
                                            style={{ background: "none", border: "none", cursor: "pointer", color: "#7c3aed", fontSize: 14, padding: 0, lineHeight: 1 }}>×</button>
                                        </span>
                                      );
                                    })}
                                    <input placeholder="+ Additional target table (Enter to add)" onKeyDown={e => { if (e.key === "Enter" && e.target.value.trim()) { const v = e.target.value.trim(); setDimMappings(dm => dm.map((d, i) => i === idx ? { ...d, additionalTargets: [...(d.additionalTargets ?? []), v] } : d)); e.target.value = ""; } }}
                                      style={{ fontSize: 11, fontFamily: "monospace", padding: "3px 8px", border: "1px dashed #c4b5fd", borderRadius: 6, background: "white", outline: "none", minWidth: 230 }} />
                                  </div>
                                  <p style={{ fontSize: 10, color: "#6d28d9", margin: 0 }}>Press Enter to add. Each target gets a filtered/transformed view of the source with a WHERE clause in the notebook.</p>
                                </div>
                              )}
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                  <div style={{ marginTop: 10 }}>
                    <button className="btn-ghost" style={{ fontSize: 12 }} onClick={() => setDimMappings(dm => [...dm, { id: `custom-${Date.now()}`, sourceTable: "", targetName: "dim_new_table", type: "dim", businessName: "New Table", description: "", enabled: true }])}>
                      <Plus size={12} /> Add Mapping
                    </button>
                  </div>
                </div>
              )}

              {/* ── Deploy Silver to Fabric ───────────────────────────── */}
              {opts.silver && (
                <div style={{ display: "flex", alignItems: "center", gap: 14, padding: "14px 18px", background: "#eff6ff", border: "2px solid #93c5fd", borderRadius: 12 }}>
                  <span style={{ fontSize: 22 }}>🥈</span>
                  <div style={{ flex: 1 }}>
                    <div style={{ fontSize: 13, fontWeight: 700, color: "#1d4ed8" }}>Deploy Silver Layer</div>
                    <div style={{ fontSize: 11, color: "#1d4ed8", marginTop: 2 }}>
                      Transform Bronze → Silver: {dimMappings.filter(m => m.enabled && m.type === "dim").length} dim + {dimMappings.filter(m => m.enabled && m.type === "fact").length} fact tables — conformed star schema
                    </div>
                  </div>
                  {layerResults.silver && (
                    <span style={{ fontSize: 11, padding: "3px 12px", borderRadius: 20, fontWeight: 700,
                      background: layerResults.silver.error ? "#fef2f2" : "#dcfce7",
                      color:      layerResults.silver.error ? "#b91c1c" : "#15803d",
                      border:     `1px solid ${layerResults.silver.error ? "#fecaca" : "#bbf7d0"}` }}>
                      {layerResults.silver.error ? "✗ Failed" : `✓ ${layerResults.silver.total ?? 0} artifact(s)`}
                    </span>
                  )}
                  <button
                    disabled={layerDeploying.silver}
                    onClick={() => deployLayer("silver")}
                    style={{ padding: "9px 20px", fontSize: 12, fontWeight: 700, borderRadius: 8, cursor: layerDeploying.silver ? "not-allowed" : "pointer", border: "2px solid #2563eb", background: "#2563eb", color: "white", display: "flex", alignItems: "center", gap: 7, flexShrink: 0 }}>
                    {layerDeploying.silver ? <><RefreshCw size={13} className="spin" /> Deploying…</> : <><Play size={13} /> Deploy Silver to Fabric</>}
                  </button>
                </div>
              )}

              {tables.length > 0 && opts.silver && (
                <div className="card" style={{ padding: 20 }}>
                  <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 12, display: "flex", alignItems: "center", gap: 8 }}>
                    <Code size={14} style={{ color: "#2563eb" }} /> Silver SQL Transform
                    <span style={{ fontSize: 11, fontWeight: 400, color: "#64748b" }}>(optional — applied on top of default transform)</span>
                  </h3>
                  <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                    {tables.map(t => (
                      <div key={t} style={{ border: "1px solid", borderColor: sqlConfig[t]?.silver?.trim() ? "#93c5fd" : "#e2e8f0", borderRadius: 8, overflow: "hidden" }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 12px", cursor: "pointer", background: sqlConfig[t]?.silver?.trim() ? "#f0f9ff" : "white" }} onClick={() => setSqlExpandedTable(sqlExpandedTable === `s-${t}` ? null : `s-${t}`)}>
                          <Table2 size={12} style={{ color: "#2563eb" }} />
                          <span style={{ fontSize: 12, fontWeight: 600, fontFamily: "monospace", flex: 1 }}>{t}</span>
                          {sqlConfig[t]?.silver?.trim() && <span style={{ fontSize: 10, padding: "1px 6px", borderRadius: 10, background: "#dbeafe", color: "#1d4ed8", fontWeight: 600 }}>SQL set</span>}
                          {standardSql.silver?.[t] && <span style={{ fontSize: 10, padding: "1px 6px", borderRadius: 10, background: "#f0fdf4", color: "#15803d", fontWeight: 600 }}>Standard ✓</span>}
                          {sqlExpandedTable === `s-${t}` ? <ChevronUp size={12} style={{ color: "#94a3b8" }} /> : <ChevronDown size={12} style={{ color: "#94a3b8" }} />}
                        </div>
                        {sqlExpandedTable === `s-${t}` && (
                          <div style={{ borderTop: "1px solid #e2e8f0", padding: 12 }}>
                            <textarea placeholder={`SELECT DISTINCT *\nFROM bronze.${t.toLowerCase()}\nWHERE 1=1  -- add cleansing filters`} value={sqlConfig[t]?.silver ?? ""} onChange={e => setSqlConfig(p => ({ ...p, [t]: { ...(p[t] ?? {}), silver: e.target.value } }))} style={{ width: "100%", height: 90, fontFamily: "monospace", fontSize: 12, padding: 8, borderRadius: 6, border: "1px solid #cbd5e1", resize: "vertical", background: "#f8fafc" }} />
                            <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4, alignItems: "center" }}>
                              <span style={{ fontSize: 11, color: "#94a3b8" }}>Transform SQL reading from bronze layer</span>
                              <div style={{ display: "flex", gap: 8 }}>
                                {standardSql.silver?.[t] && sqlConfig[t]?.silver !== standardSql.silver[t] && <button onClick={() => setSqlConfig(p => ({ ...p, [t]: { ...p[t], silver: standardSql.silver[t] } }))} style={{ fontSize: 11, color: "#6366f1", background: "none", border: "none", cursor: "pointer" }}>↺ Reset to standard</button>}
                                {sqlConfig[t]?.silver?.trim() && <button onClick={() => setSqlConfig(p => ({ ...p, [t]: { ...p[t], silver: "" } }))} style={{ fontSize: 11, color: "#b91c1c", background: "none", border: "none", cursor: "pointer" }}>Clear</button>}
                              </div>
                            </div>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </>
          )}

          {/* ══════════════════════════════════════════════════════════════════ */}
          {/* SUB-STEP 3 — Gold: Load / Aggregate / KPI Definitions             */}
          {/* ══════════════════════════════════════════════════════════════════ */}
          {deploySubStep === 3 && (
            <>
              <div className="card" style={{ padding: 20, borderLeft: "4px solid #b45309" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 14, marginBottom: 16 }}>
                  <h3 style={{ fontSize: 14, fontWeight: 700, margin: 0, flex: 1, display: "flex", alignItems: "center", gap: 8 }}>
                    <span style={{ fontSize: 18 }}>🥇</span>
                    Gold Notebook — ERL: <span style={{ color: "#b45309" }}>Load &amp; Aggregate</span>
                    <span style={{ fontSize: 10, fontWeight: 400, color: "#94a3b8", marginLeft: 4 }}>Silver → Business KPIs (gold schema)</span>
                  </h3>
                  <label style={{ display: "flex", alignItems: "center", gap: 6, cursor: "pointer", fontSize: 13 }}>
                    <input type="checkbox" checked={opts.gold} onChange={e => setOpts(o => ({ ...o, gold: e.target.checked }))} />
                    Enable Gold
                  </label>
                  {opts.gold && (
                    <div style={{ display: "flex", gap: 4 }}>
                      {[{ val: "pyspark", lbl: "PySpark", ico: Terminal }, { val: "sql", lbl: "SparkSQL", ico: Code }].map(({ val, lbl, ico: KIcon }) => (
                        <button key={val} onClick={() => setNotebookTypes(p => ({ ...p, gold: val }))}
                          style={{ padding: "4px 10px", fontSize: 11, fontWeight: 600, borderRadius: 6, cursor: "pointer", border: "1px solid", display: "flex", alignItems: "center", gap: 3, background: notebookTypes.gold === val ? "#b45309" : "white", color: notebookTypes.gold === val ? "white" : "#64748b", borderColor: notebookTypes.gold === val ? "#b45309" : "#cbd5e1" }}>
                          <KIcon size={9} /> {lbl}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
                {opts.gold && (
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14 }}>
                    <div>
                      <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "#374151", marginBottom: 8 }}>Aggregation Granularity</label>
                      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                        {["daily", "weekly", "monthly", "quarterly"].map(g => (
                          <button key={g} onClick={() => setGoldErl(v => ({ ...v, granularity: g }))}
                            style={{ padding: "6px 14px", fontSize: 12, fontWeight: 600, borderRadius: 6, cursor: "pointer", border: "1px solid", background: goldErl.granularity === g ? "#fef9c3" : "white", color: goldErl.granularity === g ? "#854d0e" : "#64748b", borderColor: goldErl.granularity === g ? "#b45309" : "#e2e8f0" }}>
                            {g.charAt(0).toUpperCase() + g.slice(1)}
                          </button>
                        ))}
                      </div>
                    </div>
                    <div style={{ display: "flex", alignItems: "center" }}>
                      <div style={{ padding: "12px 14px", borderRadius: 8, border: `2px solid ${goldErl.addCalendarDim ? "#b45309" : "#e2e8f0"}`, background: goldErl.addCalendarDim ? "#fef9c3" : "white", cursor: "pointer", flex: 1 }} onClick={() => setGoldErl(v => ({ ...v, addCalendarDim: !v.addCalendarDim }))}>
                        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                          <CheckCircle2 size={14} style={{ color: goldErl.addCalendarDim ? "#b45309" : "#cbd5e1" }} />
                          <span style={{ fontSize: 13, fontWeight: 600 }}>Add Calendar Dimension</span>
                        </div>
                        <p style={{ fontSize: 11, color: "#64748b", margin: "4px 0 0" }}>Generate dim_date with year, quarter, month, week, day attributes</p>
                      </div>
                    </div>
                  </div>
                )}
              </div>

              {opts.gold && goldKpis.length > 0 && (
                <div className="card" style={{ padding: 20 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 14 }}>
                    <h3 style={{ fontSize: 14, fontWeight: 700, margin: 0, display: "flex", alignItems: "center", gap: 8, flex: 1 }}>
                      <Zap size={14} style={{ color: "#b45309" }} />
                      Gold KPI / Aggregate Tables
                      <span style={{ fontSize: 11, fontWeight: 400, color: "#64748b" }}>Industry-standard business metrics</span>
                    </h3>
                    <span style={{ fontSize: 10, padding: "2px 8px", borderRadius: 20, background: "#fef9c3", color: "#854d0e", border: "1px solid #fde68a", fontWeight: 600 }}>
                      {goldKpis.filter(k => k.enabled).length} KPIs enabled
                    </span>
                  </div>
                  <div style={{ display: "grid", gridTemplateColumns: "auto 1fr 100px 1fr", gap: 8, padding: "6px 10px", background: "#f8fafc", borderRadius: 6, marginBottom: 6, fontSize: 10, fontWeight: 600, color: "#64748b" }}>
                    <span>En</span><span>Target Table Name (Gold)</span><span>Granularity</span><span>Business Name</span>
                  </div>
                  <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                    {goldKpis.map((k, idx) => (
                      <div key={k.id} style={{ border: "1px solid", borderRadius: 8, overflow: "hidden", borderColor: k.enabled ? "#fbbf24" : "#e2e8f0", background: k.enabled ? "#fffbeb" : "#f8fafc", opacity: k.enabled ? 1 : 0.6 }}>
                        <div style={{ display: "grid", gridTemplateColumns: "auto 1fr 100px 1fr", gap: 8, alignItems: "center", padding: "9px 10px", cursor: "pointer" }} onClick={() => setKpiExpanded(kpiExpanded === k.id ? null : k.id)}>
                          <input type="checkbox" checked={k.enabled} onClick={e => e.stopPropagation()} onChange={e => setGoldKpis(kk => kk.map((ki, i) => i === idx ? { ...ki, enabled: e.target.checked } : ki))} />
                          <span style={{ fontSize: 12, fontFamily: "monospace", fontWeight: 600, color: "#854d0e", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{k.targetTable}</span>
                          <span style={{ fontSize: 11, padding: "2px 8px", borderRadius: 10, background: "#fef9c3", color: "#854d0e", fontWeight: 600, border: "1px solid #fde68a", textAlign: "center" }}>{k.granularity ?? goldErl.granularity}</span>
                          <span style={{ fontSize: 12, color: "#374151", fontWeight: 600 }}>{k.businessName}</span>
                        </div>
                        {kpiExpanded === k.id && (
                          <div style={{ borderTop: "1px solid #e2e8f0", padding: "10px 12px", display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
                            <div>
                              <label style={{ display: "block", fontSize: 11, fontWeight: 600, color: "#374151", marginBottom: 4 }}>Target Table Name</label>
                              <input type="text" className="input" value={k.targetTable} onChange={e => setGoldKpis(kk => kk.map((ki, i) => i === idx ? { ...ki, targetTable: e.target.value } : ki))} style={{ fontFamily: "monospace", fontSize: 12 }} />
                            </div>
                            <div>
                              <label style={{ display: "block", fontSize: 11, fontWeight: 600, color: "#374151", marginBottom: 4 }}>Business Name</label>
                              <input type="text" className="input" value={k.businessName} onChange={e => setGoldKpis(kk => kk.map((ki, i) => i === idx ? { ...ki, businessName: e.target.value } : ki))} />
                            </div>
                            <div style={{ gridColumn: "1 / -1" }}>
                              <label style={{ display: "block", fontSize: 11, fontWeight: 600, color: "#374151", marginBottom: 4 }}>Description / Aggregation Logic</label>
                              <input type="text" className="input" value={k.description ?? ""} onChange={e => setGoldKpis(kk => kk.map((ki, i) => i === idx ? { ...ki, description: e.target.value } : ki))} placeholder="e.g. SUM revenue by customer, product, month" />
                            </div>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                  <div style={{ marginTop: 10 }}>
                    <button className="btn-ghost" style={{ fontSize: 12 }} onClick={() => setGoldKpis(kk => [...kk, { id: `custom-kpi-${Date.now()}`, targetTable: "fact_custom_kpi", businessName: "Custom KPI", granularity: goldErl.granularity, description: "", enabled: true }])}>
                      <Plus size={12} /> Add KPI
                    </button>
                  </div>
                </div>
              )}

              {/* ── Deploy Gold to Fabric ─────────────────────────────── */}
              {opts.gold && (
                <div style={{ display: "flex", alignItems: "center", gap: 14, padding: "14px 18px", background: "#fef9c3", border: "2px solid #fde68a", borderRadius: 12 }}>
                  <span style={{ fontSize: 22 }}>🥇</span>
                  <div style={{ flex: 1 }}>
                    <div style={{ fontSize: 13, fontWeight: 700, color: "#854d0e" }}>Deploy Gold Layer</div>
                    <div style={{ fontSize: 11, color: "#854d0e", marginTop: 2 }}>
                      Aggregate Silver → Gold: {goldKpis.filter(k => k.enabled).length} KPI table(s) · {goldErl.granularity} granularity{goldErl.addCalendarDim ? " · + dim_date" : ""}
                    </div>
                  </div>
                  {layerResults.gold && (
                    <span style={{ fontSize: 11, padding: "3px 12px", borderRadius: 20, fontWeight: 700,
                      background: layerResults.gold.error ? "#fef2f2" : "#dcfce7",
                      color:      layerResults.gold.error ? "#b91c1c" : "#15803d",
                      border:     `1px solid ${layerResults.gold.error ? "#fecaca" : "#bbf7d0"}` }}>
                      {layerResults.gold.error ? "✗ Failed" : `✓ ${layerResults.gold.total ?? 0} artifact(s)`}
                    </span>
                  )}
                  <button
                    disabled={layerDeploying.gold}
                    onClick={() => deployLayer("gold")}
                    style={{ padding: "9px 20px", fontSize: 12, fontWeight: 700, borderRadius: 8, cursor: layerDeploying.gold ? "not-allowed" : "pointer", border: "2px solid #b45309", background: "#b45309", color: "white", display: "flex", alignItems: "center", gap: 7, flexShrink: 0 }}>
                    {layerDeploying.gold ? <><RefreshCw size={13} className="spin" /> Deploying…</> : <><Play size={13} /> Deploy Gold to Fabric</>}
                  </button>
                </div>
              )}

              {tables.length > 0 && opts.gold && (
                <div className="card" style={{ padding: 20 }}>
                  <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 12, display: "flex", alignItems: "center", gap: 8 }}>
                    <Code size={14} style={{ color: "#b45309" }} /> Gold SQL Aggregation
                    <span style={{ fontSize: 11, fontWeight: 400, color: "#64748b" }}>(optional — custom aggregate query per table)</span>
                  </h3>
                  <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                    {tables.map(t => (
                      <div key={t} style={{ border: "1px solid", borderColor: sqlConfig[t]?.gold?.trim() ? "#fbbf24" : "#e2e8f0", borderRadius: 8, overflow: "hidden" }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 12px", cursor: "pointer", background: sqlConfig[t]?.gold?.trim() ? "#fffbeb" : "white" }} onClick={() => setSqlExpandedTable(sqlExpandedTable === `g-${t}` ? null : `g-${t}`)}>
                          <Table2 size={12} style={{ color: "#b45309" }} />
                          <span style={{ fontSize: 12, fontWeight: 600, fontFamily: "monospace", flex: 1 }}>{t}</span>
                          {sqlConfig[t]?.gold?.trim() && <span style={{ fontSize: 10, padding: "1px 6px", borderRadius: 10, background: "#fef9c3", color: "#854d0e", fontWeight: 600 }}>SQL set</span>}
                          {standardSql.gold?.[t] && <span style={{ fontSize: 10, padding: "1px 6px", borderRadius: 10, background: "#f0fdf4", color: "#15803d", fontWeight: 600 }}>Standard ✓</span>}
                          {sqlExpandedTable === `g-${t}` ? <ChevronUp size={12} style={{ color: "#94a3b8" }} /> : <ChevronDown size={12} style={{ color: "#94a3b8" }} />}
                        </div>
                        {sqlExpandedTable === `g-${t}` && (
                          <div style={{ borderTop: "1px solid #e2e8f0", padding: 12 }}>
                            <textarea placeholder={`SELECT date_trunc('month', created_date) AS month,\n       COUNT(*) AS total_count,\n       SUM(amount) AS total_amount\nFROM silver.${t.toLowerCase()}\nGROUP BY 1\nORDER BY 1`} value={sqlConfig[t]?.gold ?? ""} onChange={e => setSqlConfig(p => ({ ...p, [t]: { ...(p[t] ?? {}), gold: e.target.value } }))} style={{ width: "100%", height: 100, fontFamily: "monospace", fontSize: 12, padding: 8, borderRadius: 6, border: "1px solid #cbd5e1", resize: "vertical", background: "#f8fafc" }} />
                            <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4, alignItems: "center" }}>
                              <span style={{ fontSize: 11, color: "#94a3b8" }}>Aggregation SQL reading from silver layer</span>
                              <div style={{ display: "flex", gap: 8 }}>
                                {standardSql.gold?.[t] && sqlConfig[t]?.gold !== standardSql.gold[t] && <button onClick={() => setSqlConfig(p => ({ ...p, [t]: { ...p[t], gold: standardSql.gold[t] } }))} style={{ fontSize: 11, color: "#6366f1", background: "none", border: "none", cursor: "pointer" }}>↺ Reset to standard</button>}
                                {sqlConfig[t]?.gold?.trim() && <button onClick={() => setSqlConfig(p => ({ ...p, [t]: { ...p[t], gold: "" } }))} style={{ fontSize: 11, color: "#b91c1c", background: "none", border: "none", cursor: "pointer" }}>Clear</button>}
                              </div>
                            </div>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </>
          )}

          {/* ══════════════════════════════════════════════════════════════════ */}
          {/* SUB-STEP 4 — Deploy: Review, Code Preview & Publish               */}
          {/* ══════════════════════════════════════════════════════════════════ */}
          {deploySubStep === 4 && (
            <>
              <div className="card" style={{ padding: 20 }}>
                <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 14, display: "flex", alignItems: "center", gap: 8 }}>
                  <ListChecks size={14} style={{ color: "var(--color-primary)" }} /> Deployment Summary
                </h3>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 12 }}>
                  {[
                    { layer: "bronze", emoji: "🥉", color: "#92400e", border: "#fbbf24", bg: "#fffbeb", enabled: opts.bronze,
                      details: [
                        `Mode: ${bronzeErl.loadMode === "full" ? "Full Refresh" : "Incremental"}`,
                        bronzeErl.loadMode === "incremental" && bronzeErl.watermarkCol ? `Watermark: ${bronzeErl.watermarkCol}` : null,
                        `Kernel: ${notebookTypes.bronze === "sql" ? "SparkSQL" : "PySpark"}`,
                        `Tables: ${tables.length}`,
                      ].filter(Boolean)
                    },
                    { layer: "silver", emoji: "🥈", color: "#1d4ed8", border: "#93c5fd", bg: "#eff6ff", enabled: opts.silver,
                      details: [
                        `Dedup: ${silverErl.dedup ? "Yes" : "No"}`,
                        `SCD: Type ${silverErl.scdType === "type1" ? "1" : "2"}`,
                        `Dim tables: ${dimMappings.filter(m => m.enabled && m.type === "dim").length}`,
                        `Fact tables: ${dimMappings.filter(m => m.enabled && m.type === "fact").length}`,
                      ]
                    },
                    { layer: "gold", emoji: "🥇", color: "#854d0e", border: "#fde68a", bg: "#fef9c3", enabled: opts.gold,
                      details: [
                        `Granularity: ${goldErl.granularity}`,
                        `KPI tables: ${goldKpis.filter(k => k.enabled).length}`,
                        `Calendar dim: ${goldErl.addCalendarDim ? "Yes" : "No"}`,
                      ]
                    },
                  ].map(({ layer, emoji, color, border, bg, enabled, details }) => (
                    <div key={layer} style={{ padding: "12px 14px", borderRadius: 8, border: `1px solid ${enabled ? border : "#e2e8f0"}`, background: enabled ? bg : "#f8fafc", opacity: enabled ? 1 : 0.5 }}>
                      <div style={{ fontSize: 13, fontWeight: 700, color, display: "flex", alignItems: "center", gap: 6, marginBottom: 6 }}>
                        <span>{emoji}</span> {layer.charAt(0).toUpperCase() + layer.slice(1)} Layer
                        {enabled ? <CheckCircle2 size={12} style={{ color: "#16a34a", marginLeft: "auto" }} /> : <XCircle size={12} style={{ color: "#94a3b8", marginLeft: "auto" }} />}
                      </div>
                      <div style={{ fontSize: 11, color: "#64748b", display: "flex", flexDirection: "column", gap: 3 }}>
                        {details.map((d, i) => <span key={i}>{d}</span>)}
                      </div>
                    </div>
                  ))}
                </div>
                <div style={{ marginTop: 14, display: "flex", alignItems: "center", gap: 12, padding: "12px 14px", borderRadius: 8, border: `1px solid ${opts.pipeline ? "#c4b5fd" : "#e2e8f0"}`, background: opts.pipeline ? "#f5f3ff" : "#f8fafc" }}>
                  <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer", flex: 1 }}>
                    <input type="checkbox" checked={opts.pipeline} onChange={e => setOpts(o => ({ ...o, pipeline: e.target.checked }))} />
                    <GitBranch size={14} style={{ color: "#7c3aed" }} />
                    <div>
                      <span style={{ fontSize: 13, fontWeight: 600 }}>Create Data Pipeline</span>
                      <span style={{ fontSize: 11, color: "#64748b", marginLeft: 8 }}>Orchestrates Bronze → Silver → Gold in sequence</span>
                    </div>
                  </label>
                </div>
              </div>

              <div className="card" style={{ padding: 20 }}>
                <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: showPreview ? 16 : 0 }}>
                  <h3 style={{ fontSize: 14, fontWeight: 700, margin: 0, display: "flex", alignItems: "center", gap: 8, flex: 1 }}>
                    <FileCode size={15} style={{ color: "#0891b2" }} /> Preview &amp; Customize Notebook Code
                    <span style={{ fontSize: 11, fontWeight: 400, color: "#64748b" }}>(auto-populated · editable before deploy)</span>
                  </h3>
                  <button onClick={fetchPreview} disabled={previewLoading} style={{ padding: "7px 16px", fontSize: 12, fontWeight: 600, borderRadius: 8, cursor: "pointer", border: "1px solid #0891b2", background: previewLoading ? "#f0f9ff" : "#0891b2", color: previewLoading ? "#0891b2" : "white", display: "flex", alignItems: "center", gap: 6 }}>
                    {previewLoading ? <><RefreshCw size={12} className="spin" /> Generating…</> : <><Eye size={12} /> {showPreview ? "Regenerate" : "Generate Preview"}</>}
                  </button>
                  {showPreview && <button onClick={() => setShowPreview(false)} style={{ background: "none", border: "none", cursor: "pointer", fontSize: 12, color: "#64748b", padding: "4px 8px" }}>Hide</button>}
                </div>
                {showPreview && (
                  <>
                    <div style={{ display: "flex", gap: 6, marginBottom: 12 }}>
                      {["bronze", "silver", "gold", "pipeline"].filter(k => codePreview[k]).map(k => (
                        <button key={k} onClick={() => setActiveCodeTab(k)} style={{ padding: "5px 14px", fontSize: 11, fontWeight: 700, borderRadius: 7, cursor: "pointer", border: "1px solid", display: "flex", alignItems: "center", gap: 5, background: activeCodeTab === k ? layerBg[k] : "white", color: activeCodeTab === k ? layerColor[k] : "#64748b", borderColor: activeCodeTab === k ? layerColor[k] : "#e2e8f0" }}>
                          {k.charAt(0).toUpperCase() + k.slice(1)}
                          {customCode[k]?.trim() && <span style={{ width: 6, height: 6, borderRadius: "50%", background: layerColor[k], display: "inline-block" }} />}
                          {k !== "pipeline" && <span style={{ fontSize: 9, opacity: 0.8 }}>{notebookTypes[k] === "sql" ? "SparkSQL" : "PySpark"}</span>}
                        </button>
                      ))}
                    </div>
                    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
                      {isCustomised && <span style={{ fontSize: 10, padding: "2px 8px", borderRadius: 20, background: "#fef3c7", color: "#92400e", border: "1px solid #fde68a", fontWeight: 600 }}>✏ Modified</span>}
                      <span style={{ flex: 1 }} />
                      <button onClick={() => navigator.clipboard.writeText(activeCode).then(() => toast.success("Copied!"))} style={{ fontSize: 11, padding: "3px 10px", borderRadius: 6, cursor: "pointer", border: "1px solid #e2e8f0", background: "white", color: "#64748b", display: "flex", alignItems: "center", gap: 4 }}><Copy size={11} /> Copy</button>
                      {isCustomised && <button onClick={() => setCustomCode(p => { const n = { ...p }; delete n[activeCodeTab]; return n; })} style={{ fontSize: 11, padding: "3px 10px", borderRadius: 6, cursor: "pointer", border: "1px solid #fecaca", background: "#fef2f2", color: "#b91c1c" }}>Reset to auto-generated</button>}
                    </div>
                    <textarea value={activeCode} onChange={e => setCustomCode(prev => ({ ...prev, [activeCodeTab]: e.target.value }))} spellCheck={false}
                      style={{ width: "100%", height: 360, fontFamily: "'Fira Code', 'Courier New', monospace", fontSize: 12, padding: 14, borderRadius: 8, resize: "vertical", border: "1px solid", lineHeight: 1.6, borderColor: isCustomised ? "#fde68a" : "#cbd5e1", background: isCustomised ? "#fffbeb" : "#0f172a", color: isCustomised ? "#1e293b" : "#e2e8f0", outline: "none" }} />
                    <p style={{ fontSize: 11, color: "#94a3b8", margin: "6px 0 0" }}>
                      Edit code before deploying. Separator between cells: <code style={{ fontSize: 10 }}># ─── Next Cell ───</code>
                    </p>
                  </>
                )}
              </div>
            </>
          )}

          {/* ── Sub-step navigation footer ── */}
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", paddingTop: 4 }}>
            {deploySubStep === 1
              ? <button className="btn-ghost" onClick={onBack}><ChevronLeft size={15} /> Back</button>
              : <button className="btn-ghost" onClick={() => setDeploySubStep(s => s - 1)}><ChevronLeft size={15} /> Back</button>
            }
            {deploySubStep < 4 ? (
              <button className="btn-primary" onClick={() => setDeploySubStep(s => s + 1)}>
                {deploySubStep === 1 ? "Configure Silver" : deploySubStep === 2 ? "Configure Gold" : "Review & Deploy"} <ChevronRight size={15} />
              </button>
            ) : (
              <button
                className="btn-primary"
                disabled={deployMutation.isPending || !Object.values(opts).some(Boolean)}
                onClick={() => deployMutation.mutate()}
                style={{ fontSize: 14, padding: "10px 28px" }}
              >
                {deployMutation.isPending
                  ? <><RefreshCw size={14} className="spin" /> Deploying to Fabric…</>
                  : <><Play size={14} /> Deploy to Microsoft Fabric</>}
              </button>
            )}
          </div>
        </>
      )}

      {/* ── Deployment results ── */}
      {result && (
        <div style={{ border: "1px solid var(--color-border)", borderRadius: 12, overflow: "hidden" }}>
          {/* Result header */}
          <div style={{
            padding: "14px 20px", display: "flex", alignItems: "center", gap: 10,
            background: result.live ? "#f0fdf4" : "#fffbeb",
            borderBottom: `1px solid ${result.live ? "#bbf7d0" : "#fde68a"}`,
          }}>
            {result.live
              ? <CheckCircle2 size={18} style={{ color: "#16a34a" }} />
              : <AlertTriangle size={18} style={{ color: "#d97706" }} />}
            <div>
              <div style={{ fontWeight: 700, fontSize: 14, color: result.live ? "#15803d" : "#92400e" }}>
                {result.live ? "Deployed successfully to Microsoft Fabric" : "Simulated deployment completed"}
              </div>
              {result.note && (
                <div style={{ fontSize: 12, color: "#64748b", marginTop: 2 }}>{result.note}</div>
              )}
            </div>
            <span style={{
              marginLeft: "auto", fontSize: 12, fontWeight: 600,
              padding: "3px 10px", borderRadius: 20,
              background: result.live ? "#dcfce7" : "#fef9c3",
              color:      result.live ? "#15803d" : "#92400e",
            }}>
              {result.total} artifact{result.total !== 1 ? "s" : ""}
            </span>
          </div>

          {/* Artifacts table */}
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ background: "#f8fafc", borderBottom: "1px solid #f1f5f9" }}>
                {["Artifact Name", "Type", "Artifact ID", "Status"].map(h => (
                  <th key={h} style={{ padding: "10px 16px", textAlign: "left",
                                       fontSize: 11, fontWeight: 600, color: "#64748b" }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {artifacts.map((a, i) => (
                <>
                  <tr key={`row-${i}`} style={{
                    borderBottom: expandedErrors.has(i) ? "none" : "1px solid #f1f5f9",
                    background: a.status === "failed"  ? "#fff8f8" :
                                a.status === "pending" ? "#fffbeb" : "transparent",
                  }}>
                    <td style={{ padding: "10px 16px", fontFamily: "monospace",
                                 fontSize: 12, fontWeight: 600 }}>{a.name}</td>
                    <td style={{ padding: "10px 16px", fontSize: 12, color: "#64748b" }}>{a.type}</td>
                    <td style={{ padding: "10px 16px", fontFamily: "monospace",
                                 fontSize: 11, color: "#94a3b8" }}>
                      {a.id ? a.id.slice(0, 16) + "…" : "—"}
                    </td>
                    <td style={{ padding: "10px 16px" }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                        <span className={
                          a.status === "created"   ? "badge badge-green" :
                          a.status === "updated"   ? "badge badge-blue" :
                          a.status === "pending"   ? "badge badge-amber" :
                          a.status === "simulated" ? "badge badge-amber" :
                                                     "badge badge-red"
                        } style={{ fontSize: 10 }}>
                          {a.status === "updated" ? "↑ updated" : a.status}
                          {a.http_status ? ` · HTTP ${a.http_status}` : ""}
                        </span>
                        {a.error && (
                          <>
                            <button
                              onClick={() => toggleError(i)}
                              style={{
                                fontSize: 10, padding: "2px 8px", borderRadius: 6,
                                cursor: "pointer", border: "1px solid #fecaca",
                                background: "#fef2f2", color: "#b91c1c",
                                display: "flex", alignItems: "center", gap: 3,
                              }}>
                              {expandedErrors.has(i)
                                ? <><ChevronUp size={10} /> Hide error</>
                                : <><AlertTriangle size={10} /> Details</>}
                            </button>
                            <button
                              title="Copy error to clipboard"
                              onClick={() => navigator.clipboard.writeText(a.error).then(() => toast.success("Error copied"))}
                              style={{
                                fontSize: 10, padding: "2px 7px", borderRadius: 6,
                                cursor: "pointer", border: "1px solid #e2e8f0",
                                background: "white", color: "#64748b",
                                display: "flex", alignItems: "center", gap: 3,
                              }}>
                              <Copy size={10} /> Copy
                            </button>
                          </>
                        )}
                      </div>
                    </td>
                  </tr>
                  {a.error && expandedErrors.has(i) && (
                    <tr key={`err-${i}`}>
                      <td colSpan={4} style={{
                        padding: "8px 16px 12px", background: "#fef2f2",
                        borderBottom: "1px solid #fecaca",
                      }}>
                        <pre style={{
                          margin: 0, fontSize: 11, color: "#b91c1c",
                          fontFamily: "monospace", whiteSpace: "pre-wrap",
                          wordBreak: "break-all", lineHeight: 1.5,
                          background: "#fff1f2", padding: 10, borderRadius: 6,
                          border: "1px solid #fecaca",
                        }}>
                          {a.error}
                        </pre>
                      </td>
                    </tr>
                  )}
                </>
              ))}
            </tbody>
          </table>

          {/* Footer actions */}
          <div style={{
            padding: "12px 20px", background: "#f8fafc",
            display: "flex", justifyContent: "space-between", alignItems: "center",
            borderTop: "1px solid #f1f5f9", fontSize: 12, color: "#64748b",
            flexWrap: "wrap", gap: 8,
          }}>
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
                  setExpandedErrors(new Set());
                }}>
                Deploy Again
              </button>
            </div>
          </div>

          {/* ── Artifact verification panel ── */}
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
                          {confirmed ? <CheckCircle2 size={11} /> : <AlertTriangle size={11} />}
                          {art.name}
                        </span>
                      );
                    })}
                  </div>
                  {verifyItems.total > (verifyItems.matched?.length ?? 0) && (
                    <p style={{ fontSize: 11, color: "#64748b", marginTop: 8, marginBottom: 0 }}>
                      {verifyItems.total} total items in workspace ·{" "}
                      {verifyItems.total - (verifyItems.matched?.length ?? 0)} item(s) not from this deployment
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
const _WIZARD_KEY    = "ilink_wizard_state";
const _WIZARD_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days

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

  // ── Resume banner ────────────────────────────────────────────────────────
  const [resumeState, setResumeState] = useState(null);

  useEffect(() => {
    try {
      const raw = localStorage.getItem(_WIZARD_KEY);
      if (!raw) return;
      const saved = JSON.parse(raw);
      if (!saved?.savedAt) return;
      const age = Date.now() - new Date(saved.savedAt).getTime();
      if (age > _WIZARD_TTL_MS) { localStorage.removeItem(_WIZARD_KEY); return; }
      if (saved.source) setResumeState(saved);
    } catch {}
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const acceptResume = () => {
    if (!resumeState) return;
    setWizard(w => ({
      ...w,
      source:          resumeState.source          ?? w.source,
      sourceName:      resumeState.sourceName      ?? w.sourceName,
      module:          resumeState.module          ?? w.module,
      moduleName:      resumeState.moduleName      ?? w.moduleName,
      fabricWorkspace: resumeState.fabricWorkspace ?? w.fabricWorkspace,
      connectionId:    resumeState.connectionId    ?? w.connectionId,
      connectionName:  resumeState.connectionName  ?? w.connectionName,
    }));
    setResumeState(null);
  };

  const dismissResume = () => {
    localStorage.removeItem(_WIZARD_KEY);
    setResumeState(null);
  };

  const _saveWizardState = (w) => {
    try {
      localStorage.setItem(_WIZARD_KEY, JSON.stringify({
        source:          w.source,
        sourceName:      w.sourceName,
        module:          w.module,
        moduleName:      w.moduleName,
        fabricWorkspace: w.fabricWorkspace,
        connectionId:    w.connectionId,
        connectionName:  w.connectionName,
        savedAt:         new Date().toISOString(),
      }));
    } catch {}
  };

  const next = () => { _saveWizardState(wizard); setStep(s => Math.min(s + 1, STEPS.length)); };
  const back = () => setStep(s => Math.max(s - 1, 1));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      {/* Resume last session banner */}
      {resumeState && (
        <div style={{
          display: "flex", alignItems: "center", gap: 12,
          padding: "10px 16px", borderRadius: 10,
          background: "#eff6ff", border: "1px solid #bfdbfe",
          fontSize: 13,
        }}>
          <Info size={15} style={{ color: "#2563eb", flexShrink: 0 }} />
          <span style={{ flex: 1, color: "#1e40af" }}>
            Resume last session?{" "}
            <strong>{resumeState.sourceName}</strong>
            {resumeState.moduleName && <> · <strong>{resumeState.moduleName}</strong></>}
          </span>
          <button className="btn-primary" style={{ fontSize: 12, padding: "4px 14px" }}
                  onClick={acceptResume}>
            Yes, resume
          </button>
          <button className="btn-ghost" style={{ fontSize: 12, padding: "4px 12px" }}
                  onClick={dismissResume}>
            No thanks
          </button>
        </div>
      )}

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
      <div className="card" style={{ padding: 22, overflow: "hidden", minWidth: 0 }}>
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
