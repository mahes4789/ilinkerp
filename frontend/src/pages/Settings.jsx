/**
 * Settings — Fabric Connections Manager
 *
 * Supports 5 auth methods for creating Fabric connections:
 *   1. Bearer Token   — paste token directly
 *   2. Service Principal — tenant + client_id + client_secret
 *   3. Device Code    — OAuth2 interactive (browser-based)
 *   4. Managed Identity — Azure VM/Container IMDS
 *   5. Username/Password — ROPC flow (no MFA)
 *
 * Connections persist in backend/connections.json across restarts.
 */
import { useState, useRef } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import axios from "axios";
import {
  Settings as SettingsIcon,
  Key, CheckCircle2, XCircle, RefreshCw,
  Cloud, AlertTriangle, ExternalLink, ChevronRight,
  Plus, Trash2, Zap, Star, Eye, EyeOff,
  Wifi, WifiOff, Info, Copy, Monitor, Lock,
  User, Server,
} from "lucide-react";
import toast from "react-hot-toast";

const API = import.meta.env.VITE_API_URL ?? "";

// ── Auth method config ────────────────────────────────────────────────────────
const AUTH_METHODS = [
  {
    id: "bearer",
    label: "Bearer Token",
    icon: Key,
    color: "#6366f1",
    desc: "Paste an existing Azure AD Bearer token directly",
    note: "Quickest option. Token expires in ~1 hour.",
  },
  {
    id: "service_principal",
    label: "Service Principal",
    icon: Server,
    color: "#0ea5e9",
    desc: "Non-interactive — ideal for automated / server deployments",
    note: "Requires App Registration with Fabric API permissions.",
  },
  {
    id: "device_code",
    label: "Device Code (OAuth2)",
    icon: Monitor,
    color: "#10b981",
    desc: "Interactive browser-based login — works with MFA accounts",
    note: "User visits a URL and enters a code in their browser.",
  },
  {
    id: "managed_identity",
    label: "Managed Identity",
    icon: Cloud,
    color: "#f59e0b",
    desc: "Uses the Azure VM / Container's assigned identity — no secrets",
    note: "Only works when running on an Azure resource with a managed identity.",
  },
  {
    id: "username_password",
    label: "Username / Password",
    icon: User,
    color: "#8b5cf6",
    desc: "ROPC flow — enter Azure AD username and password",
    note: "⚠️ MFA-enabled accounts will fail. Use Device Code instead.",
  },
];

// ── Small helpers ─────────────────────────────────────────────────────────────
const STATUS_BADGE = {
  valid:   { bg: "#dcfce7", color: "#15803d", text: "Valid"    },
  invalid: { bg: "#fee2e2", color: "#b91c1c", text: "Invalid"  },
  unknown: { bg: "#f1f5f9", color: "#64748b", text: "Untested" },
};
function StatusBadge({ status }) {
  const s = STATUS_BADGE[status] ?? STATUS_BADGE.unknown;
  return (
    <span style={{ fontSize: 10, fontWeight: 700, padding: "2px 8px",
                   borderRadius: 20, background: s.bg, color: s.color }}>
      {s.text}
    </span>
  );
}

function Field({ label, value, onChange, type = "text", placeholder, mono, required, hint }) {
  const [show, setShow] = useState(false);
  const isPassword = type === "password";
  return (
    <div>
      <label style={{ fontSize: 12, fontWeight: 600, color: "#374151",
                      display: "block", marginBottom: 4 }}>
        {label}{required && <span style={{ color: "#ef4444" }}> *</span>}
      </label>
      <div style={{ position: "relative" }}>
        <input
          className="input"
          type={isPassword && !show ? "password" : "text"}
          value={value}
          onChange={e => onChange(e.target.value)}
          placeholder={placeholder}
          style={{ fontSize: 12, fontFamily: mono ? "monospace" : undefined,
                   paddingRight: isPassword ? 36 : undefined }}
        />
        {isPassword && (
          <button type="button" onClick={() => setShow(p => !p)}
            style={{ position: "absolute", right: 8, top: "50%", transform: "translateY(-50%)",
                     background: "none", border: "none", cursor: "pointer", color: "#64748b" }}>
            {show ? <EyeOff size={14} /> : <Eye size={14} />}
          </button>
        )}
      </div>
      {hint && <p style={{ fontSize: 11, color: "#94a3b8", margin: "3px 0 0" }}>{hint}</p>}
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────
export default function Settings() {
  const qc = useQueryClient();

  // ── Form state ──────────────────────────────────────────────────────────────
  const [activeMethod, setActiveMethod] = useState("bearer");
  const [name,         setName]         = useState("");
  const [workspaceId,  setWorkspaceId]  = useState("");
  const [note,         setNote]         = useState("");

  // Bearer
  const [bearerToken,  setBearerToken]  = useState("");
  const [showToken,    setShowToken]    = useState(false);

  // Service Principal
  const [spTenant,     setSpTenant]     = useState("");
  const [spClientId,   setSpClientId]   = useState("");
  const [spSecret,     setSpSecret]     = useState("");

  // Device Code
  const [dcTenant,     setDcTenant]     = useState("");
  const [dcClientId,   setDcClientId]   = useState("");
  const [dcSession,    setDcSession]    = useState(null); // { session_id, user_code, verification_uri }
  const [dcPolling,    setDcPolling]    = useState(false);
  const dcPollRef = useRef(null);

  // Username/Password
  const [upTenant,     setUpTenant]     = useState("");
  const [upClientId,   setUpClientId]   = useState("");
  const [upUsername,   setUpUsername]   = useState("");
  const [upPassword,   setUpPassword]   = useState("");

  const [testResults,  setTestResults]  = useState({});

  // ── Load saved connections ─────────────────────────────────────────────────
  const { data: connsData, isLoading } = useQuery({
    queryKey: ["fabric-connections"],
    queryFn:  () => axios.get(`${API}/api/fabric/fabric-connections`).then(r => r.data),
    refetchOnWindowFocus: false,
  });
  const connections = connsData?.connections ?? [];
  const activeId    = connsData?.active_id   ?? null;

  const refreshConns = () => qc.invalidateQueries(["fabric-connections"]);

  // ── Generic success handler for all auth mutations ─────────────────────────
  const onAuthSuccess = (d) => {
    refreshConns();
    resetForm();
    if (d.test_result === "valid") {
      toast.success(`"${d.name}" connected — ${d.workspaces?.length ?? 0} workspace(s)`);
    } else {
      toast("Connection saved — token could not be validated (Fabric may be unreachable)", { icon: "⚠️" });
    }
    if (d.id) setTestResults(p => ({ ...p, [d.id]: { status: d.test_result, workspaces: d.workspaces } }));
  };
  const onAuthError = (err) =>
    toast.error(err.response?.data?.detail ?? "Authentication failed");

  const resetForm = () => {
    setName(""); setWorkspaceId(""); setNote("");
    setBearerToken(""); setSpTenant(""); setSpClientId(""); setSpSecret("");
    setDcTenant(""); setDcClientId(""); setDcSession(null);
    setUpTenant(""); setUpClientId(""); setUpUsername(""); setUpPassword("");
  };

  // ── Bearer token mutation ──────────────────────────────────────────────────
  const bearerMut = useMutation({
    mutationFn: () => axios.post(`${API}/api/fabric/fabric-connections`,
      { name, token: bearerToken, workspace_id: workspaceId, note }),
    onSuccess: (res) => onAuthSuccess(res.data),
    onError:   onAuthError,
  });

  // ── Service Principal mutation ─────────────────────────────────────────────
  const spMut = useMutation({
    mutationFn: () => axios.post(`${API}/api/fabric/auth/service-principal`,
      { name, tenant_id: spTenant, client_id: spClientId, client_secret: spSecret,
        workspace_id: workspaceId, note }),
    onSuccess: (res) => onAuthSuccess(res.data),
    onError:   onAuthError,
  });

  // ── Device Code mutations ──────────────────────────────────────────────────
  const dcStartMut = useMutation({
    mutationFn: () => axios.post(`${API}/api/fabric/auth/device-code/start`,
      { name, tenant_id: dcTenant, client_id: dcClientId, workspace_id: workspaceId, note }),
    onSuccess: (res) => {
      setDcSession(res.data);
      toast("Open the URL below and enter the code shown", { icon: "🔑" });
      // Start polling every ~5s
      startDcPolling(res.data.session_id, res.data.interval ?? 5);
    },
    onError: onAuthError,
  });

  const startDcPolling = (sessionId, interval) => {
    setDcPolling(true);
    if (dcPollRef.current) clearInterval(dcPollRef.current);
    dcPollRef.current = setInterval(async () => {
      try {
        const res = await axios.post(`${API}/api/fabric/auth/device-code/poll`,
          { session_id: sessionId, name, workspace_id: workspaceId, note });
        const d = res.data;
        if (d.status !== "pending") {
          clearInterval(dcPollRef.current);
          setDcPolling(false);
          setDcSession(null);
          onAuthSuccess(d);
        }
      } catch (err) {
        clearInterval(dcPollRef.current);
        setDcPolling(false);
        setDcSession(null);
        toast.error(err.response?.data?.detail ?? "Device code auth failed");
      }
    }, interval * 1000);
  };

  const cancelDeviceCode = () => {
    if (dcPollRef.current) clearInterval(dcPollRef.current);
    setDcPolling(false);
    setDcSession(null);
  };

  // ── Managed Identity mutation ──────────────────────────────────────────────
  const miMut = useMutation({
    mutationFn: () => axios.post(`${API}/api/fabric/auth/managed-identity`,
      { name: name || "Managed Identity", workspace_id: workspaceId, note }),
    onSuccess: (res) => onAuthSuccess(res.data),
    onError:   onAuthError,
  });

  // ── Username/Password mutation ─────────────────────────────────────────────
  const upMut = useMutation({
    mutationFn: () => axios.post(`${API}/api/fabric/auth/username-password`,
      { name, tenant_id: upTenant, client_id: upClientId,
        username: upUsername, password: upPassword, workspace_id: workspaceId, note }),
    onSuccess: (res) => onAuthSuccess(res.data),
    onError:   onAuthError,
  });

  // ── Delete / Activate / Test ───────────────────────────────────────────────
  const deleteMut = useMutation({
    mutationFn: (cid) => axios.delete(`${API}/api/fabric/fabric-connections/${cid}`),
    onSuccess: () => { refreshConns(); toast.success("Connection removed"); },
    onError:   () => toast.error("Could not delete"),
  });

  const activateMut = useMutation({
    mutationFn: (cid) => axios.post(`${API}/api/fabric/fabric-connections/${cid}/activate`),
    onSuccess: (res) => { refreshConns(); toast.success(`"${res.data.name}" set as active`); },
    onError:   () => toast.error("Could not activate"),
  });

  const [testingId, setTestingId] = useState(null);
  const testConn = async (cid) => {
    setTestingId(cid);
    try {
      const res = await axios.post(`${API}/api/fabric/fabric-connections/${cid}/test`);
      const d   = res.data;
      setTestResults(p => ({ ...p, [cid]: d }));
      refreshConns();
      d.status === "valid"
        ? toast.success(`Valid — ${d.workspace_count} workspace(s)`)
        : toast.error(`Invalid: ${d.error ?? "Fabric API rejected token"}`);
    } catch { toast.error("Test failed"); }
    finally   { setTestingId(null); }
  };

  // ── Form submit dispatcher ─────────────────────────────────────────────────
  const handleSubmit = () => {
    const m = { bearer: bearerMut, service_principal: spMut,
                managed_identity: miMut, username_password: upMut };
    if (activeMethod === "device_code") { dcStartMut.mutate(); return; }
    m[activeMethod]?.mutate();
  };

  const anyPending = [bearerMut, spMut, dcStartMut, miMut, upMut]
    .some(m => m.isPending) || dcPolling;

  const formValid = () => {
    if (!name && activeMethod !== "managed_identity") return false;
    if (activeMethod === "bearer")            return !!bearerToken;
    if (activeMethod === "service_principal") return !!(spTenant && spClientId && spSecret);
    if (activeMethod === "device_code")       return !!(dcTenant && dcClientId);
    if (activeMethod === "managed_identity")  return true;
    if (activeMethod === "username_password") return !!(upTenant && upClientId && upUsername && upPassword);
    return false;
  };

  const METHOD = AUTH_METHODS.find(m => m.id === activeMethod);

  return (
    <div style={{ maxWidth: 780, display: "flex", flexDirection: "column", gap: 24 }}>

      {/* ── Header ────────────────────────────────────────────────────────────── */}
      <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
        <SettingsIcon size={26} style={{ color: "var(--color-primary)" }} />
        <div>
          <h1 style={{ fontSize: 22, fontWeight: 800, margin: 0 }}>Settings</h1>
          <p style={{ fontSize: 13, color: "var(--color-muted)", margin: 0 }}>
            Manage Microsoft Fabric authentication connections
          </p>
        </div>
      </div>

      {/* ══════════════════════════════════════════════════════════════════════
          Saved connections
      ══════════════════════════════════════════════════════════════════════ */}
      <div className="card" style={{ padding: 22 }}>
        <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 4,
                     display: "flex", alignItems: "center", gap: 8 }}>
          <Cloud size={15} style={{ color: "var(--color-primary)" }} />
          Fabric Connections
          <span style={{ marginLeft: "auto", fontSize: 11, fontWeight: 600,
                         color: "#64748b", background: "#f1f5f9",
                         padding: "2px 10px", borderRadius: 20 }}>
            {connections.length} saved
          </span>
        </h3>
        <p style={{ fontSize: 12, color: "var(--color-muted)", marginBottom: 16 }}>
          The <strong>active</strong> connection's token is used for all Fabric API calls.
          Without one, the wizard runs in <em>simulation mode</em>.
        </p>

        {isLoading ? (
          <div style={{ textAlign: "center", padding: 32, color: "#94a3b8" }}>
            <RefreshCw size={18} className="spin" style={{ marginBottom: 8 }} />
            <div style={{ fontSize: 13 }}>Loading…</div>
          </div>
        ) : connections.length === 0 ? (
          <div style={{ border: "2px dashed #e2e8f0", borderRadius: 12,
                        padding: "28px 20px", textAlign: "center" }}>
            <WifiOff size={30} style={{ color: "#cbd5e1", marginBottom: 8 }} />
            <div style={{ fontSize: 14, fontWeight: 600, color: "#64748b" }}>No connections yet</div>
            <div style={{ fontSize: 12, color: "#94a3b8", marginTop: 4 }}>
              Add one below using any of the supported auth methods
            </div>
          </div>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            {connections.map(conn => {
              const isActive = conn.id === activeId;
              const tr       = testResults[conn.id];
              const status   = tr?.status ?? conn.status ?? "unknown";
              const testing  = testingId === conn.id;
              return (
                <div key={conn.id} style={{
                  border: isActive ? "2px solid var(--color-primary)" : "1px solid #e2e8f0",
                  borderRadius: 12, padding: "14px 18px", position: "relative",
                  background: isActive ? "var(--color-primary-light)" : "white",
                }}>
                  {isActive && (
                    <div style={{
                      position: "absolute", top: -1, right: 14,
                      background: "var(--color-primary)", color: "white",
                      fontSize: 10, fontWeight: 700, padding: "2px 10px",
                      borderRadius: "0 0 8px 8px",
                    }}>ACTIVE</div>
                  )}
                  <div style={{ display: "flex", alignItems: "flex-start", gap: 12 }}>
                    <div style={{
                      width: 38, height: 38, borderRadius: 10, flexShrink: 0,
                      background: isActive ? "var(--color-primary)" : "#f1f5f9",
                      display: "flex", alignItems: "center", justifyContent: "center",
                    }}>
                      {status === "valid"
                        ? <Wifi    size={17} style={{ color: isActive ? "white" : "#22c55e" }} />
                        : status === "invalid"
                          ? <WifiOff size={17} style={{ color: "#ef4444" }} />
                          : <Cloud   size={17} style={{ color: isActive ? "white" : "#94a3b8" }} />}
                    </div>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                        <span style={{ fontWeight: 700, fontSize: 14, color: "#0f172a" }}>{conn.name}</span>
                        <StatusBadge status={status} />
                      </div>
                      <div style={{ fontSize: 11, color: "#64748b", marginTop: 2,
                                    fontFamily: "monospace" }}>{conn.token_masked}</div>
                      {conn.workspace_id && (
                        <div style={{ fontSize: 11, color: "#64748b", marginTop: 2 }}>
                          Workspace: <code style={{ fontSize: 10 }}>{conn.workspace_id}</code>
                        </div>
                      )}
                      {conn.note && (
                        <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 2 }}>{conn.note}</div>
                      )}
                      <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 3 }}>
                        Added {new Date(conn.created_at).toLocaleString()}
                      </div>
                      {tr?.status === "valid" && tr.workspaces?.length > 0 && (
                        <div style={{ marginTop: 6, display: "flex", flexWrap: "wrap", gap: 4 }}>
                          {tr.workspaces.map(ws => (
                            <span key={ws.id} style={{ fontSize: 11, padding: "2px 8px",
                                                       borderRadius: 20, background: "#dcfce7", color: "#15803d" }}>
                              {ws.name}
                            </span>
                          ))}
                        </div>
                      )}
                      {tr?.status === "invalid" && (
                        <div style={{ marginTop: 5, fontSize: 11, color: "#b91c1c",
                                      background: "#fee2e2", padding: "4px 10px", borderRadius: 6 }}>
                          {tr.error ?? "Token rejected by Fabric API"}
                        </div>
                      )}
                    </div>
                    <div style={{ display: "flex", flexDirection: "column", gap: 5, flexShrink: 0 }}>
                      <button className="btn-outline"
                        style={{ fontSize: 11, padding: "4px 10px" }}
                        disabled={testing} onClick={() => testConn(conn.id)}>
                        {testing ? <><RefreshCw size={11} className="spin" /> Testing…</>
                                 : <><Zap size={11} /> Test</>}
                      </button>
                      {!isActive && (
                        <button className="btn-primary"
                          style={{ fontSize: 11, padding: "4px 10px" }}
                          disabled={activateMut.isPending}
                          onClick={() => activateMut.mutate(conn.id)}>
                          <Star size={11} /> Set Active
                        </button>
                      )}
                      <button
                        style={{ fontSize: 11, padding: "4px 10px", border: "1px solid #fecaca",
                                 borderRadius: 6, background: "white", color: "#dc2626",
                                 cursor: "pointer", display: "flex", alignItems: "center", gap: 4 }}
                        disabled={deleteMut.isPending}
                        onClick={() => window.confirm(`Remove "${conn.name}"?`) && deleteMut.mutate(conn.id)}>
                        <Trash2 size={11} /> Remove
                      </button>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* ══════════════════════════════════════════════════════════════════════
          Add New Connection — auth method tabs
      ══════════════════════════════════════════════════════════════════════ */}
      <div className="card" style={{ padding: 22 }}>
        <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 4,
                     display: "flex", alignItems: "center", gap: 8 }}>
          <Plus size={15} style={{ color: "var(--color-primary)" }} />
          Add New Connection
        </h3>
        <p style={{ fontSize: 12, color: "var(--color-muted)", marginBottom: 16 }}>
          Choose an authentication method below.
        </p>

        {/* Auth method selector grid */}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(5,1fr)", gap: 8, marginBottom: 20 }}>
          {AUTH_METHODS.map(m => {
            const sel = activeMethod === m.id;
            const Icon = m.icon;
            return (
              <div key={m.id}
                onClick={() => { setActiveMethod(m.id); setDcSession(null); cancelDeviceCode(); }}
                style={{
                  padding: "10px 8px", borderRadius: 10, cursor: "pointer", textAlign: "center",
                  border: `2px solid ${sel ? m.color : "#e2e8f0"}`,
                  background: sel ? `${m.color}18` : "white",
                  transition: "all 0.12s",
                }}>
                <Icon size={18} style={{ color: sel ? m.color : "#94a3b8", marginBottom: 5 }} />
                <div style={{ fontSize: 10, fontWeight: 700,
                               color: sel ? m.color : "#64748b", lineHeight: 1.3 }}>
                  {m.label}
                </div>
              </div>
            );
          })}
        </div>

        {/* Method description */}
        {METHOD && (
          <div style={{ padding: "10px 14px", borderRadius: 8, marginBottom: 16,
                        background: "#f8fafc", border: "1px solid #e2e8f0",
                        display: "flex", gap: 10, alignItems: "flex-start" }}>
            <Info size={14} style={{ color: "#64748b", flexShrink: 0, marginTop: 1 }} />
            <div>
              <div style={{ fontSize: 12, color: "#374151", fontWeight: 600 }}>{METHOD.desc}</div>
              <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 2 }}>{METHOD.note}</div>
            </div>
          </div>
        )}

        {/* Common fields */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, marginBottom: 14 }}>
          {activeMethod !== "managed_identity" && (
            <Field label="Connection Name" value={name} onChange={setName}
                   placeholder="e.g. Production Fabric" required />
          )}
          <Field label="Default Workspace ID" value={workspaceId} onChange={setWorkspaceId}
                 placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" mono />
        </div>

        {/* Method-specific fields */}
        {activeMethod === "bearer" && (
          <div style={{ marginBottom: 14 }}>
            <label style={{ fontSize: 12, fontWeight: 600, color: "#374151",
                            display: "block", marginBottom: 4 }}>
              Bearer Token <span style={{ color: "#ef4444" }}>*</span>
            </label>
            <div style={{ position: "relative" }}>
              <textarea className="input"
                placeholder="eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9..."
                value={bearerToken}
                onChange={e => setBearerToken(e.target.value)}
                rows={showToken ? 4 : 2}
                style={{ fontFamily: "monospace", fontSize: 11, resize: "vertical",
                         paddingRight: 40, filter: showToken ? "none" : "blur(3px)",
                         transition: "filter 0.2s" }}
              />
              <button type="button" onClick={() => setShowToken(p => !p)}
                style={{ position: "absolute", top: 8, right: 8, background: "none",
                         border: "none", cursor: "pointer", color: "#64748b" }}>
                {showToken ? <EyeOff size={15} /> : <Eye size={15} />}
              </button>
            </div>
          </div>
        )}

        {activeMethod === "service_principal" && (
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 14 }}>
            <Field label="Tenant ID" value={spTenant} onChange={setSpTenant}
                   placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" mono required />
            <Field label="Client ID (App ID)" value={spClientId} onChange={setSpClientId}
                   placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" mono required />
            <div style={{ gridColumn: "1/-1" }}>
              <Field label="Client Secret" value={spSecret} onChange={setSpSecret}
                     type="password" placeholder="your-client-secret-value" required />
            </div>
          </div>
        )}

        {activeMethod === "device_code" && !dcSession && (
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 14 }}>
            <Field label="Tenant ID" value={dcTenant} onChange={setDcTenant}
                   placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" mono required />
            <Field label="Client ID (App ID)" value={dcClientId} onChange={setDcClientId}
                   placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" mono required />
          </div>
        )}

        {/* Device Code active flow */}
        {activeMethod === "device_code" && dcSession && (
          <div style={{ marginBottom: 14, padding: "16px 18px", borderRadius: 10,
                        background: "#f0fdf4", border: "2px solid #bbf7d0" }}>
            <div style={{ fontWeight: 700, fontSize: 13, color: "#15803d", marginBottom: 8 }}>
              🔑 Open browser and authenticate
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 10 }}>
              <a href={dcSession.verification_uri} target="_blank" rel="noreferrer"
                style={{ fontSize: 13, color: "#1d4ed8", fontWeight: 600 }}>
                {dcSession.verification_uri}
              </a>
              <ExternalLink size={13} style={{ color: "#94a3b8" }} />
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
              <div style={{ padding: "8px 16px", borderRadius: 8, background: "#dcfce7",
                            border: "1px solid #86efac", fontSize: 20, fontWeight: 800,
                            letterSpacing: "0.15em", color: "#15803d", fontFamily: "monospace" }}>
                {dcSession.user_code}
              </div>
              <button className="btn-ghost" style={{ fontSize: 11 }}
                onClick={() => { navigator.clipboard.writeText(dcSession.user_code); toast("Code copied"); }}>
                <Copy size={12} /> Copy
              </button>
            </div>
            <div style={{ marginTop: 10, display: "flex", alignItems: "center", gap: 8,
                          fontSize: 12, color: "#64748b" }}>
              {dcPolling && <><RefreshCw size={12} className="spin" /> Polling for authentication…</>}
            </div>
            <button className="btn-ghost" style={{ marginTop: 8, fontSize: 11, color: "#dc2626" }}
              onClick={cancelDeviceCode}>
              Cancel
            </button>
          </div>
        )}

        {activeMethod === "username_password" && (
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 14 }}>
            <Field label="Tenant ID" value={upTenant} onChange={setUpTenant}
                   placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" mono required />
            <Field label="Client ID (App ID)" value={upClientId} onChange={setUpClientId}
                   placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" mono required />
            <Field label="Username" value={upUsername} onChange={setUpUsername}
                   placeholder="user@contoso.com" required />
            <Field label="Password" value={upPassword} onChange={setUpPassword}
                   type="password" placeholder="••••••" required />
          </div>
        )}

        {activeMethod === "managed_identity" && (
          <div style={{ marginBottom: 14 }}>
            <Field label="Connection Name" value={name || "Managed Identity"} onChange={setName}
                   placeholder="Managed Identity" />
          </div>
        )}

        {/* Note */}
        <div style={{ marginBottom: 16 }}>
          <Field label="Note (optional)" value={note} onChange={setNote}
                 placeholder="e.g. Expires 2026-09-01, Oracle EBS prod" />
        </div>

        {/* Submit */}
        {!(activeMethod === "device_code" && dcSession) && (
          <button className="btn-primary"
            disabled={!formValid() || anyPending}
            onClick={handleSubmit}
            style={{ alignSelf: "flex-start" }}>
            {anyPending ? (
              <><RefreshCw size={13} className="spin" />
                {activeMethod === "device_code" ? " Starting device code…" : " Authenticating…"}</>
            ) : (
              <><Plus size={13} />
                {activeMethod === "device_code" ? " Start Device Code Flow" : " Add & Validate Connection"}</>
            )}
          </button>
        )}
      </div>

      {/* ══════════════════════════════════════════════════════════════════════
          Auth guide
      ══════════════════════════════════════════════════════════════════════ */}
      <div className="card" style={{ padding: 22 }}>
        <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 12,
                     display: "flex", alignItems: "center", gap: 8 }}>
          <AlertTriangle size={15} style={{ color: "#f59e0b" }} />
          Azure AD App Prerequisites
        </h3>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
          {[
            { title: "Register an App",        body: "Azure Portal → Active Directory → App registrations → New" },
            { title: "Add Fabric permissions",  body: "API permissions → Add → Fabric API → Workspace.ReadWrite.All, Item.ReadWrite.All" },
            { title: "Service Principal",       body: "Certificates & Secrets → New client secret. No user login required." },
            { title: "Device Code scope",       body: "Ensure 'Allow public client flows' is enabled under Authentication." },
          ].map(({ title, body }) => (
            <div key={title} style={{ padding: "12px 14px", borderRadius: 8,
                                      background: "#f8fafc", border: "1px solid #e2e8f0" }}>
              <div style={{ fontSize: 12, fontWeight: 700, color: "#1e293b", marginBottom: 4 }}>{title}</div>
              <div style={{ fontSize: 11, color: "#64748b" }}>{body}</div>
            </div>
          ))}
        </div>
        <a href="https://learn.microsoft.com/en-us/rest/api/fabric/articles/get-started/fabric-api-quickstart"
          target="_blank" rel="noreferrer"
          style={{ marginTop: 14, display: "inline-flex", alignItems: "center", gap: 6,
                   fontSize: 12, color: "var(--color-primary)", fontWeight: 600, textDecoration: "none" }}>
          <ExternalLink size={13} /> Microsoft Fabric REST API docs <ChevronRight size={12} />
        </a>
      </div>

      {/* ══════════════════════════════════════════════════════════════════════
          Auth method comparison table
      ══════════════════════════════════════════════════════════════════════ */}
      <div className="card" style={{ padding: 22 }}>
        <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 12,
                     display: "flex", alignItems: "center", gap: 8 }}>
          <Info size={15} style={{ color: "var(--color-primary)" }} />
          Auth Method Comparison
        </h3>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ background: "#f8fafc" }}>
                {["Method","MFA Support","Automated","Token Source","Best For"].map(h => (
                  <th key={h} style={{ padding: "8px 12px", textAlign: "left",
                                       fontWeight: 700, color: "#374151",
                                       borderBottom: "2px solid #e2e8f0" }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {[
                ["Bearer Token",       "✅ Yes", "❌ No",  "Manual paste",        "Quick demos, dev testing"],
                ["Service Principal",  "✅ Yes", "✅ Yes", "Azure AD client creds","CI/CD, server deployments"],
                ["Device Code",        "✅ Yes", "❌ No",  "Browser interactive",  "MFA accounts, one-time setup"],
                ["Managed Identity",   "✅ Yes", "✅ Yes", "Azure IMDS",           "Azure-hosted environments"],
                ["Username/Password",  "❌ No",  "✅ Yes", "ROPC flow",            "Non-MFA service accounts"],
              ].map(([method, ...cells]) => (
                <tr key={method} style={{ borderBottom: "1px solid #f1f5f9" }}>
                  <td style={{ padding: "8px 12px", fontWeight: 600, color: "#0f172a" }}>{method}</td>
                  {cells.map((c, i) => (
                    <td key={i} style={{ padding: "8px 12px", color: "#64748b" }}>{c}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

    </div>
  );
}
