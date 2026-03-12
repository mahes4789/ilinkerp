/**
 * Settings — Fabric Connections Manager
 *
 * Allows users to:
 *   · Add named Fabric connections (name + Bearer token + optional workspace)
 *   · Test each connection against the live Fabric API
 *   · Set one connection as "active" (used for all wizard operations)
 *   · Delete connections
 *
 * Connections are persisted in backend/connections.json (survives restarts).
 */
import { useState, useEffect } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import axios from "axios";
import {
  Settings as SettingsIcon,
  Key, CheckCircle2, XCircle, RefreshCw,
  Cloud, AlertTriangle, ExternalLink, ChevronRight,
  Plus, Trash2, Zap, Star, StarOff, Eye, EyeOff,
  Wifi, WifiOff, Info,
} from "lucide-react";
import toast from "react-hot-toast";

const API = import.meta.env.VITE_API_URL ?? "";

// ── small helpers ─────────────────────────────────────────────────────────────
const STATUS_BADGE = {
  valid:   { bg: "#dcfce7", color: "#15803d", text: "Valid"   },
  invalid: { bg: "#fee2e2", color: "#b91c1c", text: "Invalid" },
  unknown: { bg: "#f1f5f9", color: "#64748b", text: "Untested"},
};

function StatusBadge({ status }) {
  const s = STATUS_BADGE[status] ?? STATUS_BADGE.unknown;
  return (
    <span style={{
      fontSize: 10, fontWeight: 700, padding: "2px 8px",
      borderRadius: 20, background: s.bg, color: s.color,
    }}>
      {s.text}
    </span>
  );
}

// ── Main component ────────────────────────────────────────────────────────────
export default function Settings() {
  const qc = useQueryClient();

  // ── form state ─────────────────────────────────────────────────────────────
  const [form, setForm]         = useState({ name: "", token: "", workspace_id: "", note: "" });
  const [showToken, setShowToken] = useState(false);
  const [testResults, setTestResults] = useState({}); // cid → { status, workspaces, error }

  // ── Load saved connections ─────────────────────────────────────────────────
  const { data: connsData, isLoading: loadingConns } = useQuery({
    queryKey: ["fabric-connections"],
    queryFn:  () => axios.get(`${API}/api/fabric/fabric-connections`).then(r => r.data),
    refetchOnWindowFocus: false,
  });
  const connections = connsData?.connections ?? [];
  const activeId    = connsData?.active_id   ?? null;

  // ── Add connection ─────────────────────────────────────────────────────────
  const addMutation = useMutation({
    mutationFn: () => axios.post(`${API}/api/fabric/fabric-connections`, form),
    onSuccess: (res) => {
      const d = res.data;
      qc.invalidateQueries(["fabric-connections"]);
      setForm({ name: "", token: "", workspace_id: "", note: "" });
      if (d.test_result === "valid") {
        toast.success(`"${d.name}" connected — ${d.workspaces?.length ?? 0} workspace(s) found`);
      } else {
        toast("Connection saved — token could not be validated (live Fabric not reachable)", {
          icon: "⚠️",
        });
      }
      // store test result so we can show workspaces inline
      if (d.id) {
        setTestResults(p => ({
          ...p,
          [d.id]: { status: d.test_result, workspaces: d.workspaces },
        }));
      }
    },
    onError: (err) => toast.error(err.response?.data?.detail ?? "Failed to save connection"),
  });

  // ── Delete connection ──────────────────────────────────────────────────────
  const deleteMutation = useMutation({
    mutationFn: (cid) => axios.delete(`${API}/api/fabric/fabric-connections/${cid}`),
    onSuccess: () => {
      qc.invalidateQueries(["fabric-connections"]);
      toast.success("Connection removed");
    },
    onError: () => toast.error("Could not delete connection"),
  });

  // ── Activate connection ────────────────────────────────────────────────────
  const activateMutation = useMutation({
    mutationFn: (cid) => axios.post(`${API}/api/fabric/fabric-connections/${cid}/activate`),
    onSuccess: (res) => {
      qc.invalidateQueries(["fabric-connections"]);
      toast.success(`"${res.data.name}" set as active connection`);
    },
    onError: () => toast.error("Could not activate connection"),
  });

  // ── Test connection ────────────────────────────────────────────────────────
  const [testingId, setTestingId] = useState(null);
  const testConnection = async (cid) => {
    setTestingId(cid);
    try {
      const res = await axios.post(`${API}/api/fabric/fabric-connections/${cid}/test`);
      const d   = res.data;
      setTestResults(p => ({ ...p, [cid]: d }));
      qc.invalidateQueries(["fabric-connections"]);
      if (d.status === "valid") {
        toast.success(`Token valid — ${d.workspace_count} workspace(s) accessible`);
      } else {
        toast.error(`Token invalid: ${d.error ?? "Fabric API rejected the token"}`);
      }
    } catch {
      toast.error("Test request failed");
    } finally {
      setTestingId(null);
    }
  };

  const formOk = form.name.trim() && form.token.trim();

  return (
    <div style={{ maxWidth: 760, display: "flex", flexDirection: "column", gap: 24 }}>

      {/* ── Page header ──────────────────────────────────────────────────────── */}
      <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
        <SettingsIcon size={26} style={{ color: "var(--color-primary)" }} />
        <div>
          <h1 style={{ fontSize: 22, fontWeight: 800, margin: 0 }}>Settings</h1>
          <p style={{ fontSize: 13, color: "var(--color-muted)", margin: 0 }}>
            Manage Microsoft Fabric connections used by the ERP wizard
          </p>
        </div>
      </div>

      {/* ══════════════════════════════════════════════════════════════════════
          Saved connections list
      ══════════════════════════════════════════════════════════════════════ */}
      <div className="card" style={{ padding: 22 }}>
        <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 4,
                     display: "flex", alignItems: "center", gap: 8 }}>
          <Cloud size={15} style={{ color: "var(--color-primary)" }} />
          Fabric Connections
          <span style={{
            marginLeft: "auto", fontSize: 11, fontWeight: 600,
            color: "#64748b", background: "#f1f5f9",
            padding: "2px 10px", borderRadius: 20,
          }}>
            {connections.length} saved
          </span>
        </h3>
        <p style={{ fontSize: 12, color: "var(--color-muted)", marginBottom: 16 }}>
          The <strong>active</strong> connection's token is used for all live Fabric API calls.
          Without an active valid connection, operations run in <em>simulation mode</em>.
        </p>

        {loadingConns ? (
          <div style={{ textAlign: "center", padding: 32, color: "#94a3b8" }}>
            <RefreshCw size={18} className="spin" style={{ marginBottom: 8 }} />
            <div style={{ fontSize: 13 }}>Loading connections…</div>
          </div>
        ) : connections.length === 0 ? (
          /* Empty state */
          <div style={{
            border: "2px dashed #e2e8f0", borderRadius: 12,
            padding: "32px 20px", textAlign: "center",
          }}>
            <WifiOff size={32} style={{ color: "#cbd5e1", marginBottom: 10 }} />
            <div style={{ fontSize: 14, fontWeight: 600, color: "#64748b" }}>
              No Fabric connections yet
            </div>
            <div style={{ fontSize: 12, color: "#94a3b8", marginTop: 4 }}>
              Add a connection below to enable live Microsoft Fabric integration
            </div>
          </div>
        ) : (
          /* Connection cards */
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            {connections.map(conn => {
              const isActive  = conn.id === activeId;
              const tr        = testResults[conn.id];
              const status    = tr?.status ?? conn.status ?? "unknown";
              const activating = activateMutation.isPending;
              const deleting   = deleteMutation.isPending;
              const testing    = testingId === conn.id;

              return (
                <div key={conn.id} style={{
                  border: isActive
                    ? "2px solid var(--color-primary)"
                    : "1px solid #e2e8f0",
                  borderRadius: 12, padding: "14px 18px",
                  background: isActive ? "var(--color-primary-light)" : "white",
                  position: "relative",
                }}>
                  {/* Active ribbon */}
                  {isActive && (
                    <div style={{
                      position: "absolute", top: -1, right: 14,
                      background: "var(--color-primary)",
                      color: "white", fontSize: 10, fontWeight: 700,
                      padding: "2px 10px", borderRadius: "0 0 8px 8px",
                    }}>
                      ACTIVE
                    </div>
                  )}

                  <div style={{ display: "flex", alignItems: "flex-start", gap: 12 }}>
                    {/* Icon */}
                    <div style={{
                      width: 38, height: 38, borderRadius: 10, flexShrink: 0,
                      background: isActive ? "var(--color-primary)" : "#f1f5f9",
                      display: "flex", alignItems: "center", justifyContent: "center",
                    }}>
                      {status === "valid"
                        ? <Wifi size={18} style={{ color: isActive ? "white" : "#22c55e" }} />
                        : status === "invalid"
                          ? <WifiOff size={18} style={{ color: "#ef4444" }} />
                          : <Cloud  size={18} style={{ color: isActive ? "white" : "#94a3b8" }} />
                      }
                    </div>

                    {/* Details */}
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                        <span style={{ fontWeight: 700, fontSize: 14, color: "#0f172a" }}>
                          {conn.name}
                        </span>
                        <StatusBadge status={status} />
                      </div>

                      <div style={{ fontSize: 11, color: "#64748b", marginTop: 3,
                                    fontFamily: "monospace", letterSpacing: "0.02em" }}>
                        {conn.token_masked}
                      </div>

                      {conn.workspace_id && (
                        <div style={{ fontSize: 11, color: "#64748b", marginTop: 2 }}>
                          Workspace: <code style={{ fontSize: 10 }}>{conn.workspace_id}</code>
                        </div>
                      )}

                      {conn.note && (
                        <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 2 }}>
                          {conn.note}
                        </div>
                      )}

                      <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 4 }}>
                        Added {new Date(conn.created_at).toLocaleString()}
                      </div>

                      {/* Test result — workspace list */}
                      {tr?.status === "valid" && tr.workspaces?.length > 0 && (
                        <div style={{ marginTop: 8, display: "flex", flexWrap: "wrap", gap: 5 }}>
                          {tr.workspaces.map(ws => (
                            <span key={ws.id} style={{
                              fontSize: 11, padding: "2px 8px", borderRadius: 20,
                              background: "#dcfce7", color: "#15803d",
                            }}>
                              {ws.name}
                            </span>
                          ))}
                        </div>
                      )}
                      {tr?.status === "invalid" && (
                        <div style={{
                          marginTop: 6, fontSize: 11, color: "#b91c1c",
                          background: "#fee2e2", padding: "4px 10px", borderRadius: 6,
                        }}>
                          {tr.error ?? "Token rejected by Fabric API"}
                        </div>
                      )}
                    </div>

                    {/* Actions */}
                    <div style={{ display: "flex", flexDirection: "column", gap: 6, flexShrink: 0 }}>
                      {/* Test */}
                      <button
                        className="btn-outline"
                        style={{ fontSize: 11, padding: "4px 10px", whiteSpace: "nowrap" }}
                        disabled={testing}
                        onClick={() => testConnection(conn.id)}
                        title="Test this token against the live Fabric API"
                      >
                        {testing
                          ? <><RefreshCw size={11} className="spin" /> Testing…</>
                          : <><Zap size={11} /> Test</>
                        }
                      </button>

                      {/* Activate */}
                      {!isActive && (
                        <button
                          className="btn-primary"
                          style={{ fontSize: 11, padding: "4px 10px", whiteSpace: "nowrap" }}
                          disabled={activating}
                          onClick={() => activateMutation.mutate(conn.id)}
                          title="Use this connection for all Fabric operations"
                        >
                          <Star size={11} /> Set Active
                        </button>
                      )}

                      {/* Delete */}
                      <button
                        style={{
                          fontSize: 11, padding: "4px 10px",
                          border: "1px solid #fecaca", borderRadius: 6,
                          background: "white", color: "#dc2626",
                          cursor: "pointer", display: "flex",
                          alignItems: "center", gap: 5,
                          whiteSpace: "nowrap",
                        }}
                        disabled={deleting}
                        onClick={() => {
                          if (window.confirm(`Remove connection "${conn.name}"?`)) {
                            deleteMutation.mutate(conn.id);
                          }
                        }}
                        title="Remove this connection"
                      >
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
          Add new connection form
      ══════════════════════════════════════════════════════════════════════ */}
      <div className="card" style={{ padding: 22 }}>
        <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 4,
                     display: "flex", alignItems: "center", gap: 8 }}>
          <Plus size={15} style={{ color: "var(--color-primary)" }} />
          Add New Connection
        </h3>
        <p style={{ fontSize: 12, color: "var(--color-muted)", marginBottom: 16 }}>
          Each connection stores a Bearer token that authenticates with Microsoft Fabric.
          The first connection added is automatically set as active.
        </p>

        <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
          {/* Name */}
          <div>
            <label style={{ fontSize: 12, fontWeight: 600, color: "#374151",
                            display: "block", marginBottom: 4 }}>
              Connection Name <span style={{ color: "#ef4444" }}>*</span>
            </label>
            <input
              className="input"
              placeholder="e.g. Production Fabric, Dev Workspace"
              value={form.name}
              onChange={e => setForm(p => ({ ...p, name: e.target.value }))}
              style={{ fontSize: 13 }}
            />
          </div>

          {/* Token */}
          <div>
            <label style={{ fontSize: 12, fontWeight: 600, color: "#374151",
                            display: "block", marginBottom: 4 }}>
              Bearer Token <span style={{ color: "#ef4444" }}>*</span>
            </label>
            <div style={{ position: "relative" }}>
              <textarea
                className="input"
                placeholder="eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9..."
                value={form.token}
                onChange={e => setForm(p => ({ ...p, token: e.target.value }))}
                rows={showToken ? 4 : 2}
                style={{
                  fontFamily: "monospace", fontSize: 11,
                  resize: "vertical", paddingRight: 40,
                  filter: showToken ? "none" : "blur(3px)",
                  transition: "filter 0.2s",
                }}
              />
              <button
                type="button"
                onClick={() => setShowToken(p => !p)}
                style={{
                  position: "absolute", top: 8, right: 8,
                  background: "none", border: "none", cursor: "pointer",
                  color: "#64748b", padding: 4,
                }}
                title={showToken ? "Hide token" : "Show token"}
              >
                {showToken ? <EyeOff size={15} /> : <Eye size={15} />}
              </button>
            </div>
            <p style={{ fontSize: 11, color: "#94a3b8", marginTop: 4 }}>
              Token is stored in <code>connections.json</code> on the backend.
              Get a token from Azure AD (see guide below).
            </p>
          </div>

          {/* Workspace ID */}
          <div>
            <label style={{ fontSize: 12, fontWeight: 600, color: "#374151",
                            display: "block", marginBottom: 4 }}>
              Default Workspace ID
              <span style={{ fontWeight: 400, color: "#94a3b8" }}> (optional)</span>
            </label>
            <input
              className="input"
              placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
              value={form.workspace_id}
              onChange={e => setForm(p => ({ ...p, workspace_id: e.target.value }))}
              style={{ fontSize: 13, fontFamily: "monospace" }}
            />
          </div>

          {/* Note */}
          <div>
            <label style={{ fontSize: 12, fontWeight: 600, color: "#374151",
                            display: "block", marginBottom: 4 }}>
              Note
              <span style={{ fontWeight: 400, color: "#94a3b8" }}> (optional)</span>
            </label>
            <input
              className="input"
              placeholder="e.g. Expires 2026-06-01, use for Oracle EBS migration"
              value={form.note}
              onChange={e => setForm(p => ({ ...p, note: e.target.value }))}
              style={{ fontSize: 13 }}
            />
          </div>

          {/* Submit */}
          <button
            className="btn-primary"
            disabled={!formOk || addMutation.isPending}
            onClick={() => addMutation.mutate()}
            style={{ alignSelf: "flex-start" }}
          >
            {addMutation.isPending ? (
              <><RefreshCw size={13} className="spin" /> Saving &amp; Testing…</>
            ) : (
              <><Plus size={13} /> Add &amp; Validate Connection</>
            )}
          </button>
        </div>
      </div>

      {/* ══════════════════════════════════════════════════════════════════════
          How to get a Bearer token
      ══════════════════════════════════════════════════════════════════════ */}
      <div className="card" style={{ padding: 22 }}>
        <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 12,
                     display: "flex", alignItems: "center", gap: 8 }}>
          <AlertTriangle size={15} style={{ color: "#f59e0b" }} />
          How to Get a Fabric Bearer Token
        </h3>
        <div style={{ display: "flex", flexDirection: "column", gap: 10, fontSize: 13 }}>
          {[
            {
              step: "1",
              text: "Register an Azure AD App",
              sub:  "Azure Portal → Active Directory → App registrations → New registration",
            },
            {
              step: "2",
              text: "Grant Fabric API permissions",
              sub:  "API permissions → Add: Workspace.ReadWrite.All, Item.ReadWrite.All",
            },
            {
              step: "3",
              text: "Get a token via client credentials or device-code flow",
              sub:  "POST https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
            },
            {
              step: "4",
              text: "Quick method — copy from browser DevTools",
              sub:  "Open app.fabric.microsoft.com → DevTools (F12) → Network → any API call → Authorization header",
            },
          ].map(({ step, text, sub }) => (
            <div key={step} style={{ display: "flex", gap: 12, alignItems: "flex-start" }}>
              <span style={{
                width: 24, height: 24, borderRadius: "50%", flexShrink: 0,
                background: "var(--color-primary-light)", color: "var(--color-primary)",
                display: "flex", alignItems: "center", justifyContent: "center",
                fontSize: 11, fontWeight: 700,
              }}>
                {step}
              </span>
              <div>
                <div style={{ fontWeight: 600, color: "#1e293b" }}>{text}</div>
                <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 2 }}>{sub}</div>
              </div>
            </div>
          ))}
        </div>

        <a
          href="https://learn.microsoft.com/en-us/rest/api/fabric/articles/get-started/fabric-api-quickstart"
          target="_blank" rel="noreferrer"
          style={{
            marginTop: 14, display: "inline-flex", alignItems: "center", gap: 6,
            fontSize: 12, color: "var(--color-primary)", fontWeight: 600,
            textDecoration: "none",
          }}
        >
          <ExternalLink size={13} /> Microsoft Fabric REST API documentation
          <ChevronRight size={12} />
        </a>
      </div>

      {/* ══════════════════════════════════════════════════════════════════════
          API reference
      ══════════════════════════════════════════════════════════════════════ */}
      <div className="card" style={{ padding: 22 }}>
        <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 12,
                     display: "flex", alignItems: "center", gap: 8 }}>
          <Info size={15} style={{ color: "var(--color-primary)" }} />
          Fabric API Endpoints
        </h3>
        <div style={{
          display: "flex", flexDirection: "column", gap: 0,
          border: "1px solid #e2e8f0", borderRadius: 10, overflow: "hidden",
        }}>
          {[
            { method: "GET",    path: "/api/fabric/fabric-connections",         note: "List saved UI connections"           },
            { method: "POST",   path: "/api/fabric/fabric-connections",         note: "Add + auto-test a new connection"    },
            { method: "POST",   path: "/api/fabric/fabric-connections/{id}/activate", note: "Set connection as active"     },
            { method: "POST",   path: "/api/fabric/fabric-connections/{id}/test",     note: "Re-test a saved connection"   },
            { method: "DELETE", path: "/api/fabric/fabric-connections/{id}",    note: "Remove a connection"                 },
            { method: "GET",    path: "/api/fabric/workspaces",                 note: "List Fabric workspaces"              },
            { method: "POST",   path: "/api/fabric/create-connection",          note: "Create ERP shareable connection"     },
            { method: "POST",   path: "/api/fabric/deploy",                     note: "Deploy notebooks + pipeline"         },
          ].map(({ method, path, note }, i, arr) => {
            const colors = {
              GET:    { bg: "#dbeafe", color: "#1d4ed8" },
              POST:   { bg: "#fce7f3", color: "#9d174d" },
              DELETE: { bg: "#fee2e2", color: "#b91c1c" },
              PATCH:  { bg: "#fef9c3", color: "#a16207" },
            };
            const c = colors[method] ?? colors.GET;
            return (
              <div key={path} style={{
                display: "flex", alignItems: "center", gap: 10,
                padding: "9px 14px",
                borderBottom: i < arr.length - 1 ? "1px solid #f1f5f9" : "none",
                background: "white",
              }}>
                <span style={{
                  fontSize: 10, fontWeight: 700, padding: "2px 7px",
                  borderRadius: 4, flexShrink: 0,
                  background: c.bg, color: c.color,
                }}>
                  {method}
                </span>
                <code style={{ fontSize: 11, color: "#0f172a", fontFamily: "monospace", flex: 1 }}>
                  {path}
                </code>
                <span style={{ fontSize: 11, color: "#94a3b8" }}>{note}</span>
              </div>
            );
          })}
        </div>
      </div>

    </div>
  );
}
