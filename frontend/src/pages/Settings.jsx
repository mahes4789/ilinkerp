/**
 * Settings — MS365 token management + Fabric connection validation
 *
 * Token card:
 *   · Paste Bearer token → POST /api/fabric/set-token
 *   · Validate button    → GET  /api/fabric/validate-token
 *   · Shows ✓ workspace count on success, ✗ error on failure
 *
 * How-to card + API endpoint card
 */
import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import axios from "axios";
import {
  Settings as SettingsIcon, Key, CheckCircle2, XCircle,
  RefreshCw, LayoutDashboard, AlertTriangle, Cloud, ChevronRight,
  ExternalLink,
} from "lucide-react";
import toast from "react-hot-toast";

const API = import.meta.env.VITE_API_URL ?? "";

export default function Settings() {
  const [token,    setToken]    = useState("");
  const [validated, setValidated] = useState(null); // null | { valid, message, workspaces }

  // ── Save token ──────────────────────────────────────────────────────────────
  const saveMutation = useMutation({
    mutationFn: () =>
      axios.post(`${API}/api/fabric/set-token`, { access_token: token }),
    onSuccess: () => {
      toast.success("Token saved — click Validate to check Fabric access");
      setValidated(null);
    },
    onError: () => toast.error("Failed to save token"),
  });

  // ── Validate token (lazy — triggered manually) ─────────────────────────────
  const validateQuery = useQuery({
    queryKey:  ["validate-token"],
    queryFn:   () => axios.get(`${API}/api/fabric/validate-token`).then(r => r.data),
    enabled:   false,           // only run when refetch() is called
    retry:     false,
    onSuccess: data => setValidated(data),
    onError:   ()   => setValidated({ valid: false, message: "Validation request failed" }),
  });

  const handleValidate = () => {
    setValidated(null);
    validateQuery.refetch();
  };

  const tokenStatus = validated
    ? validated.valid
      ? "valid"
      : "invalid"
    : null;

  return (
    <div style={{ maxWidth: 700, display: "flex", flexDirection: "column", gap: 24 }}>

      {/* ── Page header ──────────────────────────────────────────────────────── */}
      <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
        <SettingsIcon size={26} style={{ color: "var(--color-primary)" }} />
        <div>
          <h1 style={{ fontSize: 22, fontWeight: 800, margin: 0 }}>Settings</h1>
          <p style={{ fontSize: 13, color: "var(--color-muted)", margin: 0 }}>
            Microsoft Fabric authentication and connection configuration
          </p>
        </div>
      </div>

      {/* ── MS365 Token card ─────────────────────────────────────────────────── */}
      <div className="card" style={{ padding: 22 }}>
        <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 4,
                     display: "flex", alignItems: "center", gap: 8 }}>
          <Key size={15} style={{ color: "var(--color-primary)" }} />
          Microsoft 365 / Fabric Bearer Token
        </h3>
        <p style={{ fontSize: 12, color: "var(--color-muted)", marginBottom: 14 }}>
          Paste a valid Azure AD Bearer token to enable live Fabric API calls.
          Without a token all operations are simulated.
        </p>

        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          {/* Textarea */}
          <textarea
            value={token}
            onChange={e => { setToken(e.target.value); setValidated(null); }}
            placeholder="eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9..."
            className="input"
            rows={4}
            style={{ fontFamily: "monospace", fontSize: 11, resize: "vertical" }}
          />

          {/* Action buttons */}
          <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
            <button
              className="btn-primary"
              disabled={!token || saveMutation.isPending}
              onClick={() => saveMutation.mutate()}
            >
              {saveMutation.isPending ? (
                <><RefreshCw size={13} className="spin" /> Saving…</>
              ) : (
                <><Key size={13} /> Save Token</>
              )}
            </button>

            <button
              className="btn-outline"
              disabled={validateQuery.isFetching}
              onClick={handleValidate}
              style={{ display: "flex", alignItems: "center", gap: 7 }}
            >
              {validateQuery.isFetching ? (
                <><RefreshCw size={13} className="spin" /> Validating…</>
              ) : (
                <><Cloud size={13} /> Validate &amp; Connect to Fabric</>
              )}
            </button>
          </div>

          {/* Validation result banner */}
          {validated && (
            <div style={{
              padding: "12px 16px", borderRadius: 10,
              border: `1px solid ${tokenStatus === "valid" ? "#bbf7d0" : "#fecaca"}`,
              background: tokenStatus === "valid" ? "#f0fdf4" : "#fef2f2",
              display: "flex", flexDirection: "column", gap: 8,
            }}>
              {/* Status row */}
              <div style={{ display: "flex", alignItems: "center", gap: 9 }}>
                {tokenStatus === "valid"
                  ? <CheckCircle2 size={16} style={{ color: "#16a34a", flexShrink: 0 }} />
                  : <XCircle      size={16} style={{ color: "#dc2626", flexShrink: 0 }} />
                }
                <span style={{
                  fontSize: 13, fontWeight: 600,
                  color: tokenStatus === "valid" ? "#15803d" : "#b91c1c",
                }}>
                  {validated.message}
                </span>
              </div>

              {/* Workspace list */}
              {tokenStatus === "valid" && validated.workspaces?.length > 0 && (
                <div>
                  <div style={{ fontSize: 11, fontWeight: 600, color: "#374151",
                                marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.04em" }}>
                    Accessible Workspaces
                  </div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                    {validated.workspaces.map(ws => (
                      <div key={ws.id} style={{
                        display: "flex", alignItems: "center", gap: 6,
                        padding: "4px 10px", borderRadius: 20,
                        background: "#dcfce7", border: "1px solid #bbf7d0",
                        fontSize: 12, color: "#15803d", fontWeight: 500,
                      }}>
                        <LayoutDashboard size={11} />
                        {ws.name}
                      </div>
                    ))}
                    {validated.workspace_count > validated.workspaces.length && (
                      <span style={{ fontSize: 11, color: "#6b7280", padding: "4px 8px" }}>
                        +{validated.workspace_count - validated.workspaces.length} more
                      </span>
                    )}
                  </div>
                </div>
              )}

              {/* Not set hint */}
              {!validated.token_set && (
                <div style={{ fontSize: 12, color: "#6b7280" }}>
                  Save a token first, then validate.
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* ── Fabric API endpoints reference ───────────────────────────────────── */}
      <div className="card" style={{ padding: 22 }}>
        <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 12,
                     display: "flex", alignItems: "center", gap: 8 }}>
          <Cloud size={15} style={{ color: "var(--color-primary)" }} />
          Fabric API Endpoints Used
        </h3>
        <div style={{ display: "flex", flexDirection: "column", gap: 0,
                      border: "1px solid #e2e8f0", borderRadius: 10, overflow: "hidden" }}>
          {[
            { method: "GET",  path: "/api/fabric/validate-token",              note: "Verify token + list workspaces"      },
            { method: "GET",  path: "/api/fabric/workspaces",                   note: "List all accessible workspaces"      },
            { method: "GET",  path: "/api/fabric/workspaces/{id}/lakehouses",   note: "List lakehouses in a workspace"      },
            { method: "GET",  path: "/api/fabric/workspaces/{id}/items",        note: "List notebooks, pipelines, etc."     },
            { method: "GET",  path: "/api/fabric/connections",                  note: "List all shareable connections"      },
            { method: "GET",  path: "/api/fabric/connections/{id}",             note: "Verify a specific connection exists" },
            { method: "POST", path: "/api/fabric/create-connection",            note: "Create ERP shareable connection"     },
            { method: "POST", path: "/api/fabric/deploy",                       note: "Deploy notebooks + pipeline"         },
          ].map(({ method, path, note }, i, arr) => (
            <div key={path} style={{
              display: "flex", alignItems: "center", gap: 10,
              padding: "9px 14px",
              borderBottom: i < arr.length - 1 ? "1px solid #f1f5f9" : "none",
              background: "white",
            }}>
              <span style={{
                fontSize: 10, fontWeight: 700, padding: "2px 7px",
                borderRadius: 4, flexShrink: 0,
                background: method === "GET" ? "#dbeafe" : "#fce7f3",
                color:      method === "GET" ? "#1d4ed8" : "#9d174d",
              }}>
                {method}
              </span>
              <code style={{ fontSize: 11, color: "#0f172a", fontFamily: "monospace", flex: 1 }}>
                {path}
              </code>
              <span style={{ fontSize: 11, color: "#94a3b8" }}>{note}</span>
            </div>
          ))}
        </div>
        <p style={{ fontSize: 11, color: "#94a3b8", marginTop: 10 }}>
          All <code>POST /connections</code> and <code>POST /deploy</code> calls use
          {" "}<strong>Fabric REST API v1</strong> with LRO (Long-Running Operation) polling
          for async 202 responses.
        </p>
      </div>

      {/* ── How to get a token ───────────────────────────────────────────────── */}
      <div className="card" style={{ padding: 22 }}>
        <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 12,
                     display: "flex", alignItems: "center", gap: 8 }}>
          <AlertTriangle size={15} style={{ color: "#f59e0b" }} />
          How to Get an MS365 Bearer Token
        </h3>
        <div style={{ display: "flex", flexDirection: "column", gap: 10, fontSize: 13 }}>
          {[
            {
              step: "1",
              text: "Register an Azure AD App in the Azure Portal",
              sub:  "Azure Active Directory → App registrations → New registration",
            },
            {
              step: "2",
              text: "Grant Fabric API delegated permissions",
              sub:  "API permissions → Add: Workspace.ReadWrite.All, Item.ReadWrite.All",
            },
            {
              step: "3",
              text: "Use client credentials or device-code flow to obtain a Bearer token",
              sub:  "POST to https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
            },
            {
              step: "4",
              text: "Paste the access_token value above and save",
              sub:  "Token is stored in memory for this session only",
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

      {/* ── Backend URL ──────────────────────────────────────────────────────── */}
      <div className="card" style={{ padding: 22 }}>
        <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 10 }}>API Endpoint</h3>
        <div style={{
          display: "flex", alignItems: "center", gap: 8, fontSize: 12,
          padding: "8px 12px", background: "#f8fafc",
          border: "1px solid #e2e8f0", borderRadius: 8, fontFamily: "monospace",
        }}>
          <span style={{ color: "#94a3b8" }}>Backend:</span>
          <span style={{ color: "#0f172a", fontWeight: 600 }}>http://localhost:8001</span>
          <span style={{ marginLeft: "auto" }}>
            <span className="badge badge-cyan">FastAPI</span>
          </span>
        </div>
        <p style={{ fontSize: 11, color: "#94a3b8", marginTop: 8 }}>
          In development the Vite dev proxy forwards <code>/api</code> → port 8001.
          In Docker, the frontend nginx config proxies to the backend container.
        </p>
      </div>

    </div>
  );
}
