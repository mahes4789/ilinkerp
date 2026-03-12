import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import axios from "axios";
import { Settings as SettingsIcon, Key, CheckCircle2, XCircle } from "lucide-react";
import toast from "react-hot-toast";

const API = import.meta.env.VITE_API_URL ?? "";

export default function Settings() {
  const [token, setToken]     = useState("");
  const [backend, setBackend] = useState(API || "http://localhost:8001");

  const setTokenMutation = useMutation({
    mutationFn: () =>
      axios.post(`${backend}/api/fabric/set-token`, { access_token: token }),
    onSuccess: () => toast.success("MS365 token saved — live Fabric deployment enabled"),
    onError: () => toast.error("Failed to save token"),
  });

  return (
    <div style={{ maxWidth: 680, display: "flex", flexDirection: "column", gap: 24 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
        <SettingsIcon size={26} style={{ color: "var(--color-primary)" }} />
        <div>
          <h1 style={{ fontSize: 22, fontWeight: 800, margin: 0 }}>Settings</h1>
          <p style={{ fontSize: 13, color: "var(--color-muted)", margin: 0 }}>
            Configure Microsoft Fabric authentication and API endpoints
          </p>
        </div>
      </div>

      {/* MS365 Token */}
      <div className="card" style={{ padding: 22 }}>
        <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 4, display: "flex", alignItems: "center", gap: 8 }}>
          <Key size={15} style={{ color: "var(--color-primary)" }} />
          Microsoft 365 / Fabric Access Token
        </h3>
        <p style={{ fontSize: 12, color: "var(--color-muted)", marginBottom: 14 }}>
          Paste a valid Azure AD Bearer token to enable live Fabric API calls (create connections, notebooks, pipelines).
          Without this token, all Fabric operations are simulated.
        </p>
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          <textarea
            value={token}
            onChange={e => setToken(e.target.value)}
            placeholder="eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9..."
            className="input"
            rows={4}
            style={{ fontFamily: "monospace", fontSize: 11, resize: "vertical" }}
          />
          <button
            className="btn-primary"
            disabled={!token || setTokenMutation.isPending}
            onClick={() => setTokenMutation.mutate()}
            style={{ alignSelf: "flex-start" }}
          >
            {setTokenMutation.isPending ? "Saving…" : "Save Token"}
          </button>
        </div>
      </div>

      {/* Backend URL info */}
      <div className="card" style={{ padding: 22 }}>
        <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 10 }}>API Endpoint</h3>
        <p style={{ fontSize: 12, color: "var(--color-muted)", marginBottom: 10 }}>
          The backend API serves ERP data dictionary and Fabric integration endpoints.
          In development, the Vite proxy forwards <code>/api</code> to port 8001.
        </p>
        <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 12,
                      padding: "8px 12px", background: "#f8fafc", border: "1px solid #e2e8f0",
                      borderRadius: 8, fontFamily: "monospace" }}>
          <span style={{ color: "#94a3b8" }}>Backend:</span>
          <span style={{ color: "#0f172a", fontWeight: 600 }}>http://localhost:8001</span>
          <span style={{ marginLeft: "auto" }}>
            <span className="badge badge-cyan">FastAPI</span>
          </span>
        </div>
      </div>

      {/* How to get token */}
      <div className="card" style={{ padding: 22 }}>
        <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 10 }}>How to Get an MS365 Token</h3>
        <div style={{ display: "flex", flexDirection: "column", gap: 8, fontSize: 13 }}>
          {[
            "Register an Azure AD App in the Azure Portal",
            "Grant Fabric API permissions: Workspace.ReadWrite.All, Item.ReadWrite.All",
            "Use the OAuth 2.0 client credentials or device code flow to obtain a Bearer token",
            "Paste the access_token value above to enable live Fabric operations",
          ].map((step, i) => (
            <div key={i} style={{ display: "flex", gap: 10, alignItems: "flex-start" }}>
              <span style={{
                width: 22, height: 22, borderRadius: "50%", flexShrink: 0,
                background: "var(--color-primary-light)", color: "var(--color-primary)",
                display: "flex", alignItems: "center", justifyContent: "center",
                fontSize: 11, fontWeight: 700, marginTop: 1,
              }}>
                {i + 1}
              </span>
              <span style={{ color: "#374151" }}>{step}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
