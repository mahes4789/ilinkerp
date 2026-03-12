import { useNavigate } from "react-router-dom";
import { useQuery }    from "@tanstack/react-query";
import axios           from "axios";
import {
  PlugZap, Server, Database, Cloud, ArrowRight,
  CheckCircle2, Zap, Globe, BookOpen,
} from "lucide-react";

const API = import.meta.env.VITE_API_URL ?? "";

export default function Dashboard() {
  const navigate = useNavigate();

  const { data: sourcesData } = useQuery({
    queryKey: ["sources"],
    queryFn:  () => axios.get(`${API}/api/erp/sources`).then(r => r.data),
    staleTime: Infinity,
  });

  const sources    = sourcesData?.sources ?? [];
  const supported  = sources.filter(s => s.supported);
  const comingSoon = sources.filter(s => !s.supported);

  const FEATURES = [
    { icon: Globe,    title: "12 ERP Systems",          desc: "Oracle, SAP, Dynamics, NetSuite, Workday and more" },
    { icon: Database, title: "50+ Modules",             desc: "AR, AP, GL, OM, INV, HCM, SCM and industry modules" },
    { icon: BookOpen, title: "Industry Data Dictionary", desc: "300+ standard tables mapped per ERP and module" },
    { icon: Cloud,    title: "Fabric API Integration",  desc: "Create connections, notebooks and pipelines in one click" },
  ];

  // Recent sessions from localStorage
  const recentSessions = (() => {
    try {
      return JSON.parse(localStorage.getItem("ilink_sessions") ?? "[]").slice(0, 5);
    } catch { return []; }
  })();

  return (
    <div className="space-y-8 fade-in">
      {/* Hero */}
      <div style={{
        background: "linear-gradient(135deg, #0c4a6e 0%, #0891b2 60%, #06b6d4 100%)",
        borderRadius: 16, padding: "36px 40px", color: "white", position: "relative", overflow: "hidden",
      }}>
        <div style={{ position: "absolute", right: 40, top: -20, opacity: 0.08, fontSize: 180, fontWeight: 900 }}>ilink</div>
        <div style={{ display: "flex", alignItems: "center", gap: 16, marginBottom: 12 }}>
          <div style={{
            width: 52, height: 52, borderRadius: 14,
            background: "rgba(255,255,255,0.15)",
            display: "flex", alignItems: "center", justifyContent: "center",
          }}>
            <Zap size={26} color="white" />
          </div>
          <div>
            <h1 style={{ fontSize: 26, fontWeight: 800, margin: 0, letterSpacing: "-0.5px" }}>
              ilinkERP Fabric Accelerate
            </h1>
            <p style={{ margin: 0, fontSize: 14, opacity: 0.8, marginTop: 4 }}>
              Connect any ERP system to Microsoft Fabric in minutes
            </p>
          </div>
        </div>
        <p style={{ fontSize: 14, opacity: 0.75, maxWidth: 600, marginBottom: 24 }}>
          Select your ERP source, configure the connection, discover tables against the industry-standard data dictionary,
          then deploy directly to Fabric — creating connections, notebooks, and pipelines automatically.
        </p>
        <button
          onClick={() => navigate("/erp-source")}
          className="btn-primary"
          style={{ background: "white", color: "#0891b2", fontWeight: 700, fontSize: 14 }}
        >
          <PlugZap size={16} /> Start New ERP Connection <ArrowRight size={14} />
        </button>
      </div>

      {/* Stats */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 16 }}>
        {FEATURES.map(({ icon: Icon, title, desc }) => (
          <div key={title} className="card" style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            <div style={{
              width: 38, height: 38, borderRadius: 10,
              background: "var(--color-primary-light)",
              display: "flex", alignItems: "center", justifyContent: "center",
            }}>
              <Icon size={18} style={{ color: "var(--color-primary)" }} />
            </div>
            <div>
              <div style={{ fontWeight: 700, fontSize: 14, marginBottom: 3 }}>{title}</div>
              <div style={{ fontSize: 12, color: "var(--color-muted)" }}>{desc}</div>
            </div>
          </div>
        ))}
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 24 }}>
        {/* Supported ERPs */}
        <div className="card">
          <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 16, display: "flex", alignItems: "center", gap: 8 }}>
            <Server size={16} style={{ color: "var(--color-primary)" }} />
            Supported ERP Systems ({supported.length})
          </h3>
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {supported.map(s => (
              <div key={s.id}
                   onClick={() => navigate("/erp-source")}
                   style={{
                     display: "flex", alignItems: "center", gap: 12,
                     padding: "8px 10px", borderRadius: 8,
                     cursor: "pointer", transition: "background 0.12s",
                   }}
                   onMouseEnter={e => e.currentTarget.style.background = "#f0f9ff"}
                   onMouseLeave={e => e.currentTarget.style.background = "transparent"}
              >
                <div style={{
                  width: 28, height: 28, borderRadius: 7,
                  background: s.color, flexShrink: 0,
                  display: "flex", alignItems: "center", justifyContent: "center",
                  fontSize: 11, fontWeight: 700, color: "white",
                }}>
                  {s.vendor.charAt(0)}
                </div>
                <div style={{ minWidth: 0 }}>
                  <div style={{ fontSize: 13, fontWeight: 600, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {s.name}
                  </div>
                  <div style={{ fontSize: 11, color: "var(--color-muted)" }}>{s.vendor}</div>
                </div>
                <CheckCircle2 size={14} style={{ color: "#0891b2", marginLeft: "auto", flexShrink: 0 }} />
              </div>
            ))}
          </div>
        </div>

        {/* How it works */}
        <div className="card">
          <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 16, display: "flex", alignItems: "center", gap: 8 }}>
            <Zap size={16} style={{ color: "var(--color-primary)" }} />
            How It Works
          </h3>
          <div style={{ display: "flex", flexDirection: "column", gap: 0 }}>
            {[
              { step: "01", title: "Select ERP & Module",         desc: "Choose from Oracle, SAP, Dynamics, NetSuite, Workday and more" },
              { step: "02", title: "Configure Connection",         desc: "Enter host, credentials, or Azure AD details for your ERP" },
              { step: "03", title: "Discover Tables",             desc: "Scan live ERP vs. industry data dictionary — see missing tables" },
              { step: "04", title: "Set Up Fabric Connection",    desc: "Choose connection type and configure your Fabric workspace" },
              { step: "05", title: "Deploy to Microsoft Fabric",  desc: "Create notebooks and pipeline automatically via Fabric REST API" },
            ].map(({ step, title, desc }, i, arr) => (
              <div key={step} style={{ display: "flex", gap: 14, paddingBottom: i < arr.length - 1 ? 16 : 0 }}>
                <div style={{ display: "flex", flexDirection: "column", alignItems: "center" }}>
                  <div style={{
                    width: 28, height: 28, borderRadius: "50%", flexShrink: 0,
                    background: "var(--color-primary-light)", color: "var(--color-primary)",
                    display: "flex", alignItems: "center", justifyContent: "center",
                    fontSize: 11, fontWeight: 700,
                  }}>
                    {step}
                  </div>
                  {i < arr.length - 1 && (
                    <div style={{ width: 2, flex: 1, minHeight: 16, background: "#e2e8f0", margin: "4px 0" }} />
                  )}
                </div>
                <div style={{ paddingTop: 4, minWidth: 0 }}>
                  <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 2 }}>{title}</div>
                  <div style={{ fontSize: 12, color: "var(--color-muted)" }}>{desc}</div>
                </div>
              </div>
            ))}
          </div>

          <button
            onClick={() => navigate("/erp-source")}
            className="btn-primary"
            style={{ marginTop: 20, width: "100%", justifyContent: "center" }}
          >
            <PlugZap size={15} /> Start ERP Source Wizard
          </button>
        </div>
      </div>

      {/* Coming soon */}
      {comingSoon.length > 0 && (
        <div className="card">
          <h3 style={{ fontSize: 13, fontWeight: 700, marginBottom: 12, color: "var(--color-muted)" }}>
            Coming Soon
          </h3>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
            {comingSoon.map(s => (
              <span key={s.id} className="badge badge-gray">{s.name}</span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
