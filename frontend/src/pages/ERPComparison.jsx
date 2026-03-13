/**
 * ERPComparison — Cross-ERP Technology Table Comparison
 * Shows equivalent tables for a selected module across Oracle, SAP, Dynamics 365, etc.
 * Data is fetched live from vendor documentation (with 24h cache).
 */
import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import axios from "axios";
import { RefreshCw, Globe, CheckCircle2, AlertTriangle, Database, Table2, ExternalLink } from "lucide-react";
import toast from "react-hot-toast";

const API = import.meta.env.VITE_API_URL ?? "";

const MODULE_OPTIONS = [
  { key: "GL",  label: "General Ledger (GL/FI)" },
  { key: "AR",  label: "Accounts Receivable (AR)" },
  { key: "AP",  label: "Accounts Payable (AP)" },
  { key: "INV", label: "Inventory Management" },
  { key: "PO",  label: "Purchasing (PO/MM)" },
  { key: "OM",  label: "Order Management (OM/SD)" },
  { key: "HCM", label: "Human Capital Management" },
  { key: "FA",  label: "Fixed Assets (FA)" },
  { key: "CM",  label: "Cash Management (CM)" },
];

const VENDOR_COLORS = {
  Oracle:    "#cc2222",
  SAP:       "#0077cc",
  Microsoft: "#0078d4",
  Workday:   "#009fd9",
};

export default function ERPComparison() {
  const qc = useQueryClient();
  const [selectedModule, setSelectedModule] = useState("GL");
  const [focusSource,    setFocusSource]    = useState("");

  // ── Cross-comparison data ──────────────────────────────────────────────────
  const { data, isLoading, error } = useQuery({
    queryKey: ["cross-comparison", selectedModule],
    queryFn:  () =>
      axios.get(`${API}/api/erp/cross-comparison?module=${selectedModule}`)
           .then(r => r.data),
    staleTime: 5 * 60 * 1000,
  });

  // ── Cache status ──────────────────────────────────────────────────────────
  const { data: cacheStatus } = useQuery({
    queryKey: ["docs-cache-status"],
    queryFn:  () => axios.get(`${API}/api/erp/docs-cache-status`).then(r => r.data),
    staleTime: 60 * 1000,
  });

  // ── Refresh mutation ──────────────────────────────────────────────────────
  const refreshMut = useMutation({
    mutationFn: (src) =>
      axios.post(`${API}/api/erp/refresh-docs${src ? `?source=${src}` : ""}`),
    onSuccess: () => {
      toast.success("Documentation refreshed");
      qc.invalidateQueries({ queryKey: ["cross-comparison"] });
      qc.invalidateQueries({ queryKey: ["docs-cache-status"] });
    },
    onError: () => toast.error("Refresh failed"),
  });

  const sources = data?.sources ?? [];
  const displaySources = focusSource
    ? sources.filter(s => s.source_id === focusSource)
    : sources;

  return (
    <div style={{ padding: "24px 28px", maxWidth: 1400 }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 22, fontWeight: 700, color: "#0f172a", marginBottom: 6 }}>
          ERP Cross-Technology Comparison
        </h1>
        <p style={{ fontSize: 14, color: "#64748b", margin: 0 }}>
          Compare equivalent module tables across ERP platforms. Data fetched from official vendor documentation (24h cache).
        </p>
      </div>

      {/* Controls */}
      <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap", marginBottom: 24 }}>
        {/* Module selector */}
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <label style={{ fontSize: 13, fontWeight: 600, color: "#374151" }}>Module:</label>
          <select
            value={selectedModule}
            onChange={e => setSelectedModule(e.target.value)}
            style={{
              padding: "7px 12px", borderRadius: 8, border: "1px solid #e2e8f0",
              fontSize: 13, background: "white", color: "#0f172a", cursor: "pointer",
            }}
          >
            {MODULE_OPTIONS.map(m => (
              <option key={m.key} value={m.key}>{m.label}</option>
            ))}
          </select>
        </div>

        {/* Focus ERP */}
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <label style={{ fontSize: 13, fontWeight: 600, color: "#374151" }}>Focus ERP:</label>
          <select
            value={focusSource}
            onChange={e => setFocusSource(e.target.value)}
            style={{
              padding: "7px 12px", borderRadius: 8, border: "1px solid #e2e8f0",
              fontSize: 13, background: "white", color: "#0f172a", cursor: "pointer",
            }}
          >
            <option value="">All ERPs</option>
            {sources.map(s => (
              <option key={s.source_id} value={s.source_id}>{s.source_name}</option>
            ))}
          </select>
        </div>

        {/* Refresh button */}
        <button
          onClick={() => refreshMut.mutate(focusSource || undefined)}
          disabled={refreshMut.isPending}
          style={{
            display: "flex", alignItems: "center", gap: 6,
            padding: "7px 14px", borderRadius: 8,
            border: "1px solid #e2e8f0", background: "white",
            fontSize: 13, fontWeight: 600, color: "#374151",
            cursor: refreshMut.isPending ? "not-allowed" : "pointer",
          }}
        >
          <RefreshCw size={13} className={refreshMut.isPending ? "spin" : ""} />
          {refreshMut.isPending ? "Refreshing\u2026" : "Refresh Docs"}
        </button>

        {/* Total tables */}
        {data && (
          <span style={{ fontSize: 13, color: "#64748b", marginLeft: "auto" }}>
            {data.total_tables} tables across {sources.length} ERPs
          </span>
        )}
      </div>

      {/* Cache status bar */}
      {cacheStatus && (
        <div style={{
          display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 20,
          padding: "10px 14px", background: "#f8fafc",
          border: "1px solid #e2e8f0", borderRadius: 10,
        }}>
          {Object.entries(cacheStatus.sources ?? {}).map(([srcId, st]) => (
            <div key={srcId} style={{
              display: "flex", alignItems: "center", gap: 4,
              fontSize: 11, color: st.stale ? "#f59e0b" : "#10b981",
              padding: "2px 8px", borderRadius: 20,
              background: st.stale ? "#fef3c7" : "#d1fae5",
            }}>
              {st.stale
                ? <AlertTriangle size={10} />
                : <CheckCircle2 size={10} />}
              {srcId.replace("_", " ")}
              {" \u00b7 "}{st.table_count} tables
              {st.stale ? " \u00b7 stale" : ` \u00b7 ${Math.round(st.age_hours)}h ago`}
            </div>
          ))}
        </div>
      )}

      {/* Loading */}
      {isLoading && (
        <div style={{ display: "flex", justifyContent: "center", padding: 60 }}>
          <RefreshCw size={20} className="spin" style={{ color: "#6366f1" }} />
        </div>
      )}

      {/* Error */}
      {error && (
        <div style={{
          padding: 16, background: "#fef2f2", border: "1px solid #fecaca",
          borderRadius: 10, color: "#b91c1c", fontSize: 13,
        }}>
          Failed to load comparison data. Please try refreshing.
        </div>
      )}

      {/* Comparison grid */}
      {!isLoading && !error && (
        <div style={{ display: "grid", gridTemplateColumns: `repeat(auto-fill, minmax(320px, 1fr))`, gap: 16 }}>
          {displaySources.map(src => (
            <div key={src.source_id} style={{
              background: "white", borderRadius: 12,
              border: `2px solid ${src.table_count > 0 ? "#e2e8f0" : "#f1f5f9"}`,
              overflow: "hidden",
            }}>
              {/* Source header */}
              <div style={{
                padding: "12px 16px",
                background: src.table_count > 0 ? "#f8fafc" : "#f1f5f9",
                borderBottom: "1px solid #e2e8f0",
                display: "flex", alignItems: "center", justifyContent: "space-between",
              }}>
                <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                  <div style={{
                    width: 28, height: 28, borderRadius: 7, flexShrink: 0,
                    background: VENDOR_COLORS[src.vendor] ?? "#888",
                    display: "flex", alignItems: "center", justifyContent: "center",
                    fontSize: 12, fontWeight: 700, color: "white",
                  }}>
                    {src.vendor.charAt(0)}
                  </div>
                  <div>
                    <div style={{ fontSize: 13, fontWeight: 700, color: "#0f172a" }}>
                      {src.source_name}
                    </div>
                    <div style={{ fontSize: 11, color: "#64748b" }}>{src.vendor}</div>
                  </div>
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                  {src.live
                    ? <span style={{ fontSize: 10, color: "#10b981", fontWeight: 600 }}>{"\u25cf"} LIVE</span>
                    : <span style={{ fontSize: 10, color: "#f59e0b", fontWeight: 600 }}>{"\u25cf"} CACHED</span>}
                  <span style={{
                    fontSize: 11, fontWeight: 700,
                    background: src.table_count > 0 ? "#dbeafe" : "#f1f5f9",
                    color:      src.table_count > 0 ? "#1d4ed8" : "#94a3b8",
                    padding: "2px 8px", borderRadius: 20,
                  }}>
                    {src.table_count} tables
                  </span>
                </div>
              </div>

              {/* Table list */}
              {src.table_count === 0 ? (
                <div style={{ padding: 16, fontSize: 13, color: "#94a3b8", textAlign: "center" }}>
                  No {selectedModule} tables found for this ERP
                </div>
              ) : (
                <div style={{ maxHeight: 340, overflowY: "auto" }}>
                  {src.tables.map((tbl, i) => (
                    <div key={tbl.table_name + i} style={{
                      padding: "9px 16px",
                      borderBottom: i < src.tables.length - 1 ? "1px solid #f1f5f9" : "none",
                      display: "flex", alignItems: "flex-start", gap: 10,
                    }}>
                      <Table2 size={13} style={{ color: "#94a3b8", marginTop: 2, flexShrink: 0 }} />
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{
                          fontSize: 12, fontWeight: 700, color: "#0f172a",
                          fontFamily: "monospace",
                        }}>
                          {tbl.table_name}
                          {tbl.is_core && (
                            <span style={{
                              marginLeft: 6, fontSize: 9, fontWeight: 600,
                              background: "#dbeafe", color: "#1d4ed8",
                              padding: "1px 5px", borderRadius: 10,
                            }}>CORE</span>
                          )}
                        </div>
                        <div style={{
                          fontSize: 11, color: "#64748b", marginTop: 2,
                          whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis",
                        }}>
                          {tbl.description}
                        </div>
                      </div>
                      {tbl.doc_url && (
                        <a href={tbl.doc_url} target="_blank" rel="noopener noreferrer"
                           style={{ color: "#94a3b8", flexShrink: 0 }}>
                          <ExternalLink size={11} />
                        </a>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
