/**
 * ERPComparison — Cross-ERP Technology Comparison
 * Tab 1: Table Comparison — equivalent tables per module across ERP platforms
 * Tab 2: Field Comparison — logical field → ERP column mapping with CSV/Excel download
 */
import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import axios from "axios";
import {
  RefreshCw, Globe, CheckCircle2, AlertTriangle, Table2, ExternalLink,
  Download, Columns, Database,
} from "lucide-react";
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

const FIELD_MODULES = [
  { key: "GL",  label: "General Ledger" },
  { key: "AR",  label: "Accounts Receivable" },
  { key: "AP",  label: "Accounts Payable" },
  { key: "INV", label: "Inventory" },
];

const FIELD_ERPS = [
  { id: "oracle_fusion",    label: "Oracle Fusion",  short: "Oracle Fusion"  },
  { id: "oracle_ebs",       label: "Oracle EBS",     short: "Oracle EBS"     },
  { id: "sap_s4hana",       label: "SAP S/4HANA",    short: "SAP S/4HANA"   },
  { id: "sap_ecc",          label: "SAP ECC",         short: "SAP ECC"        },
  { id: "dynamics_365_fo",  label: "Dynamics 365 FO", short: "D365 FO"        },
  { id: "dynamics_bc",      label: "Dynamics BC",     short: "Dynamics BC"    },
  { id: "netsuite",         label: "NetSuite",         short: "NetSuite"       },
  { id: "workday",          label: "Workday",          short: "Workday"        },
];

const VENDOR_COLORS = {
  Oracle:    "#cc2222",
  SAP:       "#0077cc",
  Microsoft: "#0078d4",
  Workday:   "#009fd9",
};

const CATEGORY_COLORS = {
  Identity:    "#6366f1",
  Date:        "#0891b2",
  Financial:   "#10b981",
  Reference:   "#f59e0b",
  Descriptive: "#8b5cf6",
  Status:      "#ec4899",
};

// ── CSV export ────────────────────────────────────────────────────────────────
function downloadCSV(fields, module) {
  const headers = ["Logical Name", "Category", "Description",
    ...FIELD_ERPS.map(e => `${e.label} Table`),
    ...FIELD_ERPS.map(e => `${e.label} Column`),
    ...FIELD_ERPS.map(e => `${e.label} Type`),
  ];

  const rows = fields.map(f => {
    const tableCols = FIELD_ERPS.map(e => f.mappings?.[e.id]?.table ?? "");
    const colCols   = FIELD_ERPS.map(e => f.mappings?.[e.id]?.column ?? "");
    const typeCols  = FIELD_ERPS.map(e => f.mappings?.[e.id]?.type ?? "");
    return [f.logical_name, f.category, f.description, ...tableCols, ...colCols, ...typeCols];
  });

  const csv = [headers, ...rows]
    .map(r => r.map(v => `"${String(v).replace(/"/g, '""')}"`).join(","))
    .join("\n");

  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement("a");
  a.href     = url;
  a.download = `field_comparison_${module}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

// ── Excel export (SheetJS) ────────────────────────────────────────────────────
async function downloadExcel(fields, module) {
  let xlsx;
  try {
    xlsx = await import("xlsx");
  } catch {
    toast.error("xlsx library not installed. Run: npm install xlsx");
    return;
  }

  const rows = fields.map(f => {
    const row = { "Logical Name": f.logical_name, Category: f.category, Description: f.description };
    FIELD_ERPS.forEach(e => {
      const m = f.mappings?.[e.id];
      row[`${e.short} Table`]  = m?.table  ?? "";
      row[`${e.short} Column`] = m?.column ?? "";
      row[`${e.short} Type`]   = m?.type   ?? "";
      row[`${e.short} Key`]    = m?.is_key ? "Yes" : (m ? "No" : "");
    });
    return row;
  });

  const wb = xlsx.utils.book_new();
  const ws = xlsx.utils.json_to_sheet(rows);
  xlsx.utils.book_append_sheet(wb, ws, `${module} Fields`);
  xlsx.writeFile(wb, `field_comparison_${module}.xlsx`);
}

// ── Main component ────────────────────────────────────────────────────────────
export default function ERPComparison() {
  const qc = useQueryClient();
  const [activeTab,      setActiveTab]      = useState("table");   // "table" | "field"
  const [selectedModule, setSelectedModule] = useState("GL");
  const [fieldModule,    setFieldModule]    = useState("GL");
  const [focusSource,    setFocusSource]    = useState("");

  // ── Table comparison data ──────────────────────────────────────────────────
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

  // ── Field comparison data ─────────────────────────────────────────────────
  const { data: fieldData, isLoading: fieldLoading } = useQuery({
    queryKey: ["field-comparison", fieldModule],
    queryFn:  () =>
      axios.get(`${API}/api/erp/field-comparison?module=${fieldModule}`)
           .then(r => r.data),
    staleTime: 10 * 60 * 1000,
    enabled: activeTab === "field",
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

  const sources        = data?.sources ?? [];
  const displaySources = focusSource
    ? sources.filter(s => s.source_id === focusSource)
    : sources;
  const fields = fieldData?.fields ?? [];

  return (
    <div style={{ padding: "24px 28px", maxWidth: 1400 }}>
      {/* Header */}
      <div style={{ marginBottom: 20 }}>
        <h1 style={{ fontSize: 22, fontWeight: 700, color: "#0f172a", marginBottom: 6 }}>
          ERP Cross-Technology Comparison
        </h1>
        <p style={{ fontSize: 14, color: "#64748b", margin: 0 }}>
          Compare equivalent tables and fields across ERP platforms.
        </p>
      </div>

      {/* Tab bar */}
      <div style={{
        display: "flex", gap: 2, marginBottom: 24,
        background: "#f1f5f9", borderRadius: 10, padding: 4,
        width: "fit-content",
      }}>
        {[
          { key: "table", label: "Table Comparison", Icon: Database },
          { key: "field", label: "Field Comparison",  Icon: Columns  },
        ].map(({ key, label, Icon }) => (
          <button
            key={key}
            onClick={() => setActiveTab(key)}
            style={{
              display: "flex", alignItems: "center", gap: 6,
              padding: "7px 16px", borderRadius: 8, border: "none",
              fontSize: 13, fontWeight: 600, cursor: "pointer",
              background: activeTab === key ? "white" : "transparent",
              color: activeTab === key ? "#0891b2" : "#64748b",
              boxShadow: activeTab === key ? "0 1px 3px rgba(0,0,0,0.1)" : "none",
              transition: "all 0.15s",
            }}
          >
            <Icon size={14} />
            {label}
          </button>
        ))}
      </div>

      {/* ══ TABLE COMPARISON TAB ══ */}
      {activeTab === "table" && (
        <>
          {/* Controls */}
          <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap", marginBottom: 24 }}>
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
              {refreshMut.isPending ? "Refreshing…" : "Refresh Docs"}
            </button>

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
                  {st.stale ? <AlertTriangle size={10} /> : <CheckCircle2 size={10} />}
                  {srcId.replace("_", " ")}
                  {" · "}{st.table_count} tables
                  {st.stale ? " · stale" : ` · ${Math.round(st.age_hours)}h ago`}
                </div>
              ))}
            </div>
          )}

          {isLoading && (
            <div style={{ display: "flex", justifyContent: "center", padding: 60 }}>
              <RefreshCw size={20} className="spin" style={{ color: "#6366f1" }} />
            </div>
          )}

          {error && (
            <div style={{
              padding: 16, background: "#fef2f2", border: "1px solid #fecaca",
              borderRadius: 10, color: "#b91c1c", fontSize: 13,
            }}>
              Failed to load comparison data. Please try refreshing.
            </div>
          )}

          {!isLoading && !error && (
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(320px, 1fr))", gap: 16 }}>
              {displaySources.map(src => (
                <div key={src.source_id} style={{
                  background: "white", borderRadius: 12,
                  border: `2px solid ${src.table_count > 0 ? "#e2e8f0" : "#f1f5f9"}`,
                  overflow: "hidden",
                }}>
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
                        ? <span style={{ fontSize: 10, color: "#10b981", fontWeight: 600 }}>● LIVE</span>
                        : <span style={{ fontSize: 10, color: "#f59e0b", fontWeight: 600 }}>● CACHED</span>}
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
                            <div style={{ fontSize: 12, fontWeight: 700, color: "#0f172a", fontFamily: "monospace" }}>
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
        </>
      )}

      {/* ══ FIELD COMPARISON TAB ══ */}
      {activeTab === "field" && (
        <>
          {/* Controls */}
          <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap", marginBottom: 20 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <label style={{ fontSize: 13, fontWeight: 600, color: "#374151" }}>Module:</label>
              <select
                value={fieldModule}
                onChange={e => setFieldModule(e.target.value)}
                style={{
                  padding: "7px 12px", borderRadius: 8, border: "1px solid #e2e8f0",
                  fontSize: 13, background: "white", color: "#0f172a", cursor: "pointer",
                }}
              >
                {FIELD_MODULES.map(m => (
                  <option key={m.key} value={m.key}>{m.label}</option>
                ))}
              </select>
            </div>

            <div style={{ marginLeft: "auto", display: "flex", gap: 8 }}>
              <button
                onClick={() => downloadCSV(fields, fieldModule)}
                disabled={fields.length === 0}
                style={{
                  display: "flex", alignItems: "center", gap: 6,
                  padding: "7px 14px", borderRadius: 8,
                  border: "1px solid #e2e8f0", background: "white",
                  fontSize: 13, fontWeight: 600, color: "#374151",
                  cursor: fields.length === 0 ? "not-allowed" : "pointer",
                  opacity: fields.length === 0 ? 0.5 : 1,
                }}
              >
                <Download size={13} /> Download CSV
              </button>
              <button
                onClick={() => downloadExcel(fields, fieldModule)}
                disabled={fields.length === 0}
                style={{
                  display: "flex", alignItems: "center", gap: 6,
                  padding: "7px 14px", borderRadius: 8,
                  border: "1px solid #10b981", background: "#f0fdf4",
                  fontSize: 13, fontWeight: 600, color: "#059669",
                  cursor: fields.length === 0 ? "not-allowed" : "pointer",
                  opacity: fields.length === 0 ? 0.5 : 1,
                }}
              >
                <Download size={13} /> Download Excel
              </button>
            </div>
          </div>

          {fieldLoading && (
            <div style={{ display: "flex", justifyContent: "center", padding: 60 }}>
              <RefreshCw size={20} className="spin" style={{ color: "#6366f1" }} />
            </div>
          )}

          {!fieldLoading && fields.length === 0 && (
            <div style={{
              padding: 32, textAlign: "center",
              background: "#f8fafc", border: "1px solid #e2e8f0",
              borderRadius: 12, color: "#94a3b8", fontSize: 14,
            }}>
              No field mapping data available for this module.
            </div>
          )}

          {!fieldLoading && fields.length > 0 && (
            <div style={{ background: "white", borderRadius: 12, border: "1px solid #e2e8f0", overflow: "hidden" }}>
              {/* Field count */}
              <div style={{
                padding: "12px 16px", background: "#f8fafc",
                borderBottom: "1px solid #e2e8f0",
                display: "flex", alignItems: "center", justifyContent: "space-between",
              }}>
                <span style={{ fontSize: 13, fontWeight: 600, color: "#374151" }}>
                  {fieldData?.module} — {fields.length} logical fields mapped across {FIELD_ERPS.length} ERP platforms
                </span>
                <span style={{ fontSize: 11, color: "#94a3b8" }}>
                  KEY = primary/unique key field
                </span>
              </div>

              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                  <thead>
                    <tr style={{ background: "#f1f5f9" }}>
                      <th style={{
                        padding: "10px 14px", textAlign: "left", fontWeight: 700,
                        color: "#374151", borderBottom: "2px solid #e2e8f0",
                        position: "sticky", left: 0, background: "#f1f5f9", zIndex: 1,
                        minWidth: 160,
                      }}>
                        Logical Field
                      </th>
                      <th style={{
                        padding: "10px 12px", textAlign: "left", fontWeight: 700,
                        color: "#374151", borderBottom: "2px solid #e2e8f0",
                        minWidth: 90,
                      }}>
                        Category
                      </th>
                      {FIELD_ERPS.map(erp => (
                        <th key={erp.id} style={{
                          padding: "10px 12px", textAlign: "left", fontWeight: 700,
                          color: "#374151", borderBottom: "2px solid #e2e8f0",
                          minWidth: 160, whiteSpace: "nowrap",
                        }}>
                          {erp.label}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {fields.map((field, idx) => {
                      const coveredCount = FIELD_ERPS.filter(e => field.mappings?.[e.id]).length;
                      return (
                        <tr key={field.logical_name} style={{
                          background: idx % 2 === 0 ? "white" : "#fafafa",
                          borderBottom: "1px solid #f1f5f9",
                        }}>
                          {/* Logical name */}
                          <td style={{
                            padding: "10px 14px",
                            position: "sticky", left: 0,
                            background: idx % 2 === 0 ? "white" : "#fafafa",
                            zIndex: 1,
                          }}>
                            <div style={{ fontWeight: 700, color: "#0f172a", fontSize: 12 }}>
                              {field.logical_name}
                            </div>
                            <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 2 }}>
                              {field.description}
                            </div>
                            <div style={{
                              marginTop: 4, fontSize: 10, fontWeight: 600,
                              color: coveredCount === FIELD_ERPS.length ? "#10b981" : "#f59e0b",
                            }}>
                              {coveredCount}/{FIELD_ERPS.length} ERPs
                            </div>
                          </td>

                          {/* Category */}
                          <td style={{ padding: "10px 12px" }}>
                            <span style={{
                              fontSize: 10, fontWeight: 700, padding: "2px 8px",
                              borderRadius: 20,
                              background: `${CATEGORY_COLORS[field.category] ?? "#6366f1"}18`,
                              color: CATEGORY_COLORS[field.category] ?? "#6366f1",
                            }}>
                              {field.category}
                            </span>
                          </td>

                          {/* ERP columns */}
                          {FIELD_ERPS.map(erp => {
                            const m = field.mappings?.[erp.id];
                            if (!m) {
                              return (
                                <td key={erp.id} style={{ padding: "10px 12px" }}>
                                  <span style={{ fontSize: 11, color: "#cbd5e1" }}>—</span>
                                </td>
                              );
                            }
                            return (
                              <td key={erp.id} style={{ padding: "10px 12px" }}>
                                <div style={{
                                  fontFamily: "monospace", fontSize: 11,
                                  color: "#1d4ed8", fontWeight: 600,
                                }}>
                                  {m.table}
                                </div>
                                <div style={{
                                  fontFamily: "monospace", fontSize: 11,
                                  color: "#0f172a", marginTop: 1,
                                  display: "flex", alignItems: "center", gap: 4,
                                }}>
                                  {m.column}
                                  {m.is_key && (
                                    <span style={{
                                      fontSize: 8, fontWeight: 700,
                                      background: "#fef3c7", color: "#b45309",
                                      padding: "1px 4px", borderRadius: 4,
                                    }}>KEY</span>
                                  )}
                                </div>
                                <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 1 }}>
                                  {m.type}
                                  {m.nullable === false && (
                                    <span style={{ marginLeft: 4, color: "#ef4444" }}>NN</span>
                                  )}
                                </div>
                              </td>
                            );
                          })}
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}
