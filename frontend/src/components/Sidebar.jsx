import { NavLink, useLocation } from "react-router-dom";
import { useState, useEffect } from "react";
import {
  LayoutDashboard, PlugZap, Settings,
  ChevronLeft, ChevronRight, Zap,
} from "lucide-react";

const NAV_ITEMS = [
  { to: "/",           Icon: LayoutDashboard, label: "Dashboard"          },
  { to: "/erp-source", Icon: PlugZap,         label: "ERP Source Wizard"  },
  { to: "/settings",   Icon: Settings,        label: "Settings"           },
];

export default function Sidebar() {
  const location  = useLocation();
  const [collapsed, setCollapsed] = useState(() => {
    try { return localStorage.getItem("ilink_sidebar_collapsed") === "true"; }
    catch { return false; }
  });

  useEffect(() => {
    try { localStorage.setItem("ilink_sidebar_collapsed", String(collapsed)); }
    catch {}
  }, [collapsed]);

  return (
    <aside className="sidebar" style={{ width: collapsed ? 60 : 220 }}>
      {/* Logo */}
      <div className="sidebar-logo" style={{ minHeight: 64 }}>
        <div style={{
          width: 34, height: 34, borderRadius: 10, flexShrink: 0,
          background: "linear-gradient(135deg,#0891b2,#0e7490)",
          display: "flex", alignItems: "center", justifyContent: "center",
        }}>
          <Zap size={17} color="white" />
        </div>
        {!collapsed && (
          <div style={{ minWidth: 0 }}>
            <div className="sidebar-logo-name truncate">ilinkERP</div>
            <div className="sidebar-logo-sub truncate">Fabric Accelerate</div>
          </div>
        )}
      </div>

      {/* Nav */}
      <nav style={{ flex: 1, padding: "8px 0", overflowY: "auto", scrollbarWidth: "none" }}>
        {!collapsed && (
          <div className="nav-group-label">Navigation</div>
        )}
        {NAV_ITEMS.map(({ to, Icon, label }) => {
          const isActive = to === "/" ? location.pathname === "/" : location.pathname.startsWith(to);
          return (
            <NavLink
              key={to}
              to={to}
              title={collapsed ? label : undefined}
              className={`nav-link${isActive ? " active" : ""}`}
              style={collapsed ? { justifyContent: "center", padding: "10px" } : {}}
            >
              <Icon size={16} className="nav-icon" />
              {!collapsed && <span className="truncate">{label}</span>}
            </NavLink>
          );
        })}
      </nav>

      {/* Collapse toggle */}
      <button
        onClick={() => setCollapsed(c => !c)}
        style={{
          margin: "0 auto 12px",
          display: "flex", alignItems: "center", justifyContent: "center",
          width: 28, height: 28, borderRadius: "50%",
          background: "rgba(255,255,255,0.06)",
          border: "1px solid rgba(255,255,255,0.1)",
          color: "#64748b", cursor: "pointer",
        }}
        title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
      >
        {collapsed ? <ChevronRight size={12} /> : <ChevronLeft size={12} />}
      </button>

      {/* Footer */}
      {!collapsed && (
        <div style={{
          padding: "10px 16px",
          borderTop: "1px solid rgba(255,255,255,0.07)",
          fontSize: 11, color: "#475569",
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <span style={{ width: 6, height: 6, borderRadius: "50%", background: "#0891b2", flexShrink: 0 }} />
            v1.0 · Microsoft Fabric
          </div>
        </div>
      )}
    </aside>
  );
}
