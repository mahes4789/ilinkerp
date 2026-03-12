import { useLocation } from "react-router-dom";
import Sidebar from "./Sidebar";

const PAGE_TITLES = {
  "/":           "Dashboard",
  "/erp-source": "ERP Source Wizard",
  "/settings":   "Settings",
};

export default function Layout({ children }) {
  const location = useLocation();
  const title    = PAGE_TITLES[location.pathname]
                ?? PAGE_TITLES[Object.keys(PAGE_TITLES).find(k => k !== "/" && location.pathname.startsWith(k))]
                ?? "ilinkERP Fabric Accelerate";

  return (
    <div className="app-shell">
      <Sidebar />

      <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
        {/* Topbar */}
        <div className="topbar">
          <span className="topbar-title">{title}</span>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <span style={{
              fontSize: 11, fontWeight: 600, padding: "3px 10px",
              borderRadius: 20, background: "#cffafe", color: "#0e7490",
            }}>
              Microsoft Fabric Accelerate
            </span>
          </div>
        </div>

        {/* Page content */}
        <main className="main-content">
          <div className="page-wrapper">
            {children}
          </div>
        </main>
      </div>
    </div>
  );
}
