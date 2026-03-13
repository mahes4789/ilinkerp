import { BrowserRouter, Routes, Route, Navigate, useLocation } from "react-router-dom";
import { AuthProvider, useAuth } from "./context/AuthContext";
import Layout        from "./components/Layout";
import Dashboard     from "./pages/Dashboard";
import ERPSource     from "./pages/ERPSource";
import Settings      from "./pages/Settings";
import ERPComparison from "./pages/ERPComparison";
import Login         from "./pages/Login";

/** Redirect unauthenticated users to /login, preserving intended destination */
function RequireAuth({ children }) {
  const { user, isLoading } = useAuth();
  const location = useLocation();

  if (isLoading) {
    return (
      <div style={{
        minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center",
        background: "#0f172a",
      }}>
        <div style={{
          width: 36, height: 36, border: "3px solid rgba(8,145,178,0.3)",
          borderTopColor: "#0891b2", borderRadius: "50%",
          animation: "spin 0.7s linear infinite",
        }} />
        <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
      </div>
    );
  }

  if (!user) {
    return <Navigate to="/login" state={{ from: location }} replace />;
  }
  return children;
}

export default function App() {
  return (
    <AuthProvider>
      <BrowserRouter>
        <Routes>
          {/* Public */}
          <Route path="/login" element={<Login />} />

          {/* Protected — wrapped in Layout + RequireAuth */}
          <Route path="/" element={
            <RequireAuth><Layout><Dashboard /></Layout></RequireAuth>
          } />
          <Route path="/erp-source" element={
            <RequireAuth><Layout><ERPSource /></Layout></RequireAuth>
          } />
          <Route path="/erp-comparison" element={
            <RequireAuth><Layout><ERPComparison /></Layout></RequireAuth>
          } />
          <Route path="/settings" element={
            <RequireAuth><Layout><Settings /></Layout></RequireAuth>
          } />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </BrowserRouter>
    </AuthProvider>
  );
}
