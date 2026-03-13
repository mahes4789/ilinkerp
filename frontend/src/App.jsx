import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import Layout        from "./components/Layout";
import Dashboard     from "./pages/Dashboard";
import ERPSource     from "./pages/ERPSource";
import Settings      from "./pages/Settings";
import ERPComparison from "./pages/ERPComparison";

export default function App() {
  return (
    <BrowserRouter>
      <Layout>
        <Routes>
          <Route path="/"               element={<Dashboard />}     />
          <Route path="/erp-source"     element={<ERPSource />}     />
          <Route path="/erp-comparison" element={<ERPComparison />} />
          <Route path="/settings"       element={<Settings />}      />
          <Route path="*"               element={<Navigate to="/" replace />} />
        </Routes>
      </Layout>
    </BrowserRouter>
  );
}
