/**
 * Login page for ilinkERP Fabric Accelerate.
 * Default credentials: admin / admin@123
 */
import { useState } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { Zap, Eye, EyeOff, LogIn } from "lucide-react";
import { useAuth } from "../context/AuthContext";

export default function Login() {
  const { login } = useAuth();
  const navigate  = useNavigate();
  const location  = useLocation();
  const from      = location.state?.from?.pathname || "/";

  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [showPw,   setShowPw]   = useState(false);
  const [error,    setError]    = useState("");
  const [loading,  setLoading]  = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      await login(username, password);
      navigate(from, { replace: true });
    } catch (err) {
      setError(err.response?.data?.detail || "Invalid username or password. Please try again.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{
      minHeight: "100vh",
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      background: "linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%)",
      padding: 24,
    }}>
      <div style={{
        width: "100%",
        maxWidth: 420,
        background: "white",
        borderRadius: 16,
        overflow: "hidden",
        boxShadow: "0 25px 50px rgba(0,0,0,0.4)",
      }}>
        {/* Header */}
        <div style={{
          padding: "32px 36px 28px",
          background: "linear-gradient(135deg, #0891b2 0%, #0e7490 100%)",
          textAlign: "center",
        }}>
          <div style={{
            width: 52, height: 52, borderRadius: 14, margin: "0 auto 16px",
            background: "rgba(255,255,255,0.15)",
            display: "flex", alignItems: "center", justifyContent: "center",
          }}>
            <Zap size={26} color="white" />
          </div>
          <h1 style={{ color: "white", fontSize: 22, fontWeight: 700, margin: 0 }}>
            ilinkERP
          </h1>
          <p style={{ color: "rgba(255,255,255,0.75)", fontSize: 13, margin: "4px 0 0" }}>
            Fabric Accelerate
          </p>
        </div>

        {/* Form */}
        <div style={{ padding: "32px 36px" }}>
          <h2 style={{ fontSize: 18, fontWeight: 700, color: "#0f172a", marginBottom: 6 }}>
            Sign in
          </h2>
          <p style={{ fontSize: 13, color: "#64748b", marginBottom: 24 }}>
            Enter your credentials to access the dashboard.
          </p>

          {error && (
            <div style={{
              padding: "10px 14px", marginBottom: 20,
              background: "#fef2f2", border: "1px solid #fecaca",
              borderRadius: 8, color: "#b91c1c", fontSize: 13,
              display: "flex", alignItems: "center", gap: 8,
            }}>
              ⚠ {error}
            </div>
          )}

          <form onSubmit={handleSubmit} style={{ display: "flex", flexDirection: "column", gap: 16 }}>
            <div>
              <label style={{ display: "block", fontSize: 13, fontWeight: 600, color: "#374151", marginBottom: 6 }}>
                Username
              </label>
              <input
                type="text"
                autoComplete="username"
                placeholder="e.g. admin"
                value={username}
                onChange={e => setUsername(e.target.value)}
                required
                style={{
                  width: "100%", padding: "10px 14px", boxSizing: "border-box",
                  border: "1.5px solid #e2e8f0", borderRadius: 8, fontSize: 14,
                  outline: "none", color: "#0f172a", background: "white",
                  transition: "border-color 0.15s",
                }}
                onFocus={e => e.target.style.borderColor = "#0891b2"}
                onBlur={e => e.target.style.borderColor = "#e2e8f0"}
              />
            </div>

            <div>
              <label style={{ display: "block", fontSize: 13, fontWeight: 600, color: "#374151", marginBottom: 6 }}>
                Password
              </label>
              <div style={{ position: "relative" }}>
                <input
                  type={showPw ? "text" : "password"}
                  autoComplete="current-password"
                  placeholder="••••••••"
                  value={password}
                  onChange={e => setPassword(e.target.value)}
                  required
                  style={{
                    width: "100%", padding: "10px 42px 10px 14px", boxSizing: "border-box",
                    border: "1.5px solid #e2e8f0", borderRadius: 8, fontSize: 14,
                    outline: "none", color: "#0f172a", background: "white",
                    transition: "border-color 0.15s",
                  }}
                  onFocus={e => e.target.style.borderColor = "#0891b2"}
                  onBlur={e => e.target.style.borderColor = "#e2e8f0"}
                />
                <button
                  type="button"
                  onClick={() => setShowPw(p => !p)}
                  style={{
                    position: "absolute", right: 12, top: "50%", transform: "translateY(-50%)",
                    background: "none", border: "none", cursor: "pointer", color: "#94a3b8", padding: 2,
                  }}
                >
                  {showPw ? <EyeOff size={16} /> : <Eye size={16} />}
                </button>
              </div>
            </div>

            <button
              type="submit"
              disabled={loading || !username || !password}
              style={{
                width: "100%", padding: "11px 0", marginTop: 4,
                background: loading ? "#94a3b8" : "linear-gradient(135deg, #0891b2, #0e7490)",
                color: "white", border: "none", borderRadius: 8,
                fontSize: 14, fontWeight: 600, cursor: loading ? "not-allowed" : "pointer",
                display: "flex", alignItems: "center", justifyContent: "center", gap: 8,
                transition: "opacity 0.15s",
              }}
            >
              {loading ? (
                <>
                  <span style={{ width: 16, height: 16, border: "2px solid rgba(255,255,255,0.3)", borderTopColor: "white", borderRadius: "50%", display: "inline-block", animation: "spin 0.7s linear infinite" }} />
                  Signing in…
                </>
              ) : (
                <><LogIn size={16} /> Sign in</>
              )}
            </button>
          </form>

          <p style={{ textAlign: "center", fontSize: 12, color: "#94a3b8", marginTop: 20 }}>
            Contact your administrator to get access.
          </p>
        </div>
      </div>

      <style>{`
        @keyframes spin { to { transform: rotate(360deg); } }
      `}</style>
    </div>
  );
}
