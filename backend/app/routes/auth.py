"""
Auth API — JWT login + user management (JSON file store).
Endpoints:
  POST /api/auth/login              → JWT access_token
  GET  /api/auth/me                 → current user info
  GET  /api/auth/users              → list users (admin only)
  POST /api/auth/users              → create user (admin only)
  PUT  /api/auth/users/{username}   → update user (admin only)
  DELETE /api/auth/users/{username} → delete user (admin only)
"""
from __future__ import annotations
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import bcrypt as _bcrypt
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from pydantic import BaseModel

router = APIRouter()

# ── Config ────────────────────────────────────────────────────────────────────
SECRET_KEY    = os.getenv("AUTH_SECRET_KEY", "ilink-erp-fabric-secret-key-2024")
ALGORITHM     = "HS256"
TOKEN_EXPIRE_H = 8
USERS_FILE    = Path(__file__).parent.parent / "users.json"

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

# ── Bootstrap default admin if no users.json ─────────────────────────────────
def _ensure_default_user():
    if not USERS_FILE.exists():
        hashed = _bcrypt.hashpw(b"admin@123", _bcrypt.gensalt(12)).decode()
        USERS_FILE.write_text(json.dumps({
            "admin": {
                "hashed_password": hashed,
                "role": "admin",
                "display_name": "Administrator",
            }
        }, indent=2))

_ensure_default_user()

# ── User store helpers ────────────────────────────────────────────────────────
def _load() -> dict:
    try:
        return json.loads(USERS_FILE.read_text())
    except Exception:
        return {}

def _save(users: dict):
    USERS_FILE.write_text(json.dumps(users, indent=2))

def _hash(pw: str) -> str:
    return _bcrypt.hashpw(pw.encode(), _bcrypt.gensalt(12)).decode()

def _check(pw: str, hashed: str) -> bool:
    try:
        return _bcrypt.checkpw(pw.encode(), hashed.encode())
    except Exception:
        return False

# ── JWT helpers ───────────────────────────────────────────────────────────────
def _make_token(username: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(hours=TOKEN_EXPIRE_H)
    return jwt.encode({"sub": username, "exp": exp}, SECRET_KEY, algorithm=ALGORITHM)

def _decode_token(token: str) -> str:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if not username:
            raise ValueError
        return username
    except (JWTError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

# ── FastAPI dependencies ──────────────────────────────────────────────────────
def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    username = _decode_token(token)
    users = _load()
    user  = users.get(username)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return {"username": username, "role": user.get("role", "viewer"), "display_name": user.get("display_name", username)}

def require_admin(current_user: dict = Depends(get_current_user)) -> dict:
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return current_user

# ── Request / Response models ─────────────────────────────────────────────────
class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    username: str
    role: str
    display_name: str

class UserCreate(BaseModel):
    username: str
    password: str
    display_name: str = ""
    role: str = "viewer"

class UserUpdate(BaseModel):
    password: Optional[str] = None
    display_name: Optional[str] = None
    role: Optional[str] = None

# ── Endpoints ─────────────────────────────────────────────────────────────────
@router.post("/login", response_model=TokenResponse)
def login(form: OAuth2PasswordRequestForm = Depends()):
    users = _load()
    user  = users.get(form.username)
    if not user or not _check(form.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return TokenResponse(
        access_token=_make_token(form.username),
        username=form.username,
        role=user.get("role", "viewer"),
        display_name=user.get("display_name", form.username),
    )

@router.get("/me")
def me(current_user: dict = Depends(get_current_user)):
    return current_user

@router.get("/users")
def list_users(admin: dict = Depends(require_admin)):
    users = _load()
    return [
        {"username": u, "display_name": d.get("display_name", u), "role": d.get("role", "viewer")}
        for u, d in users.items()
    ]

@router.post("/users", status_code=201)
def add_user(body: UserCreate, admin: dict = Depends(require_admin)):
    if not body.username or not body.password:
        raise HTTPException(400, "Username and password required")
    if body.role not in ("admin", "viewer"):
        raise HTTPException(400, "Role must be admin or viewer")
    users = _load()
    if body.username in users:
        raise HTTPException(400, f"User '{body.username}' already exists")
    users[body.username] = {
        "hashed_password": _hash(body.password),
        "role": body.role,
        "display_name": body.display_name or body.username,
    }
    _save(users)
    return {"message": f"User '{body.username}' created"}

@router.put("/users/{username}")
def edit_user(username: str, body: UserUpdate, admin: dict = Depends(require_admin)):
    users = _load()
    if username not in users:
        raise HTTPException(404, f"User '{username}' not found")
    if body.password:
        users[username]["hashed_password"] = _hash(body.password)
    if body.display_name is not None:
        users[username]["display_name"] = body.display_name
    if body.role is not None:
        if body.role not in ("admin", "viewer"):
            raise HTTPException(400, "Role must be admin or viewer")
        users[username]["role"] = body.role
    _save(users)
    return {"message": f"User '{username}' updated"}

@router.delete("/users/{username}")
def delete_user(username: str, admin: dict = Depends(require_admin)):
    if username == admin["username"]:
        raise HTTPException(400, "Cannot delete your own account")
    users = _load()
    if username not in users:
        raise HTTPException(404, f"User '{username}' not found")
    del users[username]
    _save(users)
    return {"message": f"User '{username}' deleted"}
