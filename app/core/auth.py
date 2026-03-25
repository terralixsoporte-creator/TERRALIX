# Script Control:
# - Role: Supabase authentication (login, password reset).
"""
Autenticacion contra Supabase Auth REST API usando requests.
Se evita la libreria supabase-py que tiene conflictos con anyio/httpx
cuando la app esta empaquetada con PyInstaller.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.append(str(_ROOT))

from app.core.paths import get_env_path as _get_env_path

load_dotenv(str(_get_env_path()), override=False)

# ── Fix certificados SSL para ejecutable frozen ────────────────────────────
def _fix_ssl() -> None:
    try:
        import certifi
        bundle = certifi.where()
        os.environ.setdefault("SSL_CERT_FILE",      bundle)
        os.environ.setdefault("REQUESTS_CA_BUNDLE", bundle)
    except Exception:
        pass

_fix_ssl()

_TIMEOUT_CONNECT = 6
_TIMEOUT_READ = 20

# ── Log de debug a archivo (permite diagnosticar el exe frozen) ────────────
import logging as _logging

def _log(msg: str, level: str = "info") -> None:
    try:
        from app.core.paths import get_logs_dir
        log_file = get_logs_dir() / "auth_debug.log"
        logger = _logging.getLogger("terralix.auth")
        if not logger.handlers:
            fh = _logging.FileHandler(str(log_file), encoding="utf-8")
            fh.setFormatter(_logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            logger.addHandler(fh)
            logger.setLevel(_logging.DEBUG)
        getattr(logger, level)(msg)
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────
# Helpers internos
# ──────────────────────────────────────────────────────────────────────────

def _get_config() -> tuple[str, str]:
    """Devuelve (url, anon_key) o lanza RuntimeError."""
    url = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
    key = os.getenv("SUPABASE_ANON_KEY", "").strip()
    _log(f"_get_config: url={'SET' if url else 'EMPTY'} key={'SET' if key else 'EMPTY'}")
    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL y SUPABASE_ANON_KEY deben estar configurados en config.env"
        )
    return url, key


def _headers(extra: dict | None = None, token: str = "") -> dict:
    _, key = _get_config()
    h = {
        "apikey":       key,
        "Content-Type": "application/json",
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    if extra:
        h.update(extra)
    return h


def _post(endpoint: str, payload: dict, token: str = "") -> dict:
    """POST a la Supabase Auth REST API. Devuelve el JSON de respuesta."""
    import requests as _req
    url, _ = _get_config()
    resp = _req.post(
        f"{url}/auth/v1/{endpoint}",
        json=payload,
        headers=_headers(token=token),
        timeout=(_TIMEOUT_CONNECT, _TIMEOUT_READ),
    )
    try:
        data = resp.json()
    except Exception:
        txt = (resp.text or "").strip()
        data = {"error": f"HTTP {resp.status_code}: {txt[:240] or 'Respuesta no-JSON'}"}

    if resp.status_code >= 400:
        data.setdefault("error", f"HTTP {resp.status_code}")
        data.setdefault("http_status", resp.status_code)
    return data


# ──────────────────────────────────────────────────────────────────────────
# API pública
# ──────────────────────────────────────────────────────────────────────────

def sign_in(email: str, password: str) -> dict:
    """
    Inicia sesion con email y password via Supabase Auth REST.
    Timeout: 20 s. Devuelve {"ok": True, "user": {...}} o {"ok": False, "error": "..."}.
    """
    try:
        _log(
            f"sign_in: request timeout connect={_TIMEOUT_CONNECT}s read={_TIMEOUT_READ}s"
        )
        try:
            import certifi
            _log(f"sign_in: certifi={certifi.where()}")
        except Exception as ce:
            _log(f"sign_in: certifi error={ce}", "warning")

        data = _post("token?grant_type=password", {"email": email, "password": password})
        _log(f"sign_in: respuesta keys={list(data.keys())}")

        # Supabase devuelve "error" o "error_description" si falla
        if "error" in data or "error_description" in data:
            msg = (
                data.get("error_description")
                or data.get("msg")
                or data.get("error")
                or "Error desconocido"
            )
            err_code = str(data.get("error_code") or data.get("error") or "")
            _log(f"sign_in: error Supabase={msg}", "error")
            if "Invalid login credentials" in msg or "invalid_grant" in err_code or "invalid_credentials" in err_code:
                return {"ok": False, "error": "Email o contrasena incorrectos."}
            if "Email not confirmed" in msg or "email_not_confirmed" in err_code:
                return {"ok": False, "error": "Email no confirmado. Revisa tu correo."}
            return {"ok": False, "error": msg}

        user = data.get("user") or {}
        _log(f"sign_in: OK email={user.get('email')}")
        return {
            "ok":   True,
            "user": {"id": user.get("id", ""), "email": user.get("email", email)},
        }

    except Exception as e:
        import requests as _req
        msg = str(e)
        _log(f"sign_in: excepcion={type(e).__name__}: {msg}", "error")
        if isinstance(e, _req.exceptions.Timeout) or "timeout" in msg.lower():
            return {"ok": False, "error": "Tiempo de conexion agotado. Verifica tu internet."}
        if isinstance(e, _req.exceptions.RequestException):
            return {"ok": False, "error": "No se pudo conectar. Revisa internet, proxy o firewall."}
        return {"ok": False, "error": "No se pudo conectar. Revisa internet, proxy o firewall."}


def reset_password(email: str) -> dict:
    """Envia email de recuperacion de contrasena."""
    try:
        _, _ = _get_config()
        _post("recover", {"email": email})
        return {"ok": True}
    except Exception as e:
        import requests as _req
        if isinstance(e, _req.exceptions.Timeout):
            return {"ok": False, "error": "Tiempo de conexion agotado."}
        return {"ok": False, "error": str(e)}


def update_password_with_token(access_token: str, new_password: str) -> dict:
    """Actualiza la contrasena usando un access_token de recuperacion."""
    import requests as _req
    try:
        url, _ = _get_config()
        resp = _req.put(
            f"{url}/auth/v1/user",
            json={"password": new_password},
            headers=_headers(token=access_token),
            timeout=(_TIMEOUT_CONNECT, _TIMEOUT_READ),
        )
        data = resp.json()
        if "error" in data:
            return {"ok": False, "error": data.get("error_description") or data["error"]}
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ──────────────────────────────────────────────────────────────────────────
# Credenciales guardadas (Windows Credential Manager)
# ──────────────────────────────────────────────────────────────────────────

_KEYRING_SERVICE = "TerralixERP"


def save_credentials(email: str, password: str) -> None:
    import keyring
    keyring.set_password(_KEYRING_SERVICE, "email", email)
    keyring.set_password(_KEYRING_SERVICE, email, password)


def load_credentials() -> tuple[str, str]:
    import keyring
    try:
        email = keyring.get_password(_KEYRING_SERVICE, "email") or ""
        if email:
            password = keyring.get_password(_KEYRING_SERVICE, email) or ""
            return email, password
    except Exception:
        pass
    return "", ""


def clear_credentials() -> None:
    import keyring
    try:
        email = keyring.get_password(_KEYRING_SERVICE, "email") or ""
        if email:
            keyring.delete_password(_KEYRING_SERVICE, email)
        keyring.delete_password(_KEYRING_SERVICE, "email")
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────
# Servidor local para reset de contrasena
# ──────────────────────────────────────────────────────────────────────────

_RESET_PORT     = 8457
_RESET_REDIRECT = f"http://localhost:{_RESET_PORT}"


def start_reset_server(on_token_received) -> None:
    """Servidor HTTP temporal que captura el token del link de recuperacion."""
    import threading
    from http.server import HTTPServer, BaseHTTPRequestHandler

    _HTML_PAGE = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Terralix - Recuperar contrasena</title>
<style>
  body{font-family:'Segoe UI',sans-serif;display:flex;justify-content:center;
       align-items:center;min-height:100vh;margin:0;background:#f5f5f5;}
  .card{background:white;padding:40px;border-radius:12px;
        box-shadow:0 2px 12px rgba(0,0,0,.1);text-align:center;max-width:400px;}
  h2{color:#0F6645;margin-bottom:10px;}
  .ok{color:#0F6645;font-weight:bold;} .err{color:#CC0000;}
</style></head>
<body><div class="card">
  <h2>Terralix ERP</h2>
  <p id="msg">Procesando enlace de recuperacion...</p>
</div>
<script>
  const params=new URLSearchParams(window.location.hash.substring(1));
  const token=params.get('access_token');
  if(token){
    fetch('/callback',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({access_token:token})
    }).then(()=>{
      document.getElementById('msg').innerHTML=
        '<span class="ok">Token recibido. Vuelve a Terralix para escribir tu nueva contrasena.</span>';
    }).catch(()=>{
      document.getElementById('msg').innerHTML=
        '<span class="err">Error al enviar el token. Intenta de nuevo.</span>';
    });
  } else {
    document.getElementById('msg').innerHTML=
      '<span class="err">No se encontro token en el enlace.</span>';
  }
</script></body></html>"""

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(_HTML_PAGE.encode("utf-8"))

        def do_POST(self):
            if self.path == "/callback":
                import json
                length = int(self.headers.get("Content-Length", 0))
                body   = json.loads(self.rfile.read(length)) if length else {}
                token  = body.get("access_token", "")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"ok":true}')
                if token:
                    on_token_received(token)
                    threading.Thread(
                        target=lambda: self.server.shutdown(), daemon=True
                    ).start()
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, *_):
            pass

    def _run():
        try:
            server = HTTPServer(("127.0.0.1", _RESET_PORT), _Handler)
            server.timeout = 300
            server.serve_forever()
        except Exception:
            pass

    threading.Thread(target=_run, daemon=True).start()
