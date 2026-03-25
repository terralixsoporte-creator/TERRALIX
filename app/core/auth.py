# Script Control:
# - Role: Supabase authentication (login, password reset).
"""Modulo de autenticacion con Supabase Auth."""

import os
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client

# Asegura que config.env este cargado
_ENV_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "config.env"
if _ENV_PATH.is_file():
    load_dotenv(str(_ENV_PATH))


def _get_client() -> Client:
    """Crea y retorna el cliente Supabase."""
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_ANON_KEY", "")
    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL y SUPABASE_ANON_KEY deben estar configurados en config.env"
        )
    return create_client(url, key)


def sign_in(email: str, password: str) -> dict:
    """Inicia sesion con email y password.

    Returns:
        dict con "ok": True/False, "error": str si fallo,
        "user": datos del usuario si ok.
    """
    try:
        client = _get_client()
        response = client.auth.sign_in_with_password(
            {"email": email, "password": password}
        )
        return {
            "ok": True,
            "user": {
                "id": response.user.id,
                "email": response.user.email,
            },
        }
    except Exception as e:
        msg = str(e)
        if "Invalid login credentials" in msg:
            return {"ok": False, "error": "Email o contrasena incorrectos."}
        if "Email not confirmed" in msg:
            return {"ok": False, "error": "Email no confirmado. Revisa tu correo."}
        return {"ok": False, "error": f"Error de conexion: {msg}"}


_KEYRING_SERVICE = "TerralixERP"


def save_credentials(email: str, password: str) -> None:
    """Guarda credenciales en Windows Credential Manager."""
    import keyring
    keyring.set_password(_KEYRING_SERVICE, "email", email)
    keyring.set_password(_KEYRING_SERVICE, email, password)


def load_credentials() -> tuple[str, str]:
    """Carga credenciales guardadas. Retorna (email, password) o ('', '')."""
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
    """Elimina credenciales guardadas."""
    import keyring
    try:
        email = keyring.get_password(_KEYRING_SERVICE, "email") or ""
        if email:
            keyring.delete_password(_KEYRING_SERVICE, email)
        keyring.delete_password(_KEYRING_SERVICE, "email")
    except Exception:
        pass


_RESET_PORT = 8457
_RESET_REDIRECT = f"http://localhost:{_RESET_PORT}"


def reset_password(email: str) -> dict:
    """Envia un email de recuperacion de contrasena.

    Returns:
        dict con "ok": True/False, "error": str si fallo.
    """
    try:
        client = _get_client()
        client.auth.reset_password_email(
            email, {"redirect_to": _RESET_REDIRECT}
        )
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def update_password_with_token(access_token: str, new_password: str) -> dict:
    """Actualiza la contrasena usando un token de recuperacion.

    Returns:
        dict con "ok": True/False, "error": str si fallo.
    """
    try:
        url = os.getenv("SUPABASE_URL", "")
        key = os.getenv("SUPABASE_ANON_KEY", "")
        client = create_client(url, key)
        # Establecer sesion con el token de recuperacion
        client.auth.set_session(access_token, "")
        client.auth.update_user({"password": new_password})
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def start_reset_server(on_token_received) -> None:
    """Inicia un servidor HTTP local temporal para capturar el token de recuperacion.

    Cuando el usuario clickea el link del email, el navegador redirige a
    localhost:{port}/#access_token=...  El servidor sirve una pagina HTML
    que lee el fragment y lo envia de vuelta via POST.

    Args:
        on_token_received: callback(access_token: str) llamado cuando se recibe el token.
    """
    import threading
    from http.server import HTTPServer, BaseHTTPRequestHandler

    _HTML_PAGE = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Terralix - Recuperar contrasena</title>
<style>
  body { font-family: 'Segoe UI', sans-serif; display: flex; justify-content: center;
         align-items: center; min-height: 100vh; margin: 0; background: #f5f5f5; }
  .card { background: white; padding: 40px; border-radius: 12px;
          box-shadow: 0 2px 12px rgba(0,0,0,0.1); text-align: center; max-width: 400px; }
  h2 { color: #0F6645; margin-bottom: 10px; }
  p { color: #333; }
  .ok { color: #0F6645; font-weight: bold; }
  .err { color: #CC0000; }
</style></head>
<body><div class="card">
  <h2>Terralix ERP</h2>
  <p id="msg">Procesando enlace de recuperacion...</p>
</div>
<script>
  const hash = window.location.hash.substring(1);
  const params = new URLSearchParams(hash);
  const token = params.get('access_token');
  if (token) {
    fetch('/callback', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({access_token: token})
    }).then(() => {
      document.getElementById('msg').innerHTML =
        '<span class="ok">Token recibido. Vuelve a la aplicacion Terralix para establecer tu nueva contrasena.</span>';
    }).catch(() => {
      document.getElementById('msg').innerHTML =
        '<span class="err">Error al enviar el token. Intenta de nuevo.</span>';
    });
  } else {
    document.getElementById('msg').innerHTML =
      '<span class="err">No se encontro token de recuperacion en el enlace.</span>';
  }
</script></body></html>"""

    class ResetHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(_HTML_PAGE.encode("utf-8"))

        def do_POST(self):
            if self.path == "/callback":
                import json
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length)) if length else {}
                token = body.get("access_token", "")

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"ok":true}')

                if token:
                    on_token_received(token)
                    # Apagar servidor despues de recibir el token
                    threading.Thread(
                        target=lambda: self.server.shutdown(), daemon=True
                    ).start()
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            pass  # Silenciar logs del servidor

    def _run():
        try:
            server = HTTPServer(("127.0.0.1", _RESET_PORT), ResetHandler)
            server.timeout = 300  # 5 minutos maximo de espera
            server.serve_forever()
        except Exception:
            pass

    threading.Thread(target=_run, daemon=True).start()
