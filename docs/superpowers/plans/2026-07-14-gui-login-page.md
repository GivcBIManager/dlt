# GUI Login Page Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `?token=` / `OASIS_GUI_TOKEN` auth in the OASIS GUI with a username/password login page backed by a signed Flask session cookie.

**Architecture:** `gui/security.py` gains credential helpers (`gui_credentials`, `credentials_match`, `load_or_create_secret_key`) and loses the token helpers. `gui/app.py` swaps its token gate for a session gate with `/login` + `/logout` routes; the signing key persists in `gui/state/secret_key`. A standalone `login.html` template renders the form; `base.html` gets a sign-out button.

**Tech Stack:** Python 3.13, Flask (built-in `session`), pytest. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-07-14-gui-login-page-design.md`

## Global Constraints

- Credentials come ONLY from `OASIS_GUI_USER` + `OASIS_GUI_PASSWORD` env vars (both required, non-blank).
- Loopback clients (127.0.0.0/8, ::1, `localhost`) bypass auth entirely — unchanged behavior.
- After this change NO code path reads `request.args.get("token")`, `X-Auth-Token`, `Authorization: Bearer`, or the `oasis_token` cookie. `?token=` in a URL must have zero effect.
- All credential comparisons use `hmac.compare_digest` (constant-time).
- Secret key file: `gui/state/secret_key`, 32 random bytes, hardened with existing `security.harden_file` (0600 where supported). `gui/state/` is already gitignored.
- Mutating requests must still be `application/json` (CSRF rule), with `/login` and `/logout` as the only form-POST exceptions.
- Run tests from the repo root with: `python -m pytest tests/<file> -v` (conftest adds `gui/` to `sys.path`, so tests import `security` and `app` flat).

---

### Task 1: security.py — credential helpers replace token helpers

**Files:**
- Modify: `gui/security.py`
- Test: `tests/test_security.py`

**Interfaces:**
- Produces: `gui_credentials() -> tuple[str, str] | None`
- Produces: `credentials_match(user: str | None, password: str | None, expected: tuple[str, str] | None) -> bool`
- Produces: `load_or_create_secret_key(path: Path | str) -> bytes`
- Produces: `check_bind(host: str, credentials: tuple[str, str] | None) -> None` (changed signature meaning: second arg is now the credentials tuple)
- Produces: `request_authorized(remote_addr: str | None, logged_in: bool) -> bool` (changed signature: 2 args)
- Removes: `gui_token()`, `token_matches()` — Task 2 must not reference them.

- [ ] **Step 1: Rewrite the security tests to describe the new API**

Replace the entire contents of `tests/test_security.py` with:

```python
"""Tests for the GUI security gate (bind safety, auth, command lockdown)."""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("OASIS_GUI_USER", raising=False)
    monkeypatch.delenv("OASIS_GUI_PASSWORD", raising=False)
    monkeypatch.delenv("OASIS_ALLOW_CUSTOM_CMD", raising=False)


# --- is_loopback ----------------------------------------------------------- #
@pytest.mark.parametrize("addr", ["127.0.0.1", "127.5.6.7", "::1", "localhost"])
def test_is_loopback_true(addr):
    import security
    assert security.is_loopback(addr) is True


@pytest.mark.parametrize("addr", ["0.0.0.0", "10.0.0.4", "192.168.1.5", "8.8.8.8", None, ""])
def test_is_loopback_false(addr):
    import security
    assert security.is_loopback(addr) is False


# --- gui_credentials ------------------------------------------------------- #
def test_gui_credentials_unset_is_none():
    import security
    assert security.gui_credentials() is None


def test_gui_credentials_requires_both(monkeypatch):
    import security
    monkeypatch.setenv("OASIS_GUI_USER", "admin")
    assert security.gui_credentials() is None
    monkeypatch.delenv("OASIS_GUI_USER")
    monkeypatch.setenv("OASIS_GUI_PASSWORD", "pw")
    assert security.gui_credentials() is None


def test_gui_credentials_set(monkeypatch):
    import security
    monkeypatch.setenv("OASIS_GUI_USER", "  admin  ")
    monkeypatch.setenv("OASIS_GUI_PASSWORD", "s3cret")
    assert security.gui_credentials() == ("admin", "s3cret")


def test_gui_credentials_blank_password_is_none(monkeypatch):
    import security
    monkeypatch.setenv("OASIS_GUI_USER", "admin")
    monkeypatch.setenv("OASIS_GUI_PASSWORD", "   ")
    assert security.gui_credentials() is None


# --- credentials_match ----------------------------------------------------- #
def test_credentials_match_ok():
    import security
    assert security.credentials_match("admin", "pw", ("admin", "pw")) is True


@pytest.mark.parametrize("user,password", [
    ("admin", "wrong"), ("wrong", "pw"), ("", "pw"), ("admin", ""), (None, None),
])
def test_credentials_match_rejects(user, password):
    import security
    assert security.credentials_match(user, password, ("admin", "pw")) is False


def test_credentials_match_no_expected():
    import security
    assert security.credentials_match("admin", "pw", None) is False


# --- load_or_create_secret_key --------------------------------------------- #
def test_secret_key_created_and_persisted(tmp_path):
    import security
    path = tmp_path / "secret_key"
    key = security.load_or_create_secret_key(path)
    assert len(key) == 32
    assert path.read_bytes() == key
    # second call returns the same key
    assert security.load_or_create_secret_key(path) == key


def test_secret_key_regenerated_when_corrupt(tmp_path):
    import security
    path = tmp_path / "secret_key"
    path.write_bytes(b"short")  # < 16 bytes: treated as corrupt
    key = security.load_or_create_secret_key(path)
    assert len(key) == 32
    assert path.read_bytes() == key


# --- check_bind (fail-closed on public bind without credentials) ----------- #
def test_check_bind_allows_loopback_without_credentials():
    import security
    security.check_bind("127.0.0.1", None)  # must not raise


def test_check_bind_rejects_public_without_credentials():
    import security
    with pytest.raises(RuntimeError):
        security.check_bind("0.0.0.0", None)


def test_check_bind_allows_public_with_credentials():
    import security
    security.check_bind("0.0.0.0", ("admin", "pw"))  # must not raise


# --- debugger_allowed ------------------------------------------------------ #
def test_debugger_allowed_only_on_loopback():
    import security
    assert security.debugger_allowed("127.0.0.1") is True
    assert security.debugger_allowed("0.0.0.0") is False


# --- request_authorized ---------------------------------------------------- #
def test_request_authorized_loopback_without_login():
    import security
    assert security.request_authorized("127.0.0.1", False) is True


def test_request_authorized_public_requires_login():
    import security
    assert security.request_authorized("10.0.0.9", False) is False
    assert security.request_authorized("10.0.0.9", True) is True


# --- custom command lockdown ----------------------------------------------- #
def test_custom_commands_disabled_by_default():
    import security
    assert security.custom_commands_allowed() is False


def test_custom_commands_enabled_by_env(monkeypatch):
    import security
    monkeypatch.setenv("OASIS_ALLOW_CUSTOM_CMD", "1")
    assert security.custom_commands_allowed() is True
```

- [ ] **Step 2: Run the tests to verify the new ones fail**

Run: `python -m pytest tests/test_security.py -v`
Expected: FAIL — `AttributeError: module 'security' has no attribute 'gui_credentials'` (and friends). The `is_loopback` / `debugger_allowed` / custom-command tests still pass.

- [ ] **Step 3: Rewrite gui/security.py**

Replace lines 1–68 of `gui/security.py` (docstring through `token_matches`) with the following. Keep `SECRET_BACKUPS_KEEP`, `harden_file`, and `prune_backups` unchanged, and replace `request_authorized` at the bottom (shown after).

```python
"""GUI security gate: bind safety, request auth, and command lockdown.

The control panel can launch processes and edit pipeline config, so exposing it
on a non-loopback interface without authentication is a remote-code-execution
risk. This module centralises the guard rails:

* ``check_bind`` fails closed when bound to a public interface with no
  credentials configured.
* ``request_authorized`` allows local (loopback) use with no login but requires
  a signed-in session for any non-loopback client.
* ``custom_commands_allowed`` keeps the free-form ``custom`` run script off by
  default (it runs arbitrary argv).

Configuration (all optional):
* ``OASIS_GUI_USER`` / ``OASIS_GUI_PASSWORD``  login credentials required for
  non-loopback clients (both must be set).
* ``OASIS_ALLOW_CUSTOM_CMD`` set to ``1`` to permit the ``custom`` run script.
"""
from __future__ import annotations

import hmac
import ipaddress
import os
import secrets
from pathlib import Path

_LOOPBACK_NAMES = {"localhost", "localhost.localdomain"}


def is_loopback(addr: str | None) -> bool:
    """True for loopback hosts (127.0.0.0/8, ::1, ``localhost``)."""
    if not addr:
        return False
    if addr in _LOOPBACK_NAMES:
        return True
    try:
        return ipaddress.ip_address(addr).is_loopback
    except ValueError:
        return False


def gui_credentials() -> tuple[str, str] | None:
    """The configured (user, password) pair, or ``None`` unless both are set."""
    user = (os.environ.get("OASIS_GUI_USER") or "").strip()
    password = os.environ.get("OASIS_GUI_PASSWORD") or ""
    if not user or not password.strip():
        return None
    return user, password


def credentials_match(user: str | None, password: str | None,
                      expected: tuple[str, str] | None) -> bool:
    """Constant-time credential comparison; False if anything is missing."""
    if not user or not password or not expected:
        return False
    ok_user = hmac.compare_digest(str(user), expected[0])
    ok_password = hmac.compare_digest(str(password), expected[1])
    return ok_user and ok_password


def load_or_create_secret_key(path: Path | str) -> bytes:
    """Return the persisted session-signing key, creating it on first use.

    A short or unreadable file is treated as corrupt and regenerated (existing
    sessions are invalidated, users just sign in again) — never crash over it.
    """
    path = Path(path)
    try:
        data = path.read_bytes()
        if len(data) >= 16:
            return data
    except OSError:
        pass
    key = secrets.token_bytes(32)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(key)
        harden_file(path)
    except OSError:
        pass  # ephemeral key: sessions won't survive a restart, but the app runs
    return key


def custom_commands_allowed() -> bool:
    """Whether the free-form ``custom`` run script is permitted."""
    return (os.environ.get("OASIS_ALLOW_CUSTOM_CMD") or "").strip() == "1"


def check_bind(host: str, credentials: tuple[str, str] | None) -> None:
    """Fail closed: refuse a non-loopback bind unless credentials are configured."""
    if not is_loopback(host) and not credentials:
        raise RuntimeError(
            f"Refusing to bind the GUI to {host!r} without authentication. "
            "Set OASIS_GUI_USER and OASIS_GUI_PASSWORD, or bind to 127.0.0.1."
        )


def debugger_allowed(host: str) -> bool:
    """The Werkzeug debugger is an RCE vector; only allow it on a loopback bind."""
    return is_loopback(host)
```

And replace the old `request_authorized` (currently the last function in the file) with:

```python
def request_authorized(remote_addr: str | None, logged_in: bool) -> bool:
    """Authorize a request.

    Loopback clients are trusted (single-user local tool). Any other client
    must have signed in through the login page (session flag). When no
    credentials are configured the deployment is loopback-only (guaranteed by
    ``check_bind``), so only local clients reach here.
    """
    if is_loopback(remote_addr):
        return True
    return logged_in
```

- [ ] **Step 4: Run the security tests**

Run: `python -m pytest tests/test_security.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add gui/security.py tests/test_security.py
git commit -m "feat(gui): credential helpers replace token helpers in security module"
```

Note: `tests/test_app_auth.py` is now broken (it references `OASIS_GUI_TOKEN` behavior via `security.gui_token` used in `app.py`, which no longer exists). Task 2 fixes app.py and those tests together — this intermediate commit is expected to have a red `test_app_auth.py`.

---

### Task 2: app.py — session gate, /login, /logout; delete every token path

**Files:**
- Modify: `gui/app.py:19` (imports), `gui/app.py:40-43` (app setup), `gui/app.py:70-115` (security gate), `gui/app.py:657-671` (`main()`)
- Test: `tests/test_app_auth.py`

**Interfaces:**
- Consumes: `security.gui_credentials()`, `security.credentials_match(user, password, expected)`, `security.load_or_create_secret_key(path)`, `security.request_authorized(remote_addr, logged_in)`, `security.check_bind(host, credentials)` from Task 1.
- Produces: routes `GET/POST /login` (endpoint function `page_login`), `POST /logout` (endpoint `logout`). Task 3's templates post to these.
- Produces: `render_template("login.html", error=...)` — Task 3 creates that template. Until Task 3, `/login` raises TemplateNotFound; the Task 2 tests below avoid asserting on the login page body, only on status codes and redirects, EXCEPT `test_login_wrong_credentials` and `test_login_page_renders` which need the template — so Task 2 Step 3 also creates a minimal placeholder `gui/templates/login.html` that Task 3 replaces with the styled version.

- [ ] **Step 1: Rewrite the app auth tests**

Replace the entire contents of `tests/test_app_auth.py` with:

```python
"""Auth gate (login sessions) + CSRF content-type checks on the Flask app."""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("OASIS_GUI_USER", raising=False)
    monkeypatch.delenv("OASIS_GUI_PASSWORD", raising=False)
    monkeypatch.delenv("OASIS_ALLOW_CUSTOM_CMD", raising=False)


@pytest.fixture
def client():
    import app as gui_app
    return gui_app.app.test_client()


REMOTE = {"REMOTE_ADDR": "10.0.0.5"}  # simulate a non-loopback client


def _set_creds(monkeypatch):
    monkeypatch.setenv("OASIS_GUI_USER", "admin")
    monkeypatch.setenv("OASIS_GUI_PASSWORD", "s3cret")


def _login(client, username="admin", password="s3cret"):
    return client.post("/login", data={"username": username, "password": password},
                       environ_base=REMOTE)


# --- auth ------------------------------------------------------------------ #
def test_loopback_allowed_without_login(client):
    # test_client defaults to REMOTE_ADDR=127.0.0.1
    resp = client.get("/api/runs")
    assert resp.status_code == 200


def test_remote_api_blocked_without_login(client, monkeypatch):
    _set_creds(monkeypatch)
    resp = client.get("/api/runs", environ_base=REMOTE)
    assert resp.status_code == 401


def test_remote_page_redirects_to_login(client, monkeypatch):
    _set_creds(monkeypatch)
    resp = client.get("/", environ_base=REMOTE,
                      headers={"Accept": "text/html"})
    assert resp.status_code == 302
    assert "/login" in resp.headers["Location"]


def test_login_page_renders_for_remote(client, monkeypatch):
    _set_creds(monkeypatch)
    resp = client.get("/login", environ_base=REMOTE)
    assert resp.status_code == 200


def test_login_wrong_credentials_rejected(client, monkeypatch):
    _set_creds(monkeypatch)
    resp = _login(client, password="wrong")
    assert resp.status_code == 401
    # still unauthorized afterwards
    assert client.get("/api/runs", environ_base=REMOTE).status_code == 401


def test_login_success_grants_session(client, monkeypatch):
    _set_creds(monkeypatch)
    resp = _login(client)
    assert resp.status_code == 302
    assert client.get("/api/runs", environ_base=REMOTE).status_code == 200


def test_login_rejected_when_no_credentials_configured(client):
    # No env vars set: nobody can log in remotely (loopback-only deployment).
    resp = _login(client)
    assert resp.status_code == 401


def test_logout_clears_session(client, monkeypatch):
    _set_creds(monkeypatch)
    _login(client)
    resp = client.post("/logout", environ_base=REMOTE)
    assert resp.status_code == 302
    assert client.get("/api/runs", environ_base=REMOTE).status_code == 401


# --- the old token paths must be dead -------------------------------------- #
def test_query_token_has_no_effect(client, monkeypatch):
    _set_creds(monkeypatch)
    resp = client.get("/api/runs?token=s3cret", environ_base=REMOTE)
    assert resp.status_code == 401
    resp = client.get("/api/runs?token=admin", environ_base=REMOTE)
    assert resp.status_code == 401


def test_header_token_has_no_effect(client, monkeypatch):
    _set_creds(monkeypatch)
    resp = client.get("/api/runs", environ_base=REMOTE,
                      headers={"X-Auth-Token": "s3cret"})
    assert resp.status_code == 401
    resp = client.get("/api/runs", environ_base=REMOTE,
                      headers={"Authorization": "Bearer s3cret"})
    assert resp.status_code == 401


def test_old_cookie_has_no_effect(client, monkeypatch):
    _set_creds(monkeypatch)
    client.set_cookie("oasis_token", "s3cret")
    resp = client.get("/api/runs", environ_base=REMOTE)
    assert resp.status_code == 401


def test_healthz_exempt_from_auth(client, monkeypatch):
    _set_creds(monkeypatch)
    resp = client.get("/healthz", environ_base=REMOTE)
    assert resp.status_code == 200


# --- CSRF: mutating requests must be JSON (blocks cross-site form posts) ---- #
def test_json_post_allowed(client):
    resp = client.post("/api/command/preview", json={"script": "oracle_to_iceberg"})
    assert resp.status_code == 200


def test_form_post_rejected(client):
    resp = client.post("/api/command/preview", data={"script": "oracle_to_iceberg"},
                       content_type="application/x-www-form-urlencoded")
    assert resp.status_code == 415


def test_login_form_post_not_subject_to_json_rule(client, monkeypatch):
    _set_creds(monkeypatch)
    resp = _login(client)
    assert resp.status_code in (302, 401)  # never 415


def test_get_not_subject_to_csrf(client):
    resp = client.get("/api/runs")
    assert resp.status_code == 200
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/test_app_auth.py -v`
Expected: FAIL — `app.py` still calls `security.gui_token()` which no longer exists, so the import/collection or every request errors.

- [ ] **Step 3: Rewrite the auth section of gui/app.py and add a placeholder login template**

3a. Change the Flask import line (`gui/app.py:19`) to:

```python
from flask import (Flask, Response, jsonify, redirect, render_template, request,  # noqa: E402
                   session, url_for)
```

(If `Response` is no longer referenced after removing `_persist_token_cookie`, drop it from the import — check with a grep for `Response` in app.py first.)

3b. After `app = Flask(__name__)` / `ensure_dirs()` (lines 40–42), configure sessions:

```python
app = Flask(__name__)
runner = RunManager()
ensure_dirs()
# Signed-session key persists across restarts so logins survive a server bounce.
app.secret_key = security.load_or_create_secret_key(config.STATE_DIR / "secret_key")
app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE="Lax")
```

3c. Replace the whole security-gate block (`gui/app.py:70-115` — the `_AUTH_EXEMPT_PATHS` constants, `_provided_token`, `_auth_gate`, and `_persist_token_cookie`) with:

```python
# --------------------------------------------------------------------------- #
# Security gate: session login for non-loopback clients, JSON-only mutations
# --------------------------------------------------------------------------- #
_AUTH_EXEMPT_PATHS = {"/healthz", "/login"}
# Login/logout are browser form posts; the session cookie is SameSite=Lax so a
# cross-site form cannot ride an existing session.
_CSRF_EXEMPT_PATHS = {"/login", "/logout"}
_MUTATING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}


@app.before_request
def _auth_gate():
    path = request.path
    if path.startswith("/static/") or path in _AUTH_EXEMPT_PATHS:
        return None
    if not security.request_authorized(request.remote_addr, "user" in session):
        # Browsers land on the login page; API clients get a JSON 401.
        if request.method == "GET" and request.accept_mimetypes.accept_html:
            return redirect(url_for("page_login"))
        return jsonify({"error": "unauthorized"}), 401
    # CSRF: a cross-site <form> POST cannot set an application/json content-type
    # without triggering a CORS preflight, so require JSON on mutating requests.
    if request.method in _MUTATING_METHODS and path not in _CSRF_EXEMPT_PATHS:
        if not request.is_json:
            return jsonify({"error": "mutating requests must be application/json"}), 415
    return None
```

3d. Add the login/logout routes right after the gate (before the `# Pages` section):

```python
@app.route("/login", methods=["GET", "POST"])
def page_login():
    already_in = security.request_authorized(request.remote_addr, "user" in session)
    if request.method == "GET":
        if already_in:
            return redirect(url_for("page_dashboard"))
        return render_template("login.html", error=None)
    creds = security.gui_credentials()
    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""
    if security.credentials_match(username, password, creds):
        session["user"] = username
        return redirect(url_for("page_dashboard"))
    return render_template("login.html", error="Invalid username or password"), 401


@app.post("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("page_login"))
```

3e. In `main()` (`gui/app.py:657-671`), replace the token lines:

```python
def main() -> None:
    host = os.environ.get("OASIS_GUI_HOST", "127.0.0.1")
    port = int(os.environ.get("OASIS_GUI_PORT", "8765"))
    # Fail closed: refuse to expose the panel on a public interface unless
    # login credentials are configured (it can launch processes and edit config).
    security.check_bind(host, security.gui_credentials())
    # The Werkzeug debugger is an RCE vector; never enable it on a public bind.
    debug = os.environ.get("OASIS_GUI_DEBUG", "0") == "1"
    if debug and not security.debugger_allowed(host):
        print(f"[warn] debugger disabled: refusing debug mode on public bind {host!r}")
        debug = False
    if not security.is_loopback(host):
        print("[info] login required for non-loopback clients")
```

(the rest of `main()` is unchanged.)

3f. Create a minimal placeholder `gui/templates/login.html` so the routes render (Task 3 replaces it with the styled page):

```html
<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>Sign in</title></head>
<body>
  <form method="post" action="/login">
    {% if error %}<p>{{ error }}</p>{% endif %}
    <input name="username" autocomplete="username">
    <input name="password" type="password" autocomplete="current-password">
    <button type="submit">Sign in</button>
  </form>
</body>
</html>
```

3g. Verify no token remnants: `grep -n "token" gui/app.py` must return no auth-related hits (only unrelated words if any).

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_app_auth.py tests/test_security.py -v`
Expected: all PASS.

- [ ] **Step 5: Run the full suite to catch collateral breakage**

Run: `python -m pytest tests/ -v`
Expected: all PASS (other suites don't touch auth, but `app.py` import changes could break e.g. `tests/test_app_runs_endpoint.py`).

- [ ] **Step 6: Commit**

```bash
git add gui/app.py gui/templates/login.html tests/test_app_auth.py
git commit -m "feat(gui): session login page replaces URL token auth"
```

---

### Task 3: Styled login page + sign-out button

**Files:**
- Modify: `gui/templates/login.html` (replace Task 2's placeholder)
- Modify: `gui/templates/base.html:71-73` (topbar-right)
- Modify: `gui/static/style.css` (append login styles)

**Interfaces:**
- Consumes: `POST /login` with form fields `username`, `password`; `error` template variable; `POST /logout` (from Task 2).
- Consumes: Flask's `session` object is available in Jinja templates by default (`session.get('user')`).

- [ ] **Step 1: Replace login.html with the styled page**

Replace the entire contents of `gui/templates/login.html` with:

```html
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Sign in - HNH ETLPipeline Manager</title>
  <link rel="icon" type="image/svg+xml" href="{{ url_for('static', filename='logo-mark.svg') }}">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="{{ url_for('static', filename='style.css') }}">
</head>
<body class="login-body">
  <div class="login-card">
    <img class="login-logo" src="{{ url_for('static', filename='logo-mark.svg') }}" alt="HNH">
    <h1>HNH ETL</h1>
    <p class="login-sub">Sign in to the pipeline manager</p>
    {% if error %}<div class="login-error">{{ error }}</div>{% endif %}
    <form method="post" action="{{ url_for('page_login') }}">
      <label for="login-username">Username</label>
      <input id="login-username" name="username" autocomplete="username" autofocus required>
      <label for="login-password">Password</label>
      <input id="login-password" name="password" type="password" autocomplete="current-password" required>
      <button type="submit" class="login-submit">Sign in</button>
    </form>
  </div>
</body>
</html>
```

- [ ] **Step 2: Append login styles to gui/static/style.css**

Append at the end of the file:

```css
/* ---- Login page ---------------------------------------------------------- */
.login-body {
  display: flex; align-items: center; justify-content: center;
  min-height: 100vh; background: var(--background);
}
.login-card {
  width: min(380px, 92vw); background: var(--surface);
  border: 1px solid var(--border-soft); border-radius: var(--radius-xl);
  box-shadow: var(--shadow-lg); padding: 40px 36px; text-align: center;
}
.login-logo { width: 56px; height: 56px; margin-bottom: 12px; }
.login-card h1 { font-size: 1.35rem; font-weight: 800; }
.login-sub { color: var(--text-muted); font-size: .9rem; margin-bottom: 22px; }
.login-error {
  background: var(--error-light); color: var(--error);
  border-radius: var(--radius-md); padding: 8px 12px;
  font-size: .85rem; font-weight: 600; margin-bottom: 16px;
}
.login-card form { text-align: left; display: flex; flex-direction: column; gap: 6px; }
.login-card label {
  font-size: .72rem; font-weight: 700; color: var(--text-muted);
  text-transform: uppercase; letter-spacing: .5px; margin-top: 10px;
}
.login-card input {
  padding: 10px 12px; border: 1px solid var(--border);
  border-radius: var(--radius-md); font: inherit; background: var(--surface-low);
}
.login-card input:focus { outline: 2px solid var(--primary-light); border-color: var(--primary); }
.login-submit {
  margin-top: 20px; padding: 11px; border: 0; border-radius: var(--radius-full);
  background: var(--gradient-primary); color: var(--on-primary);
  font: inherit; font-weight: 700; cursor: pointer;
}
.login-submit:hover { filter: brightness(1.08); }

/* Sign-out button in the top bar */
.logout-form { display: inline; }
.logout-btn {
  display: inline-flex; align-items: center; gap: 6px;
  border: 1px solid var(--border-soft); background: transparent;
  border-radius: var(--radius-full); padding: 6px 14px;
  font: inherit; font-size: .82rem; font-weight: 600;
  color: var(--text-muted); cursor: pointer;
}
.logout-btn:hover { border-color: var(--primary); color: var(--primary); }
```

- [ ] **Step 3: Add the sign-out button to base.html**

In `gui/templates/base.html`, replace the topbar-right block (lines 71–73):

```html
    <div class="topbar-right">
      {% if session.get('user') %}
      <form class="logout-form" method="post" action="{{ url_for('logout') }}">
        <button type="submit" class="logout-btn" title="Sign out {{ session.get('user') }}">
          <span class="material-symbols-outlined" aria-hidden="true">logout</span>
          Sign out
        </button>
      </form>
      {% endif %}
      <span class="avatar"><span class="material-symbols-outlined" aria-hidden="true">account_circle</span></span>
    </div>
```

- [ ] **Step 4: Verify rendering with the test client**

Run this one-liner from the repo root (renders both pages through Flask):

```bash
python -c "
import sys; sys.path.insert(0, 'gui')
import app as gui_app
c = gui_app.app.test_client()
r = c.get('/login', environ_base={'REMOTE_ADDR': '10.0.0.5'})
assert r.status_code == 200 and b'login-card' in r.data, r.status_code
r2 = c.get('/')
assert r2.status_code == 200 and b'topbar-right' in r2.data, r2.status_code
print('login + dashboard render OK')
"
```

Expected output: `login + dashboard render OK`

- [ ] **Step 5: Run the auth tests again (template swap must not break them)**

Run: `python -m pytest tests/test_app_auth.py -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add gui/templates/login.html gui/templates/base.html gui/static/style.css
git commit -m "feat(gui): styled login page and sign-out button"
```

---

### Task 4: README + final verification

**Files:**
- Modify: `gui/README.md:44` (the `OASIS_GUI_TOKEN` row)

**Interfaces:**
- Consumes: everything above; documentation only.

- [ ] **Step 1: Update the README env-var table**

Replace line 44 of `gui/README.md` (the `OASIS_GUI_TOKEN` row) with these two rows:

```markdown
| `OASIS_GUI_USER`          | _(unset)_   | Login username; **required** (with `OASIS_GUI_PASSWORD`) for any non-loopback bind. Remote browsers sign in at `/login`; loopback clients need no login. |
| `OASIS_GUI_PASSWORD`      | _(unset)_   | Login password. The old `OASIS_GUI_TOKEN` / `?token=` URL authentication has been removed and no longer works. |
```

- [ ] **Step 2: Confirm no stale token references remain**

Run: `grep -rn "OASIS_GUI_TOKEN\|oasis_token\|X-Auth-Token" gui/ tests/ --include="*.py" --include="*.md" --include="*.html"`
Expected: the only hits are the README line explaining the removal and the `test_*_has_no_effect` tests (which assert the old paths are dead).

- [ ] **Step 3: Run the full test suite**

Run: `python -m pytest tests/ -v`
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add gui/README.md
git commit -m "docs(gui): document login credentials, note token auth removal"
```
