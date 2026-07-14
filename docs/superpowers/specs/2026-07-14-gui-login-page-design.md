# GUI login page — replace URL token authentication

**Date:** 2026-07-14
**Status:** Approved design, pending implementation plan

## Goal

Replace the current `?token=` / `OASIS_GUI_TOKEN` authentication of the OASIS
GUI control panel with a username/password login page backed by a Flask signed
session cookie. After this change **no URL ever carries a credential**: the
`?token=` query parameter, the `X-Auth-Token` header, the `Authorization:
Bearer` header, and the `oasis_token` cookie are all removed and no longer
grant access.

## Decisions (agreed with user)

1. **Credentials:** single user from environment variables `OASIS_GUI_USER`
   and `OASIS_GUI_PASSWORD`.
2. **Loopback bypass stays:** clients connecting from 127.0.0.0/8 / ::1 /
   `localhost` never see the login page, exactly like today.
3. **Sessions only:** `OASIS_GUI_TOKEN` and all token transport paths are
   deleted. Remote programmatic API access is not supported; local scripts
   keep working via the loopback bypass.
4. **Hard requirement:** `?token=` in the URL must stop working entirely —
   it must not authenticate, and no code path may read it.

## Architecture

Flask's built-in signed session cookie (`flask.session`) holds the logged-in
state (`session["user"]`). The signing key is a 32-byte random secret,
auto-generated on first run and persisted to `gui/state/secret_key`
(owner-only permissions via the existing `security.harden_file`), so sessions
survive server restarts with zero configuration. The file is created if
missing and read otherwise; `gui/state/` is already git-ignored.

## Components

### `gui/security.py`

* **Remove** `gui_token()` and `token_matches()`.
* **Add** `gui_credentials() -> tuple[str, str] | None` — reads
  `OASIS_GUI_USER` and `OASIS_GUI_PASSWORD`; returns `None` unless both are
  non-blank.
* **Add** `credentials_match(user, password, expected) -> bool` — constant
  time comparison (`hmac.compare_digest`) of both fields; False when
  anything is missing.
* **Add** `load_or_create_secret_key(path) -> bytes` — returns the persisted
  key, creating it with `secrets.token_bytes(32)` + `harden_file` on first
  use.
* **Change** `check_bind(host, credentials)` — refuses a non-loopback bind
  unless credentials are configured. Error message now says to set
  `OASIS_GUI_USER` / `OASIS_GUI_PASSWORD` or bind to 127.0.0.1.
* **Change** `request_authorized(remote_addr, logged_in)` — True for
  loopback, else `logged_in` (the session flag). Doc comment updated.
* Module docstring updated (no more token wording).

### `gui/app.py`

* `app.secret_key = security.load_or_create_secret_key(...)` at startup;
  session cookie configured `HttpOnly`, `SameSite=Lax` (Lax so the redirect
  to `/login` and the form POST work while still blocking cross-site POSTs).
* **Remove** `_provided_token()` and the `_persist_token_cookie`
  after-request hook entirely. No code reads `request.args.get("token")`,
  `X-Auth-Token`, `Authorization`, or the `oasis_token` cookie anymore.
* **`GET /login`** — renders `login.html`. If the client is already
  authorized (loopback or valid session) redirect to `/`.
* **`POST /login`** — regular form POST (`username`, `password`). On
  success: `session["user"] = username`, redirect to `/`. On failure:
  re-render the form with a generic "invalid username or password" error
  and HTTP 401. No indication of which field was wrong.
* **`POST /logout`** — clears the session, redirects to `/login`.
* **`before_request` gate** (revised):
  1. `/static/*`, `/healthz`, `/login` are exempt.
  2. Loopback clients pass.
  3. Clients with `session.get("user")` pass.
  4. Otherwise: requests that accept HTML (browser page loads) get a 302
     redirect to `/login`; everything else (the JSON API) gets the existing
     `401 {"error": "unauthorized"}`.
* **CSRF rule stays:** mutating requests must be `application/json`, with
  `/login` and `/logout` as the only form-POST exceptions (protected by the
  `SameSite=Lax` session cookie). The old `X-Auth-Token` header exemption
  in that rule is removed.
* **`main()`** — `check_bind` uses credentials; startup log line says
  "login required for non-loopback clients" when bound publicly.

### `gui/templates/login.html`

Standalone page (does not extend the app chrome): centered card with the
existing logo, username + password fields, submit button, and an error line
when authentication failed. Reuses `static/style.css` variables so it matches
the panel's look.

### `gui/templates/base.html`

A "Sign out" control in the nav, rendered only when `session["user"]` is set
(hidden for loopback users who never logged in). It submits `POST /logout`.

### `gui/README.md`

Replace the `OASIS_GUI_TOKEN` row with `OASIS_GUI_USER` /
`OASIS_GUI_PASSWORD` and describe the login-page flow. Explicitly note that
`?token=` URLs no longer work.

## Error handling

* Both env vars unset + non-loopback bind → `check_bind` raises at startup
  (fail closed), same behavior as today.
* Failed login → generic error, 401, form re-rendered. No lockout /
  rate-limiting in scope (single-user internal panel; can be added later).
* Secret key file unreadable/corrupt → regenerate it (sessions reset, users
  re-login); never crash the app over it.
* Session cookie invalid/tampered → Flask treats it as no session → redirect
  to `/login`.

## Testing

Manual verification (no existing test suite for the GUI):

1. Loopback: open `http://127.0.0.1:8765` — no login page, full access.
2. Non-loopback bind without credentials → startup refusal.
3. Non-loopback client, no session → `/` redirects to `/login`; API URL
   returns 401 JSON.
4. Wrong credentials → error message, still unauthenticated.
5. Correct credentials → redirected to dashboard; API calls work; restart
   the server → still logged in (persisted secret key).
6. `?token=<anything>` appended to any URL → has no effect: unauthenticated
   remote clients still land on `/login`.
7. Logout → back to `/login`, session gone.

## Out of scope

* Multiple users / password hashing store.
* Rate limiting or account lockout.
* Remote API tokens for automation.
