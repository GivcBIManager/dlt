def test_load_smtp_reads_section(tmp_path, monkeypatch):
    from orchestrator import email, state
    secrets = tmp_path / "secrets.toml"
    secrets.write_text(
        '[smtp]\nhost="mail.x"\nport=587\nusername="u"\npassword="p"\n'
        'from="oasis@x"\nuse_tls=true\n')
    monkeypatch.setattr(state, "secrets_path", lambda: secrets)
    smtp = email.load_smtp()
    assert smtp["host"] == "mail.x" and smtp["port"] == 587 and smtp["use_tls"] is True


def test_load_smtp_none_when_incomplete(tmp_path, monkeypatch):
    from orchestrator import email, state
    secrets = tmp_path / "secrets.toml"
    secrets.write_text('[smtp]\nhost="mail.x"\n')  # missing from/port
    monkeypatch.setattr(state, "secrets_path", lambda: secrets)
    assert email.load_smtp() is None


def test_render_body_contains_link_and_status():
    from orchestrator import email
    body = email.render_body("nightly", "SUCCEEDED", "abc123", "t0", "t1",
                             "http://h:3000/runs/abc123")
    assert "nightly" in body and "SUCCEEDED" in body and "runs/abc123" in body


def test_send_email_uses_smtp(monkeypatch):
    from orchestrator import email
    sent = {}

    class FakeSMTP:
        def __init__(self, host, port, timeout=0): sent["addr"] = (host, port)
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def starttls(self): sent["tls"] = True
        def login(self, u, p): sent["login"] = (u, p)
        def send_message(self, msg): sent["to"] = msg["To"]

    monkeypatch.setattr(email.smtplib, "SMTP", FakeSMTP)
    smtp = {"host": "h", "port": 587, "username": "u", "password": "p",
            "from": "f@x", "use_tls": True}
    email.send_email(smtp, ["a@x", "b@x"], "subj", "body")
    assert sent["addr"] == ("h", 587) and sent["tls"] is True
    assert sent["login"] == ("u", "p") and "a@x" in sent["to"]


# --- run links: a wildcard bind must not leak into the email ---------------- #

def test_run_url_keeps_explicit_bind_host(monkeypatch):
    from orchestrator import email
    monkeypatch.setenv("OASIS_DAGSTER_HOST", "10.1.2.3")
    monkeypatch.setenv("OASIS_DAGSTER_PORT", "3000")
    monkeypatch.delenv("OASIS_DAGSTER_PUBLIC_HOST", raising=False)
    assert email.run_url("abc") == "http://10.1.2.3:3000/runs/abc"


def test_wildcard_bind_resolves_to_interface_addresses(monkeypatch):
    from orchestrator import email
    monkeypatch.setenv("OASIS_DAGSTER_HOST", "0.0.0.0")
    monkeypatch.setenv("OASIS_DAGSTER_PORT", "3000")
    monkeypatch.delenv("OASIS_DAGSTER_PUBLIC_HOST", raising=False)
    monkeypatch.setattr(email, "_route_address", lambda: "192.168.1.50")
    monkeypatch.setattr(email, "_hostname_addresses",
                        lambda: ["127.0.0.1", "192.168.1.50", "10.8.0.2",
                                 "169.254.7.7", "fe80::1"])
    urls = email.run_urls("abc")
    assert urls == ["http://192.168.1.50:3000/runs/abc",
                    "http://10.8.0.2:3000/runs/abc"]
    assert "0.0.0.0" not in " ".join(urls)


def test_wildcard_bind_falls_back_to_loopback(monkeypatch):
    from orchestrator import email
    monkeypatch.setenv("OASIS_DAGSTER_HOST", "::")
    monkeypatch.delenv("OASIS_DAGSTER_PUBLIC_HOST", raising=False)
    monkeypatch.setattr(email, "_route_address", lambda: None)
    monkeypatch.setattr(email, "_hostname_addresses", lambda: [])
    assert email.host_addresses() == ["127.0.0.1"]


def test_public_host_override_wins(monkeypatch):
    from orchestrator import email
    monkeypatch.setenv("OASIS_DAGSTER_HOST", "0.0.0.0")
    monkeypatch.setenv("OASIS_DAGSTER_PORT", "3000")
    monkeypatch.setenv("OASIS_DAGSTER_PUBLIC_HOST", "etl.example.com, 10.0.0.9")
    assert email.run_urls("abc") == ["http://etl.example.com:3000/runs/abc",
                                     "http://10.0.0.9:3000/runs/abc"]


def test_ipv6_address_is_bracketed(monkeypatch):
    from orchestrator import email
    monkeypatch.setenv("OASIS_DAGSTER_HOST", "0.0.0.0")
    monkeypatch.setenv("OASIS_DAGSTER_PORT", "3000")
    monkeypatch.delenv("OASIS_DAGSTER_PUBLIC_HOST", raising=False)
    monkeypatch.setattr(email, "_route_address", lambda: "2001:db8::5")
    monkeypatch.setattr(email, "_hostname_addresses", lambda: [])
    assert email.run_urls("abc") == ["http://[2001:db8::5]:3000/runs/abc"]


def test_render_body_lists_every_address():
    from orchestrator import email
    body = email.render_body(
        "nightly", "SUCCEEDED", "abc123", "t0", "t1",
        ["http://192.168.1.50:3000/runs/abc123",
         "http://10.8.0.2:3000/runs/abc123"])
    assert "Dagster: http://192.168.1.50:3000/runs/abc123" in body
    assert "         http://10.8.0.2:3000/runs/abc123" in body
