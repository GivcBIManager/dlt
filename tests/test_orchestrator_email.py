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
