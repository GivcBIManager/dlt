def test_save_and_get_smtp(tmp_path, monkeypatch):
    import config
    import smtp_config as sc
    secrets = tmp_path / "secrets.toml"
    secrets.write_text('[oracle_branches.jazan]\nhost="db"\nport=1521\n'
                       'username="u"\ndatabase="X"\npassword="p"\n')
    monkeypatch.setattr(config, "SECRETS_TOML", secrets)
    monkeypatch.setattr(config, "STATE_DIR", tmp_path)

    sc.save_smtp({"host": "mail.x", "port": 587, "username": "user",
                  "from": "oasis@x", "use_tls": True, "password": "secret"})
    got = sc.get_smtp()
    assert got["host"] == "mail.x" and got["has_password"] is True
    assert "password" not in got
    # the oracle branch must be byte-preserved
    assert "[oracle_branches.jazan]" in secrets.read_text()


def test_save_smtp_keeps_password_when_blank(tmp_path, monkeypatch):
    import config
    import smtp_config as sc
    secrets = tmp_path / "secrets.toml"
    secrets.write_text('[smtp]\nhost="h"\nport=25\nfrom="f@x"\npassword="keep"\n')
    monkeypatch.setattr(config, "SECRETS_TOML", secrets)
    monkeypatch.setattr(config, "STATE_DIR", tmp_path)
    sc.save_smtp({"host": "h2", "port": 26, "from": "f@x", "use_tls": False})
    assert 'password = "keep"' in secrets.read_text()
    assert 'host = "h2"' in secrets.read_text()
