# tests/unit/test_auth_util.py
from pathlib import Path
from unittest.mock import patch
import json
from src.clients.auth_util import load_creds_or_die


class _FakeCreds:
    token = "new-token"
    refresh_token = "refresh-token"
    expired = True
    valid = True

    def refresh(self, _):  # mimic google.auth.transport.requests.Request
        self.token = "new-token"

    def to_json(self):  # <-- add this
        return json.dumps(
            {
                "token": self.token,
                "refresh_token": self.refresh_token,
                "scopes": ["openid"],
            }
        )


def test_refresh_and_resave(tmp_path, monkeypatch):
    secrets = tmp_path / "client_secret.json"
    token = tmp_path / "token.json"

    secrets.write_text(
        json.dumps({"installed": {"client_id": "x", "client_secret": "y"}})
    )
    token.write_text(
        json.dumps(
            {"token": "old", "refresh_token": "refresh-token", "scopes": ["openid"]}
        )
    )

    with patch(
        "src.clients.auth_util.Credentials.from_authorized_user_file",
        return_value=_FakeCreds(),
    ):
        creds = load_creds_or_die(str(secrets), str(token))
    assert creds.token == "new-token"
    saved = json.loads(token.read_text())
    assert saved["token"] == "new-token"
