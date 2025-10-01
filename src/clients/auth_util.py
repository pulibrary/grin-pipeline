# auth_util.py (or inline in your client module)

import os, json, time
from pathlib import Path
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

REQUIRED_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]


def load_creds_or_die(secrets_path: str, token_path: str, scopes=None) -> Credentials:
    """
    Loads creds from token_path, refreshes if needed, and re-saves.
    Assumes you've already created the token using the loopback/device flow.
    """
    scopes = scopes or REQUIRED_SCOPES
    sp = Path(secrets_path)
    tp = Path(token_path)

    # Sanity checks (fast failures beat mysterious 400s)
    assert sp.exists(), f"Secrets file missing: {sp}"
    assert tp.exists(), f"Token file missing: {tp} (did you complete auth?)"

    creds = Credentials.from_authorized_user_file(str(tp), scopes=scopes)

    # If expired and refreshable, refresh now so all downstream calls use a fresh access token
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        tp.write_text(creds.to_json())

    # Optional: print quick debug info
    # print("Creds valid:", creds.valid, "Expired:", creds.expired, "Has refresh:", bool(creds.refresh_token))
    return creds


def build_auth_header(creds: Credentials) -> dict:
    # Ensure we have a non-expired token. (If your client lives a long time, refresh periodically.)
    if not creds.valid and creds.refresh_token:
        creds.refresh(Request())
    return {"Authorization": f"Bearer {creds.token}"}
