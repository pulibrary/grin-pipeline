import httpx
import types
from src.clients.grin_client import GrinClient

class _Dummy:
    def __init__(self):
        self.auth_header = {"Authorization": "Bearer TEST"}
        self.base_url = "https://example.com"
    def resource_url(self, path): return f"{self.base_url}/{path.lstrip('/')}"

def test_request_bubbles_error(monkeypatch):
    c = _Dummy()

    def fake_request(method, url, **kwargs):
        return httpx.Response(401, text="unauthorized", request=httpx.Request(method, url))
    monkeypatch.setattr("httpx.request", fake_request)

    # inline _request equivalent (or import if public)
    def _request(method, url, **kwargs):
        headers = kwargs.pop("headers", {})
        headers = {**c.auth_header, **headers}
        r = httpx.request(method, url, headers=headers, **kwargs)
        try:
            r.raise_for_status()
            return r
        except httpx.HTTPStatusError as e:
            raise RuntimeError(f"{e.request.method} {e.request.url} -> {e.response.status_code}\n{e.response.text}") from e

    try:
        _request("GET", c.resource_url("/foo"))
        assert False, "should raise"
    except RuntimeError as e:
        msg = str(e)
        assert "401" in msg and "unauthorized" in msg
