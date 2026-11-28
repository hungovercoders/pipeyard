"""Signing in with an OpenID Connect provider (authorization code flow with PKCE).

FakeProvider is a small but honest provider: it publishes discovery and keys over HTTP,
checks the PKCE verifier, and signs real RS256 ID tokens.
"""

import base64
import dataclasses
import hashlib
import json
import secrets
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import jwt
import pytest
from conftest import register
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

from pipeyard.app import create_app
from pipeyard.auth import COOKIE, LOGIN_COOKIE
from pipeyard.config import Settings

CLIENT_ID = "pipeyard-web"


class FakeProvider:
    def __init__(self):
        self.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.codes: dict[str, dict] = {}
        self.token_requests: list[dict] = []
        self.claims: dict = {"org_id": "acme"}  # what the provider says about the person
        self.tamper: dict = {}  # overrides for the ID token, to simulate a bad provider
        self.issuer_override: str | None = None
        provider = self

        class Handler(BaseHTTPRequestHandler):
            def _json(self, body, status=200):
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):  # noqa: N802
                if self.path == "/.well-known/openid-configuration":
                    self._json(
                        {
                            "issuer": provider.issuer_override or provider.issuer,
                            "authorization_endpoint": provider.issuer + "/authorize",
                            "token_endpoint": provider.issuer + "/token",
                            "jwks_uri": provider.issuer + "/jwks",
                            "id_token_signing_alg_values_supported": ["RS256"],
                        }
                    )
                elif self.path == "/jwks":
                    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(provider.key.public_key()))
                    self._json({"keys": [{**jwk, "kid": "k1", "use": "sig", "alg": "RS256"}]})
                else:
                    self._json({}, 404)

            def do_POST(self):  # noqa: N802
                form = {
                    k: v[0]
                    for k, v in parse_qs(
                        self.rfile.read(int(self.headers["Content-Length"])).decode()
                    ).items()
                }
                provider.token_requests.append(
                    {"form": form, "auth": self.headers.get("Authorization")}
                )
                grant = provider.codes.pop(form.get("code", ""), None)  # a code works once
                digest = hashlib.sha256(form.get("code_verifier", "").encode()).digest()
                challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
                if (
                    grant is None
                    or grant["code_challenge"] != challenge  # PKCE
                    or grant["redirect_uri"] != form.get("redirect_uri")
                    or form.get("client_id") != CLIENT_ID
                ):
                    return self._json({"error": "invalid_grant"}, 400)
                self._json(
                    {"id_token": provider.id_token(nonce=grant["nonce"]), "token_type": "Bearer"}
                )

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.issuer = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def id_token(self, **claims) -> str:
        now = int(time.time())
        body = {"iss": self.issuer, "aud": CLIENT_ID, "sub": "user-1", "iat": now, "exp": now + 300}
        body |= self.claims | claims | self.tamper
        key = body.pop("signing_key", self.key)
        return jwt.encode(body, key, algorithm="RS256", headers={"kid": "k1"})

    def authorize(self, url: str) -> tuple[str, dict]:
        """What the provider does when the browser arrives: returns (code, the request)."""
        query = {k: v[0] for k, v in parse_qs(urlparse(url).query).items()}
        code = secrets.token_urlsafe(8)
        self.codes[code] = query
        return code, query


@pytest.fixture
def provider():
    provider = FakeProvider()
    yield provider
    provider.server.shutdown()


@pytest.fixture
def oidc(settings, runner, provider):
    """make(**settings) -> a browser-like client of an app that signs in through the provider."""

    def make(**changes) -> TestClient:
        configured = dataclasses.replace(
            settings, oidc_issuer=provider.issuer, oidc_client_id=CLIENT_ID, **changes
        )
        return TestClient(create_app(configured, runner), follow_redirects=False)

    return make


def sign_in(client, provider, path="/pipelines"):
    """Walk the whole flow as a browser would. Returns the final response."""
    to_login = client.get(path)
    to_provider = client.get(to_login.headers["location"])
    code, request = provider.authorize(to_provider.headers["location"])
    return client.get(f"/auth/callback?code={code}&state={request['state']}")


def test_signing_in(oidc, provider):
    client = oidc()
    to_login = client.get("/datasets")
    assert (to_login.status_code, to_login.headers["location"]) == (303, "/login?next=/datasets")

    to_provider = client.get(to_login.headers["location"])
    assert to_provider.status_code == 303
    assert to_provider.headers["location"].startswith(provider.issuer + "/authorize?")
    assert "HttpOnly" in to_provider.headers["set-cookie"] and LOGIN_COOKIE in client.cookies
    code, request = provider.authorize(to_provider.headers["location"])
    assert request["response_type"] == "code" and request["client_id"] == CLIENT_ID
    assert request["redirect_uri"] == "http://testserver/auth/callback"
    assert request["scope"] == "openid email profile"
    assert request["code_challenge_method"] == "S256" and len(request["code_challenge"]) == 43
    assert len(request["state"]) >= 24 and len(request["nonce"]) >= 24

    done = client.get(f"/auth/callback?code={code}&state={request['state']}")
    assert (done.status_code, done.headers["location"]) == (303, "/datasets")
    assert "HttpOnly" in done.headers["set-cookie"]
    assert LOGIN_COOKIE not in client.cookies  # the attempt is spent
    # the provider was given the PKCE verifier, and no password ever passed through pipeyard
    (exchange,) = provider.token_requests
    assert exchange["form"]["grant_type"] == "authorization_code" and exchange["auth"] is None
    assert len(exchange["form"]["code_verifier"]) >= 43

    assert client.get("/datasets").status_code == 200
    assert "acme" in client.get("/pipelines").text  # signed in as the org the provider named


def test_the_session_is_pipeyards_own_and_scoped_to_the_org(oidc, provider, example_yaml):
    a = oidc()
    sign_in(a, provider)
    register(a, example_yaml)
    session = jwt.decode(a.cookies[COOKIE], options={"verify_signature": False})
    assert session["org_id"] == "acme" and session["sub"] == "user-1"
    assert 7.9 * 3600 < session["exp"] - time.time() <= 8 * 3600  # default session length

    provider.claims = {"org_id": "globex"}
    b = TestClient(a.app, follow_redirects=False)
    sign_in(b, provider)
    assert b.get("/api/pipelines").json() == []  # another org sees nothing of acme's
    assert [p["name"] for p in a.get("/api/pipelines").json()] == ["fake-transactions"]


def test_api_clients_use_the_providers_id_token(oidc, provider):
    client = oidc()
    headers = {"Authorization": f"Bearer {provider.id_token()}"}
    assert client.get("/api/pipelines", headers=headers).status_code == 200
    assert client.get("/api/pipelines").status_code == 401


BAD_TOKENS = {
    "wrong audience": {"aud": "another-app"},
    "wrong issuer": {"iss": "https://evil.example"},
    "expired": {"exp": int(time.time()) - 10},
    "no org": {"org_id": None},
    "org is not a valid id": {"org_id": "../acme"},
    "signed by someone else": {
        "signing_key": rsa.generate_private_key(public_exponent=65537, key_size=2048)
    },
}


@pytest.mark.parametrize("name", BAD_TOKENS)
def test_bad_id_tokens_are_rejected(oidc, provider, name):
    token = provider.id_token(**BAD_TOKENS[name])
    assert (
        oidc().get("/api/pipelines", headers={"Authorization": f"Bearer {token}"}).status_code
        == 401
    )


def test_shared_secret_tokens_cannot_pass_as_provider_or_session_tokens(oidc, provider, settings):
    client = oidc()
    now = int(time.time())
    forged = [
        # the old dev stub's token, signed with the secret that is in the source code
        jwt.encode(
            {"custom:org_id": "acme", "org_id": "acme", "exp": now + 60},
            settings.dev_secret,
            "HS256",
        ),
        # a "session" signed with a guessable secret
        jwt.encode(
            {"aud": "pipeyard-session", "org_id": "acme", "exp": now + 60}, "x" * 40, "HS256"
        ),
        # an ID-token lookalike signed with the public client id
        jwt.encode(
            {"iss": provider.issuer, "aud": CLIENT_ID, "org_id": "acme", "exp": now + 60},
            CLIENT_ID * 4,
            "HS256",
        ),
        jwt.encode(
            {"iss": provider.issuer, "aud": CLIENT_ID, "org_id": "acme", "exp": now + 60},
            None,
            "none",
        ),
    ]
    for token in forged:
        assert (
            client.get("/api/pipelines", headers={"Authorization": f"Bearer {token}"}).status_code
            == 401
        )
        assert (
            client.get("/api/pipelines", headers={"Cookie": f"{COOKIE}={token}"}).status_code == 401
        )
    assert client.post("/dev/token").status_code == 404  # the dev stub is gone


def test_the_sign_in_cookie_is_not_a_session(oidc, provider):
    client = oidc()
    client.get(client.get("/pipelines").headers["location"])
    attempt = client.cookies[LOGIN_COOKIE]
    assert (
        client.get("/api/pipelines", headers={"Cookie": f"{COOKIE}={attempt}"}).status_code == 401
    )


def callback_error(client, url) -> str:
    response = client.get(url)
    assert response.status_code == 401 and COOKIE not in client.cookies
    assert "Sign-in did not complete" in response.text
    return response.text


def test_a_response_that_does_not_match_the_request_is_refused(oidc, provider):
    client = oidc()
    to_provider = client.get("/login")
    code, request = provider.authorize(to_provider.headers["location"])
    assert "does not match the request" in callback_error(
        client, f"/auth/callback?code={code}&state=forged"
    )
    # and without the cookie from the browser that started it (a login-CSRF attempt)
    stranger = TestClient(client.app, follow_redirects=False)
    assert "expired" in callback_error(
        stranger, f"/auth/callback?code={code}&state={request['state']}"
    )


def test_a_code_cannot_be_used_twice(oidc, provider):
    client = oidc()
    to_provider = client.get("/login")
    code, request = provider.authorize(to_provider.headers["location"])
    url = f"/auth/callback?code={code}&state={request['state']}"
    attempt = client.cookies[LOGIN_COOKIE]
    assert client.get(url).status_code == 303
    replay = TestClient(client.app, follow_redirects=False)
    replay.cookies.set(LOGIN_COOKIE, attempt)
    assert "did not complete the sign-in" in callback_error(replay, url)


def test_an_id_token_for_another_sign_in_attempt_is_refused(oidc, provider):
    client = oidc()
    provider.tamper = {"nonce": "someone-elses"}
    code, request = provider.authorize(client.get("/login").headers["location"])
    text = callback_error(client, f"/auth/callback?code={code}&state={request['state']}")
    assert "does not belong to this sign-in attempt" in text


def test_signing_in_without_an_org_explains_what_is_missing(oidc, provider):
    client = oidc()
    provider.claims = {}
    code, request = provider.authorize(client.get("/login").headers["location"])
    text = callback_error(client, f"/auth/callback?code={code}&state={request['state']}")
    assert "has no &#39;org_id&#39; claim" in text and "Sign in" in text
    assert client.get("/pipelines").status_code == 303  # still signed out, and no redirect loop


def test_the_provider_refusing_is_shown(oidc):
    text = callback_error(
        oidc(), "/auth/callback?error=access_denied&error_description=Not+allowed+in"
    )
    assert "the sign-in provider reported: Not allowed in" in text


def test_the_org_claim_scopes_and_public_address_are_configurable(oidc, provider):
    client = oidc(
        oidc_org_claim="https://example.com/tenant", oidc_scopes="openid groups",
        public_url="https://pipeyard.example.com/",
    )  # fmt: skip
    provider.claims = {"https://example.com/tenant": "initech"}
    to_provider = client.get("/login?next=/datasets")
    assert "Secure" in to_provider.headers["set-cookie"]  # an https address means secure cookies
    code, request = provider.authorize(to_provider.headers["location"])
    assert request["redirect_uri"] == "https://pipeyard.example.com/auth/callback"
    assert request["scope"] == "openid groups"
    # A Secure cookie is not sent back over the test client's plain http, so pass it by hand.
    attempt = to_provider.headers["set-cookie"].split(";")[0]
    done = client.get(
        f"/auth/callback?code={code}&state={request['state']}", headers={"Cookie": attempt}
    )
    assert done.status_code == 303 and "Secure" in done.headers["set-cookie"]
    session = done.headers["set-cookie"].split(";")[0].split("=", 1)[1]
    assert jwt.decode(session, options={"verify_signature": False})["org_id"] == "initech"


def test_a_confidential_client_authenticates_to_the_provider(oidc, provider):
    client = oidc(oidc_client_secret="s3cret")
    assert sign_in(client, provider).status_code == 303
    expected = "Basic " + base64.b64encode(f"{CLIENT_ID}:s3cret".encode()).decode()
    assert provider.token_requests[0]["auth"] == expected


def test_sessions_expire(oidc, provider):
    client = oidc(session_hours=1 / 3600)  # one second
    sign_in(client, provider)
    assert client.get("/api/pipelines").status_code == 200
    time.sleep(1.2)
    assert client.get("/api/pipelines").status_code == 401
    assert client.get("/pipelines").headers["location"].startswith("/login")


def test_sessions_do_not_survive_a_restart_unless_a_secret_is_configured(oidc, provider):
    first = oidc()
    sign_in(first, provider)
    restarted = oidc()
    assert (
        restarted.get(
            "/api/pipelines", headers={"Cookie": f"{COOKIE}={first.cookies[COOKIE]}"}
        ).status_code
        == 401
    )

    kept = oidc(session_secret="a-long-configured-secret-shared-by-every-instance")
    sign_in(kept, provider)
    again = oidc(session_secret="a-long-configured-secret-shared-by-every-instance")
    assert (
        again.get(
            "/api/pipelines", headers={"Cookie": f"{COOKIE}={kept.cookies[COOKIE]}"}
        ).status_code
        == 200
    )


def test_signing_out(oidc, provider):
    client = oidc()
    sign_in(client, provider)
    out = client.get("/logout")
    assert out.headers["location"] == "/login?signed_out=true" and COOKIE not in client.cookies
    page = client.get(out.headers["location"])
    assert page.status_code == 200 and "You are signed out" in page.text  # not bounced back in
    assert client.get("/api/pipelines").status_code == 401


def test_only_redirects_back_into_pipeyard(oidc, provider):
    client = oidc()
    code, request = provider.authorize(
        client.get("/login?next=//evil.example/x").headers["location"]
    )
    done = client.get(f"/auth/callback?code={code}&state={request['state']}")
    assert done.headers["location"] == "/"


def test_a_provider_that_cannot_be_reached_or_trusted(settings, runner, provider):
    def app_for(issuer):
        configured = dataclasses.replace(settings, oidc_issuer=issuer, oidc_client_id=CLIENT_ID)
        return TestClient(create_app(configured, runner), follow_redirects=False)

    down = app_for("http://127.0.0.1:1").get("/login")
    assert down.status_code == 502 and "could not read the sign-in provider" in down.text

    provider.issuer_override = "https://somewhere-else.example"
    lying = app_for(provider.issuer).get("/login")
    assert lying.status_code == 502 and "says its issuer is" in lying.text


def test_settings_are_checked():
    with pytest.raises(ValueError, match="PIPEYARD_OIDC_CLIENT_ID is needed"):
        Settings(oidc_issuer="https://idp.example")


def test_report_apps_work_with_a_session(oidc, provider, runner, example_yaml):
    client = oidc()
    sign_in(client, provider)
    register(client, example_yaml)
    report = "/api/pipelines/fake-transactions/reports/transactions-dashboard"
    client.post(f"{report}/start")
    assert client.get(report).json()["status"] == "running"
    answer = client.get("/apps/fake-transactions/transactions-dashboard/").json()
    assert answer["app"] == "acme" and "cookie" not in answer["headers"]  # session not forwarded
    with client.websocket_connect("/apps/fake-transactions/transactions-dashboard/ws") as ws:
        ws.send_text("hi")
        assert ws.receive_text() == "acme heard hi"
