"""Who is calling, and for which org. The org is only ever read from a verified token.

There are three ways to sign in, chosen by configuration:

  oidc     Any OpenID Connect provider (Okta, Entra ID, Keycloak, Google, Cognito's hosted
           UI...). The browser is sent to the provider and comes back with a code
           (authorization code flow with PKCE); pipeyard never sees a password. Used
           whenever ``PIPEYARD_OIDC_ISSUER`` is set.
  cognito  A username/password form checked against a Cognito user pool. The fallback in
           AWS when there is no HTTPS address for a provider to redirect back to.
  dev      Local only: everyone is signed in as one org. No passwords, no provider.

API callers send ``Authorization: Bearer <token>``: an ID token from the provider (oidc,
cognito) or a dev token. Browsers hold a cookie: in oidc mode a session token that pipeyard
signs itself after a successful sign-in, otherwise the same token an API caller would send.
"""

import base64
import hashlib
import secrets
import time
from dataclasses import dataclass
from urllib.parse import urlencode

import boto3
import httpx
import jwt
from botocore.exceptions import ClientError

from .config import Settings
from .contract import ORG_ID_RE

COGNITO_ORG_CLAIM = "custom:org_id"
ORG_CLAIM = COGNITO_ORG_CLAIM  # what dev tokens and Cognito tokens call the org
COOKIE = "pipeyard_token"
LOGIN_COOKIE = "pipeyard_login"  # carries one sign-in attempt to the provider and back
SESSION = "pipeyard-session"
LOGIN = "pipeyard-login"
# Providers sign ID tokens with a private key. Shared-secret algorithms are never accepted
# for them, so a token signed with something an attacker knows cannot pass as one.
PROVIDER_ALGORITHMS = ["RS256", "RS384", "RS512", "ES256", "ES384", "PS256"]


class AuthError(Exception):
    pass


@dataclass(frozen=True)
class LoginAttempt:
    state: str
    nonce: str
    verifier: str
    next: str


class Auth:
    def __init__(self, settings: Settings):
        self.settings = settings
        if settings.oidc_issuer:
            self.kind = "oidc"
            self.issuer = settings.oidc_issuer.rstrip("/")
            self.client_id = settings.oidc_client_id
            self.org_claim = settings.oidc_org_claim
        elif settings.is_aws:
            self.kind = "cognito"
            self.issuer = (
                f"https://cognito-idp.{settings.region}.amazonaws.com/"
                f"{settings.cognito_user_pool_id}"
            )
            self.client_id = settings.cognito_client_id
            self.org_claim = COGNITO_ORG_CLAIM
            self._jwks = jwt.PyJWKClient(f"{self.issuer}/.well-known/jwks.json")
        else:
            self.kind = "dev"
            self.org_claim = ORG_CLAIM
        # Signs session and sign-in cookies. Random per process unless configured, so
        # restarting pipeyard signs browsers out (they are sent through the provider again).
        self._secret = settings.session_secret or secrets.token_urlsafe(48)
        self._provider: dict | None = None

    # --- verifying tokens ---------------------------------------------------------------

    def org_id(self, token: str) -> str:
        """Verify the token and return its org. Raises AuthError on anything unexpected."""
        try:
            if self.kind == "dev":
                claims = jwt.decode(
                    token,
                    self.settings.dev_secret,
                    algorithms=["HS256"],
                    options={"require": ["exp"]},
                )
            elif self.kind == "oidc" and jwt.get_unverified_header(token).get("alg") == "HS256":
                # Only pipeyard's own session tokens are HS256, signed with its own secret.
                claims = jwt.decode(
                    token, self._secret, algorithms=["HS256"], audience=SESSION,
                    options={"require": ["exp", "aud"]},
                )  # fmt: skip
                return self._org(claims.get("org_id"), "org_id")
            else:
                claims = self._provider_claims(token)
        except jwt.PyJWTError as e:
            raise AuthError(f"invalid token: {e}") from e
        return self._org(claims.get(self.org_claim), self.org_claim)

    def _org(self, value, claim: str) -> str:
        if not isinstance(value, str) or not ORG_ID_RE.fullmatch(value):
            raise AuthError(f"token has no valid {claim} claim")
        return value

    def _provider_claims(self, token: str, nonce: str | None = None) -> dict:
        """Verify an ID token issued by the provider (or by Cognito) for this client."""
        if self.kind == "oidc":
            provider = self.provider()
            algorithms = [
                a
                for a in provider.get("id_token_signing_alg_values_supported", ["RS256"])
                if a in PROVIDER_ALGORITHMS
            ] or ["RS256"]
            jwks = provider["jwks_client"]
        else:
            algorithms, jwks = ["RS256"], self._jwks
        claims = jwt.decode(
            token,
            jwks.get_signing_key_from_jwt(token).key,
            algorithms=algorithms,
            issuer=self.issuer,
            audience=self.client_id,
            options={"require": ["exp", "iss", "aud"]},
        )
        if claims.get("token_use", "id") != "id":  # Cognito also issues access tokens
            raise AuthError("not an ID token")
        if nonce is not None and claims.get("nonce") != nonce:
            raise AuthError("the sign-in response does not belong to this sign-in attempt")
        return claims

    # --- OpenID Connect -----------------------------------------------------------------

    def provider(self) -> dict:
        """The provider's published configuration (OIDC discovery), fetched once."""
        if self._provider is None:
            url = f"{self.issuer}/.well-known/openid-configuration"
            try:
                response = httpx.get(url, timeout=10)
                response.raise_for_status()
                config = response.json()
            except (httpx.HTTPError, ValueError) as e:
                raise AuthError(f"could not read the sign-in provider's configuration: {e}") from e
            if config.get("issuer", "").rstrip("/") != self.issuer:
                raise AuthError(
                    f"the provider at {self.issuer} says its issuer is {config.get('issuer')!r}"
                )
            for required in ("authorization_endpoint", "token_endpoint", "jwks_uri"):
                if not config.get(required):
                    raise AuthError(f"the sign-in provider's configuration has no {required}")
            config["jwks_client"] = jwt.PyJWKClient(config["jwks_uri"])
            self._provider = config
        return self._provider

    def begin_login(self, next_path: str, redirect_uri: str) -> tuple[str, str]:
        """Start a sign-in: (URL to send the browser to, value for the sign-in cookie)."""
        attempt = LoginAttempt(
            state=secrets.token_urlsafe(24),
            nonce=secrets.token_urlsafe(24),
            verifier=secrets.token_urlsafe(48),
            next=next_path,
        )
        challenge = base64.urlsafe_b64encode(hashlib.sha256(attempt.verifier.encode()).digest())
        query = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": redirect_uri,
            "scope": self.settings.oidc_scopes,
            "state": attempt.state,
            "nonce": attempt.nonce,
            "code_challenge": challenge.rstrip(b"=").decode(),
            "code_challenge_method": "S256",
        }
        endpoint = self.provider()["authorization_endpoint"]
        separator = "&" if "?" in endpoint else "?"
        cookie = jwt.encode(
            {**attempt.__dict__, "aud": LOGIN, "exp": int(time.time()) + 600},
            self._secret,
            algorithm="HS256",
        )
        return f"{endpoint}{separator}{urlencode(query)}", cookie

    def finish_login(self, cookie: str | None, state: str, code: str, redirect_uri: str):
        """Complete a sign-in: returns (session token, where to send the browser)."""
        try:
            saved = jwt.decode(cookie or "", self._secret, algorithms=["HS256"], audience=LOGIN)
        except jwt.PyJWTError as e:
            raise AuthError("this sign-in attempt has expired; please try again") from e
        if not state or not secrets.compare_digest(state, saved["state"]):
            raise AuthError("the sign-in response does not match the request; please try again")
        form = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": self.client_id,
            "code_verifier": saved["verifier"],
        }
        secret = self.settings.oidc_client_secret
        try:
            response = httpx.post(
                self.provider()["token_endpoint"],
                data=form,
                auth=(self.client_id, secret) if secret else None,
                timeout=10,
            )
            response.raise_for_status()
            id_token = response.json()["id_token"]
        except (httpx.HTTPError, ValueError, KeyError) as e:
            raise AuthError(f"the sign-in provider did not complete the sign-in: {e}") from e
        try:
            claims = self._provider_claims(id_token, nonce=saved["nonce"])
        except jwt.PyJWTError as e:
            raise AuthError(f"the sign-in provider returned an invalid token: {e}") from e
        try:
            org_id = self._org(claims.get(self.org_claim), self.org_claim)
        except AuthError as e:
            raise AuthError(
                f"you signed in, but your account has no '{self.org_claim}' claim saying which "
                "org you belong to; ask whoever manages sign-in to add it"
            ) from e
        now = int(time.time())
        session = jwt.encode(
            {
                "aud": SESSION,
                "sub": str(claims.get("sub", "")),
                "org_id": org_id,
                "iat": now,
                "exp": now + self.session_seconds,
            },
            self._secret,
            algorithm="HS256",
        )
        return session, saved["next"]

    @property
    def session_seconds(self) -> int:
        """How long a browser stays signed in."""
        if self.kind == "oidc":
            return int(self.settings.session_hours * 3600)
        return 3600 if self.kind == "cognito" else 12 * 3600

    # --- the other two ways in ----------------------------------------------------------

    def dev_token(self, org_id: str | None = None) -> str:
        """The local stub. Only exists when nothing else is configured."""
        if self.kind != "dev":
            raise AuthError("dev tokens are disabled when a sign-in provider is configured")
        now = int(time.time())
        claims = {
            "sub": "dev-user",
            ORG_CLAIM: org_id or self.settings.dev_org_id,
            "iat": now,
            "exp": now + 12 * 3600,
        }
        return jwt.encode(claims, self.settings.dev_secret, algorithm="HS256")

    def cognito_login(self, username: str, password: str) -> str:
        """Username + password -> Cognito ID token (USER_PASSWORD_AUTH)."""
        idp = boto3.client("cognito-idp", region_name=self.settings.region)
        try:
            result = idp.initiate_auth(
                ClientId=self.settings.cognito_client_id,
                AuthFlow="USER_PASSWORD_AUTH",
                AuthParameters={"USERNAME": username, "PASSWORD": password},
            )
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("NotAuthorizedException", "UserNotFoundException"):
                raise AuthError("Incorrect username or password") from e
            raise
        if "AuthenticationResult" not in result:
            raise AuthError(
                f"Sign-in needs an extra step ({result.get('ChallengeName')}); "
                "set a permanent password with `task create-user`"
            )
        return result["AuthenticationResult"]["IdToken"]
