"""Token -> org_id. The org is only ever read from a verified token claim.

Both modes use the same claim name, ``custom:org_id``, so the rest of the app cannot tell
them apart:
  - aws:   Cognito ID token, RS256, verified against the user pool's JWKS.
  - local: HS256 token from the dev stub, signed with a local-only secret.
"""

import time

import boto3
import jwt
from botocore.exceptions import ClientError

from .config import Settings
from .contract import ORG_ID_RE

ORG_CLAIM = "custom:org_id"
COOKIE = "pipeyard_token"


class AuthError(Exception):
    pass


class Auth:
    def __init__(self, settings: Settings):
        self.settings = settings
        if settings.is_aws:
            self.issuer = (
                f"https://cognito-idp.{settings.region}.amazonaws.com/"
                f"{settings.cognito_user_pool_id}"
            )
            self.jwks = jwt.PyJWKClient(f"{self.issuer}/.well-known/jwks.json")

    def org_id(self, token: str) -> str:
        """Verify the token and return its org. Raises AuthError on anything unexpected."""
        try:
            if self.settings.is_aws:
                claims = jwt.decode(
                    token,
                    self.jwks.get_signing_key_from_jwt(token).key,
                    algorithms=["RS256"],
                    issuer=self.issuer,
                    audience=self.settings.cognito_client_id,
                    options={"require": ["exp", "iss", "aud"]},
                )
                if claims.get("token_use") != "id":
                    raise AuthError("not an ID token")
            else:
                claims = jwt.decode(
                    token,
                    self.settings.dev_secret,
                    algorithms=["HS256"],
                    options={"require": ["exp"]},
                )
        except jwt.PyJWTError as e:
            raise AuthError(f"invalid token: {e}") from e
        org_id = claims.get(ORG_CLAIM)
        if not isinstance(org_id, str) or not ORG_ID_RE.fullmatch(org_id):
            raise AuthError(f"token has no valid {ORG_CLAIM} claim")
        return org_id

    def dev_token(self, org_id: str | None = None) -> str:
        """The local stub. Refuses to exist in AWS mode."""
        if self.settings.is_aws:
            raise AuthError("dev tokens are disabled in aws mode")
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
