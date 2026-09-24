"""Cloudflare Access JWT verification (defense-in-depth, layer 2).

Cloudflare Access authenticates the human at the edge (layer 1) and, on every
request it proxies to the origin, injects a signed JWT in the
`Cf-Access-Jwt-Assertion` header. This module verifies that JWT *inside the
app* so access no longer relies on trusting the network: a request that reaches
the container without a valid, CF-signed token (e.g. CF Access bypassed,
misconfigured, or someone hitting the container directly) is rejected.

Zero manual steps for users — the browser sends nothing; CF injects the token
automatically once the user has logged in through Access, from any device.

Config (env, set on the VM):
  CF_ACCESS_TEAM_DOMAIN   your team domain — "myteam" or
                          "https://myteam.cloudflareaccess.com"
  CF_ACCESS_AUD           the Access application's Audience (AUD) tag
                          (Zero Trust → Access → Applications → your app →
                          Overview → Application Audience (AUD) Tag)
  CF_ACCESS_ALLOWED_EMAILS  (optional) comma-separated allow-list; if set, the
                          token's email claim must match one of these.

Leave TEAM_DOMAIN / AUD unset to disable (local dev) — `enabled` is False and
the middleware no-ops.
"""
import os
import logging

log = logging.getLogger(__name__)

_TEAM = os.getenv("CF_ACCESS_TEAM_DOMAIN", "").strip().rstrip("/")
_AUD = os.getenv("CF_ACCESS_AUD", "").strip()
_ALLOWED = {e.strip().lower()
            for e in os.getenv("CF_ACCESS_ALLOWED_EMAILS", "").split(",")
            if e.strip()}

# Accept either bare team name or a full URL.
if _TEAM and not _TEAM.startswith("http"):
    _TEAM = f"https://{_TEAM}.cloudflareaccess.com"

# Verification is active only when both the team domain and the app's AUD are
# configured — without the AUD we can't bind the token to *this* application.
enabled = bool(_TEAM and _AUD)

_jwk_client = None


def _client():
    """Lazily build a PyJWKClient. It fetches CF's signing keys from the certs
    endpoint and caches them internally (refreshing as needed)."""
    global _jwk_client
    if _jwk_client is None:
        import jwt
        _jwk_client = jwt.PyJWKClient(f"{_TEAM}/cdn-cgi/access/certs")
    return _jwk_client


def verify(token: str):
    """Return the token's claims dict if the CF Access JWT is valid for this
    application, else None. Blocking (network on first/expired key) — call from
    a thread in async code."""
    if not enabled or not token:
        return None
    try:
        import jwt
        signing_key = _client().get_signing_key_from_jwt(token).key
        claims = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            audience=_AUD,
            issuer=_TEAM,
        )
    except Exception as e:  # signature/exp/aud/iss failure, key fetch error, etc.
        log.warning("CF Access JWT rejected: %s", e)
        return None

    if _ALLOWED:
        email = (claims.get("email") or "").lower()
        if email not in _ALLOWED:
            log.warning("CF Access email not in allow-list: %s", email)
            return None
    return claims
