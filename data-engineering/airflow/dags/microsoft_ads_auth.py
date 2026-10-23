"""Microsoft OAuth renewal with durable refresh-token rotation."""
from __future__ import annotations

import requests
from google.cloud import secretmanager

from campaign_daily_common import request_with_retry


TOKEN_URL = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
TOKEN_SCOPE = "https://ads.microsoft.com/msads.manage offline_access"
REFRESH_TOKEN_SECRET = "airflow-variables-MICROSOFT_ADS_REFRESH_TOKEN"


def microsoft_access_token(
    client_id: str, client_secret: str, secret_project_id: str
) -> str:
    """Read the current token and persist its replacement before using access.

    Read Secret Manager directly so Airflow's variable cache cannot keep using
    an older token. Both Microsoft DAGs share this secret. Microsoft currently
    permits concurrent refreshes; each returned replacement remains valid.
    Never log the request/response bodies or put tokens in task return values.
    """
    client = secretmanager.SecretManagerServiceClient()
    parent = f"projects/{secret_project_id}/secrets/{REFRESH_TOKEN_SECRET}"
    current = client.access_secret_version(
        request={"name": f"{parent}/versions/latest"}
    ).payload.data.decode("utf-8")
    try:
        response = request_with_retry(
            "POST",
            TOKEN_URL,
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": current,
                "grant_type": "refresh_token",
                "scope": TOKEN_SCOPE,
            },
            timeout=60,
        )
    except requests.HTTPError as error:
        # Do not include Microsoft's free-form response, which may echo inputs.
        try:
            invalid_grant = error.response.json().get("error") == "invalid_grant"
        except (AttributeError, ValueError):
            invalid_grant = False
        if invalid_grant:
            raise RuntimeError(
                "Microsoft Ads OAuth invalid_grant: reauthorize the Microsoft "
                f"Ads account and add a new version of {REFRESH_TOKEN_SECRET}."
            ) from None
        raise

    payload = response.json()
    access_token = payload.get("access_token")
    if not isinstance(access_token, str) or not access_token:
        raise RuntimeError("Microsoft Ads OAuth returned no valid access token")
    replacement = payload.get("refresh_token")
    if "refresh_token" in payload and (
        not isinstance(replacement, str) or not replacement
    ):
        raise RuntimeError("Microsoft Ads OAuth returned an invalid refresh token")
    if replacement and replacement != current:
        # A failed write must fail the task: success without persistence would
        # silently bring back the expired-token incident. Keep prior versions.
        client.add_secret_version(
            request={"parent": parent, "payload": {"data": replacement.encode("utf-8")}}
        )
    return access_token
