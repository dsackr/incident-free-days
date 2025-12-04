import json
import os
from typing import Any, Dict, List, Optional

try:
    import requests
except ImportError:  # pragma: no cover - optional dependency for offline tests
    requests = None

DEFAULT_BASE_URL = "https://api.incident.io"


class IncidentAPIError(Exception):
    """Raised when the incident.io API cannot be reached or returns an error."""


def _build_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "incident-free-days-sync",
    }


def _extract_next_cursor(payload: Dict[str, Any]) -> Optional[str]:
    pagination = (
        payload.get("pagination")
        or payload.get("page")
        or payload.get("pagination_meta")
        or {}
    )
    for key in ("next_cursor", "next", "after"):
        next_cursor = pagination.get(key)
        if next_cursor:
            return str(next_cursor)

    return payload.get("next_cursor") or payload.get("next") or payload.get("after")


def _coerce_incidents(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates = payload.get("incidents")
    if isinstance(candidates, list):
        return [item for item in candidates if isinstance(item, dict)]

    candidates = payload.get("data")
    if isinstance(candidates, list):
        return [item for item in candidates if isinstance(item, dict)]

    return []


def fetch_incidents(
    *,
    base_url: Optional[str] = None,
    token: Optional[str] = None,
    page_size: int = 50,
    limit: Optional[int] = None,
    timeout: int = 15,
) -> List[Dict[str, Any]]:
    """Fetch incidents from the incident.io API with pagination.

    Args:
        base_url: Optional override for the API base URL. Defaults to the
            ``INCIDENT_IO_BASE_URL`` environment variable, falling back to
            ``https://api.incident.io``.
        token: Optional API token. Defaults to the ``INCIDENT_IO_API_TOKEN``
            environment variable.
        page_size: Number of incidents to request per page.
        limit: Optional maximum number of incidents to return.
        timeout: Request timeout in seconds.

    Raises:
        IncidentAPIError: When the request fails or returns a non-200 status.
    """

    auth_token = token or os.getenv("INCIDENT_IO_API_TOKEN")
    if not auth_token:
        raise IncidentAPIError("INCIDENT_IO_API_TOKEN is not set")

    if requests is None:
        raise IncidentAPIError("The 'requests' package is required to sync incidents")

    resolved_base_url = base_url or os.getenv("INCIDENT_IO_BASE_URL") or DEFAULT_BASE_URL
    url = f"{resolved_base_url.rstrip('/')}/v2/incidents"

    params: Dict[str, Any] = {"page_size": page_size}
    incidents: List[Dict[str, Any]] = []

    while True:
        try:
            response = requests.get(url, headers=_build_headers(auth_token), params=params, timeout=timeout)
        except requests.RequestException as exc:
            raise IncidentAPIError(f"Failed to reach incident.io: {exc}") from exc

        if response.status_code != 200:
            raise IncidentAPIError(
                f"incident.io returned {response.status_code}: {response.text[:200]}"
            )

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise IncidentAPIError("incident.io returned an invalid JSON response") from exc

        page_incidents = _coerce_incidents(payload)
        incidents.extend(page_incidents)

        if limit is not None and len(incidents) >= limit:
            return incidents[:limit]

        next_cursor = _extract_next_cursor(payload)
        if not next_cursor:
            break

        params = {
            "page_size": page_size,
            # Support both documented "page_after" and observed "after" pagination cursors
            "page_after": next_cursor,
            "after": next_cursor,
        }

    return incidents
