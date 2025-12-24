import requests

CHUNK_SIZE = 4096
DEFAULT_TIMEOUT = 15


def send_display_buffer(display_ip, payload_bytes, *, save_name=None, timeout=DEFAULT_TIMEOUT):
    """Push a binary buffer to the ESP32 display controller.

    Returns a tuple of (success: bool, message: str).
    """

    base_url = f"http://{display_ip}".rstrip("/")
    session = requests.Session()

    try:
        start_params = {"save": save_name} if save_name else None
        response = session.post(
            f"{base_url}/display/start", params=start_params, timeout=timeout
        )
        response.raise_for_status()

        for offset in range(0, len(payload_bytes), CHUNK_SIZE):
            chunk = payload_bytes[offset : offset + CHUNK_SIZE]
            hex_body = chunk.hex()
            resp = session.post(
                f"{base_url}/display/chunk",
                data=hex_body,
                headers={"Content-Type": "text/plain"},
                timeout=timeout,
            )
            resp.raise_for_status()
    except requests.RequestException as exc:
        return False, f"Display request failed: {exc}"

    try:
        end_resp = session.post(f"{base_url}/display/end", timeout=timeout)
        end_resp.raise_for_status()
    except requests.ReadTimeout as exc:
        return True, f"Display update sent but controller timed out waiting for confirmation: {exc}"
    except requests.RequestException as exc:
        return False, f"Display request failed: {exc}"

    return True, "ok"
