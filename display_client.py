import requests

CHUNK_SIZE = 4096
DEFAULT_TIMEOUT = 15


def send_display_buffer(
    display_ip,
    payload_bytes,
    *,
    save_name=None,
    timeout=DEFAULT_TIMEOUT,
    progress_callback=None,
):
    """Push a binary buffer to the ESP32 display controller.

    Returns a tuple of (success: bool, message: str).
    """

    base_url = f"http://{display_ip}".rstrip("/")
    session = requests.Session()

    total_chunks = max(1, (len(payload_bytes) + CHUNK_SIZE - 1) // CHUNK_SIZE)

    def report(stage, chunk_index=0, message=None):
        if not progress_callback:
            return

        try:
            progress_callback(stage, chunk_index, total_chunks, message)
        except Exception:
            # Progress updates should never break the send loop.
            pass

    try:
        start_params = {"save": save_name} if save_name else None
        response = session.post(
            f"{base_url}/display/start", params=start_params, timeout=timeout
        )
        response.raise_for_status()

        report("start")

        for idx, offset in enumerate(range(0, len(payload_bytes), CHUNK_SIZE), start=1):
            chunk = payload_bytes[offset : offset + CHUNK_SIZE]
            hex_body = chunk.hex()
            resp = session.post(
                f"{base_url}/display/chunk",
                data=hex_body,
                headers={"Content-Type": "text/plain"},
                timeout=timeout,
            )
            resp.raise_for_status()
            report("chunk", idx)
    except requests.RequestException as exc:
        report("error", message=str(exc))
        return False, f"Display request failed: {exc}"

    try:
        end_resp = session.post(f"{base_url}/display/end", timeout=timeout)
        end_resp.raise_for_status()
    except requests.ReadTimeout as exc:
        report("done", total_chunks, str(exc))
        return True, f"Display update sent but controller timed out waiting for confirmation: {exc}"
    except requests.RequestException as exc:
        report("error", message=str(exc))
        return False, f"Display request failed: {exc}"

    report("done", total_chunks)
    return True, "ok"
