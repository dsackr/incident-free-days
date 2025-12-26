import io

import requests

DISPLAY_FRAME_BYTES = 192000
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

    The updated firmware expects a single multipart upload containing exactly
    192000 bytes (4bpp, two pixels per byte). Returns (success, message).
    """

    base_url = f"http://{display_ip}".rstrip("/")
    session = requests.Session()

    total_chunks = 1

    def report(stage, chunk_index=0, message=None):
        if not progress_callback:
            return

        try:
            progress_callback(stage, chunk_index, total_chunks, message)
        except Exception:
            # Progress updates should never break the send loop.
            pass

    if len(payload_bytes) != DISPLAY_FRAME_BYTES:
        return False, f"Payload must be {DISPLAY_FRAME_BYTES} bytes"

    try:
        files = {"file": (save_name or "frame.bin", io.BytesIO(payload_bytes))}
        response = session.post(
            f"{base_url}/display/upload",
            files=files,
            timeout=timeout,
        )
        response.raise_for_status()
        report("done", total_chunks)
    except requests.ReadTimeout as exc:
        report("done", total_chunks, str(exc))
        return True, f"Display update sent but controller timed out waiting for confirmation: {exc}"
    except requests.RequestException as exc:
        report("error", message=str(exc))
        return False, f"Display request failed: {exc}"

    return True, "ok"
