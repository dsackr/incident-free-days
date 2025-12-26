import os
import tempfile
from datetime import datetime, timedelta, timezone
from io import BytesIO
import unittest
from unittest import mock

import requests

from PIL import Image

import app
import display_client
from display_client import DISPLAY_FRAME_BYTES


class DisplayConversionTests(unittest.TestCase):
    def test_binary_length(self):
        img = Image.new("RGB", (800, 480), "white")
        data = app.convert_osha_image_to_binary(img)
        self.assertEqual(len(data), 192000)

    def test_palette_mapping_order(self):
        img = Image.new("RGB", (800, 480), (255, 255, 255))
        img.putpixel((0, 0), (0, 0, 0))  # black
        img.putpixel((1, 0), (255, 255, 255))  # white

        data = app.convert_osha_image_to_binary(img)
        self.assertEqual(data[0], (0x0 << 4) | 0x1)


class DisplayClientTests(unittest.TestCase):
    @mock.patch("display_client.requests.Session")
    def test_send_display_buffer_sends_single_upload(self, mock_session_cls):
        session = mock_session_cls.return_value
        ok_response = mock.Mock()
        ok_response.raise_for_status = mock.Mock()
        session.post.side_effect = [ok_response]

        payload = b"\x00" * DISPLAY_FRAME_BYTES
        success, message = display_client.send_display_buffer("1.2.3.4", payload)

        self.assertTrue(success)
        self.assertEqual(message, "ok")
        self.assertEqual(session.post.call_count, 1)
        call_kwargs = session.post.call_args.kwargs
        self.assertIn("files", call_kwargs)
        self.assertIn("file", call_kwargs["files"])

    @mock.patch("display_client.requests.Session")
    def test_send_display_buffer_allows_timeout_after_end(self, mock_session_cls):
        session = mock_session_cls.return_value
        ok_response = mock.Mock()
        ok_response.raise_for_status = mock.Mock()
        timeout_exc = requests.ReadTimeout("timed out")

        # upload succeeds, post-call times out when waiting for response body
        session.post.side_effect = [timeout_exc]

        payload = b"\x00" * DISPLAY_FRAME_BYTES
        success, message = display_client.send_display_buffer("1.2.3.4", payload)

        self.assertTrue(success)
        self.assertIn("timed out", message)


class DisplaySendEndpointTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_data_file = app.DATA_FILE
        self.original_osha_output = app.OSHA_OUTPUT_IMAGE
        self.original_log_file = app.LOG_FILE
        self.original_osha_binary = app.OSHA_OUTPUT_BINARY

        app.DATA_FILE = os.path.join(self.temp_dir.name, "incidents.json")
        app.OSHA_OUTPUT_IMAGE = os.path.join(self.temp_dir.name, "current_sign.png")
        app.OSHA_OUTPUT_BINARY = os.path.join(self.temp_dir.name, "current_sign.bin")
        app.LOG_FILE = os.path.join(self.temp_dir.name, "app.log")
        app._configure_logging(app.LOG_FILE)

    def tearDown(self):
        app.DATA_FILE = self.original_data_file
        app.OSHA_OUTPUT_IMAGE = self.original_osha_output
        app.LOG_FILE = self.original_log_file
        app.OSHA_OUTPUT_BINARY = self.original_osha_binary
        app._configure_logging(app.LOG_FILE)
        self.temp_dir.cleanup()

    def test_endpoint_requires_ip(self):
        client = app.app.test_client()
        response = client.get("/api/osha/send_to_display")

        self.assertEqual(response.status_code, 400)
        payload = response.get_json()
        self.assertEqual(payload["status"], "error")

    def test_endpoint_queues_background_send(self):
        client = app.app.test_client()

        def fake_generate(**_kwargs):
            img = Image.new("RGB", (10, 10), "white")
            img.save(app.OSHA_OUTPUT_IMAGE)
            return True

        called = {}

        def fake_send(ip, **_kwargs):
            called["ip"] = ip
            return True, "ok"

        class InstantThread:
            def __init__(self, target=None, args=None, kwargs=None, daemon=None):
                self._target = target
                self._args = args or []
                self._kwargs = kwargs or {}

            def start(self):
                if self._target:
                    self._target(*self._args, **self._kwargs)

        with (
            mock.patch("app.generate_osha_sign", side_effect=fake_generate),
            mock.patch("app.send_osha_sign_to_ip", side_effect=fake_send),
            mock.patch("app.threading.Thread", InstantThread),
        ):
            response = client.get("/api/osha/send_to_display?ip=10.0.0.5")

        self.assertEqual(response.status_code, 202)
        self.assertEqual(called["ip"], "10.0.0.5")

    def test_logs_endpoint_returns_recent_entries(self):
        cutoff_line = datetime.now(timezone.utc) - timedelta(hours=25)
        recent_line = datetime.now(timezone.utc)

        with open(app.LOG_FILE, "w", encoding="utf-8") as handle:
            handle.write(cutoff_line.strftime("%Y-%m-%dT%H:%M:%SZ old entry") + "\n")
            handle.write(recent_line.strftime("%Y-%m-%dT%H:%M:%SZ new entry") + "\n")

        client = app.app.test_client()
        response = client.get("/logs")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("new entry", body)
        self.assertNotIn("old entry", body)

    def test_display_frame_endpoint_supports_etag(self):
        payload = b"\x01" * DISPLAY_FRAME_BYTES
        with open(app.OSHA_OUTPUT_BINARY, "wb") as handle:
            handle.write(payload)

        client = app.app.test_client()
        response = client.get("/display/frame")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(int(response.headers.get("Content-Length")), DISPLAY_FRAME_BYTES)
        etag = response.headers.get("ETag")
        self.assertIsNotNone(etag)

        cached = client.get("/display/frame", headers={"If-None-Match": etag})
        self.assertEqual(cached.status_code, 304)

    def test_upload_endpoint_validates_and_sets_frame(self):
        client = app.app.test_client()

        too_small = client.post(
            "/display/upload",
            data={"file": (BytesIO(b"123"), "bad.bin")},
            content_type="multipart/form-data",
        )
        self.assertEqual(too_small.status_code, 400)

        payload = b"\x02" * DISPLAY_FRAME_BYTES
        upload = client.post(
            "/display/upload",
            data={"file": (BytesIO(payload), "frame.bin")},
            content_type="multipart/form-data",
        )

        self.assertEqual(upload.status_code, 200)
        etag = upload.get_json().get("etag")

        served = client.get("/display/frame")
        self.assertEqual(served.data, payload)
        self.assertEqual(served.headers.get("ETag"), etag)


if __name__ == "__main__":
    unittest.main()
