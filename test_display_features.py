import os
import tempfile
import unittest
from unittest import mock

from PIL import Image

import app
import display_client


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
    def test_send_display_buffer_calls_chunk_endpoints(self, mock_session_cls):
        session = mock_session_cls.return_value
        ok_response = mock.Mock()
        ok_response.raise_for_status = mock.Mock()
        session.post.side_effect = [ok_response, ok_response, ok_response, ok_response]

        payload = b"\x00" * 5000
        success, message = display_client.send_display_buffer("1.2.3.4", payload)

        self.assertTrue(success)
        self.assertEqual(message, "ok")
        # start + 2 chunks + end
        self.assertEqual(session.post.call_count, 3 + 1)
        chunk_calls = session.post.call_args_list[1:-1]
        self.assertTrue(all(call.kwargs.get("headers", {}).get("Content-Type") == "text/plain" for call in chunk_calls))


class DisplayTriggerTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_sync_config = app.SYNC_CONFIG_FILE
        self.original_data_file = app.DATA_FILE

        app.SYNC_CONFIG_FILE = os.path.join(self.temp_dir.name, "sync_config.json")
        app.DATA_FILE = os.path.join(self.temp_dir.name, "incidents.json")

    def tearDown(self):
        app.SYNC_CONFIG_FILE = self.original_sync_config
        app.DATA_FILE = self.original_data_file
        self.temp_dir.cleanup()

    def test_new_procedural_triggers_once(self):
        incidents = [
            {
                "inc_number": "INC-100",
                "date": "2025-01-01",
                "reported_at": "2025-01-01T00:00:00",
                "rca_classification": "Deploy",
                "event_type": "Operational Incident",
            }
        ]

        app.save_events(app.DATA_FILE, incidents)
        config = app.load_display_config()
        config.update(
            {
                "display_ip": "1.2.3.4",
                "display_enabled": True,
                "push_on_new_procedural": True,
            }
        )
        app.save_display_config(config)

        with mock.patch("app.send_osha_sign_to_display") as mock_push:
            mock_push.return_value = (True, "ok")

            first = app.push_display_for_new_procedural_if_needed(incidents)
            second = app.push_display_for_new_procedural_if_needed(incidents)

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(mock_push.call_count, 1)


if __name__ == "__main__":
    unittest.main()
