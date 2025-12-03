import os
import tempfile
import unittest
from unittest import mock

import app


class IncidentSyncTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.incidents_file = os.path.join(self.temp_dir.name, "incidents.json")
        self.other_file = os.path.join(self.temp_dir.name, "others.json")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_normalize_incident_payloads_computes_duration_and_pillar(self):
        mapping = {"Search": "Platform"}
        api_incident = {
            "incident_number": "INC-123",
            "severity": {"name": "Sev2"},
            "incident_type": {"name": "Operational Incident"},
            "started_at": "2024-05-01T10:00:00Z",
            "resolved_at": "2024-05-01T11:00:00Z",
            "services": [{"name": "Search"}],
        }

        payloads = app.normalize_incident_payloads(api_incident, mapping=mapping)
        self.assertEqual(len(payloads), 1)
        payload = payloads[0]

        self.assertEqual(payload["inc_number"], "INC-123")
        self.assertEqual(payload["pillar"], "Platform")
        self.assertEqual(payload["duration_seconds"], 3600)
        self.assertEqual(payload["reported_at"], "2024-05-01T05:00:00")
        self.assertEqual(payload["closed_at"], "2024-05-01T06:00:00")

    @mock.patch("incident_io_client.fetch_incidents")
    def test_sync_incidents_dry_run_skips_writes(self, mock_fetch_incidents):
        api_incident = {
            "incident_number": "INC-200",
            "severity": "Sev3",
            "incident_type": {"name": "Operational Incident"},
            "started_at": "2024-06-01T12:00:00Z",
            "resolved_at": "2024-06-01T13:00:00Z",
            "products": ["Checkout"],
        }

        mock_fetch_incidents.return_value = [api_incident]

        summary = app.sync_incidents_from_api(
            dry_run=True,
            incidents_file=self.incidents_file,
            other_events_file=self.other_file,
        )

        self.assertEqual(summary["added_incidents"], 1)
        self.assertEqual(summary["added_other_events"], 0)
        self.assertEqual(summary["fetched"], 1)
        self.assertTrue(summary["dry_run"])

        self.assertFalse(os.path.exists(self.incidents_file))
        self.assertFalse(os.path.exists(self.other_file))


if __name__ == "__main__":
    unittest.main()
