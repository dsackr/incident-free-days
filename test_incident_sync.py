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
        self.sync_config_file = os.path.join(self.temp_dir.name, "sync_config.json")

        self.original_sync_config = app.SYNC_CONFIG_FILE
        self.original_product_key = app.PRODUCT_KEY_FILE
        app.SYNC_CONFIG_FILE = self.sync_config_file
        app.PRODUCT_KEY_FILE = os.path.join(self.temp_dir.name, "product_pillar_key.json")

    def tearDown(self):
        app.SYNC_CONFIG_FILE = self.original_sync_config
        app.PRODUCT_KEY_FILE = self.original_product_key
        self.temp_dir.cleanup()

    def test_normalize_incident_payloads_maps_core_fields(self):
        api_incident = {
            "reference": "INC-670",
            "id": "01KBK5GFE3NX1BRBCQ86R5NW4M",
            "severity": {"name": "Other (Sev 6)"},
            "incident_timestamp_values": [
                {
                    "incident_timestamp": {"name": "Reported At"},
                    "value": {"value": "2025-12-03T22:32:14.019Z"},
                }
            ],
            "custom_field_entries": [
                {
                    "custom_field": {"name": "product"},
                    "values": [
                        {"value_catalog_entry": {"name": "SIS Ohio"}},
                    ],
                },
                {
                    "custom_field": {"name": "solution pillar"},
                    "values": [
                        {"value_catalog_entry": {"name": "Student Solutions"}},
                    ],
                },
            ],
        }

        payloads = app.normalize_incident_payloads(api_incident)
        self.assertEqual(len(payloads), 1)
        payload = payloads[0]

        self.assertEqual(payload["inc_number"], "INC-670")
        self.assertEqual(payload["date"], "2025-12-03")
        self.assertEqual(payload["severity"], "Other (Sev 6)")
        self.assertEqual(payload["product"], "SIS Ohio")
        self.assertEqual(payload["pillar"], "Student Solutions")

    def test_normalize_incident_payloads_fallbacks_to_created_and_unknowns(self):
        api_incident = {
            "id": "INC-401",
            "severity": "Sev 1",
            "created_at": "2024-02-01T00:00:00Z",
        }

        payloads = app.normalize_incident_payloads(api_incident)
        self.assertEqual(len(payloads), 1)
        payload = payloads[0]

        self.assertEqual(payload["inc_number"], "INC-401")
        self.assertEqual(payload["date"], "2024-02-01")
        self.assertEqual(payload["severity"], "Sev 1")
        self.assertEqual(payload["product"], "Unknown")
        self.assertEqual(payload["pillar"], "Unknown")

    @mock.patch("incident_io_client.fetch_incidents")
    def test_sync_incidents_dry_run_skips_writes(self, mock_fetch_incidents):
        api_incident = {
            "reference": "INC-200",
            "id": "01ABC",
            "severity": {"name": "Sev3"},
            "created_at": "2024-06-01T12:00:00Z",
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

    @mock.patch("incident_io_client.fetch_incidents")
    def test_sync_filters_by_date_and_returns_samples(self, mock_fetch_incidents):
        api_incidents = [
            {
                "reference": "INC-300",
                "severity": {"name": "Sev2"},
                "incident_timestamp_values": [
                    {
                        "incident_timestamp": {"name": "Reported at"},
                        "value": {"value": "2024-07-10T01:00:00Z"},
                    }
                ],
            },
            {
                "reference": "INC-301",
                "severity": {"name": "Sev3"},
                "incident_timestamp_values": [
                    {
                        "incident_timestamp": {"name": "Reported at"},
                        "value": {"value": "2023-05-01T10:00:00Z"},
                    }
                ],
            },
        ]

        mock_fetch_incidents.return_value = api_incidents

        summary = app.sync_incidents_from_api(
            dry_run=True,
            start_date="2024-01-01",
            end_date="2024-12-31",
            include_samples=True,
            incidents_file=self.incidents_file,
            other_events_file=self.other_file,
        )

        self.assertEqual(summary["fetched"], 2)
        self.assertEqual(summary["added_incidents"], 1)
        self.assertEqual(len(summary.get("samples", [])), 1)


if __name__ == "__main__":
    unittest.main()
