import json
import os
import tempfile
import unittest
from datetime import date, timedelta, datetime, timezone
from unittest import mock

import app
import incident_io_client


class IncidentSyncTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.incidents_file = os.path.join(self.temp_dir.name, "incidents.json")
        self.other_file = os.path.join(self.temp_dir.name, "others.json")
        self.sync_config_file = os.path.join(self.temp_dir.name, "sync_config.json")

        self.original_sync_config = app.SYNC_CONFIG_FILE
        self.original_product_key = app.PRODUCT_KEY_FILE
        self.original_data_file = app.DATA_FILE
        self.original_other_file = app.OTHER_EVENTS_FILE
        self.original_osha_file = app.OSHA_DATA_FILE
        app.SYNC_CONFIG_FILE = self.sync_config_file
        app.PRODUCT_KEY_FILE = os.path.join(self.temp_dir.name, "product_pillar_key.json")
        app.DATA_FILE = self.incidents_file
        app.OTHER_EVENTS_FILE = self.other_file
        app.OSHA_DATA_FILE = os.path.join(self.temp_dir.name, "osha_data.json")

    def tearDown(self):
        app.SYNC_CONFIG_FILE = self.original_sync_config
        app.PRODUCT_KEY_FILE = self.original_product_key
        app.DATA_FILE = self.original_data_file
        app.OTHER_EVENTS_FILE = self.original_other_file
        app.OSHA_DATA_FILE = self.original_osha_file
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
        self.assertEqual(payload["reported_at"], "2025-12-03T17:32:14-05:00")
        self.assertEqual(payload["rca_classification"], "Not Classified")

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
        self.assertEqual(payload["date"], "2024-01-31")
        self.assertEqual(payload["severity"], "Sev 1")
        self.assertEqual(payload["product"], "Unknown")
        self.assertEqual(payload["pillar"], "Unknown")
        self.assertEqual(payload["reported_at"], "2024-01-31T19:00:00-05:00")

    def test_normalize_incident_payloads_splits_multiple_products(self):
        with open(app.PRODUCT_KEY_FILE, "w", encoding="utf-8") as f:
            json.dump({"Product A": "Pillar A", "Product B": "Pillar B"}, f)

        api_incident = {
            "reference": "INC-999",
            "severity": {"name": "Other (Sev 6)"},
            "incident_timestamp_values": [
                {
                    "incident_timestamp": {"name": "Reported at"},
                    "value": {"value": "2024-07-10T01:00:00Z"},
                }
            ],
            "custom_field_entries": [
                {
                    "custom_field": {"name": "Product"},
                    "values": [
                        {"value_catalog_entry": {"name": "Product A"}},
                        {"value_catalog_entry": {"name": "Product B"}},
                    ],
                },
                {
                    "custom_field": {"name": "Solution Pillar"},
                    "values": [
                        {"value_catalog_entry": {"name": "Business Solutions"}},
                    ],
                },
            ],
        }

        payloads = app.normalize_incident_payloads(api_incident)
        self.assertEqual(len(payloads), 2)
        self.assertEqual({p["product"] for p in payloads}, {"Product A", "Product B"})
        for payload in payloads:
            if payload["product"] == "Product A":
                self.assertEqual(payload["pillar"], "Pillar A")
            elif payload["product"] == "Product B":
                self.assertEqual(payload["pillar"], "Pillar B")
            self.assertEqual(payload["date"], "2024-07-09")
            self.assertEqual(payload["reported_at"], "2024-07-09T21:00:00-04:00")

    def test_normalize_incident_payloads_drops_unknown_when_products_present(self):
        api_incident = {
            "reference": "INC-321",
            "severity": {"name": "Sev2"},
            "incident_timestamp_values": [
                {
                    "incident_timestamp": {"name": "Reported at"},
                    "value": {"value": "2024-09-01T05:30:00Z"},
                }
            ],
            "custom_field_entries": [
                {
                    "custom_field": {"name": "Product"},
                    "values": [
                        {"value_catalog_entry": {"name": "Product Z"}},
                        {"value_catalog_entry": {"name": "Unknown"}},
                    ],
                }
            ],
        }

        payloads = app.normalize_incident_payloads(api_incident)
        self.assertEqual(len(payloads), 1)
        self.assertEqual(payloads[0]["product"], "Product Z")

    def test_normalize_incident_payloads_uses_incident_type(self):
        api_incident = {
            "reference": "INC-777",
            "severity": {"name": "Sev4"},
            "incident_type": {"name": "Deployment Event"},
            "incident_timestamp_values": [
                {
                    "incident_timestamp": {"name": "Reported at"},
                    "value": {"value": "2024-08-15T10:00:00Z"},
                }
            ],
            "custom_field_entries": [
                {
                    "custom_field": {"name": "Product"},
                    "values": [
                        {"value_catalog_entry": {"name": "Service X"}},
                    ],
                }
            ],
        }

        payloads = app.normalize_incident_payloads(api_incident)
        self.assertEqual(len(payloads), 1)
        payload = payloads[0]
        self.assertEqual(payload["event_type"], "Deployment Event")
        self.assertEqual(payload["product"], "Service X")
        self.assertEqual(payload["date"], "2024-08-15")

    def test_normalize_incident_payloads_includes_rca_classification(self):
        api_incident = {
            "reference": "INC-1234",
            "severity": {"name": "Sev2"},
            "incident_timestamp_values": [
                {
                    "incident_timestamp": {"name": "Reported at"},
                    "value": {"value": "2024-11-05T09:15:00Z"},
                }
            ],
            "custom_field_entries": [
                {
                    "custom_field": {"name": "Product"},
                    "values": [{"value_catalog_entry": {"name": "Widget"}}],
                },
                {
                    "custom_field": {"name": "RCA Classification"},
                    "values": [
                        {"value": "Code"},
                        {"value_text": "Ignored"},
                    ],
                },
            ],
        }

        payloads = app.normalize_incident_payloads(api_incident)

        self.assertEqual(len(payloads), 1)
        payload = payloads[0]
        self.assertEqual(payload["rca_classification"], "Code")

    def test_normalize_incident_payloads_uses_rca_field_id(self):
        api_incident = {
            "reference": "INC-5678",
            "severity": {"name": "Sev3"},
            "incident_timestamp_values": [
                {
                    "incident_timestamp": {"name": "Reported at"},
                    "value": {"value": "2024-11-06T09:15:00Z"},
                }
            ],
            "custom_field_entries": [
                {
                    "custom_field": {"id": "01JZ0PNKHCB3M6NX0AHPABS59D", "name": "Other"},
                    "values": [
                        {"value": "Process"},
                    ],
                }
            ],
        }

        payloads = app.normalize_incident_payloads(api_incident)

        self.assertEqual(len(payloads), 1)
        payload = payloads[0]
        self.assertEqual(payload["rca_classification"], "Process")

    def test_normalize_incident_payloads_includes_client_impact_duration(self):
        api_incident = {
            "reference": "INC-2468",
            "severity": {"name": "Sev3"},
            "incident_timestamp_values": [
                {
                    "incident_timestamp": {"name": "Reported at"},
                    "value": {"value": "2025-01-10T10:00:00Z"},
                }
            ],
            "custom_field_entries": [
                {
                    "custom_field": {"name": "Product"},
                    "values": [{"value_catalog_entry": {"name": "Widget"}}],
                },
                {
                    "custom_field": {"name": "Client Impact Duration"},
                    "values": [{"value_seconds": 90061}],
                },
            ],
        }

        payloads = app.normalize_incident_payloads(api_incident)

        self.assertEqual(len(payloads), 1)
        payload = payloads[0]
        self.assertEqual(payload["client_impact_duration_seconds"], 90061)

    def test_normalize_incident_payloads_reads_duration_metric(self):
        api_incident = {
            "reference": "INC-8642",
            "severity": {"name": "Sev2"},
            "incident_timestamp_values": [
                {
                    "incident_timestamp": {"name": "Reported at"},
                    "value": {"value": "2025-02-10T12:00:00Z"},
                }
            ],
            "duration_metrics": [
                {
                    "duration_metric": {
                        "id": "01HX7EADYMMC7HK85MEY0361D1",
                        "name": "Client Impact Duration",
                    },
                    "value_seconds": 1080,
                },
                {
                    "duration_metric": {"name": "Incident duration"},
                    "value_seconds": 8580,
                },
            ],
            "custom_field_entries": [
                {
                    "custom_field": {"name": "Product"},
                    "values": [{"value_catalog_entry": {"name": "Gadget"}}],
                }
            ],
        }

        payloads = app.normalize_incident_payloads(api_incident)

        self.assertEqual(len(payloads), 1)
        payload = payloads[0]
        self.assertEqual(payload["client_impact_duration_seconds"], 1080)

    def test_compute_event_dates_uses_client_impact_duration(self):
        event = {
            "event_type": "Operational Incident",
            "reported_at": "2025-02-01T00:00:00Z",
            "client_impact_duration_seconds": 90000,
        }

        dates = app.compute_event_dates(event, duration_enabled=True)

        self.assertIn(date(2025, 2, 1), dates)
        self.assertIn(date(2025, 2, 2), dates)
        self.assertEqual(len(dates), 2)

    def test_normalize_incident_payloads_defaults_rca_when_missing_values(self):
        api_incident = {
            "reference": "INC-1234",
            "severity": {"name": "Sev2"},
            "incident_timestamp_values": [
                {
                    "incident_timestamp": {"name": "Reported at"},
                    "value": {"value": "2024-11-05T09:15:00Z"},
                }
            ],
            "custom_field_entries": [
                {
                    "custom_field": {
                        "id": "01JZ0PNKHCB3M6NX0AHPABS59D",
                        "name": "RCA Classification",
                    },
                    "values": [
                        {},
                    ],
                },
            ],
        }

        payloads = app.normalize_incident_payloads(api_incident)

        self.assertEqual(len(payloads), 1)
        payload = payloads[0]
        self.assertEqual(payload["rca_classification"], "Not Classified")

    def test_normalize_incident_payloads_handles_value_option_rca(self):
        api_incident = {
            "reference": "INC-9999",
            "severity": {"name": "Sev4"},
            "incident_timestamp_values": [
                {
                    "incident_timestamp": {"name": "Reported at"},
                    "value": {"value": "2024-12-05T10:00:00Z"},
                }
            ],
            "custom_field_entries": [
                {
                    "custom_field": {
                        "id": "01JZ0PNKHCB3M6NX0AHPABS59D",
                        "name": "RCA Classification",
                        "options": [
                            {
                                "id": "01K239XGE8SV013XQY1GJS2V8K",
                                "custom_field_id": "01JZ0PNKHCB3M6NX0AHPABS59D",
                                "value": "Non-Procedural Incident",
                                "sort_key": 40,
                            }
                        ],
                    },
                    "values": [
                        {
                            "value_option": {
                                "id": "01K239XGE8SV013XQY1GJS2V8K",
                                "custom_field_id": "01JZ0PNKHCB3M6NX0AHPABS59D",
                                "value": "Non-Procedural Incident",
                                "sort_key": 40,
                            }
                        }
                    ],
                }
            ],
        }

        payloads = app.normalize_incident_payloads(api_incident)

        self.assertEqual(len(payloads), 1)
        payload = payloads[0]
        self.assertEqual(payload["rca_classification"], "Non-Procedural Incident")

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
            start_date="2024-01-01",
            end_date="2024-12-31",
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
    def test_sync_routes_non_operational_events(self, mock_fetch_incidents):
        api_incident = {
            "reference": "INC-500", 
            "incident_type": {"name": "Deployment Event"},
            "severity": {"name": "Sev2"},
            "incident_timestamp_values": [
                {
                    "incident_timestamp": {"name": "Reported at"},
                    "value": {"value": "2024-09-01T15:00:00Z"},
                }
            ],
            "custom_field_entries": [
                {
                    "custom_field": {"name": "Product"},
                    "values": [
                        {"value_catalog_entry": {"name": "Product X"}},
                    ],
                }
            ],
        }

        mock_fetch_incidents.return_value = [api_incident]

        summary = app.sync_incidents_from_api(
            dry_run=False,
            start_date="2024-08-01",
            end_date="2024-09-30",
            incidents_file=self.incidents_file,
            other_events_file=self.other_file,
        )

        self.assertEqual(summary["added_incidents"], 0)
        self.assertEqual(summary["added_other_events"], 1)

        with open(self.other_file, "r") as f:
            stored = json.load(f)

        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0]["event_type"], "Deployment Event")

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

    @mock.patch("incident_io_client.fetch_incidents")
    def test_sync_removes_unknown_when_known_product_arrives(self, mock_fetch_incidents):
        with open(self.incidents_file, "w", encoding="utf-8") as f:
            json.dump(
                [
                    {
                        "inc_number": "INC-732",
                        "product": "Unknown",
                        "pillar": "Unknown",
                        "event_type": "Operational Incident",
                        "reported_at": "2026-01-06T14:16:00-05:00",
                    }
                ],
                f,
            )

        api_incident = {
            "reference": "INC-732",
            "incident_timestamp_values": [
                {
                    "incident_timestamp": {"name": "Reported at"},
                    "value": {"value": "2026-01-06T19:16:00Z"},
                }
            ],
            "custom_field_entries": [
                {
                    "custom_field": {"name": "Product"},
                    "values": [{"value_catalog_entry": {"name": "Absence Management"}}],
                }
            ],
        }

        mock_fetch_incidents.return_value = [api_incident]

        summary = app.sync_incidents_from_api(
            dry_run=False,
            start_date="2026-01-01",
            end_date="2026-01-31",
            incidents_file=self.incidents_file,
            other_events_file=self.other_file,
        )

        self.assertEqual(summary["added_incidents"], 1)

        with open(self.incidents_file, "r", encoding="utf-8") as f:
            stored = json.load(f)

        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0]["product"], "Absence Management")

    @mock.patch("incident_io_client.requests")
    def test_fetch_incidents_handles_pagination_meta(self, mock_requests):
        first_payload = {
            "incidents": [{"id": "01"}],
            "pagination_meta": {"after": "cursor-1"},
        }
        second_payload = {"incidents": [{"id": "02"}]}

        def _mock_response(payload):
            resp = mock.Mock()
            resp.status_code = 200
            resp.json.return_value = payload
            resp.text = json.dumps(payload)
            return resp

        mock_requests.get.side_effect = [
            _mock_response(first_payload),
            _mock_response(second_payload),
        ]

        incidents = incident_io_client.fetch_incidents(token="token", base_url="http://example.com")

        self.assertEqual(len(incidents), 2)
        # Ensure the second page was requested with the observed "after" cursor
        _, second_call_kwargs = mock_requests.get.call_args_list[1]
        params = second_call_kwargs.get("params") or {}
        self.assertEqual(params.get("after"), "cursor-1")

    @mock.patch("incident_io_client.fetch_incidents")
    def test_sync_respects_configured_date_window(self, mock_fetch_incidents):
        with open(self.sync_config_file, "w") as f:
            json.dump({"start_date": "2024-07-01", "end_date": "2024-07-31"}, f)

        api_incidents = [
            {
                "reference": "INC-400",
                "incident_timestamp_values": [
                    {
                        "incident_timestamp": {"name": "Reported at"},
                        "value": {"value": "2024-07-10T01:00:00Z"},
                    }
                ],
            },
            {
                "reference": "INC-401",
                "incident_timestamp_values": [
                    {
                        "incident_timestamp": {"name": "Reported at"},
                        "value": {"value": "2024-08-15T01:00:00Z"},
                    }
                ],
            },
        ]

        mock_fetch_incidents.return_value = api_incidents

        summary = app.sync_incidents_from_api(
            dry_run=False,
            incidents_file=self.incidents_file,
            other_events_file=self.other_file,
        )

        self.assertEqual(summary["fetched"], 2)
        self.assertEqual(summary["added_incidents"], 1)

        with open(self.incidents_file, "r") as f:
            saved = json.load(f)

        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0].get("inc_number"), "INC-400")

    @mock.patch("incident_io_client.fetch_incidents")
    def test_sync_defaults_to_last_14_days(self, mock_fetch_incidents):
        recent_date = date.today() - timedelta(days=5)
        older_date = date.today() - timedelta(days=20)

        api_incidents = [
            {
                "reference": "INC-500",
                "incident_timestamp_values": [
                    {
                        "incident_timestamp": {"name": "Reported at"},
                        "value": {"value": f"{recent_date.isoformat()}T01:00:00Z"},
                    }
                ],
            },
            {
                "reference": "INC-501",
                "incident_timestamp_values": [
                    {
                        "incident_timestamp": {"name": "Reported at"},
                        "value": {"value": f"{older_date.isoformat()}T01:00:00Z"},
                    }
                ],
            },
        ]

        mock_fetch_incidents.return_value = api_incidents

        summary = app.sync_incidents_from_api(
            dry_run=False,
            incidents_file=self.incidents_file,
            other_events_file=self.other_file,
        )

        self.assertEqual(summary["fetched"], 2)
        self.assertEqual(summary["added_incidents"], 1)

        with open(self.incidents_file, "r") as f:
            saved = json.load(f)

        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0].get("inc_number"), "INC-500")

    @mock.patch("incident_io_client.fetch_incidents")
    def test_sync_updates_existing_events_within_window(self, mock_fetch_incidents):
        today = date.today()
        existing_event = {
            "inc_number": "INC-777",
            "product": "Payroll",
            "pillar": "HCM",
            "severity": "Sev2",
            "reported_at": f"{today.isoformat()}T00:00:00",
            "event_type": "Operational Incident",
            "date": today.isoformat(),
        }

        app.save_events(self.incidents_file, [existing_event])

        api_incidents = [
            {
                "reference": "INC-777",
                "severity": {"name": "Sev1"},
                "incident_timestamp_values": [
                    {
                        "incident_timestamp": {"name": "Reported at"},
                        "value": {"value": f"{today.isoformat()}T05:00:00Z"},
                    }
                ],
                "custom_field_entries": [
                    {
                        "custom_field": {"name": "Product"},
                        "values": [
                            {"value_catalog_entry": {"name": "Payroll"}},
                        ],
                    }
                ],
            }
        ]

        mock_fetch_incidents.return_value = api_incidents

        summary = app.sync_incidents_from_api(
            dry_run=False,
            incidents_file=self.incidents_file,
            other_events_file=self.other_file,
        )

        self.assertEqual(summary["updated_events"], 1)
        self.assertEqual(summary["added_incidents"], 0)

        with open(self.incidents_file, "r") as f:
            saved = json.load(f)

        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0].get("severity"), "Sev1")
        self.assertEqual(saved[0].get("reported_at"), f"{today.isoformat()}T00:00:00-05:00")

    def test_compute_osha_state_from_incidents_uses_latest_procedural(self):
        today = date.today()
        incidents = [
            {
                "inc_number": "INC-300",
                "date": (today - timedelta(days=1)).isoformat(),
                "rca_classification": "Non-Procedural Incident",
            },
            {
                "inc_number": "INC-200",
                "date": (today - timedelta(days=3)).isoformat(),
                "rca_classification": "Deploy",
            },
            {
                "inc_number": "INC-100",
                "date": (today - timedelta(days=10)).isoformat(),
                "rca_classification": "Change",
            },
        ]

        state = app.compute_osha_state_from_incidents(incidents)

        self.assertEqual(state["incident_number"], "200")
        self.assertEqual(state["reason"], "Deploy")
        self.assertEqual(state["days_since"], 3)
        self.assertEqual(state["prior_count"], 7)

    def test_compute_osha_state_from_incidents_counts_missed_task(self):
        today = date.today()
        incidents = [
            {
                "inc_number": "INC-400",
                "date": (today - timedelta(days=2)).isoformat(),
                "rca_classification": "Missed Task Incident",
            },
            {
                "inc_number": "INC-300",
                "date": (today - timedelta(days=5)).isoformat(),
                "rca_classification": "Change",
            },
        ]

        state = app.compute_osha_state_from_incidents(incidents)

        self.assertEqual(state["incident_number"], "400")
        self.assertEqual(state["reason"], "Missed")
        self.assertEqual(state["days_since"], 2)

    def test_compute_osha_state_from_incidents_skips_same_day_prior(self):
        today = date.today()
        incidents = [
            {
                "inc_number": "INC-300",
                "date": (today - timedelta(days=1)).isoformat(),
                "rca_classification": "Deploy",
            },
            {
                "inc_number": "INC-250",
                "date": (today - timedelta(days=1)).isoformat(),
                "rca_classification": "Change",
            },
            {
                "inc_number": "INC-200",
                "date": (today - timedelta(days=9)).isoformat(),
                "rca_classification": "Change",
            },
        ]

        state = app.compute_osha_state_from_incidents(incidents)

        self.assertEqual(state["incident_number"], "300")
        self.assertEqual(state["prior_count"], 8)
        self.assertEqual(
            state["prior_incident_date"],
            (today - timedelta(days=9)).isoformat(),
        )

    def test_compute_osha_state_from_incidents_fallbacks_to_saved_data(self):
        today = date.today()
        stored = {
            "incident_number": "555",
            "incident_date": (today - timedelta(days=4)).isoformat(),
            "prior_incident_date": (today - timedelta(days=9)).isoformat(),
            "reason": "Change",
            "last_reset": "2025-01-01T00:00:00",
        }

        with open(app.OSHA_DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(stored, f)

        state = app.compute_osha_state_from_incidents([])

        self.assertEqual(state["incident_number"], "555")
        self.assertEqual(state["days_since"], 4)
        self.assertEqual(state["prior_count"], 5)

    def test_wipe_endpoint_clears_local_files_and_sync_state(self):
        app.save_events(self.incidents_file, [{"inc_number": "INC-1", "event_type": "Operational Incident"}])
        app.save_events(self.other_file, [{"inc_number": "EV-1", "event_type": "Maintenance"}])

        with open(self.sync_config_file, "w") as f:
            json.dump({"last_sync": {"timestamp": "2024-08-01T00:00:00Z", "added_incidents": 1}}, f)

        client = app.app.test_client()
        response = client.post("/sync/wipe")

        self.assertEqual(response.status_code, 200)
        self.assertFalse(os.path.exists(self.incidents_file))
        self.assertFalse(os.path.exists(self.other_file))

        with open(self.sync_config_file, "r") as f:
            updated_config = json.load(f)

        self.assertFalse(updated_config.get("last_sync"))


class StatsViewTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.incident_path = os.path.join(self.temp_dir.name, "incidents.json")
        self.product_key_path = os.path.join(self.temp_dir.name, "product_pillar_key.json")

        self.original_data_file = app.DATA_FILE
        self.original_product_key_file = app.PRODUCT_KEY_FILE

        app.DATA_FILE = self.incident_path
        app.PRODUCT_KEY_FILE = self.product_key_path

        self.addCleanup(self._restore_paths)

        with open(self.product_key_path, "w", encoding="utf-8") as f:
            json.dump({}, f)

    def _restore_paths(self):
        app.DATA_FILE = self.original_data_file
        app.PRODUCT_KEY_FILE = self.original_product_key_file

    def test_stats_view_counts_unique_incidents(self):
        incidents = [
            {
                "inc_number": "INC-100",
                "date": "2025-01-10",
                "reported_at": "2025-01-10T12:00:00Z",
                "severity": "Sev 1",
                "product": "Product A",
                "pillar": "Pillar X",
                "rca_classification": "Code",
            },
            {
                "inc_number": "INC-100",
                "date": "2025-01-10",
                "reported_at": "2025-01-10T12:00:00Z",
                "severity": "Sev 1",
                "product": "Product B",
                "pillar": "Pillar Y",
                "rca_classification": "Code",
            },
        ]

        with open(self.incident_path, "w", encoding="utf-8") as f:
            json.dump(incidents, f)

        with app.app.test_request_context("/stats?year=2025"):
            with mock.patch("app.render_template") as render_template_mock:
                render_template_mock.return_value = "rendered"

                result = app.stats_view()

        self.assertEqual(result, "rendered")
        render_template_mock.assert_called_once()
        _, kwargs = render_template_mock.call_args

        self.assertEqual(kwargs["total_incidents"], 1)
        self.assertEqual(
            kwargs["classification_counts"],
            {"self_inflicted": 1, "non_procedural": 0, "unknown": 0},
        )


class AutoSyncTests(unittest.TestCase):
    def test_auto_sync_due_when_never_synced(self):
        config = {"cadence": "hourly", "token": "abc", "last_sync": {}}
        now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)

        self.assertTrue(app.is_auto_sync_due(config, now=now))

    def test_auto_sync_skips_without_token(self):
        config = {"cadence": "hourly", "last_sync": {}}
        now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)

        self.assertFalse(app.is_auto_sync_due(config, now=now))

    def test_auto_sync_waits_until_interval_passes(self):
        config = {
            "cadence": "daily",
            "token": "abc",
            "last_sync": {"timestamp": "2025-01-01T10:00:00Z"},
        }
        now = datetime(2025, 1, 1, 15, 0, tzinfo=timezone.utc)

        self.assertFalse(app.is_auto_sync_due(config, now=now))

        later = datetime(2025, 1, 2, 10, 1, tzinfo=timezone.utc)
        self.assertTrue(app.is_auto_sync_due(config, now=later))


if __name__ == "__main__":
    unittest.main()
