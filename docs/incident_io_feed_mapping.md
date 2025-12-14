# Incident.io feed normalization

This app pulls incidents from the incident.io v2 API and normalizes them into the local `incidents.json` and `others.json` files. The logic below explains how RCA Classification is identified and what other fields are extracted from each API record.

## RCA Classification detection
- The normalizer looks for the custom field named **"RCA Classification"** (case-insensitive) or the custom field with ID `01JZ0PNKHCB3M6NX0AHPABS59D`.
- For the matching custom field entry, it inspects each value and returns the first non-empty option value (`value`, `name`, `value_text`, or the nested `value_option.value/name`).
- If the field is absent or empty, the RCA Classification defaults to `"Not Classified"`.

## Other fields pulled from the incident.io payload
Each incident returned by the API is transformed into one or more normalized payloads (one per associated product). The following fields are extracted:
- **Incident number (`inc_number`)** — Taken from the incident `reference` or `id`.
- **Reported date/time (`reported_at` and derived `date`)** — Uses the "Reported At" incident timestamp value when present; otherwise falls back to `created_at`. The timestamp is shifted to the America/New_York timezone and the date portion becomes the incident day.
- **Severity (`severity`)** — Uses the severity object’s `name`/`label` or the raw severity string.
- **Event type (`event_type`)** — Uses the incident type name/label or raw value; defaults to `Operational Incident` when missing.
- **Title (`title`)** — Prefers the incident `name`, then `title`/`incident_title`, and finally the incident number.
- **Products (`product`)** — Pulled from the catalog custom field named "Product". If multiple products are listed, the incident is expanded into multiple payloads (one per product).
- **Solution pillar (`pillar`)** — Pulled from the catalog custom field named "Solution Pillar" and optionally remapped via `product_pillar_key.json` so each product uses the mapped pillar when available.
- **Client impact duration (`client_impact_duration_seconds`)** — Reads the duration metric named "Client Impact Duration" (seconds) and falls back to the similarly named custom field when the metric is missing.

## Storage behavior
- Operational incidents are written to `incidents.json`; other event types are written to `others.json`.
- Duplicate incident/product combinations are updated in place; new combinations are appended.
- When running a dry run, the normalization logic is executed without writing to disk.
