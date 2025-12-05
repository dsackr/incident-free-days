from flask import Flask, jsonify, render_template, request, redirect, send_file, url_for
import re
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
import csv
import calendar
import json
import os
import datetime as dt
from io import StringIO, BytesIO
from PIL import Image, ImageDraw, ImageFont

import incident_io_client

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "incidents.json")
OTHER_EVENTS_FILE = os.path.join(BASE_DIR, "others.json")
PRODUCT_KEY_FILE = os.path.join(BASE_DIR, "product_pillar_key.json")
SYNC_CONFIG_FILE = os.path.join(BASE_DIR, "sync_config.json")
DEFAULT_YEAR = 2025

DEFAULT_FIELD_MAPPING = {
    "inc_number": ["incident_number", "incident_id", "id", "reference", "name"],
    "severity": ["severity", "severity_level"],
    "event_type": ["incident_type", "type"],
    "reported_at": ["started_at", "started", "created_at", "created", "detected_at"],
    "closed_at": ["resolved_at", "closed_at", "ended_at", "completed_at"],
    "duration_seconds": ["duration_seconds", "duration"],
    "products": ["products", "services", "service", "teams", "impacted_services", "components"],
}

RECENT_SYNC_EVENT_LIMIT = 25
DEFAULT_SYNC_WINDOW_DAYS = 14


def load_events(path):
    if not os.path.exists(path):
        return []

    with open(path, "r") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return []

    if not isinstance(data, list):
        return []

    return [entry for entry in data if isinstance(entry, dict)]


def save_events(path, events):
    with open(path, "w") as f:
        json.dump(events, f, indent=2)


def load_product_key():
    if not os.path.exists(PRODUCT_KEY_FILE):
        return {}

    try:
        with open(PRODUCT_KEY_FILE, "r") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}

    if not isinstance(data, dict):
        return {}

    # Normalize keys to strip whitespace for consistent lookups
    return {str(k).strip(): str(v).strip() for k, v in data.items() if str(k).strip()}


def normalize_field_mapping(raw_mapping):
    raw_mapping = raw_mapping or {}
    normalized = {}

    for field, defaults in DEFAULT_FIELD_MAPPING.items():
        value = raw_mapping.get(field)
        entries = []

        if isinstance(value, str):
            entries = [item.strip() for item in value.split(",") if item.strip()]
        elif isinstance(value, list):
            for item in value:
                cleaned = str(item).strip()
                if cleaned:
                    entries.append(cleaned)

        normalized[field] = entries or list(defaults)

    return normalized


def load_sync_config():
    if not os.path.exists(SYNC_CONFIG_FILE):
        return {"cadence": "daily", "field_mapping": DEFAULT_FIELD_MAPPING}

    try:
        with open(SYNC_CONFIG_FILE, "r") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"cadence": "daily", "field_mapping": DEFAULT_FIELD_MAPPING}

    if not isinstance(data, dict):
        return {"cadence": "daily", "field_mapping": DEFAULT_FIELD_MAPPING}

    data.setdefault("cadence", "daily")
    data.setdefault("field_mapping", DEFAULT_FIELD_MAPPING)
    data["field_mapping"] = normalize_field_mapping(data.get("field_mapping"))
    return data


def save_sync_config(config):
    normalized_mapping = normalize_field_mapping(config.get("field_mapping"))
    payload = {
        "cadence": config.get("cadence", "daily"),
        "base_url": config.get("base_url") or "",
        "token": config.get("token") or "",
        "start_date": config.get("start_date") or "",
        "end_date": config.get("end_date") or "",
        "last_sync": config.get("last_sync") or {},
        "field_mapping": normalized_mapping,
    }

    try:
        with open(SYNC_CONFIG_FILE, "w") as f:
            json.dump(payload, f, indent=2)
    except OSError:
        # If we cannot persist, fall back to in-memory only behavior.
        pass


def build_sync_config_view(raw_config):
    raw_config = raw_config or {}
    last_sync_raw = raw_config.get("last_sync") or {}

    last_sync_total = last_sync_raw.get("total_events")
    if last_sync_total is None:
        last_sync_total = (last_sync_raw.get("added_incidents") or 0) + (
            last_sync_raw.get("added_other_events") or 0
        ) + (
            last_sync_raw.get("updated_events") or 0
        )

    return {
        "cadence": raw_config.get("cadence", "daily"),
        "base_url": raw_config.get("base_url", ""),
        "start_date": raw_config.get("start_date", ""),
        "end_date": raw_config.get("end_date", ""),
        "token_saved": bool(raw_config.get("token")),
        "last_sync": last_sync_raw,
        "last_sync_display": format_sync_timestamp(last_sync_raw.get("timestamp")),
        "last_sync_total": last_sync_total,
        "last_sync_events": last_sync_raw.get("events") or [],
        "field_mapping": raw_config.get("field_mapping", DEFAULT_FIELD_MAPPING),
    }


def parse_date(raw_value):
    if not raw_value:
        return None

    raw_value = raw_value.strip()
    # Normalize trailing Z to a timezone-aware ISO string
    candidates = [raw_value]
    if raw_value.endswith("Z"):
        candidates.append(raw_value[:-1] + "+00:00")

    for value in candidates:
        for fmt in (
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S%z",
        ):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue

        try:
            return datetime.fromisoformat(value).date()
        except ValueError:
            continue

    return None


def parse_time_component(raw_value):
    if not raw_value:
        return None

    raw_value = raw_value.strip()
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(raw_value, fmt).time()
        except ValueError:
            continue

    try:
        return time.fromisoformat(raw_value)
    except ValueError:
        return None


def parse_datetime(raw_value):
    if not raw_value:
        return None

    raw_value = raw_value.strip()
    candidates = [raw_value]
    if raw_value.endswith("Z"):
        candidates.append(raw_value[:-1] + "+00:00")

    for value in candidates:
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S%z",
            "%m/%d/%Y %H:%M",
            "%m/%d/%Y %H:%M:%S",
        ):
            try:
                parsed = datetime.strptime(value, fmt)
                return parsed.replace(tzinfo=None)
            except ValueError:
                continue

        try:
            parsed = datetime.fromisoformat(value)
            return parsed.replace(tzinfo=None)
        except ValueError:
            continue

    return None


def format_sync_timestamp(raw_value, tz_name="America/New_York"):
    if not raw_value:
        return ""

    text = str(raw_value).strip()
    if not text:
        return ""

    candidate = text[:-1] + "+00:00" if text.endswith("Z") else text

    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return text

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    try:
        target_tz = ZoneInfo(tz_name)
    except Exception:
        target_tz = timezone.utc

    localized = parsed.astimezone(target_tz)
    return localized.strftime("%m/%d/%Y %H:%M")


def shift_utc_to_est(dt):
    if dt is None:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(ZoneInfo("America/New_York"))


def is_sev6(value):
    return str(value) == "Other (Sev 6)"


def normalize_severity_label(value):
    """Extract the numeric severity value from a variety of label formats."""

    text = (str(value) or "").strip()
    if not text:
        return "?"

    if text.lower().startswith("other"):
        return "6"

    match = re.search(r"([1-6])", text)
    if match:
        return match.group(1)

    return text


def compute_event_dates(event, duration_enabled=False):
    """Return a list of dates covered by an event using reported/closed times."""

    reported_at = parse_datetime(event.get("reported_at"))
    closed_at = parse_datetime(event.get("closed_at"))

    if reported_at is None:
        try:
            fallback_date = datetime.strptime(event.get("date", ""), "%Y-%m-%d").date()
            reported_at = datetime.combine(fallback_date, time.min)
        except (TypeError, ValueError):
            return []

    if closed_at is None:
        try:
            duration_seconds = int(event.get("duration_seconds", 0) or 0)
        except (TypeError, ValueError):
            duration_seconds = 0

        closed_at = reported_at + timedelta(seconds=max(duration_seconds, 0))

    start_date = reported_at.date()
    end_date = max(closed_at.date(), start_date)

    if not duration_enabled:
        return [start_date]

    days = []
    current = start_date
    while current <= end_date:
        days.append(current)
        current += timedelta(days=1)

    return days


def build_calendar(year, entries_by_date, classify_fn):
    """Return data structure describing all 12 months with color info per day."""

    today = date.today()
    cal = calendar.Calendar(firstweekday=6)  # Sunday = 6

    months = []
    for month in range(1, 13):
        weeks = []
        for week in cal.monthdatescalendar(year, month):
            week_cells = []
            for d in week:
                if d.month != month:
                    week_cells.append(
                        {"day": "", "date_str": None, "css_class": "empty"}
                    )
                    continue

                iso = d.isoformat()
                css_class = classify_fn(d, entries_by_date.get(iso, []), today)

                week_cells.append(
                    {
                        "day": d.day,
                        "date_str": iso,
                        "css_class": css_class,
                    }
                )
            weeks.append(week_cells)

        months.append(
            {
                "name": calendar.month_name[month],
                "weeks": weeks,
            }
        )

    return months


def resolve_pillar(product, provided_pillar="", mapping=None):
    mapping = mapping if mapping is not None else load_product_key()
    product_key = (product or "").strip()
    if mapping and product_key:
        return mapping.get(product_key, provided_pillar)
    return provided_pillar


def incident_exists(incidents, inc_number):
    return any(inc.get("inc_number") == inc_number for inc in incidents)


def get_client_ip(req):
    """Extract the client's IP address from the request.

    Priority is given to the first value in X-Forwarded-For, followed by
    X-Real-IP, and finally Flask's remote_addr. The value is normalized by
    stripping whitespace and falling back to "Unknown" when no candidate is
    available.
    """

    forwarded_for = req.headers.get("X-Forwarded-For", "")
    if forwarded_for:
        first_ip = forwarded_for.split(",")[0].strip()
        if first_ip:
            return first_ip

    real_ip = (req.headers.get("X-Real-IP") or "").strip()
    if real_ip:
        return real_ip

    return (req.remote_addr or "Unknown").strip()


def _extract_product_values(api_incident, product_keys=None):
    products = []
    candidate_keys = product_keys or DEFAULT_FIELD_MAPPING["products"]

    def add_value(value):
        if value is None:
            return

        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned:
                products.append(cleaned)
            return

        if isinstance(value, dict):
            for key in ("name", "full_name", "slug", "id"):
                candidate = value.get(key)
                if candidate:
                    products.append(str(candidate).strip())
                    return
            return

        if isinstance(value, list):
            for item in value:
                add_value(item)

    for key in candidate_keys:
        add_value(api_incident.get(key))

    if not products:
        return [""]

    deduped = []
    seen = set()
    for product in products:
        if product and product not in seen:
            seen.add(product)
            deduped.append(product)

    return deduped or [""]


def _extract_first_value(api_incident, keys):
    for key in keys:
        value = api_incident.get(key)
        if value is not None:
            return value
    return None


def _extract_reported_date(api_incident):
    for entry in api_incident.get("incident_timestamp_values") or []:
        incident_timestamp = entry.get("incident_timestamp") or {}
        timestamp_name = (incident_timestamp.get("name") or "").strip()
        if timestamp_name.casefold() != "reported at".casefold():
            continue

        raw_value = (entry.get("value") or {}).get("value")
        parsed = parse_datetime(raw_value)
        if parsed:
            reported = shift_utc_to_est(parsed)
            return reported.date(), reported.isoformat(timespec="seconds")

    created_raw = api_incident.get("created_at")
    created_dt = parse_datetime(created_raw)
    if created_dt:
        reported = shift_utc_to_est(created_dt)
        return reported.date(), reported.isoformat(timespec="seconds")

    return None, None


def _get_catalog_custom_values(api_incident, field_name, default="Unknown"):
    target = (field_name or "").strip().casefold()
    results = []

    for entry in api_incident.get("custom_field_entries") or []:
        custom_field = entry.get("custom_field") or {}
        name = (custom_field.get("name") or "").strip().casefold()
        if name != target:
            continue

        for value in entry.get("values") or []:
            catalog_entry = value.get("value_catalog_entry") or {}
            name_value = catalog_entry.get("name")
            if name_value:
                results.append(name_value)

    if results:
        return results

    return [default]


def _get_custom_field_value(api_incident, field_name, default="Unknown"):
    target = (field_name or "").strip().casefold()

    for entry in api_incident.get("custom_field_entries") or []:
        custom_field = entry.get("custom_field") or {}
        name = (custom_field.get("name") or "").strip().casefold()
        if name != target:
            continue

        values = entry.get("values") or []
        extracted = []
        for value in values:
            if not isinstance(value, dict):
                continue

            catalog_entry = value.get("value_catalog_entry")
            if isinstance(catalog_entry, dict) and catalog_entry.get("name"):
                extracted.append(catalog_entry.get("name"))
                continue

            for key in ("value", "value_text", "value_numeric", "value_boolean"):
                raw_value = value.get(key)
                if raw_value not in (None, ""):
                    extracted.append(str(raw_value))
                    break

        if extracted:
            return ", ".join(str(val) for val in extracted if str(val).strip())

        raw_value = entry.get("value")
        if raw_value not in (None, ""):
            return str(raw_value)

    return default


def normalize_incident_payloads(api_incident, mapping=None, field_mapping=None):
    mapping = mapping if mapping is not None else load_product_key()
    field_mapping = normalize_field_mapping(field_mapping)

    inc_number = (api_incident.get("reference") or api_incident.get("id") or "").strip()
    if not inc_number:
        return []

    severity_raw = api_incident.get("severity")
    if isinstance(severity_raw, dict):
        severity = severity_raw.get("name") or severity_raw.get("label") or ""
    else:
        severity = str(severity_raw).strip() if severity_raw else ""

    reported_date, reported_raw = _extract_reported_date(api_incident)
    if not reported_date:
        return []

    products = _get_catalog_custom_values(api_incident, "Product", default="Unknown")
    pillar_values = _get_catalog_custom_values(
        api_incident, "Solution Pillar", default="Unknown"
    )
    pillar_hint = pillar_values[0] if pillar_values else "Unknown"

    rca_classification = _get_custom_field_value(api_incident, "RCA Classification", default="Unknown")

    incident_type_raw = api_incident.get("incident_type") or api_incident.get("type")
    if isinstance(incident_type_raw, dict):
        event_type = incident_type_raw.get("name") or incident_type_raw.get("label") or ""
    else:
        event_type = str(incident_type_raw).strip() if incident_type_raw else ""

    event_type = event_type or "Operational Incident"

    title = (
        api_incident.get("name")
        or api_incident.get("title")
        or api_incident.get("incident_title")
        or inc_number
    )

    payloads = []
    for product in products:
        resolved_pillar = resolve_pillar(
            product,
            provided_pillar=pillar_hint,
            mapping=mapping,
        )

        payloads.append(
            {
                "inc_number": inc_number,
                "date": reported_date.isoformat(),
                "severity": severity,
                "product": product or "Unknown",
                "pillar": resolved_pillar or pillar_hint or "Unknown",
                "reported_at": reported_raw
                or f"{reported_date.isoformat()}T00:00:00",
                "event_type": event_type,
                "title": title,
                "rca_classification": rca_classification,
            }
        )

    return payloads


def sync_incidents_from_api(
    *,
    dry_run=False,
    start_date=None,
    end_date=None,
    token=None,
    base_url=None,
    include_samples=False,
    incidents_file=DATA_FILE,
    other_events_file=OTHER_EVENTS_FILE,
    field_mapping=None,
):
    config_defaults = load_sync_config()
    today = date.today()
    default_start = today - timedelta(days=DEFAULT_SYNC_WINDOW_DAYS - 1)

    effective_start = start_date or config_defaults.get("start_date") or default_start.isoformat()
    effective_end = end_date or config_defaults.get("end_date") or today.isoformat()

    start_date_obj = parse_date(effective_start) if effective_start else None
    end_date_obj = parse_date(effective_end) if effective_end else None
    mapping = load_product_key()
    field_mapping = normalize_field_mapping(field_mapping)

    incidents = load_events(incidents_file)
    other_events = load_events(other_events_file)

    existing_incidents = {
        (
            inc.get("inc_number"),
            (inc.get("product") or "").strip(),
        )
        for inc in incidents
        if inc.get("inc_number")
    }

    existing_other = {
        (
            evt.get("inc_number"),
            (evt.get("product") or "").strip(),
        )
        for evt in other_events
        if evt.get("inc_number")
    }

    try:
        fetched = incident_io_client.fetch_incidents(
            base_url=base_url,
            token=token,
        )
    except incident_io_client.IncidentAPIError as exc:
        return {
            "error": str(exc),
            "added_incidents": 0,
            "added_other_events": 0,
            "updated_events": 0,
            "dry_run": dry_run,
        }

    added_incidents = 0
    added_other_events = 0
    updated_events = 0
    sample_payloads = []
    added_event_details = []

    incident_seen = set(existing_incidents)
    other_seen = set(existing_other)

    incident_lookup = {
        (
            inc.get("inc_number"),
            (inc.get("product") or "").strip(),
        ): inc
        for inc in incidents
        if inc.get("inc_number")
    }
    other_lookup = {
        (
            evt.get("inc_number"),
            (evt.get("product") or "").strip(),
        ): evt
        for evt in other_events
        if evt.get("inc_number")
    }

    for api_incident in fetched:
        normalized = normalize_incident_payloads(api_incident, mapping=mapping, field_mapping=field_mapping)

        filtered_payloads = []
        for payload in normalized:
            reported_value = payload.get("reported_at")
            reported_dt = parse_datetime(reported_value)
            reported_date = reported_dt.date() if reported_dt else None

            if start_date_obj and (not reported_date or reported_date < start_date_obj):
                continue
            if end_date_obj and (not reported_date or reported_date > end_date_obj):
                continue

            filtered_payloads.append(payload)

        for payload in filtered_payloads:
            lookup_key = (payload.get("inc_number"), (payload.get("product") or "").strip())
            is_operational = payload.get("event_type") == "Operational Incident"

            target_lookup = incident_lookup if is_operational else other_lookup
            target_collection = incidents if is_operational else other_events
            seen_collection = incident_seen if is_operational else other_seen

            existing = target_lookup.get(lookup_key)
            if existing:
                differences = {
                    field: value for field, value in payload.items() if existing.get(field) != value
                }

                if differences:
                    updated_events += 1
                    if not dry_run:
                        existing.update(differences)
                        if len(added_event_details) < RECENT_SYNC_EVENT_LIMIT:
                            added_event_details.append(
                                {
                                    "inc_number": payload.get("inc_number", ""),
                                    "event_type": payload.get("event_type", ""),
                                    "title": payload.get("title", payload.get("inc_number", "")),
                                    "pillar": payload.get("pillar", ""),
                                    "product": payload.get("product", ""),
                                    "severity": payload.get("severity", ""),
                                    "change_type": "updated",
                                }
                            )
                continue

            if is_operational:
                added_incidents += 1
                seen_collection.add(lookup_key)
                if not dry_run:
                    incidents.append(payload)
                    incident_lookup[lookup_key] = payload
            else:
                added_other_events += 1
                seen_collection.add(lookup_key)
                if not dry_run:
                    other_events.append(payload)
                    other_lookup[lookup_key] = payload

            if not dry_run and len(added_event_details) < RECENT_SYNC_EVENT_LIMIT:
                added_event_details.append(
                    {
                        "inc_number": payload.get("inc_number", ""),
                        "event_type": payload.get("event_type", ""),
                        "title": payload.get("title", payload.get("inc_number", "")),
                        "pillar": payload.get("pillar", ""),
                        "product": payload.get("product", ""),
                        "severity": payload.get("severity", ""),
                        "change_type": "added",
                    }
                )

            if include_samples and len(sample_payloads) < 10:
                sample_payloads.append(
                    {
                        "source": {
                            "id": api_incident.get("id") or api_incident.get("incident_id"),
                            "name": api_incident.get("name"),
                            "severity": api_incident.get("severity"),
                            "started_at": payload.get("reported_at"),
                            "resolved_at": payload.get("closed_at"),
                        },
                        "normalized": payload,
                    }
                )

    if not dry_run:
        save_events(incidents_file, incidents)
        save_events(other_events_file, other_events)

        config = load_sync_config()
        config["last_sync"] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "added_incidents": added_incidents,
            "added_other_events": added_other_events,
            "updated_events": updated_events,
            "start_date": effective_start or "",
            "end_date": effective_end or "",
            "events": added_event_details[:RECENT_SYNC_EVENT_LIMIT],
            "total_events": len(added_event_details),
        }
        save_sync_config(config)

    return {
        "fetched": len(fetched),
        "added_incidents": added_incidents,
        "added_other_events": added_other_events,
        "updated_events": updated_events,
        "dry_run": dry_run,
        "samples": sample_payloads if include_samples else [],
    }


def render_dashboard(tab_override=None, show_config_tab=False):
    year_str = request.args.get("year")
    view_mode = request.args.get("view", "yearly")
    month_str = request.args.get("month")
    incident_duration_enabled = request.args.get("incident_duration") == "1"
    incident_multi_day_only = incident_duration_enabled and request.args.get(
        "incident_multi_day"
    ) == "1"

    other_duration_enabled = request.args.get("other_duration") == "1"
    other_multi_day_only = other_duration_enabled and request.args.get(
        "other_multi_day"
    ) == "1"
    pillar_filter = request.args.get("pillar") or None
    product_filter = request.args.get("product") or None
    severity_params = [value for value in request.args.getlist("severity") if value]
    event_type_params = [value for value in request.args.getlist("event_type") if value]
    severity_param_supplied = "severity" in request.args
    key_missing = request.args.get("key_missing") == "1"
    key_uploaded = request.args.get("key_uploaded") == "1"
    key_error = request.args.get("key_error")
    active_tab = tab_override or request.args.get("tab", "incidents")
    allowed_tabs = {"incidents", "others"}
    if show_config_tab:
        allowed_tabs.add("form")

    if active_tab not in allowed_tabs:
        active_tab = "incidents"
    if not show_config_tab and active_tab == "form":
        return redirect(url_for("ioadmin"))

    sync_config_raw = load_sync_config()
    sync_config = build_sync_config_view(sync_config_raw)
    env_token_available = bool(os.getenv("INCIDENT_IO_API_TOKEN"))

    try:
        year = int(year_str) if year_str else DEFAULT_YEAR
    except ValueError:
        year = DEFAULT_YEAR

    try:
        month_selection = int(month_str) if month_str else None
    except ValueError:
        month_selection = None

    if view_mode not in {"yearly", "monthly"}:
        view_mode = "yearly"

    incidents = [
        event
        for event in load_events(DATA_FILE)
        if (event.get("event_type") or "Operational Incident") == "Operational Incident"
    ]
    other_events = [
        event
        for event in load_events(OTHER_EVENTS_FILE)
        if (event.get("event_type") or "") != "Operational Incident"
    ]

    key_mapping = load_product_key()
    key_present = bool(key_mapping)

    all_events = incidents + other_events

    # Build product ↔ pillar relationships
    product_pillar_map = {k: v for k, v in key_mapping.items() if k and v}
    for event in all_events:
        product = (event.get("product") or "").strip()
        pillar = (event.get("pillar") or "").strip()

        if not product:
            continue

        if not pillar:
            pillar = resolve_pillar(product, mapping=key_mapping)

        if pillar:
            product_pillar_map.setdefault(product, pillar)
        elif product not in product_pillar_map:
            product_pillar_map[product] = None

    products_by_pillar = {}
    for product, pillar in product_pillar_map.items():
        products_by_pillar.setdefault(pillar, set()).add(product)

    products_by_pillar = {
        pillar: sorted(values)
        for pillar, values in products_by_pillar.items()
        if pillar is not None
    }
    products_by_pillar["__all__"] = sorted(product_pillar_map.keys())

    pillars = sorted({value for value in product_pillar_map.values() if value})
    products = products_by_pillar["__all__"]
    severities = sorted({inc.get("severity") for inc in incidents if inc.get("severity")})
    event_types = sorted(
        {
            event.get("event_type")
            for event in other_events
            if event.get("event_type")
        }
    )

    if severity_params:
        severity_filter = severity_params
    else:
        severity_filter = []

    if event_type_params:
        event_type_filter = event_type_params
    else:
        event_type_filter = []

    if product_filter:
        resolved_pillar = product_pillar_map.get(product_filter) or resolve_pillar(
            product_filter, mapping=key_mapping
        )
        if resolved_pillar:
            pillar_filter = resolved_pillar

    if pillar_filter:
        products = products_by_pillar.get(pillar_filter, [])
        if product_filter and product_filter not in products:
            product_filter = None

    if view_mode == "monthly":
        if month_selection is None:
            today = date.today()
            month_selection = today.month if today.year == year else 1
        if not (1 <= month_selection <= 12):
            month_selection = None
    else:
        month_selection = None

    def filter_and_group(
        events,
        *,
        duration_enabled,
        multi_day_only,
        apply_event_type_filter=False,
        include_severity_filter=False,
    ):
        filtered = []
        grouped = {}
        dates_with_non_sev6 = set()

        for event in events:
            if pillar_filter and event.get("pillar") != pillar_filter:
                continue
            if product_filter and event.get("product") != product_filter:
                continue
            if apply_event_type_filter and event_type_filter and event.get("event_type") not in event_type_filter:
                continue

            if include_severity_filter and event.get("event_type") == "Operational Incident":
                if severity_filter and not any(
                    event.get("severity") == selected for selected in severity_filter
                ):
                    continue

            covered_dates = compute_event_dates(event, duration_enabled)
            if not covered_dates:
                continue

            if multi_day_only and len(set(covered_dates)) < 2:
                continue

            in_month_view = view_mode == "monthly" and month_selection
            if in_month_view:
                covered_dates = [
                    d
                    for d in covered_dates
                    if d.year == year and d.month == month_selection
                ]
            else:
                covered_dates = [d for d in covered_dates if d.year == year]

            if not covered_dates:
                continue

            filtered.append(event)

            for d in covered_dates:
                iso = d.isoformat()
                grouped.setdefault(iso, []).append(event)
                if event.get("event_type") == "Operational Incident" and not is_sev6(
                    event.get("severity")
                ):
                    dates_with_non_sev6.add(d)

        return filtered, grouped, dates_with_non_sev6

    incidents_filtered, incidents_by_date, incident_dates = filter_and_group(
        incidents,
        duration_enabled=incident_duration_enabled,
        multi_day_only=incident_multi_day_only,
        apply_event_type_filter=False,
        include_severity_filter=True,
    )
    other_filtered, other_by_date, _ = filter_and_group(
        other_events,
        duration_enabled=other_duration_enabled,
        multi_day_only=other_multi_day_only,
        apply_event_type_filter=True,
        include_severity_filter=False,
    )

    unique_incident_count = len(
        {inc.get("inc_number") for inc in incidents_filtered if inc.get("inc_number")}
    )

    if view_mode == "monthly" and month_selection:
        days_in_range = calendar.monthrange(year, month_selection)[1]
        days_with_incidents = {d for d in incident_dates if d.month == month_selection}
    else:
        days_in_range = (date(year + 1, 1, 1) - date(year, 1, 1)).days
        days_with_incidents = incident_dates

    incident_free_days = max(days_in_range - len(days_with_incidents), 0)

    def classify_operational(day, day_events, today):
        if day_events:
            sev6_only = all(is_sev6(evt.get("severity")) for evt in day_events)
            if sev6_only:
                return "day-sev6"

            incident_count = len(day_events)
            if incident_count >= 4:
                intensity_class = "day-incident-heavy"
            elif incident_count >= 2:
                intensity_class = "day-incident-medium"
            else:
                intensity_class = "day-incident-light"

            return intensity_class

        return "day-ok" if day < today else "day-none"

    def classify_other(day, day_events, _today):
        if not day_events:
            return "day-none"

        types = {evt.get("event_type") for evt in day_events}
        if "War Room" in types:
            return "day-war-room"
        if "Deployment Event" in types:
            return "day-deployment"
        if "Operational Event" in types:
            return "day-operational-event"
        return "day-other-event"

    incident_months = build_calendar(year, incidents_by_date, classify_operational)
    other_months = build_calendar(year, other_by_date, classify_other)

    if view_mode == "monthly" and month_selection:
        incident_months = [incident_months[month_selection - 1]]
        other_months = [other_months[month_selection - 1]]

    incident_filters = []
    other_filters = []

    def build_remove_link(kind, value=None, tab=None):
        target_tab = tab or active_tab
        params = {"view": view_mode, "year": year, "tab": target_tab}
        if month_selection:
            params["month"] = month_selection
        if target_tab == "incidents":
            if incident_duration_enabled:
                params["incident_duration"] = "1"
            if incident_multi_day_only:
                params["incident_multi_day"] = "1"
        if target_tab == "others":
            if other_duration_enabled:
                params["other_duration"] = "1"
            if other_multi_day_only:
                params["other_multi_day"] = "1"

        if kind != "pillar" and pillar_filter:
            params["pillar"] = pillar_filter
        if kind != "product" and product_filter:
            params["product"] = product_filter

        if target_tab == "incidents":
            if kind == "severity":
                remaining = [sev for sev in severity_filter if sev != value]
                if remaining:
                    params["severity"] = remaining
                elif severity_param_supplied:
                    params["severity"] = [""]
            else:
                if severity_filter:
                    params["severity"] = severity_filter
                elif severity_param_supplied:
                    params["severity"] = [""]

        if target_tab == "others":
            if kind == "event_type":
                remaining_types = [etype for etype in event_type_filter if etype != value]
                if remaining_types:
                    params["event_type"] = remaining_types
            elif event_type_filter:
                params["event_type"] = event_type_filter

        return url_for("index", **params)

    if pillar_filter:
        incident_filters.append(
            {
                "label": "Pillar",
                "value": pillar_filter,
                "remove_link": build_remove_link("pillar", tab="incidents"),
            }
        )
        other_filters.append(
            {
                "label": "Pillar",
                "value": pillar_filter,
                "remove_link": build_remove_link("pillar", tab="others"),
            }
        )
    if product_filter:
        incident_filters.append(
            {
                "label": "Product",
                "value": product_filter,
                "remove_link": build_remove_link("product", tab="incidents"),
            }
        )
        other_filters.append(
            {
                "label": "Product",
                "value": product_filter,
                "remove_link": build_remove_link("product", tab="others"),
            }
        )
    if severity_filter:
        for severity in severity_filter:
            incident_filters.append(
                {
                    "label": "Severity",
                    "value": severity,
                    "remove_link": build_remove_link("severity", severity, tab="incidents"),
                }
            )
    if event_type_filter:
        for event_type in event_type_filter:
            other_filters.append(
                {
                    "label": "Event Type",
                    "value": event_type,
                    "remove_link": build_remove_link("event_type", event_type, tab="others"),
                }
            )

    def build_link(target_view, target_year, target_month=None, tab=None):
        target_tab = tab or active_tab
        params = {"view": target_view, "year": target_year, "tab": target_tab}
        if target_month:
            params["month"] = target_month
        if target_tab == "incidents":
            if incident_duration_enabled:
                params["incident_duration"] = "1"
            if incident_multi_day_only:
                params["incident_multi_day"] = "1"
        if target_tab == "others":
            if other_duration_enabled:
                params["other_duration"] = "1"
            if other_multi_day_only:
                params["other_multi_day"] = "1"
        if pillar_filter:
            params["pillar"] = pillar_filter
        if product_filter:
            params["product"] = product_filter
        if target_tab == "incidents":
            if severity_filter:
                params["severity"] = severity_filter
            elif severity_param_supplied:
                params["severity"] = [""]
        if target_tab == "others" and event_type_filter:
            params["event_type"] = event_type_filter
        return url_for("index", **params)

    if view_mode == "monthly" and month_selection:
        current_period_label = f"{calendar.month_name[month_selection]} {year}"
        prev_year = year
        prev_month = month_selection - 1
        if prev_month < 1:
            prev_month = 12
            prev_year -= 1

        next_year = year
        next_month = month_selection + 1
        if next_month > 12:
            next_month = 1
            next_year += 1

        prev_link = build_link("monthly", prev_year, prev_month)
        next_link = build_link("monthly", next_year, next_month)
    else:
        current_period_label = str(year)
        prev_link = build_link("yearly", year - 1)
        next_link = build_link("yearly", year + 1)

    target_month_for_toggle = month_selection or date.today().month
    yearly_view_link = build_link("yearly", year)
    monthly_view_link = build_link("monthly", year, target_month_for_toggle)

    # sort newest-first for display
    incidents_sorted = sorted(
        incidents_filtered,
        key=lambda x: (x.get("reported_at") or x.get("date") or ""),
        reverse=True,
    )
    other_sorted = sorted(
        other_filtered,
        key=lambda x: (x.get("reported_at") or x.get("date") or ""),
        reverse=True,
    )

    client_ip = get_client_ip(request)

    return render_template(
        "index.html",
        year=year,
        incident_months=incident_months,
        other_months=other_months,
        incidents=incidents_sorted,
        other_events=other_sorted,
        view_mode=view_mode,
        month_selection=month_selection,
        pillars=pillars,
        products=products,
        severities=severities,
        event_types=event_types,
        products_by_pillar=products_by_pillar,
        product_pillar_map=product_pillar_map,
        pillar_filter=pillar_filter,
        product_filter=product_filter,
        severity_filter=severity_filter,
        event_type_filter=event_type_filter,
        calendar=calendar,
        incidents_by_date=incidents_by_date,
        other_by_date=other_by_date,
        incident_filters=incident_filters,
        other_filters=other_filters,
        incident_count=unique_incident_count,
        incident_free_days=incident_free_days,
        prev_link=prev_link,
        next_link=next_link,
        current_period_label=current_period_label,
        yearly_view_link=yearly_view_link,
        monthly_view_link=monthly_view_link,
        incident_duration_enabled=incident_duration_enabled,
        incident_multi_day_only=incident_multi_day_only,
        other_duration_enabled=other_duration_enabled,
        other_multi_day_only=other_multi_day_only,
        key_present=key_present,
        key_missing=key_missing,
        key_uploaded=key_uploaded,
        key_error=key_error,
        active_tab=active_tab,
        client_ip=client_ip,
        sync_config=sync_config,
        env_token_available=env_token_available,
        show_config_tab=show_config_tab,
    )


@app.route("/", methods=["GET"])
def index():
    return render_dashboard()


@app.route("/ioadmin", methods=["GET"])
def ioadmin():
    return render_dashboard(tab_override="form", show_config_tab=True)


@app.route("/calendar/eink.png")
def calendar_eink():
    # Default to current month/year if not provided
    now = dt.datetime.now()
    year = int(request.args.get("year", now.year))
    month = int(request.args.get("month", now.month))

    # E-ink panel resolution
    W, H = 1600, 1200

    # White background, RGB
    img = Image.new("RGB", (W, H), (255, 255, 255))
    draw = ImageDraw.Draw(img)

    # Prefer a readable TrueType font; fall back to default bitmap
    font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
    font_bold_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
    try:
        font_title = ImageFont.truetype(font_bold_path, 48)
        font_headers = ImageFont.truetype(font_bold_path, 32)
        font_days = ImageFont.truetype(font_path, 26)
        font_incident = ImageFont.truetype(font_path, 20)
    except OSError:
        font_title = ImageFont.load_default()
        font_headers = font_title
        font_days = font_title
        font_incident = font_title

    # Title: "December 2025"
    title = f"{calendar.month_name[month]} {year}"

    # Use textbbox for measurement to avoid removed FreeTypeFont.getsize
    title_bbox = draw.textbbox((0, 0), title, font=font_title)
    tw = title_bbox[2] - title_bbox[0]
    draw.text(((W - tw) // 2, 20), title, font=font_title, fill=(0, 0, 0))

    # Layout margins
    left_margin = 60
    right_margin = 60
    top_margin = 100
    bottom_margin = 60

    # 1 row for weekday headers + up to 6 weeks
    header_rows = 1
    week_rows = 6

    cell_w = (W - left_margin - right_margin) // 7
    cell_h = (H - top_margin - bottom_margin) // (header_rows + week_rows)

    # Weekday headers (Sun–Sat)
    calendar.setfirstweekday(calendar.SUNDAY)
    first = calendar.firstweekday()
    weekdays = [(first + i) % 7 for i in range(7)]

    for i, wd in enumerate(weekdays):
        label = calendar.day_abbr[wd]
        label_bbox = draw.textbbox((0, 0), label, font=font_headers)
        lw = label_bbox[2] - label_bbox[0]
        lh = label_bbox[3] - label_bbox[1]
        x = left_margin + i * cell_w + (cell_w - lw) // 2
        y = top_margin + (cell_h - lh) // 2
        draw.text((x, y), label, font=font_headers, fill=(0, 0, 0))

    # Pull incidents and classify each calendar day
    incidents = [
        event
        for event in load_events(DATA_FILE)
        if (event.get("event_type") or "Operational Incident") == "Operational Incident"
    ]

    incidents_by_date = {}
    incidents_by_inc_number = {}
    for event in incidents:
        inc_number_raw = (event.get("inc_number") or "").strip()
        severity_raw = (event.get("severity") or "").strip()

        for day in compute_event_dates(event, duration_enabled=False):
            if day.year == year and day.month == month:
                incidents_by_date.setdefault(day, []).append(event)
                if inc_number_raw:
                    incidents_by_inc_number.setdefault(day, {})[inc_number_raw] = severity_raw

    colors = {
        "none": (208, 208, 208),  # grey
        "ok": (46, 125, 50),  # green
        "light": (246, 188, 188),  # light red
        "medium": (229, 115, 115),  # medium red
        "heavy": (183, 28, 28),  # dark red
        "sev6": (249, 168, 37),  # yellow
    }

    text_colors = {
        "none": (0, 0, 0),
        "ok": (255, 255, 255),
        "light": (63, 29, 29),
        "medium": (255, 255, 255),
        "heavy": (255, 255, 255),
        "sev6": (31, 42, 59),
    }

    today = date.today()

    def classify_day(day_obj):
        events = incidents_by_date.get(day_obj, [])
        if events:
            sev6_only = all(is_sev6(evt.get("severity")) for evt in events)
            if sev6_only:
                return "sev6"

            count = len(events)
            if count >= 4:
                return "heavy"
            if count >= 2:
                return "medium"
            return "light"

        return "ok" if day_obj < today else "none"

    # Calendar grid
    cal = calendar.Calendar(firstweekday=calendar.SUNDAY)
    y_offset = top_margin + cell_h  # below weekday headers

    for row_idx, week in enumerate(cal.monthdatescalendar(year, month)):
        for col_idx, day in enumerate(week):
            x0 = left_margin + col_idx * cell_w
            y0 = y_offset + row_idx * cell_h
            x1 = x0 + cell_w
            y1 = y0 + cell_h

            if day.month == month:
                status = classify_day(day)
                fill_color = colors[status]
                text_color = text_colors[status]
                outline_color = (0, 0, 0)
            else:
                fill_color = (240, 240, 240)
                text_color = (120, 120, 120)
                outline_color = (180, 180, 180)

            # Cell fill and border
            draw.rectangle([x0, y0, x1, y1], fill=fill_color, outline=outline_color)

            # Day number in top-left of cell
            if day.month == month:
                label = str(day.day)
                draw.text((x0 + 8, y0 + 6), label, font=font_days, fill=text_color)

                incidents_for_day = incidents_by_inc_number.get(day, {})
                if incidents_for_day:
                    text_y = y0 + 34
                    line_spacing = 4
                    for inc_number, severity_value in sorted(incidents_for_day.items()):
                        normalized_inc = (
                            inc_number if inc_number.upper().startswith("INC-") else f"INC-{inc_number}"
                        )
                        severity_label = f"Sev{normalize_severity_label(severity_value)}"
                        line = f"{normalized_inc}: {severity_label}"

                        line_bbox = draw.textbbox((0, 0), line, font=font_incident)
                        line_height = line_bbox[3] - line_bbox[1]
                        draw.text((x0 + 8, text_y), line, font=font_incident, fill=text_color)
                        text_y += line_height + line_spacing

    # Return as PNG
    buf = BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return send_file(buf, mimetype="image/png")


@app.route("/add", methods=["POST"])
def add_incident():
    inc_number = request.form.get("inc_number", "").strip()
    raw_date = request.form.get("date", "").strip()
    start_time_raw = request.form.get("start_time", "").strip()
    closed_date_raw = request.form.get("closed_date", "").strip()
    closed_time_raw = request.form.get("closed_time", "").strip()
    duration_raw = request.form.get("duration_seconds", "").strip()
    severity = request.form.get("severity", "").strip()
    product = request.form.get("product", "").strip()
    event_type = request.form.get("event_type", "Operational Incident").strip() or "Operational Incident"

    # basic validation
    if not inc_number or not raw_date:
        return redirect(url_for("index"))

    # accept either HTML date (YYYY-MM-DD) or MM/DD/YYYY
    parsed_date = parse_date(raw_date)
    if parsed_date is None:
        return redirect(url_for("index"))

    target_file = DATA_FILE if event_type == "Operational Incident" else OTHER_EVENTS_FILE
    events = load_events(target_file)

    if incident_exists(events, inc_number):
        return redirect(url_for("index", year=parsed_date.year))

    pillar = resolve_pillar(product)

    start_time_value = parse_time_component(start_time_raw)
    reported_dt = None
    if parsed_date:
        reported_dt = datetime.combine(parsed_date, start_time_value or time.min)

    closed_dt = None
    if closed_date_raw:
        closed_date = parse_date(closed_date_raw)
        closed_time_value = parse_time_component(closed_time_raw)
        if closed_date:
            closed_dt = datetime.combine(closed_date, closed_time_value or time.min)

    try:
        duration_seconds = int(duration_raw) if duration_raw else 0
    except ValueError:
        duration_seconds = 0

    if reported_dt and closed_dt and closed_dt < reported_dt:
        closed_dt = reported_dt

    events.append(
        {
            "inc_number": inc_number,
            "reported_at": reported_dt.isoformat() if reported_dt else "",
            "closed_at": closed_dt.isoformat() if closed_dt else "",
            "duration_seconds": duration_seconds,
            "severity": severity,
            "pillar": pillar,
            "product": product,
            "event_type": event_type,
        }
    )

    save_events(target_file, events)

    return redirect(url_for("index", year=parsed_date.year, tab="incidents" if event_type == "Operational Incident" else "others"))


@app.route("/upload", methods=["POST"])
def upload_csv():
    file = request.files.get("file")
    if not file:
        return redirect(url_for("index"))

    mapping = load_product_key()
    if not mapping:
        return redirect(url_for("index", key_missing=1))

    import_mode = request.form.get("mode", "update")

    try:
        content = file.read().decode("utf-8-sig")
    except UnicodeDecodeError:
        return redirect(url_for("index"))

    reader = csv.DictReader(StringIO(content))
    replace_mode = import_mode == "replace"
    incidents = [] if replace_mode else load_events(DATA_FILE)
    other_events = [] if replace_mode else load_events(OTHER_EVENTS_FILE)
    existing_numbers = (
        set()
        if replace_mode
        else {
            (
                inc.get("inc_number"),
                (inc.get("product") or "").strip(),
            )
            for inc in incidents
            if inc.get("inc_number")
        }
    )
    other_existing_numbers = (
        set()
        if replace_mode
        else {
            (
                evt.get("inc_number"),
                (evt.get("product") or "").strip(),
            )
            for evt in other_events
            if evt.get("inc_number")
        }
    )

    last_year = None
    for row in reader:
        inc_number = (row.get("ID") or "").strip()
        severity = (row.get("Severity") or "").strip()
        product_raw = (row.get("Product") or "").strip()
        raw_reported_at = (row.get("Reported at") or "").strip()
        raw_closed_at = (row.get("Closed at") or "").strip()
        raw_duration = (row.get("Client Impact Duration") or "").strip()
        event_type = (row.get("Incident Type") or "Operational Incident").strip()

        reported_dt = shift_utc_to_est(parse_datetime(raw_reported_at))
        closed_dt = shift_utc_to_est(parse_datetime(raw_closed_at))
        if not inc_number or reported_dt is None:
            continue

        last_year = reported_dt.year

        product_values = [p.strip() for p in product_raw.split(",") if p.strip()] or [""]

        try:
            duration_seconds = int(raw_duration) if raw_duration else 0
        except ValueError:
            duration_seconds = 0

        for product in product_values:
            lookup_key = (inc_number, product)
            if event_type == "Operational Incident" and lookup_key in existing_numbers:
                continue
            if event_type != "Operational Incident" and lookup_key in other_existing_numbers:
                continue

            pillar = resolve_pillar(product, mapping=mapping)

            payload = {
                "inc_number": inc_number,
                "reported_at": reported_dt.isoformat(),
                "closed_at": closed_dt.isoformat() if closed_dt else "",
                "duration_seconds": duration_seconds,
                "severity": severity,
                "pillar": pillar,
                "product": product,
                "event_type": event_type,
            }

            if event_type == "Operational Incident":
                incidents.append(payload)
                existing_numbers.add(lookup_key)
            else:
                other_events.append(payload)
                other_existing_numbers.add(lookup_key)

    save_events(DATA_FILE, incidents)
    save_events(OTHER_EVENTS_FILE, other_events)

    redirect_year = last_year if last_year is not None else None
    if redirect_year:
        return redirect(url_for("index", year=redirect_year))
    return redirect(url_for("index"))


@app.route("/upload-key", methods=["POST"])
def upload_key_file():
    file = request.files.get("key_file")
    if not file or not file.filename:
        return redirect(url_for("index", key_error="missing"))

    try:
        content = file.read().decode("utf-8-sig")
    except UnicodeDecodeError:
        return redirect(url_for("index", key_error="decode"))

    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        return redirect(url_for("index", key_error="invalid"))

    if not isinstance(data, dict):
        return redirect(url_for("index", key_error="format"))

    normalized = {str(k).strip(): str(v).strip() for k, v in data.items() if str(k).strip()}

    with open(PRODUCT_KEY_FILE, "w") as f:
        json.dump(normalized, f, indent=2)

    # Recalculate pillar assignments for all incidents using the new key
    incidents = load_events(DATA_FILE)
    other_events = load_events(OTHER_EVENTS_FILE)

    for collection, path in ((incidents, DATA_FILE), (other_events, OTHER_EVENTS_FILE)):
        if not collection:
            continue

        for incident in collection:
            incident["pillar"] = resolve_pillar(
                incident.get("product", ""), incident.get("pillar", ""), mapping=normalized
            )

        save_events(path, collection)

    return redirect(url_for("index", key_uploaded=1))


@app.route("/sync/incidents", methods=["GET", "POST"])
def sync_incidents_endpoint():
    payload = request.get_json(silent=True) or {}

    dry_run = (
        request.args.get("dry_run") == "1"
        or request.form.get("dry_run") == "1"
        or payload.get("dry_run") in (True, "1", "true", "True")
    )

    start_date = payload.get("start_date") or request.args.get("start_date")
    end_date = payload.get("end_date") or request.args.get("end_date")
    base_url = payload.get("base_url") or request.args.get("base_url")
    token = payload.get("token") or request.args.get("token")
    include_samples = payload.get("include_samples", dry_run)
    field_mapping = payload.get("field_mapping") or {}

    config = load_sync_config()
    effective_token = token or config.get("token")
    effective_base_url = base_url or config.get("base_url") or None
    effective_start_date = start_date or config.get("start_date") or None
    effective_end_date = end_date or config.get("end_date") or None
    effective_field_mapping = field_mapping or config.get("field_mapping")

    if payload.get("persist_settings"):
        config.update(
            {
                "token": token or config.get("token") or "",
                "base_url": base_url or config.get("base_url") or "",
                "start_date": start_date or config.get("start_date") or "",
                "end_date": end_date or config.get("end_date") or "",
                "cadence": payload.get("cadence") or config.get("cadence", "daily"),
                "field_mapping": effective_field_mapping,
            }
        )
        save_sync_config(config)

    result = sync_incidents_from_api(
        dry_run=dry_run,
        start_date=effective_start_date,
        end_date=effective_end_date,
        token=effective_token,
        base_url=effective_base_url,
        include_samples=include_samples,
        field_mapping=effective_field_mapping,
    )
    status_code = 200 if not result.get("error") else 500
    return jsonify(result), status_code


@app.route("/sync/config", methods=["POST"])
def update_sync_config():
    payload = request.get_json(silent=True) or {}
    config = load_sync_config()

    updated_mapping = normalize_field_mapping(payload.get("field_mapping") or config.get("field_mapping"))

    updated = {
        "cadence": payload.get("cadence") or config.get("cadence", "daily"),
        "base_url": payload.get("base_url") or "",
        "token": payload.get("token") or config.get("token") or "",
        "start_date": payload.get("start_date") or "",
        "end_date": payload.get("end_date") or "",
        "last_sync": config.get("last_sync") or {},
        "field_mapping": updated_mapping,
    }

    save_sync_config(updated)
    view_config = build_sync_config_view(updated)
    return jsonify({"config": view_config})


@app.route("/sync/wipe", methods=["POST"])
def wipe_local_data():
    for path in (DATA_FILE, OTHER_EVENTS_FILE):
        try:
            os.remove(path)
        except FileNotFoundError:
            continue
        except OSError:
            continue

    config = load_sync_config()
    if config:
        config["last_sync"] = {}
        save_sync_config(config)

    return jsonify({"status": "ok"})


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Incident Free Days")
    parser.add_argument(
        "--sync-incidents",
        action="store_true",
        help="Fetch incidents from incident.io and persist them to the local data files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the sync without writing to incidents.json/others.json.",
    )
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind the Flask server")
    parser.add_argument("--port", default=8080, type=int, help="Port for the Flask server")

    args = parser.parse_args()

    if args.sync_incidents:
        summary = sync_incidents_from_api(dry_run=args.dry_run)
        print(json.dumps(summary, indent=2))
    else:
        app.run(host=args.host, port=args.port, debug=False)
