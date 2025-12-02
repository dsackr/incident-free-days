from flask import Flask, render_template, request, redirect, url_for
from datetime import date, datetime, time, timedelta
import csv
import calendar
import json
import os
from io import StringIO

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "incidents.json")
OTHER_EVENTS_FILE = os.path.join(BASE_DIR, "others.json")
PRODUCT_KEY_FILE = os.path.join(BASE_DIR, "product_pillar_key.json")
DEFAULT_YEAR = 2025


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


def is_sev6(value):
    return str(value) == "Other (Sev 6)"


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


@app.route("/", methods=["GET"])
def index():
    year_str = request.args.get("year")
    view_mode = request.args.get("view", "yearly")
    month_str = request.args.get("month")
    duration_enabled = request.args.get("duration") == "1"
    multi_day_only = duration_enabled and request.args.get("multi_day") == "1"
    pillar_filter = request.args.get("pillar") or None
    product_filter = request.args.get("product") or None
    severity_params = [value for value in request.args.getlist("severity") if value]
    event_type_params = [value for value in request.args.getlist("event_type") if value]
    severity_param_supplied = "severity" in request.args
    key_missing = request.args.get("key_missing") == "1"
    key_uploaded = request.args.get("key_uploaded") == "1"
    key_error = request.args.get("key_error")
    active_tab = request.args.get("tab", "incidents")

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
            for event in all_events
            if event.get("event_type")
        }
    )

    if severity_params:
        severity_filter = severity_params
    else:
        if severity_param_supplied:
            severity_filter = []
        else:
            severity_filter = [sev for sev in severities if sev and not is_sev6(sev)]

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

    def filter_and_group(events):
        filtered = []
        grouped = {}
        dates_with_non_sev6 = set()

        for event in events:
            if pillar_filter and event.get("pillar") != pillar_filter:
                continue
            if product_filter and event.get("product") != product_filter:
                continue
            if event_type_filter and event.get("event_type") not in event_type_filter:
                continue

            if event.get("event_type") == "Operational Incident":
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

    incidents_filtered, incidents_by_date, incident_dates = filter_and_group(incidents)
    other_filtered, other_by_date, _ = filter_and_group(other_events)

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
            return "day-sev6" if sev6_only else "day-incident"
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

    active_filters = []
    def build_remove_link(kind, value=None):
        params = {"view": view_mode, "year": year, "tab": active_tab}
        if month_selection:
            params["month"] = month_selection
        if duration_enabled:
            params["duration"] = "1"
        if multi_day_only:
            params["multi_day"] = "1"

        if kind != "pillar" and pillar_filter:
            params["pillar"] = pillar_filter
        if kind != "product" and product_filter:
            params["product"] = product_filter

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

        if kind == "event_type":
            remaining_types = [etype for etype in event_type_filter if etype != value]
            if remaining_types:
                params["event_type"] = remaining_types
        elif event_type_filter:
            params["event_type"] = event_type_filter

        return url_for("index", **params)

    if pillar_filter:
        active_filters.append(
            {"label": "Pillar", "value": pillar_filter, "remove_link": build_remove_link("pillar")}
        )
    if product_filter:
        active_filters.append(
            {"label": "Product", "value": product_filter, "remove_link": build_remove_link("product")}
        )
    if severity_filter:
        for severity in severity_filter:
            active_filters.append(
                {
                    "label": "Severity",
                    "value": severity,
                    "remove_link": build_remove_link("severity", severity),
                }
            )
    if event_type_filter:
        for event_type in event_type_filter:
            active_filters.append(
                {
                    "label": "Event Type",
                    "value": event_type,
                    "remove_link": build_remove_link("event_type", event_type),
                }
            )

    def build_link(target_view, target_year, target_month=None):
        params = {"view": target_view, "year": target_year, "tab": active_tab}
        if target_month:
            params["month"] = target_month
        if duration_enabled:
            params["duration"] = "1"
        if multi_day_only:
            params["multi_day"] = "1"
        if pillar_filter:
            params["pillar"] = pillar_filter
        if product_filter:
            params["product"] = product_filter
        if severity_filter:
            params["severity"] = severity_filter
        elif severity_param_supplied:
            params["severity"] = [""]
        if event_type_filter:
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
        active_filters=active_filters,
        incident_count=unique_incident_count,
        incident_free_days=incident_free_days,
        prev_link=prev_link,
        next_link=next_link,
        current_period_label=current_period_label,
        yearly_view_link=yearly_view_link,
        monthly_view_link=monthly_view_link,
        duration_enabled=duration_enabled,
        multi_day_only=multi_day_only,
        key_present=key_present,
        key_missing=key_missing,
        key_uploaded=key_uploaded,
        key_error=key_error,
        active_tab=active_tab,
    )


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
        else {inc.get("inc_number") for inc in incidents if inc.get("inc_number")}
    )
    other_existing_numbers = (
        set()
        if replace_mode
        else {evt.get("inc_number") for evt in other_events if evt.get("inc_number")}
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

        reported_dt = parse_datetime(raw_reported_at)
        closed_dt = parse_datetime(raw_closed_at)
        if not inc_number or reported_dt is None:
            continue

        if event_type == "Operational Incident" and inc_number in existing_numbers:
            continue
        if event_type != "Operational Incident" and inc_number in other_existing_numbers:
            continue

        last_year = reported_dt.year
        product = product_raw.split(",")[0].strip() if product_raw else ""
        pillar = resolve_pillar(product, mapping=mapping)

        try:
            duration_seconds = int(raw_duration) if raw_duration else 0
        except ValueError:
            duration_seconds = 0

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
            existing_numbers.add(inc_number)
        else:
            other_events.append(payload)
            other_existing_numbers.add(inc_number)

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


if __name__ == "__main__":
    # For the Pi
    app.run(host="0.0.0.0", port=8080, debug=False)
