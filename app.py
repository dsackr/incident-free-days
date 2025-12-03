from flask import Flask, render_template, request, redirect, send_file, url_for
from datetime import date, datetime, time, timedelta
import csv
import calendar
import json
import os
import datetime as dt
from io import StringIO, BytesIO
from PIL import Image, ImageDraw, ImageFont

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


def shift_utc_to_est(dt):
    if dt is None:
        return None

    return dt - timedelta(hours=5)


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


@app.route("/", methods=["GET"])
def index():
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

            sev1_present = any(str(evt.get("severity")) == "1" for evt in day_events)
            if sev1_present:
                return f"{intensity_class} day-sev1-marker"
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
    )


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

    # Simple bitmap fonts (no TrueType required)
    font_title = ImageFont.load_default()
    font_days = ImageFont.load_default()

    # Title: "December 2025"
    title = f"{calendar.month_name[month]} {year}"

    # Use ONLY textsize for measurement to avoid textbbox/TrueType issues
    tw, th = draw.textsize(title, font=font_title)
    draw.text(((W - tw) // 2, 10), title, font=font_title, fill=(0, 0, 0))

    # Layout margins
    left_margin = 40
    right_margin = 40
    top_margin = 60
    bottom_margin = 40

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
        x = left_margin + i * cell_w + 5
        y = top_margin
        draw.text((x, y), label, font=font_days, fill=(0, 0, 0))

    # Calendar grid
    cal = calendar.Calendar(firstweekday=calendar.SUNDAY)
    y_offset = top_margin + cell_h  # below weekday headers

    for row_idx, week in enumerate(cal.monthdayscalendar(year, month)):
        for col_idx, day in enumerate(week):
            x0 = left_margin + col_idx * cell_w
            y0 = y_offset + row_idx * cell_h
            x1 = x0 + cell_w
            y1 = y0 + cell_h

            # Cell border
            draw.rectangle([x0, y0, x1, y1], outline=(0, 0, 0))

            if day != 0:
                # Day number in top-left of cell
                draw.text((x0 + 5, y0 + 5), str(day), font=font_days, fill=(0, 0, 0))

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


if __name__ == "__main__":
    # For the Pi
    app.run(host="0.0.0.0", port=8080, debug=False)
