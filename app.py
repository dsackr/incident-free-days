from flask import Flask, render_template, request, redirect, url_for
from datetime import date, datetime
import csv
import calendar
import json
import os
from io import StringIO

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "incidents.json")
PRODUCT_KEY_FILE = os.path.join(BASE_DIR, "product_pillar_key.json")
DEFAULT_YEAR = 2025


def load_incidents():
    if not os.path.exists(DATA_FILE):
        return []
    with open(DATA_FILE, "r") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return []

    # Ensure the data is a list of dictionaries before continuing. If the file
    # was corrupted or manually edited into an unexpected shape, fail closed
    # so the UI remains usable instead of raising 500 errors when iterating
    # over the incidents.
    if not isinstance(data, list):
        return []

    return [entry for entry in data if isinstance(entry, dict)]


def save_incidents(incidents):
    with open(DATA_FILE, "w") as f:
        json.dump(incidents, f, indent=2)


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


def normalize_severity(value):
    """Normalize severity labels for consistent comparisons."""
    if value is None:
        return ""

    cleaned = "".join(ch for ch in str(value).lower() if ch.isalnum())
    return cleaned


def is_sev6(value):
    normalized = normalize_severity(value)
    return normalized in {"sev6", "6"}


def build_calendar(year, incidents):
    """Return data structure describing all 12 months with color info per day."""
    # map YYYY-MM-DD -> list of incidents
    incidents_by_date = {}
    for inc in incidents:
        try:
            d = datetime.strptime(inc["date"], "%Y-%m-%d").date()
        except (KeyError, TypeError, ValueError):
            continue
        if d.year != year:
            continue
        key = d.isoformat()
        incidents_by_date.setdefault(key, []).append(inc)

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

                if iso in incidents_by_date:
                    incidents_for_day = incidents_by_date[iso]
                    sev6_only = all(
                        is_sev6(inc.get("severity")) for inc in incidents_for_day
                    )

                    css_class = "day-sev6" if sev6_only else "day-incident"
                elif d < today:
                    css_class = "day-ok"         # green
                else:
                    css_class = "day-none"       # grey

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


def group_incidents_by_date(incidents):
    grouped = {}
    for inc in incidents:
        date_value = inc.get("date")
        if not date_value:
            continue
        grouped.setdefault(date_value, []).append(inc)
    return grouped


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
    pillar_filter = request.args.get("pillar") or None
    product_filter = request.args.get("product") or None
    severity_params = [value for value in request.args.getlist("severity") if value]
    severity_param_supplied = "severity" in request.args
    key_missing = request.args.get("key_missing") == "1"
    key_uploaded = request.args.get("key_uploaded") == "1"
    key_error = request.args.get("key_error")

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

    incidents = load_incidents()

    key_mapping = load_product_key()
    key_present = bool(key_mapping)

    # Build product ↔ pillar relationships
    product_pillar_map = {k: v for k, v in key_mapping.items() if k and v}
    for inc in incidents:
        product = (inc.get("product") or "").strip()
        pillar = (inc.get("pillar") or "").strip()

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

    if severity_params:
        severity_filter = severity_params
    else:
        if severity_param_supplied:
            severity_filter = []
        else:
            severity_filter = [sev for sev in severities if sev and not is_sev6(sev)]

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

    incidents_filtered = []
    incident_dates = set()
    for inc in incidents:
        if pillar_filter and inc.get("pillar") != pillar_filter:
            continue
        if product_filter and inc.get("product") != product_filter:
            continue
        if severity_filter and not any(
            normalize_severity(inc.get("severity"))
            == normalize_severity(selected)
            for selected in severity_filter
        ):
            continue

        try:
            inc_date = datetime.strptime(inc.get("date", ""), "%Y-%m-%d").date()
        except (TypeError, ValueError):
            continue

        in_month_view = view_mode == "monthly" and month_selection
        if in_month_view:
            if not (inc_date.year == year and inc_date.month == month_selection):
                continue
        else:
            if inc_date.year != year:
                continue

        incidents_filtered.append(inc)
        if not is_sev6(inc.get("severity")):
            incident_dates.add(inc_date)

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

    months = build_calendar(year, incidents_filtered)
    incidents_by_date = group_incidents_by_date(incidents_filtered)

    if view_mode == "monthly" and month_selection:
        months = [months[month_selection - 1]]

    active_filters = []
    def build_remove_link(kind, value=None):
        params = {"view": view_mode, "year": year}
        if month_selection:
            params["month"] = month_selection

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

    def build_link(target_view, target_year, target_month=None):
        params = {"view": target_view, "year": target_year}
        if target_month:
            params["month"] = target_month
        if pillar_filter:
            params["pillar"] = pillar_filter
        if product_filter:
            params["product"] = product_filter
        if severity_filter:
            params["severity"] = severity_filter
        elif severity_param_supplied:
            params["severity"] = [""]
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

    # sort incidents newest-first for display
    incidents_sorted = sorted(
        incidents_filtered, key=lambda x: x.get("date") or "", reverse=True
    )

    return render_template(
        "index.html",
        year=year,
        months=months,
        incidents=incidents_sorted,
        view_mode=view_mode,
        month_selection=month_selection,
        pillars=pillars,
        products=products,
        severities=severities,
        products_by_pillar=products_by_pillar,
        product_pillar_map=product_pillar_map,
        pillar_filter=pillar_filter,
        product_filter=product_filter,
        severity_filter=severity_filter,
        calendar=calendar,
        incidents_by_date=incidents_by_date,
        active_filters=active_filters,
        incident_count=unique_incident_count,
        incident_free_days=incident_free_days,
        prev_link=prev_link,
        next_link=next_link,
        current_period_label=current_period_label,
        yearly_view_link=yearly_view_link,
        monthly_view_link=monthly_view_link,
        key_present=key_present,
        key_missing=key_missing,
        key_uploaded=key_uploaded,
        key_error=key_error,
    )


@app.route("/add", methods=["POST"])
def add_incident():
    inc_number = request.form.get("inc_number", "").strip()
    raw_date = request.form.get("date", "").strip()
    severity = request.form.get("severity", "").strip()
    product = request.form.get("product", "").strip()

    # basic validation
    if not inc_number or not raw_date:
        return redirect(url_for("index"))

    # accept either HTML date (YYYY-MM-DD) or MM/DD/YYYY
    parsed_date = parse_date(raw_date)
    if parsed_date is None:
        return redirect(url_for("index"))

    incidents = load_incidents()

    if incident_exists(incidents, inc_number):
        return redirect(url_for("index", year=parsed_date.year))

    pillar = resolve_pillar(product)

    incidents.append(
        {
            "inc_number": inc_number,
            "date": parsed_date.isoformat(),
            "severity": severity,
            "pillar": pillar,
            "product": product,
        }
    )

    save_incidents(incidents)

    return redirect(url_for("index", year=parsed_date.year))


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
    incidents = [] if replace_mode else load_incidents()
    existing_numbers = (
        set() if replace_mode else {inc.get("inc_number") for inc in incidents if inc.get("inc_number")}
    )

    last_year = None
    for row in reader:
        inc_number = (row.get("ID") or "").strip()
        severity = (row.get("Severity") or "").strip()
        product_raw = (row.get("Product") or "").strip()
        raw_date = (row.get("Reported at") or "").strip()

        parsed_date = parse_date(raw_date)
        if not inc_number or parsed_date is None:
            continue

        if inc_number in existing_numbers:
            continue

        last_year = parsed_date.year
        product = product_raw.split(",")[0].strip() if product_raw else ""
        pillar = resolve_pillar(product, mapping=mapping)

        incidents.append(
            {
                "inc_number": inc_number,
                "date": parsed_date.isoformat(),
                "severity": severity,
                "pillar": pillar,
                "product": product,
            }
        )
        existing_numbers.add(inc_number)

    save_incidents(incidents)

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
    incidents = load_incidents()
    if incidents:
        for incident in incidents:
            incident["pillar"] = resolve_pillar(
                incident.get("product", ""), incident.get("pillar", ""), mapping=normalized
            )
        save_incidents(incidents)

    return redirect(url_for("index", key_uploaded=1))


if __name__ == "__main__":
    # For the Pi
    app.run(host="0.0.0.0", port=8080, debug=False)
