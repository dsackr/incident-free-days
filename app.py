from flask import Flask, render_template, request, redirect, url_for
from datetime import date, datetime
import csv
import calendar
import json
import os
from io import StringIO

app = Flask(__name__)

DATA_FILE = "incidents.json"
DEFAULT_YEAR = 2025


def load_incidents():
    if not os.path.exists(DATA_FILE):
        return []
    with open(DATA_FILE, "r") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []


def save_incidents(incidents):
    with open(DATA_FILE, "w") as f:
        json.dump(incidents, f, indent=2)


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


def build_calendar(year, incidents):
    """Return data structure describing all 12 months with color info per day."""
    # map YYYY-MM-DD -> list of incidents
    incidents_by_date = {}
    for inc in incidents:
        try:
            d = datetime.strptime(inc["date"], "%Y-%m-%d").date()
        except (KeyError, ValueError):
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
                    css_class = "day-incident"   # red
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


@app.route("/", methods=["GET"])
def index():
    year_str = request.args.get("year")
    view_mode = request.args.get("view", "yearly")
    month_str = request.args.get("month")
    pillar_filter = request.args.get("pillar") or None
    product_filter = request.args.get("product") or None

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

    # Build filter choices
    pillars = sorted({inc.get("pillar") for inc in incidents if inc.get("pillar")})
    products = sorted({inc.get("product") for inc in incidents if inc.get("product")})

    incidents_filtered = []
    for inc in incidents:
        if pillar_filter and inc.get("pillar") != pillar_filter:
            continue
        if product_filter and inc.get("product") != product_filter:
            continue
        incidents_filtered.append(inc)

    months = build_calendar(year, incidents_filtered)
    incidents_by_date = group_incidents_by_date(incidents_filtered)

    if view_mode == "monthly":
        if month_selection is None:
            today = date.today()
            month_selection = today.month if today.year == year else 1
        if 1 <= month_selection <= 12:
            months = [months[month_selection - 1]]
        else:
            month_selection = None
    else:
        month_selection = None

    # sort incidents newest-first for display
    incidents_sorted = sorted(
        incidents_filtered, key=lambda x: x.get("date", ""), reverse=True
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
        pillar_filter=pillar_filter,
        product_filter=product_filter,
        calendar=calendar,
        incidents_by_date=incidents_by_date,
    )


@app.route("/add", methods=["POST"])
def add_incident():
    inc_number = request.form.get("inc_number", "").strip()
    raw_date = request.form.get("date", "").strip()
    severity = request.form.get("severity", "").strip()
    pillar = request.form.get("pillar", "").strip()
    product = request.form.get("product", "").strip()

    # basic validation
    if not inc_number or not raw_date:
        return redirect(url_for("index"))

    # accept either HTML date (YYYY-MM-DD) or MM/DD/YYYY
    parsed_date = parse_date(raw_date)
    if parsed_date is None:
        return redirect(url_for("index"))

    incidents = load_incidents()
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

    try:
        content = file.read().decode("utf-8-sig")
    except UnicodeDecodeError:
        return redirect(url_for("index"))

    reader = csv.DictReader(StringIO(content))
    incidents = load_incidents()

    last_year = None
    for row in reader:
        inc_number = (row.get("ID") or "").strip()
        severity = (row.get("Severity") or "").strip()
        pillar = (row.get("Solution Pillar") or "").strip()
        product_raw = (row.get("Product") or "").strip()
        raw_date = (row.get("Reported at") or "").strip()

        parsed_date = parse_date(raw_date)
        if not inc_number or parsed_date is None:
            continue

        last_year = parsed_date.year
        products = [p.strip() for p in product_raw.split(",") if p.strip()]
        if not products:
            products = [""]

        for product in products:
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

    redirect_year = last_year if last_year is not None else None
    if redirect_year:
        return redirect(url_for("index", year=redirect_year))
    return redirect(url_for("index"))


if __name__ == "__main__":
    # For the Pi
    app.run(host="0.0.0.0", port=5080, debug=False)
