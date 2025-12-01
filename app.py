from flask import Flask, render_template, request, redirect, url_for
from datetime import date, datetime
import calendar
import json
import os

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


@app.route("/", methods=["GET"])
def index():
    year_str = request.args.get("year")
    try:
        year = int(year_str) if year_str else DEFAULT_YEAR
    except ValueError:
        year = DEFAULT_YEAR

    incidents = load_incidents()
    months = build_calendar(year, incidents)

    # sort incidents newest-first for display
    incidents_sorted = sorted(
        incidents, key=lambda x: x.get("date", ""), reverse=True
    )

    return render_template(
        "index.html",
        year=year,
        months=months,
        incidents=incidents_sorted,
    )


@app.route("/add", methods=["POST"])
def add_incident():
    inc_number = request.form.get("inc_number", "").strip()
    raw_date = request.form.get("date", "").strip()
    severity = request.form.get("severity", "").strip()
    pillar = request.form.get("pillar", "").strip()

    # basic validation
    if not inc_number or not raw_date:
        return redirect(url_for("index"))

    # accept either HTML date (YYYY-MM-DD) or MM/DD/YYYY
    parsed_date = None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            parsed_date = datetime.strptime(raw_date, fmt).date()
            break
        except ValueError:
            continue

    if parsed_date is None:
        return redirect(url_for("index"))

    incidents = load_incidents()
    incidents.append(
        {
            "inc_number": inc_number,
            "date": parsed_date.isoformat(),
            "severity": severity,
            "pillar": pillar,
        }
    )
    save_incidents(incidents)

    return redirect(url_for("index", year=parsed_date.year))


if __name__ == "__main__":
    # For the Pi
    app.run(host="0.0.0.0", port=5080, debug=False)
