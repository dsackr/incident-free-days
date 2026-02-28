import json
import os
from datetime import datetime

import requests

FORM_URL = "https://docs.google.com/forms/d/e/1FAIpQLSfZJwdbdzTjJqomGqFQ8oAgjzjgklyDgq2p9x77u4M4I0f2LA/formResponse"
SUBMITTED_SELF_INFLICTED_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "submitted_self_inflicted.json"
)


def _load_submitted_numbers(path):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}

    return data if isinstance(data, dict) else {}


def _save_submitted_numbers(path, submitted):
    try:
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(submitted, handle, indent=2, sort_keys=True)
    except OSError:
        pass


def _split_inc_date(inc_date):
    parsed = datetime.strptime(inc_date, "%Y-%m-%d")
    return str(parsed.year), str(parsed.month), str(parsed.day)


def submit_self_inflicted_rows(
    rows,
    form_url=FORM_URL,
    submitted_path=SUBMITTED_SELF_INFLICTED_FILE,
    timeout_seconds=20,
):
    ok_count = 0
    failed_count = 0

    submitted = _load_submitted_numbers(submitted_path)

    for row in rows or []:
        try:
            inc_number = str(row.get("INC Number") or "").strip()
            if not inc_number or inc_number.casefold() == "unknown":
                continue
            if inc_number in submitted:
                continue

            year, month, day = _split_inc_date(str(row.get("INC Date") or "").strip())
            payload = {
                "entry.1754507418": inc_number,
                "entry.1055082223": str(row.get("RCA Classification") or "").strip(),
                "entry.1976073667_year": year,
                "entry.1976073667_month": month,
                "entry.1976073667_day": day,
            }
            response = requests.post(form_url, data=payload, timeout=timeout_seconds)
            if response.ok:
                submitted[inc_number] = datetime.utcnow().isoformat()
                ok_count += 1
            else:
                failed_count += 1
        except Exception:
            failed_count += 1

    _save_submitted_numbers(submitted_path, submitted)
    return ok_count, failed_count
