from flask import (
    Flask,
    jsonify,
    render_template,
    request,
    redirect,
    send_file,
    url_for,
    Response,
)
import re
import glob
import logging
from logging.handlers import TimedRotatingFileHandler
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
import csv
import calendar
import json
import os
import datetime as dt
import threading
import time as time_module
import hashlib
from io import StringIO, BytesIO
from PIL import Image, ImageDraw, ImageFont
import requests

import incident_io_client
import display_client

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "incidents.json")
OTHER_EVENTS_FILE = os.path.join(BASE_DIR, "others.json")
PRODUCT_KEY_FILE = os.path.join(BASE_DIR, "product_pillar_key.json")
SYNC_CONFIG_FILE = os.path.join(BASE_DIR, "sync_config.json")
OSHA_DATA_FILE = os.path.join(BASE_DIR, "osha_data.json")
OSHA_BACKGROUND_IMAGE = os.path.join(BASE_DIR, "static", "background.png")
OSHA_OUTPUT_IMAGE = os.path.join(BASE_DIR, "static", "current_sign.png")
OSHA_OUTPUT_BINARY = os.path.join(BASE_DIR, "static", "current_sign.bin")
OSHA_EINK_DISPLAY_IP = os.getenv("OSHA_EINK_DISPLAY_IP", "192.168.86.120")
OSHA_EINK_DISPLAY_PORT = int(os.getenv("OSHA_EINK_DISPLAY_PORT", "5000"))
OSHA_USE_LOCAL_EINK = os.getenv("OSHA_USE_LOCAL_EINK", "false").lower() in {
    "1",
    "true",
    "yes",
}
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "app.log")
PROCEDURAL_RCA_EXCLUSIONS = {"non-procedural incident", "not classified"}
MISSING_RCA_VALUES = {"not classified", "unknown", "unclassified"}
LONG_IMPACT_THRESHOLD_SECONDS = 24 * 60 * 60

# 6-color palette for E-Paper display
OSHA_PALETTE = {
    "black": (0, 0, 0, 0x0),
    "white": (255, 255, 255, 0x1),
    "yellow": (255, 215, 0, 0x2),
    "red": (200, 80, 50, 0x3),
    "blue": (100, 120, 180, 0x5),
    # Use a deeper green to make yellow tones quantize to the yellow palette entry instead of green.
    "green": (0, 150, 0, 0x6),
}

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
AUTO_SYNC_WINDOW_DAYS = 30
AUTO_SYNC_POLL_SECONDS = 300
DISPLAY_FRAME_BYTES = 192000
CADENCE_TO_INTERVAL = {
    "hourly": timedelta(hours=1),
    "daily": timedelta(days=1),
    "weekly": timedelta(weeks=1),
}


_EVENT_CACHE = {}
_PRODUCT_KEY_CACHE = {}
_CACHE_LOCK = threading.Lock()


def _configure_logging(log_path=LOG_FILE):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Replace any existing timed handlers to avoid duplicate log streams when reconfiguring
    for handler in list(logger.handlers):
        if isinstance(handler, TimedRotatingFileHandler):
            logger.removeHandler(handler)
            handler.close()

    formatter = logging.Formatter(
        "%(asctime)sZ %(levelname)s [%(name)s] %(message)s", "%Y-%m-%dT%H:%M:%S"
    )
    file_handler = TimedRotatingFileHandler(
        log_path, when="midnight", interval=1, backupCount=1, utc=True
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if not any(isinstance(handler, logging.StreamHandler) for handler in logger.handlers):
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    # Reduce noisy request logs for endpoints that refresh frequently (e.g. /logs).
    class ExcludePathsFilter(logging.Filter):
        def __init__(self, excluded):
            super().__init__()
            self.excluded = tuple(excluded)

        def filter(self, record):
            message = record.getMessage()
            return not any(path in message for path in self.excluded)

    noisy_paths = ["GET /logs", "GET /logs/"]
    werkzeug_logger = logging.getLogger("werkzeug")
    werkzeug_logger.addFilter(ExcludePathsFilter(noisy_paths))


_configure_logging()


def load_app_config():
    if not os.path.exists(SYNC_CONFIG_FILE):
        return {}

    try:
        with open(SYNC_CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}

    return data if isinstance(data, dict) else {}


def save_app_config(config):
    payload = config or {}
    try:
        with open(SYNC_CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except OSError:
        pass


def load_osha_data():
    if os.path.exists(OSHA_DATA_FILE):
        try:
            with open(OSHA_DATA_FILE, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except (json.JSONDecodeError, OSError):
            pass

    return {
        "days_since": 1,
        "prior_count": 2,
        "incident_number": "540",
        "incident_date": "2025-10-03",
        "prior_incident_date": "2025-10-01",
        "reason": "Deploy",
        "last_reset": datetime.now().isoformat(),
    }


def save_osha_data(data):
    try:
        with open(OSHA_DATA_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except OSError:
        pass


def compute_osha_state(raw_data=None):
    return compute_osha_state_from_incidents(load_events(DATA_FILE), raw_data=raw_data)


def _normalize_incident_number(raw_value):
    inc_number = str(raw_value or "").strip()

    if inc_number.upper().startswith("INC-"):
        return inc_number[4:]

    return inc_number


def _infer_osha_reason(rca_classification):
    normalized = (rca_classification or "").strip().casefold()

    if "deploy" in normalized:
        return "Deploy"
    if "miss" in normalized:
        return "Missed"
    if "change" in normalized:
        return "Change"

    return "Change"


def _procedural_incidents(incidents):
    results = []

    for incident in incidents or []:
        classification_raw = (incident.get("rca_classification") or "").strip()
        if not classification_raw:
            continue

        if classification_raw.casefold() in PROCEDURAL_RCA_EXCLUSIONS:
            continue

        normalized_classification = normalize_rca_category(classification_raw)
        if normalized_classification not in {"deploy", "change", "missing-task"}:
            continue

        incident_date = parse_date(incident.get("date") or incident.get("reported_at"))
        if not incident_date:
            continue

        reported_at = parse_datetime(incident.get("reported_at"))
        if not reported_at:
            reported_at = datetime.combine(incident_date, time.min)

        results.append(
            {
                "incident": incident,
                "classification": classification_raw,
                "incident_date": incident_date,
                "reported_at": reported_at,
            }
        )

    results.sort(key=lambda entry: (entry["incident_date"], entry["reported_at"]), reverse=True)
    return results


def calculate_longest_procedural_gap(incidents, *, period_start, period_end):
    period_start = period_start
    period_end = period_end
    if period_start > period_end:
        period_start, period_end = period_end, period_start

    procedural = _procedural_incidents(incidents)
    incident_dates = sorted(
        {
            entry["incident_date"]
            for entry in procedural
            if period_start <= entry["incident_date"] <= period_end
        }
    )

    gap_start = period_start
    gap_end = period_end
    gap_days = (period_end - period_start).days
    if not incident_dates:
        return {
            "gap_days": gap_days,
            "gap_start": gap_start,
            "gap_end": gap_end,
            "incident_count": 0,
        }

    previous_date = period_start
    for incident_date in incident_dates:
        gap = (incident_date - previous_date).days
        if gap > gap_days:
            gap_days = gap
            gap_start = previous_date
            gap_end = incident_date
        previous_date = incident_date

    end_gap = (period_end - previous_date).days
    if end_gap > gap_days:
        gap_days = end_gap
        gap_start = previous_date
        gap_end = period_end

    return {
        "gap_days": gap_days,
        "gap_start": gap_start,
        "gap_end": gap_end,
        "incident_count": len(incident_dates),
    }


def compute_osha_state_from_incidents(incidents, raw_data=None):
    procedural = _procedural_incidents(incidents)

    if procedural:
        latest = procedural[0]
        previous = procedural[1] if len(procedural) > 1 else None

        incident_date = latest["incident_date"]
        prior_entry = previous
        skipped_same_day = False
        if previous and previous["incident_date"] == incident_date:
            prior_entry = next(
                (
                    entry
                    for entry in procedural[1:]
                    if entry["incident_date"] < incident_date
                ),
                None,
            )
            skipped_same_day = prior_entry is not None

        previous_date = prior_entry["incident_date"] if prior_entry else None

        data = {
            "incident_number": _normalize_incident_number(
                latest["incident"].get("inc_number") or latest["incident"].get("id")
            ),
            "incident_date": incident_date.isoformat(),
            "prior_incident_date": previous_date.isoformat() if previous_date else "",
            "days_since": max((date.today() - incident_date).days, 0),
            "prior_count": max((incident_date - previous_date).days, 0) if previous_date else 0,
            "prior_count_has_same_day_skips": skipped_same_day,
            "reason": _infer_osha_reason(latest["classification"]),
            "last_reset": datetime.now().isoformat(),
        }

        save_osha_data(data)
        return data

    data = dict(raw_data or load_osha_data())

    if data.get("incident_date"):
        try:
            incident_date = datetime.fromisoformat(str(data["incident_date"]))
            data["days_since"] = (datetime.now().date() - incident_date.date()).days
        except ValueError:
            data["days_since"] = data.get("days_since", 0)

    if data.get("prior_incident_date") and data.get("incident_date"):
        try:
            prior_date = datetime.fromisoformat(str(data["prior_incident_date"]))
            incident_date = datetime.fromisoformat(str(data["incident_date"]))
            data["prior_count"] = (incident_date.date() - prior_date.date()).days
        except ValueError:
            data["prior_count"] = data.get("prior_count", 0)

    return data


def osha_rgb_to_palette_code(r, g, b):
    """Find closest color in 6-color palette"""
    min_distance = float("inf")
    closest_code = 0x1

    for _color_name, (pr, pg, pb, code) in OSHA_PALETTE.items():
        distance = (r - pr) ** 2 + (g - pg) ** 2 + (b - pb) ** 2
        if distance < min_distance:
            min_distance = distance
            closest_code = code

    return closest_code


def convert_osha_image_to_binary(img):
    if img.mode != "RGB":
        img = img.convert("RGB")

    display_width = 800
    display_height = 480
    img_ratio = img.width / img.height
    display_ratio = display_width / display_height

    if img_ratio > display_ratio:
        new_width = display_width
        new_height = int(display_width / img_ratio)
    else:
        new_height = display_height
        new_width = int(display_height * img_ratio)

    resized = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
    letterboxed = Image.new("RGB", (display_width, display_height), "white")
    left = (display_width - new_width) // 2
    top = (display_height - new_height) // 2
    letterboxed.paste(resized, (left, top))
    img = letterboxed

    palette_data = []
    for color_name in ("black", "white", "yellow", "red", "blue", "green"):
        r, g, b, _code = OSHA_PALETTE[color_name]
        palette_data.extend([r, g, b])
    palette_img = Image.new("P", (1, 1))
    palette_img.putpalette(palette_data + [0] * (256 * 3 - len(palette_data)))
    img = img.quantize(palette=palette_img, dither=Image.Dither.FLOYDSTEINBERG)
    img = img.convert("RGB")

    binary_data = bytearray(DISPLAY_FRAME_BYTES)

    for row in range(480):
        for col in range(0, 800, 2):
            r1, g1, b1 = img.getpixel((col, row))
            r2, g2, b2 = img.getpixel((col + 1, row))

            code1 = osha_rgb_to_palette_code(r1, g1, b1)
            code2 = osha_rgb_to_palette_code(r2, g2, b2)

            byte_index = row * 400 + col // 2
            binary_data[byte_index] = (code1 << 4) | code2

    return bytes(binary_data)


def _save_display_frame(frame_bytes):
    try:
        with open(OSHA_OUTPUT_BINARY, "wb") as handle:
            handle.write(frame_bytes)
    except OSError:
        pass


def _load_cached_display_frame():
    if not os.path.exists(OSHA_OUTPUT_BINARY):
        return None

    try:
        with open(OSHA_OUTPUT_BINARY, "rb") as handle:
            data = handle.read()
    except OSError:
        return None

    if len(data) != DISPLAY_FRAME_BYTES:
        return None

    return data


def _render_latest_frame():
    if not os.path.exists(OSHA_OUTPUT_IMAGE):
        generate_osha_sign(incidents=load_events(DATA_FILE))

    if not os.path.exists(OSHA_OUTPUT_IMAGE):
        return None

    try:
        img = Image.open(OSHA_OUTPUT_IMAGE)
        frame = convert_osha_image_to_binary(img)
    except Exception:
        return None

    if len(frame) == DISPLAY_FRAME_BYTES:
        _save_display_frame(frame)

    return frame


def load_display_frame_bytes():
    cached = _load_cached_display_frame()
    if cached is not None:
        return cached

    return _render_latest_frame()


def display_frame_etag(frame_bytes):
    if not frame_bytes:
        return None

    return hashlib.sha256(frame_bytes).hexdigest()


def latest_procedural_incident_number(incidents):
    procedural = _procedural_incidents(incidents)
    if not procedural:
        return ""

    incident = procedural[0]["incident"]
    return _normalize_incident_number(
        incident.get("inc_number") or incident.get("id") or incident.get("reference")
    )


def send_osha_sign_to_ip(display_ip, *, incidents=None, progress_callback=None):
    display_ip = (display_ip or "").strip()
    if not display_ip:
        return False, "Display IP is required"

    incidents = incidents or load_events(DATA_FILE)
    generate_osha_sign(incidents=incidents)

    if not os.path.exists(OSHA_OUTPUT_IMAGE):
        return False, "OSHA output image not found"

    binary_data = load_display_frame_bytes()
    if not binary_data:
        return False, "Failed to render OSHA image"

    success, message = display_client.send_display_buffer(
        display_ip, binary_data, progress_callback=progress_callback
    )

    log_message = (
        f"Sent OSHA sign to {display_ip}: {message}" if success else f"OSHA send failed for {display_ip}: {message}"
    )
    if success:
        app.logger.info(log_message)
    else:
        app.logger.error(log_message)

    return success, message


def _parse_log_timestamp(line):
    """Best-effort parser for the ISO-ish timestamp at the start of a log line."""

    if not line:
        return None

    ts_part = line.split(" ", maxsplit=1)[0].strip()
    if not ts_part:
        return None

    ts_value = ts_part.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(ts_value)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def read_recent_logs(hours=24):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    entries = []

    for path in sorted(glob.glob(f"{LOG_FILE}*")):
        try:
            with open(path, "r", encoding="utf-8") as handle:
                for line in handle:
                    parsed_ts = _parse_log_timestamp(line)
                    if parsed_ts and parsed_ts >= cutoff:
                        entries.append((parsed_ts, line.rstrip()))
        except OSError:
            continue

    entries.sort(key=lambda item: item[0], reverse=True)
    return [line for _ts, line in entries]


def display_osha_on_local_epaper(img_path):
    if not os.path.exists(img_path):
        return False

    try:
        from inky.auto import auto as auto_inky
    except ImportError:
        auto_inky = None

    if auto_inky:
        try:
            display = auto_inky()
            img = Image.open(img_path).convert("RGB").resize(display.resolution)
            display.set_image(img)
            display.show()
            return True
        except Exception:
            # Fall back to other drivers if auto-detection fails
            pass

    try:
        from waveshare_epd import epd7in5_V2
    except ImportError:
        epd7in5_V2 = None

    if epd7in5_V2:
        try:
            epd = epd7in5_V2.EPD()
            epd.init()
            epd.Clear()

            img = Image.open(img_path).convert("1").resize((epd.width, epd.height))
            epd.display(epd.getbuffer(img))
            epd.sleep()
            return True
        except Exception:
            return False

    return False


def display_osha_on_epaper(img_path):
    if not os.path.exists(img_path):
        return False

    try:
        frame_bytes = load_display_frame_bytes()
        if not frame_bytes:
            return False

        response = requests.post(
            f"http://{OSHA_EINK_DISPLAY_IP}:{OSHA_EINK_DISPLAY_PORT}/display/upload",
            files={"file": ("sign.bin", frame_bytes)},
            headers={"Connection": "keep-alive"},
            timeout=120,
        )

        return response.status_code == 200
    except Exception:
        return False


def send_osha_to_any_epaper(img_path):
    prefer_local = OSHA_USE_LOCAL_EINK

    if prefer_local and display_osha_on_local_epaper(img_path):
        return True

    if display_osha_on_epaper(img_path):
        return True

    if not prefer_local:
        return display_osha_on_local_epaper(img_path)

    return False


def generate_osha_sign(auto_display=False, incidents=None):
    data = compute_osha_state_from_incidents(incidents or load_events(DATA_FILE))

    if not os.path.exists(OSHA_BACKGROUND_IMAGE):
        return False

    img = Image.open(OSHA_BACKGROUND_IMAGE)
    draw = ImageDraw.Draw(img)

    img_width, _ = img.size

    font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
    try:
        days_font = ImageFont.truetype(font_path, 400)
        count_font = ImageFont.truetype(font_path, 130)
        asterisk_font = ImageFont.truetype(font_path, 60)
        inc_font = ImageFont.truetype(font_path, 100)
        inc_date_font = ImageFont.truetype(font_path, 30)
        check_font = ImageFont.truetype(font_path, 80)
    except OSError:
        days_font = count_font = asterisk_font = inc_font = inc_date_font = check_font = ImageFont.load_default()

    days_text = str(data.get("days_since", 0))
    days_bbox = draw.textbbox((0, 0), days_text, font=days_font)
    days_width = days_bbox[2] - days_bbox[0]
    days_x = (img_width - days_width) // 2
    days_y = 160
    draw.text((days_x, days_y), days_text, font=days_font, fill="black")

    prior_text = str(data.get("prior_count", 0))
    prior_bbox = draw.textbbox((0, 0), prior_text, font=count_font)
    prior_width = prior_bbox[2] - prior_bbox[0]
    prior_x = 240 - (prior_width // 2)
    prior_y = 670
    draw.text((prior_x, prior_y), prior_text, font=count_font, fill="white")
    if data.get("prior_count_has_same_day_skips"):
        asterisk_text = "*"
        asterisk_bbox = draw.textbbox((0, 0), asterisk_text, font=asterisk_font)
        asterisk_width = asterisk_bbox[2] - asterisk_bbox[0]
        asterisk_height = asterisk_bbox[3] - asterisk_bbox[1]
        asterisk_x = prior_x + prior_width + 6
        asterisk_y = prior_y - (asterisk_height // 2) + 50
        draw.text((asterisk_x, asterisk_y), asterisk_text, font=asterisk_font, fill="white")

    inc_text = data.get("incident_number", "")
    inc_bbox = draw.textbbox((0, 0), inc_text, font=inc_font)
    inc_width = inc_bbox[2] - inc_bbox[0]
    inc_x = (img_width // 2) - (inc_width // 2)
    inc_y = 660
    draw.text((inc_x, inc_y), inc_text, font=inc_font, fill="white")

    incident_date_text = ""
    incident_date_raw = data.get("incident_date")
    if incident_date_raw:
        try:
            incident_date = datetime.fromisoformat(str(incident_date_raw))
            incident_date_text = incident_date.strftime("%m/%d/%Y")
        except ValueError:
            incident_date_text = ""

    if incident_date_text:
        inc_height = inc_bbox[3] - inc_bbox[1]
        date_bbox = draw.textbbox((0, 0), incident_date_text, font=inc_date_font)
        date_width = date_bbox[2] - date_bbox[0]
        date_x = (img_width // 2) - (date_width // 2) + 50
        # Add a little extra spacing so the date sits lower in the red box
        # (roughly 20px lower than before, leaving it just above the yellow border).
        date_y = inc_y + inc_height + 40
        draw.text((date_x, date_y), incident_date_text, font=inc_date_font, fill="white")

    reason_positions = {
        "Change": (920, 590),
        "Deploy": (920, 660),
        "Missed": (920, 730),
    }

    if data.get("reason") in reason_positions:
        check_x, check_y = reason_positions[data["reason"]]
        draw.text((check_x, check_y), "✓", font=check_font, fill="black")

    img.save(OSHA_OUTPUT_IMAGE)

    try:
        frame_bytes = convert_osha_image_to_binary(img)
        if len(frame_bytes) == DISPLAY_FRAME_BYTES:
            _save_display_frame(frame_bytes)
    except Exception:
        pass

    if auto_display:
        send_osha_to_any_epaper(OSHA_OUTPUT_IMAGE)

    return True


def load_events(path):
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return []

    with _CACHE_LOCK:
        cached = _EVENT_CACHE.get(path)
        if cached and cached["mtime"] == mtime:
            return [dict(entry) for entry in cached["data"]]

    try:
        with open(path, "r") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return []

    if not isinstance(data, list):
        return []

    cleaned = [dict(entry) for entry in data if isinstance(entry, dict)]

    with _CACHE_LOCK:
        _EVENT_CACHE[path] = {"mtime": mtime, "data": cleaned}

    return [dict(entry) for entry in cleaned]


def save_events(path, events):
    with open(path, "w") as f:
        json.dump(events, f, indent=2)

    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return

    with _CACHE_LOCK:
        _EVENT_CACHE[path] = {
            "mtime": mtime,
            "data": [dict(event) for event in events if isinstance(event, dict)],
        }


def load_product_key():
    try:
        mtime = os.path.getmtime(PRODUCT_KEY_FILE)
    except OSError:
        return {}

    with _CACHE_LOCK:
        cached = _PRODUCT_KEY_CACHE.get(PRODUCT_KEY_FILE)
        if cached and cached["mtime"] == mtime:
            return dict(cached["data"])

    try:
        with open(PRODUCT_KEY_FILE, "r") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}

    if not isinstance(data, dict):
        return {}

    normalized = {
        str(k).strip(): str(v).strip() for k, v in data.items() if str(k).strip()
    }

    with _CACHE_LOCK:
        _PRODUCT_KEY_CACHE[PRODUCT_KEY_FILE] = {"mtime": mtime, "data": normalized}

    return dict(normalized)


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
    data = load_app_config()

    if not isinstance(data, dict):
        data = {}

    data.setdefault("cadence", "daily")
    data.setdefault("field_mapping", DEFAULT_FIELD_MAPPING)
    data["field_mapping"] = normalize_field_mapping(data.get("field_mapping"))
    return data


def save_sync_config(config):
    normalized_mapping = normalize_field_mapping(config.get("field_mapping"))
    payload = load_app_config()
    last_sync_value = config.get("last_sync") if "last_sync" in config else None
    payload.update(
        {
            "cadence": config.get("cadence", "daily"),
            "base_url": config.get("base_url") or "",
            "token": config.get("token") or payload.get("token") or "",
            "start_date": config.get("start_date") or payload.get("start_date") or "",
            "end_date": config.get("end_date") or payload.get("end_date") or "",
            "last_sync": last_sync_value
            if last_sync_value is not None
            else payload.get("last_sync")
            or {},
            "field_mapping": normalized_mapping,
        }
    )

    save_app_config(payload)


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


def _normalize_timestamp(raw_value):
    if not raw_value:
        return None

    parsed = parse_datetime(str(raw_value))
    if not parsed:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def is_auto_sync_due(config, *, now=None):
    cadence = (config.get("cadence") or "").casefold()
    token = config.get("token") or os.getenv("INCIDENT_IO_API_TOKEN")

    if cadence not in CADENCE_TO_INTERVAL:
        return False

    if not token:
        return False

    now = now or datetime.now(timezone.utc)
    last_sync_raw = config.get("last_sync") or {}
    last_synced_at = _normalize_timestamp(last_sync_raw.get("timestamp"))

    if not last_synced_at:
        return True

    interval = CADENCE_TO_INTERVAL[cadence]
    return now - last_synced_at >= interval


def build_auto_sync_window(now=None):
    now = now or datetime.now(timezone.utc)
    end_date = now.date()
    start_date = end_date - timedelta(days=AUTO_SYNC_WINDOW_DAYS - 1)
    return start_date.isoformat(), end_date.isoformat()


def auto_sync_loop(stop_event):
    while not stop_event.is_set():
        try:
            config = load_sync_config()
            if is_auto_sync_due(config):
                start_date, end_date = build_auto_sync_window()
                sync_incidents_from_api(
                    dry_run=False,
                    start_date=start_date,
                    end_date=end_date,
                    token=config.get("token") or os.getenv("INCIDENT_IO_API_TOKEN"),
                    base_url=config.get("base_url") or None,
                    include_samples=False,
                    field_mapping=config.get("field_mapping"),
                )
        except Exception as exc:  # pragma: no cover - safeguard for background loop
            app.logger.exception("Auto-sync error: %s", exc)
        finally:
            stop_event.wait(AUTO_SYNC_POLL_SECONDS)


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


def normalize_rca_category(rca_value):
    normalized = (rca_value or "").strip().casefold()

    if "non-procedural" in normalized:
        return "non-procedural"
    if "missing" in normalized and "task" in normalized:
        return "missing-task"
    if "deploy" in normalized:
        return "deploy"
    if "change" in normalized:
        return "change"
    if normalized:
        return "other"

    return "unknown"


def build_incident_rca_rows(incidents, year):
    today = date.today()
    events_by_day = defaultdict(list)

    for incident in incidents:
        incident_date = parse_date(incident.get("date") or incident.get("reported_at"))
        if not incident_date:
            continue

        events_by_day[incident_date].append(incident)

    priority = [
        "non-procedural",
        "missing-task",
        "deploy",
        "change",
        "other",
        "unknown",
    ]

    month_rows = []

    for month in range(1, 13):
        _, days_in_month = calendar.monthrange(year, month)
        cells = []
        for day in range(1, 32):
            if day > days_in_month:
                cells.append({"day": day, "status": "not-in-month", "label": ""})
                continue

            day_date = date(year, month, day)

            if day_date > today:
                cells.append({"day": day, "status": "future", "label": day_date.isoformat()})
                continue

            incidents_for_day = events_by_day.get(day_date, [])
            if not incidents_for_day:
                cells.append({"day": day, "status": "no-incidents", "label": day_date.isoformat()})
                continue

            classifications = [normalize_rca_category(inc.get("rca_classification")) for inc in incidents_for_day]
            counts = Counter(classifications)
            primary = min(classifications, key=lambda value: priority.index(value) if value in priority else len(priority))

            details = ", ".join(f"{counts[name]} {name}" for name in priority if counts.get(name))
            label = f"{calendar.month_name[month]} {day}: {details}" if details else day_date.isoformat()

            cells.append({"day": day, "status": primary, "label": label})

        month_rows.append({"month": month, "label": calendar.month_name[month], "cells": cells})

    return month_rows


def _build_product_pillar_maps(events, mapping=None):
    mapping = mapping if mapping is not None else load_product_key()
    product_pillar_map = {k: v for k, v in (mapping or {}).items() if k}

    for event in events:
        product = (event.get("product") or "").strip()
        pillar = (event.get("pillar") or "").strip()

        if not product:
            continue

        if not pillar:
            pillar = resolve_pillar(product, mapping=mapping)

        if pillar:
            product_pillar_map.setdefault(product, pillar)
        elif product not in product_pillar_map:
            product_pillar_map[product] = None

    products_by_pillar = {}
    for product, pillar in product_pillar_map.items():
        if pillar is None:
            continue
        products_by_pillar.setdefault(pillar, set()).add(product)

    products_by_pillar = {pillar: sorted(values) for pillar, values in products_by_pillar.items()}
    products_by_pillar["__all__"] = sorted(product_pillar_map.keys())

    pillars = sorted({value for value in product_pillar_map.values() if value})

    return product_pillar_map, products_by_pillar, pillars


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


def _coerce_duration_value(raw_value):
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return 0

    return max(value, 0)


def get_client_impact_duration_seconds(event):
    seconds = _coerce_duration_value(event.get("client_impact_duration_seconds"))
    if seconds:
        return seconds

    seconds = _coerce_duration_value(event.get("client_impact_duration"))
    if seconds:
        return seconds

    return 0


def format_duration_short(seconds):
    seconds = max(int(seconds or 0), 0)
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes = remainder // 60

    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes or not parts:
        parts.append(f"{minutes}m")

    return " ".join(parts)


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

    start_date = reported_at.date()

    if not duration_enabled:
        return [start_date]

    if (event.get("event_type") or "Operational Incident") == "Operational Incident":
        duration_seconds = get_client_impact_duration_seconds(event)
        end_date = (reported_at + timedelta(seconds=duration_seconds)).date() if duration_seconds else start_date
    else:
        if closed_at is None:
            duration_seconds = _coerce_duration_value(event.get("duration_seconds"))
            closed_at = reported_at + timedelta(seconds=duration_seconds)

        end_date = max(closed_at.date(), start_date)

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


def _extract_rca_classification(api_incident, default="Not Classified"):
    target_name = "rca classification"
    target_id = "01JZ0PNKHCB3M6NX0AHPABS59D"

    for entry in api_incident.get("custom_field_entries") or []:
        custom_field = entry.get("custom_field") or {}
        name = (custom_field.get("name") or "").strip().casefold()
        field_id = (custom_field.get("id") or "").strip()

        if name != target_name and field_id != target_id:
            continue

        for value_entry in entry.get("values") or []:
            if not isinstance(value_entry, dict):
                continue

            value_option = value_entry.get("value_option") or {}
            raw_value = (
                value_option.get("value")
                or value_option.get("name")
                or value_entry.get("value")
                or value_entry.get("value_text")
            )

            if raw_value not in (None, ""):
                return str(raw_value)

        return default

    return default


def _extract_client_impact_duration_seconds(api_incident):
    target_name = "client impact duration"

    for entry in api_incident.get("duration_metrics") or []:
        metric = entry.get("duration_metric") or {}
        name = (metric.get("name") or "").strip().casefold()

        if name != target_name:
            continue

        for key in ("value_seconds", "value", "value_numeric", "value_integer"):
            raw_value = entry.get(key)
            try:
                seconds = int(raw_value)
            except (TypeError, ValueError):
                continue

            return max(seconds, 0)

    for entry in api_incident.get("custom_field_entries") or []:
        custom_field = entry.get("custom_field") or {}
        name = (custom_field.get("name") or "").strip().casefold()

        if name != target_name:
            continue

        for value_entry in entry.get("values") or []:
            if not isinstance(value_entry, dict):
                continue

            for key in ("value_seconds", "value_integer", "value_numeric", "value"):
                raw_value = value_entry.get(key)
                try:
                    seconds = int(raw_value)
                except (TypeError, ValueError):
                    continue

                return max(seconds, 0)

        return 0

    return 0


def _extract_incident_lead(api_incident):
    def extract_name(value):
        if value is None:
            return None
        if isinstance(value, str):
            cleaned = value.strip()
            return cleaned or None
        if isinstance(value, dict):
            for key in (
                "name",
                "full_name",
                "display_name",
                "email",
                "id",
            ):
                candidate = value.get(key)
                if candidate:
                    return str(candidate).strip()

            for nested_key in ("user", "assignee", "owner"):
                candidate = extract_name(value.get(nested_key))
                if candidate:
                    return candidate
            return None
        if isinstance(value, list):
            for item in value:
                candidate = extract_name(item)
                if candidate:
                    return candidate
        return None

    for key in (
        "incident_lead",
        "incident_lead_user",
        "lead",
        "lead_user",
        "lead_assignee",
        "incident_manager",
    ):
        candidate = extract_name(api_incident.get(key))
        if candidate:
            return candidate

    for entry in api_incident.get("incident_role_assignments") or []:
        role = entry.get("role") or {}
        role_name = (role.get("name") or "").strip().casefold()
        if role_name not in {"incident lead", "lead"}:
            continue
        candidate = extract_name(entry.get("assignee") or entry.get("user"))
        if candidate:
            return candidate

    return ""


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

    status_raw = api_incident.get("status") or api_incident.get("incident_status")
    if isinstance(status_raw, dict):
        status = (
            status_raw.get("name")
            or status_raw.get("label")
            or status_raw.get("summary")
            or ""
        )
    else:
        status = str(status_raw).strip() if status_raw else ""

    reported_date, reported_raw = _extract_reported_date(api_incident)
    if not reported_date:
        return []

    products = _get_catalog_custom_values(api_incident, "Product", default="Unknown")
    normalized_products = [p for p in products if p and p.casefold() != "unknown"]
    if normalized_products:
        products = normalized_products
    pillar_values = _get_catalog_custom_values(
        api_incident, "Solution Pillar", default="Unknown"
    )
    pillar_hint = pillar_values[0] if pillar_values else "Unknown"

    rca_classification = _extract_rca_classification(api_incident)

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

    permalink = api_incident.get("permalink")
    external_issue_reference = api_incident.get("external_issue_reference") or {}

    client_impact_seconds = _extract_client_impact_duration_seconds(api_incident)
    incident_lead = _extract_incident_lead(api_incident)

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
                "status": status,
                "product": product or "Unknown",
                "pillar": resolved_pillar or pillar_hint or "Unknown",
                "reported_at": reported_raw
                or f"{reported_date.isoformat()}T00:00:00",
                "event_type": event_type,
                "title": title,
                "rca_classification": rca_classification,
                "client_impact_duration_seconds": client_impact_seconds,
                "incident_lead": incident_lead,
                "permalink": permalink,
                "external_issue_reference": external_issue_reference,
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

    def is_unknown_product(product_value):
        return (product_value or "").strip().casefold() in {"", "unknown"}

    def purge_unknown_entries(collection, lookup, seen, inc_numbers):
        if not inc_numbers:
            return

        retained = []
        for entry in collection:
            inc_number = entry.get("inc_number")
            product = entry.get("product")
            lookup_key = (inc_number, (product or "").strip())
            if inc_number in inc_numbers and is_unknown_product(product):
                lookup.pop(lookup_key, None)
                seen.discard(lookup_key)
                continue
            retained.append(entry)

        collection[:] = retained

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

        known_operational_inc_numbers = {
            payload.get("inc_number")
            for payload in filtered_payloads
            if payload.get("event_type") == "Operational Incident"
            and not is_unknown_product(payload.get("product"))
        }
        known_other_inc_numbers = {
            payload.get("inc_number")
            for payload in filtered_payloads
            if payload.get("event_type") != "Operational Incident"
            and not is_unknown_product(payload.get("product"))
        }

        if not dry_run:
            purge_unknown_entries(
                incidents,
                incident_lookup,
                incident_seen,
                known_operational_inc_numbers,
            )
            purge_unknown_entries(
                other_events,
                other_lookup,
                other_seen,
                known_other_inc_numbers,
            )

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
            "timestamp": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
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
    quarter_str = request.args.get("quarter")
    weekly_roundup = request.args.get("weekly_roundup") == "1"
    roundup_start_param = request.args.get("roundup_start") or ""
    missing_only = request.args.get("missing_only") == "1"
    incident_duration_enabled = request.args.get("incident_duration") == "1"
    incident_multi_day_only = incident_duration_enabled and request.args.get(
        "incident_multi_day"
    ) == "1"

    other_duration_enabled = request.args.get("other_duration") == "1"
    other_multi_day_only = other_duration_enabled and request.args.get(
        "other_multi_day"
    ) == "1"
    pillar_filter = [value for value in request.args.getlist("pillar") if value]
    product_filter = [value for value in request.args.getlist("product") if value]
    severity_params = [value for value in request.args.getlist("severity") if value]
    event_type_params = [value for value in request.args.getlist("event_type") if value]
    rca_classification_filter = [
        value for value in request.args.getlist("rca_classification") if value
    ]
    severity_param_supplied = "severity" in request.args
    rca_classification_param_supplied = "rca_classification" in request.args
    key_missing = request.args.get("key_missing") == "1"
    key_uploaded = request.args.get("key_uploaded") == "1"
    key_error = request.args.get("key_error")
    active_tab = tab_override or request.args.get("tab", "incidents")
    allowed_tabs = {"incidents", "others", "osha", "table"}
    if show_config_tab:
        allowed_tabs.add("form")

    if active_tab not in allowed_tabs:
        active_tab = "incidents"
    if not show_config_tab and active_tab == "form":
        return redirect(url_for("ioadmin"))

    sync_config_raw = load_sync_config()
    sync_config = build_sync_config_view(sync_config_raw)
    env_token_available = bool(os.getenv("INCIDENT_IO_API_TOKEN"))
    current_year = date.today().year
    osha_period_type = request.args.get("osha_period", "yearly")
    osha_year_param = request.args.get("osha_year")
    osha_quarter_param = request.args.get("osha_quarter")

    try:
        year = int(year_str) if year_str else current_year
    except ValueError:
        year = current_year

    try:
        month_selection = int(month_str) if month_str else None
    except ValueError:
        month_selection = None

    try:
        quarter_selection = int(quarter_str) if quarter_str else None
    except ValueError:
        quarter_selection = None

    if view_mode not in {"yearly", "monthly", "quarterly"}:
        view_mode = "yearly"

    current_month = date.today().month
    if view_mode == "monthly":
        month_selection = month_selection or current_month
        quarter_selection = (month_selection - 1) // 3 + 1
    elif view_mode == "quarterly":
        if quarter_selection not in {1, 2, 3, 4}:
            source_month = month_selection or current_month
            quarter_selection = (source_month - 1) // 3 + 1
        month_selection = None
    else:
        month_selection = None

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

    procedural_years = {
        entry["incident_date"].year for entry in _procedural_incidents(incidents)
    }
    min_procedural_year = min(procedural_years) if procedural_years else current_year
    osha_available_years = list(range(current_year, min_procedural_year - 1, -1))

    if osha_period_type not in {"yearly", "quarterly"}:
        osha_period_type = "yearly"

    try:
        osha_year = int(osha_year_param) if osha_year_param else current_year
    except ValueError:
        osha_year = current_year

    if osha_year > current_year:
        osha_year = current_year
    if osha_year < min_procedural_year:
        osha_year = min_procedural_year

    try:
        osha_quarter = int(osha_quarter_param) if osha_quarter_param else None
    except ValueError:
        osha_quarter = None

    if osha_period_type == "quarterly":
        if osha_quarter not in {1, 2, 3, 4}:
            if osha_year == current_year:
                osha_quarter = (date.today().month - 1) // 3 + 1
            else:
                osha_quarter = 4
        quarter_start_month = (osha_quarter - 1) * 3 + 1
        quarter_end_month = quarter_start_month + 2
        quarter_end_day = calendar.monthrange(osha_year, quarter_end_month)[1]
        osha_period_start = date(osha_year, quarter_start_month, 1)
        osha_period_end = date(osha_year, quarter_end_month, quarter_end_day)
        osha_period_label = f"Q{osha_quarter} {osha_year}"
    else:
        osha_period_start = date(osha_year, 1, 1)
        osha_period_end = date(osha_year, 12, 31)
        osha_period_label = str(osha_year)

    osha_longest_gap = calculate_longest_procedural_gap(
        incidents,
        period_start=osha_period_start,
        period_end=osha_period_end,
    )

    key_mapping = load_product_key()
    key_present = bool(key_mapping)

    all_events = incidents + other_events
    osha_background_exists = os.path.exists(OSHA_BACKGROUND_IMAGE)
    osha_data = compute_osha_state_from_incidents(incidents)
    if osha_background_exists:
        generate_osha_sign(incidents=incidents)

    osha_image_exists = os.path.exists(OSHA_OUTPUT_IMAGE)
    osha_status = request.args.get("osha_status")

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
    rca_classifications = sorted(
        {
            inc.get("rca_classification")
            for inc in incidents
            if inc.get("rca_classification")
        }
    )
    event_types = sorted(
        {
            event.get("event_type")
            for event in other_events
            if event.get("event_type")
        }
    )

    if severity_params:
        severity_filter = severity_params
    elif severities:
        severity_filter = [severity for severity in severities if not is_sev6(severity)]
        severity_param_supplied = True
    else:
        severity_filter = []

    if event_type_params:
        event_type_filter = event_type_params
    else:
        event_type_filter = []

    if product_filter:
        resolved_pillars = set(pillar_filter)
        for product_value in product_filter:
            resolved_pillar = product_pillar_map.get(product_value) or resolve_pillar(
                product_value, mapping=key_mapping
            )
            if resolved_pillar:
                resolved_pillars.add(resolved_pillar)

        pillar_filter = sorted(resolved_pillars)

    if view_mode == "monthly":
        if month_selection is None:
            today = date.today()
            month_selection = today.month if today.year == year else 1
        if not (1 <= month_selection <= 12):
            month_selection = None
    else:
        month_selection = None

    product_filter_set = set(product_filter)
    pillar_filter_set = set(pillar_filter)
    rca_classification_filter_set = set(rca_classification_filter)
    severity_filter_set = set(severity_filter)
    event_type_filter_set = set(event_type_filter)

    quarter_months = None
    if view_mode == "quarterly" and quarter_selection:
        start_month = (quarter_selection - 1) * 3 + 1
        quarter_months = {start_month, start_month + 1, start_month + 2}

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
            if product_filter_set and event.get("product") not in product_filter_set:
                continue
            if (
                pillar_filter_set
                and not product_filter_set
                and event.get("pillar") not in pillar_filter_set
            ):
                continue
            if rca_classification_filter_set:
                if (
                    (event.get("event_type") or "Operational Incident")
                    == "Operational Incident"
                    and event.get("rca_classification")
                    not in rca_classification_filter_set
                ):
                    continue
            if (
                apply_event_type_filter
                and event_type_filter_set
                and event.get("event_type") not in event_type_filter_set
            ):
                continue

            if include_severity_filter and event.get("event_type") == "Operational Incident":
                if severity_filter_set and event.get("severity") not in severity_filter_set:
                    continue

            if (event.get("event_type") or "Operational Incident") == "Operational Incident":
                client_duration = get_client_impact_duration_seconds(event)
                event["client_impact_duration_seconds"] = client_duration
                if client_duration:
                    event["client_impact_duration_label"] = format_duration_short(client_duration)

            covered_dates = compute_event_dates(event, duration_enabled)
            if not covered_dates:
                continue

            if multi_day_only and len(set(covered_dates)) < 2:
                continue

            if view_mode == "monthly" and month_selection:
                covered_dates = [
                    d
                    for d in covered_dates
                    if d.year == year and d.month == month_selection
                ]
            elif view_mode == "quarterly" and quarter_months:
                covered_dates = [
                    d for d in covered_dates if d.year == year and d.month in quarter_months
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
    elif view_mode == "quarterly" and quarter_selection:
        start_month = (quarter_selection - 1) * 3 + 1
        end_month = start_month + 2
        start_date = date(year, start_month, 1)
        end_of_end_month = calendar.monthrange(year, end_month)[1]
        end_date = date(year, end_month, end_of_end_month)
        today = date.today()
        effective_end = min(end_date, today) if year == today.year else end_date
        days_in_range = max((effective_end - start_date).days + 1, 0)
        days_with_incidents = {
            d
            for d in incident_dates
            if d.month in {start_month, start_month + 1, end_month}
        }
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

    def months_for_quarter(months, quarter):
        start_index = (quarter - 1) * 3
        return months[start_index : start_index + 3]

    if view_mode == "monthly" and month_selection:
        incident_months = [incident_months[month_selection - 1]]
        other_months = [other_months[month_selection - 1]]
    elif view_mode == "quarterly" and quarter_selection:
        incident_months = months_for_quarter(incident_months, quarter_selection)
        other_months = months_for_quarter(other_months, quarter_selection)

    def compute_roundup_window():
        app_today = datetime.now(ZoneInfo("America/New_York")).date()
        explicit_date = parse_date(roundup_start_param) if roundup_start_param else None

        if explicit_date:
            start_date = explicit_date - timedelta(days=explicit_date.weekday())
            end_date = start_date + timedelta(days=6)
        else:
            current_week_start = app_today - timedelta(days=app_today.weekday())
            end_date = current_week_start - timedelta(days=1)
            start_date = end_date - timedelta(days=6)

        return start_date, end_date

    roundup_start, roundup_end = compute_roundup_window()
    prior_roundup_start = roundup_start - timedelta(days=7)
    prior_roundup_end = roundup_end - timedelta(days=7)
    roundup_start_value = roundup_start.isoformat()

    incident_filters = []
    other_filters = []

    def build_remove_link(kind, value=None, tab=None):
        target_tab = tab or active_tab
        params = {"view": view_mode, "year": year, "tab": target_tab}
        if month_selection:
            params["month"] = month_selection
        if quarter_selection:
            params["quarter"] = quarter_selection
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

        def remaining(values, to_remove=None):
            if not values:
                return []
            if to_remove is None:
                return list(values)
            return [val for val in values if val != to_remove]

        pillars_remaining = (
            remaining(pillar_filter, value if kind == "pillar" else None)
            if kind == "pillar"
            else pillar_filter
        )
        products_remaining = (
            remaining(product_filter, value if kind == "product" else None)
            if kind == "product"
            else product_filter
        )
        rca_remaining = (
            remaining(rca_classification_filter, value if kind == "rca" else None)
            if kind == "rca"
            else rca_classification_filter
        )

        if pillars_remaining:
            params["pillar"] = pillars_remaining
        if products_remaining:
            params["product"] = products_remaining
        if rca_remaining:
            params["rca_classification"] = rca_remaining
        elif rca_classification_param_supplied:
            params["rca_classification"] = [""]

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
        if target_tab == "table" and weekly_roundup:
            params["weekly_roundup"] = "1"
            params["roundup_start"] = roundup_start_value
            if missing_only:
                params["missing_only"] = "1"

        return url_for("index", **params)

    for pillar in pillar_filter:
        incident_filters.append(
            {
                "label": "Pillar",
                "value": pillar,
                "remove_link": build_remove_link("pillar", pillar, tab="incidents"),
            }
        )
        other_filters.append(
            {
                "label": "Pillar",
                "value": pillar,
                "remove_link": build_remove_link("pillar", pillar, tab="others"),
            }
        )
    for product in product_filter:
        incident_filters.append(
            {
                "label": "Product",
                "value": product,
                "remove_link": build_remove_link("product", product, tab="incidents"),
            }
        )
        other_filters.append(
            {
                "label": "Product",
                "value": product,
                "remove_link": build_remove_link("product", product, tab="others"),
            }
        )
    for rca_classification in rca_classification_filter:
        incident_filters.append(
            {
                "label": "RCA Classification",
                "value": rca_classification,
                "remove_link": build_remove_link("rca", rca_classification, tab="incidents"),
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

    def build_link(target_view, target_year, target_month=None, tab=None, target_quarter=None):
        target_tab = tab or active_tab
        params = {"view": target_view, "year": target_year, "tab": target_tab}
        if target_view == "monthly" and target_month:
            params["month"] = target_month
        if target_view == "quarterly" and target_quarter:
            params["quarter"] = target_quarter
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
            if rca_classification_filter:
                params["rca_classification"] = rca_classification_filter
            elif rca_classification_param_supplied:
                params["rca_classification"] = [""]
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
    elif view_mode == "quarterly" and quarter_selection:
        current_period_label = f"Q{quarter_selection} {year}"
        prev_year = year
        prev_quarter = quarter_selection - 1
        if prev_quarter < 1:
            prev_quarter = 4
            prev_year -= 1

        next_year = year
        next_quarter = quarter_selection + 1
        if next_quarter > 4:
            next_quarter = 1
            next_year += 1

        prev_link = build_link("quarterly", prev_year, target_quarter=prev_quarter)
        next_link = build_link("quarterly", next_year, target_quarter=next_quarter)
    else:
        current_period_label = str(year)
        prev_link = build_link("yearly", year - 1)
        next_link = build_link("yearly", year + 1)

    target_month_for_toggle = month_selection or date.today().month
    target_quarter_for_toggle = quarter_selection or ((target_month_for_toggle - 1) // 3 + 1)
    yearly_view_link = build_link("yearly", year)
    monthly_view_link = build_link("monthly", year, target_month_for_toggle)
    quarterly_view_link = build_link("quarterly", year, target_quarter=target_quarter_for_toggle)

    today = date.today()

    def annotate_incident_for_table(incident):
        rca_classification = (incident.get("rca_classification") or "").strip()
        normalized = rca_classification.casefold()
        is_missing_rca = (
            not rca_classification
            or normalized in MISSING_RCA_VALUES
            or normalized.startswith("not classified")
        )
        is_procedural = (
            bool(rca_classification)
            and "procedural" in normalized
            and "non-procedural" not in normalized
            and "non procedural" not in normalized
        )
        duration_seconds = get_client_impact_duration_seconds(incident)
        is_long_impact = duration_seconds > LONG_IMPACT_THRESHOLD_SECONDS
        row_classes = []
        if is_procedural:
            row_classes.append("incident-row-procedural")
        if is_long_impact:
            row_classes.append("incident-row-long-impact")
        incident["table_row_class"] = " ".join(row_classes)
        reported_source = incident.get("reported_at") or incident.get("date") or ""
        reported_date = parse_date(reported_source) if reported_source else None
        incident["reported_at_display"] = (
            reported_date.strftime("%m/%d/%Y") if reported_date else reported_source
        )

        if is_missing_rca:
            incident_date = parse_date(incident.get("reported_at") or incident.get("date"))
            age_days = (today - incident_date).days if incident_date else None
            if age_days is not None and age_days < 5:
                incident["rca_cell_class"] = "rca-missing-recent"
            else:
                incident["rca_cell_class"] = "rca-missing-stale"
        else:
            incident["rca_cell_class"] = ""

    for incident in incidents_filtered:
        annotate_incident_for_table(incident)

    def is_missing_rca_value(value):
        normalized = (value or "").strip().casefold()
        return not normalized or normalized in MISSING_RCA_VALUES or normalized.startswith(
            "not classified"
        )

    def format_range_label(start, end):
        if not start or not end:
            return ""
        if start.year == end.year:
            if start.month == end.month:
                return f"{start.strftime('%b')} {start.day}–{end.day}, {start.year}"
            return (
                f"{start.strftime('%b')} {start.day}–"
                f"{end.strftime('%b')} {end.day}, {start.year}"
            )
        return (
            f"{start.strftime('%b')} {start.day}, {start.year}–"
            f"{end.strftime('%b')} {end.day}, {end.year}"
        )

    def incident_matches_filters(event):
        if product_filter_set and event.get("product") not in product_filter_set:
            return False
        if pillar_filter_set and not product_filter_set and event.get("pillar") not in pillar_filter_set:
            return False
        if rca_classification_filter_set and event.get("rca_classification") not in rca_classification_filter_set:
            return False
        if severity_filter_set and event.get("severity") not in severity_filter_set:
            return False
        return True

    def build_roundup_row(incident):
        reported_date = parse_date(incident.get("reported_at") or incident.get("date"))
        reported_display = (
            reported_date.strftime("%m/%d/%Y") if reported_date else incident.get("date") or ""
        )
        incident_id = incident.get("inc_number") or incident.get("id") or ""
        incident_label = incident_id or "—"
        incident_url_id = incident_id.replace("INC-", "") if incident_id else ""
        incident_url = (
            f"https://app.incident.io/myfrontline/incidents/{incident_url_id or incident_id}"
            if incident_id
            else ""
        )
        external_ref = incident.get("external_issue_reference") or {}
        jira_url = (
            external_ref.get("issue_permalink")
            if external_ref.get("provider") == "jira"
            else None
        )
        if not jira_url:
            jira_url = incident.get("permalink") or incident_url

        missing_fields = []
        severity = (incident.get("severity") or "").strip()
        if not severity:
            missing_fields.append("Severity")
        pillar = (incident.get("pillar") or "").strip()
        if not pillar:
            missing_fields.append("Pillar")
        product = (incident.get("product") or "").strip()
        if not product:
            missing_fields.append("Product")
        duration_seconds = get_client_impact_duration_seconds(incident)
        is_long_impact = duration_seconds > LONG_IMPACT_THRESHOLD_SECONDS
        if not duration_seconds:
            missing_fields.append("Impact duration")
        rca_classification = (incident.get("rca_classification") or "").strip()
        if is_missing_rca_value(rca_classification):
            missing_fields.append("RCA classification")
        incident_lead = (incident.get("incident_lead") or "").strip()
        if not incident_lead:
            missing_fields.append("Incident lead")

        return {
            "reported_at_display": reported_display or "—",
            "reported_at_sort": reported_date.isoformat() if reported_date else "",
            "incident_id": incident_id,
            "incident_label": incident_label,
            "incident_url": incident_url,
            "jira_url": jira_url,
            "pillar": pillar,
            "severity": severity,
            "product": product,
            "title": incident.get("title") or "",
            "duration_seconds": duration_seconds,
            "duration_label": format_duration_short(duration_seconds)
            if duration_seconds
            else "Missing",
            "long_impact": is_long_impact,
            "rca_classification": rca_classification or "Missing",
            "incident_lead": incident_lead,
            "missing_fields": missing_fields,
            "missing_severity": not severity,
            "missing_pillar": not pillar,
            "missing_product": not product,
            "missing_duration": not duration_seconds,
            "missing_rca": is_missing_rca_value(rca_classification),
            "missing_incident_lead": not incident_lead,
        }

    def build_roundup_week(label, start_date, end_date):
        rows = []
        duration_by_incident = {}
        for index, incident in enumerate(incidents):
            if not incident_matches_filters(incident):
                continue
            reported_date = parse_date(incident.get("reported_at") or incident.get("date"))
            if not reported_date or not (start_date <= reported_date <= end_date):
                continue

            row = build_roundup_row(incident)
            if missing_only and not row["missing_fields"]:
                continue
            rows.append(row)
            incident_key = row["incident_id"] or f"row-{index}"
            duration = row["duration_seconds"] or 0
            if incident_key in duration_by_incident:
                duration_by_incident[incident_key] = max(duration_by_incident[incident_key], duration)
            else:
                duration_by_incident[incident_key] = duration

        rows.sort(key=lambda item: item["reported_at_sort"], reverse=True)
        grouped_by_pillar = defaultdict(list)
        for row in rows:
            pillar_label = row["pillar"] or "Missing"
            grouped_by_pillar[pillar_label].append(row)

        pillar_groups = []
        for pillar_label, group_rows in grouped_by_pillar.items():
            group_rows.sort(
                key=lambda item: (
                    not (item["product"] or "").strip(),
                    (item["product"] or "").strip().casefold(),
                )
            )
            pillar_groups.append(
                {
                    "pillar": pillar_label,
                    "incident_count": len(group_rows),
                    "incidents": group_rows,
                }
            )
        pillar_groups.sort(
            key=lambda group: (
                group["pillar"] == "Missing",
                group["pillar"].casefold(),
            )
        )

        total_duration = sum(duration_by_incident.values())
        return {
            "label": label,
            "range_label": format_range_label(start_date, end_date),
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "incident_count": len(rows),
            "total_duration_label": format_duration_short(total_duration),
            "total_duration_seconds": total_duration,
            "incidents": rows,
            "pillar_groups": pillar_groups,
        }

    weekly_roundup_weeks = [
        build_roundup_week("Prior week (Mon–Sun)", roundup_start, roundup_end),
        build_roundup_week("Week before (Mon–Sun)", prior_roundup_start, prior_roundup_end),
    ]
    weekly_roundup_header = (
        f"Weeks of {format_range_label(roundup_start, roundup_end)} and "
        f"{format_range_label(prior_roundup_start, prior_roundup_end)}"
    )

    def build_table_view_link(weekly=False):
        params = {"view": view_mode, "year": year, "tab": "table"}
        if month_selection:
            params["month"] = month_selection
        if quarter_selection:
            params["quarter"] = quarter_selection
        if pillar_filter:
            params["pillar"] = pillar_filter
        if product_filter:
            params["product"] = product_filter
        if rca_classification_filter:
            params["rca_classification"] = rca_classification_filter
        elif rca_classification_param_supplied:
            params["rca_classification"] = [""]
        if severity_filter:
            params["severity"] = severity_filter
        elif severity_param_supplied:
            params["severity"] = [""]
        if weekly:
            params["weekly_roundup"] = "1"
            params["roundup_start"] = roundup_start_value
            if missing_only:
                params["missing_only"] = "1"
        return url_for("index", **params)

    table_view_link = build_table_view_link(weekly=False)
    weekly_roundup_link = build_table_view_link(weekly=True)

    def build_table_export_link():
        params = {"view": view_mode, "year": year}
        if month_selection:
            params["month"] = month_selection
        if quarter_selection:
            params["quarter"] = quarter_selection
        if pillar_filter:
            params["pillar"] = pillar_filter
        if product_filter:
            params["product"] = product_filter
        if rca_classification_filter:
            params["rca_classification"] = rca_classification_filter
        elif rca_classification_param_supplied:
            params["rca_classification"] = [""]
        if severity_filter:
            params["severity"] = severity_filter
        elif severity_param_supplied:
            params["severity"] = [""]
        if incident_duration_enabled:
            params["incident_duration"] = "1"
        if incident_multi_day_only:
            params["incident_multi_day"] = "1"
        return url_for("export_incident_table", **params)

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
        quarter_selection=quarter_selection,
        pillars=pillars,
        products=products,
        severities=severities,
        rca_classifications=rca_classifications,
        event_types=event_types,
        products_by_pillar=products_by_pillar,
        product_pillar_map=product_pillar_map,
        pillar_filter=pillar_filter,
        product_filter=product_filter,
        rca_classification_filter=rca_classification_filter,
        rca_classification_param_supplied=rca_classification_param_supplied,
        severity_filter=severity_filter,
        severity_param_supplied=severity_param_supplied,
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
        quarterly_view_link=quarterly_view_link,
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
        osha_data=osha_data,
        osha_image_exists=osha_image_exists,
        osha_background_exists=osha_background_exists,
        osha_status=osha_status,
        osha_period_type=osha_period_type,
        osha_period_year=osha_year,
        osha_period_quarter=osha_quarter,
        osha_available_years=osha_available_years,
        osha_period_label=osha_period_label,
        osha_period_start=osha_period_start,
        osha_period_end=osha_period_end,
        osha_longest_gap=osha_longest_gap,
        weekly_roundup=weekly_roundup,
        weekly_roundup_weeks=weekly_roundup_weeks,
        weekly_roundup_header=weekly_roundup_header,
        roundup_start_value=roundup_start_value,
        missing_only=missing_only,
        table_view_link=table_view_link,
        table_export_link=build_table_export_link(),
        weekly_roundup_link=weekly_roundup_link,
    )


def filter_incidents_for_table_export(
    incidents,
    *,
    year,
    view_mode,
    month_selection,
    quarter_selection,
    pillar_filter,
    product_filter,
    rca_classification_filter,
    severity_filter,
    incident_duration_enabled,
    incident_multi_day_only,
):
    filtered = []
    product_filter_set = set(product_filter)
    pillar_filter_set = set(pillar_filter)
    rca_filter_set = set(rca_classification_filter)
    severity_filter_set = set(severity_filter)

    quarter_months = None
    if view_mode == "quarterly" and quarter_selection:
        start_month = (quarter_selection - 1) * 3 + 1
        quarter_months = {start_month, start_month + 1, start_month + 2}

    for incident in incidents:
        if product_filter_set and incident.get("product") not in product_filter_set:
            continue
        if (
            pillar_filter_set
            and not product_filter_set
            and incident.get("pillar") not in pillar_filter_set
        ):
            continue
        if rca_filter_set and incident.get("rca_classification") not in rca_filter_set:
            continue
        if severity_filter_set and incident.get("severity") not in severity_filter_set:
            continue

        covered_dates = compute_event_dates(incident, incident_duration_enabled)
        if not covered_dates:
            continue

        if incident_multi_day_only and len(set(covered_dates)) < 2:
            continue

        if view_mode == "monthly" and month_selection:
            covered_dates = [
                d for d in covered_dates if d.year == year and d.month == month_selection
            ]
        elif view_mode == "quarterly" and quarter_months:
            covered_dates = [
                d for d in covered_dates if d.year == year and d.month in quarter_months
            ]
        else:
            covered_dates = [d for d in covered_dates if d.year == year]

        if not covered_dates:
            continue

        filtered.append(incident)

    return sorted(
        filtered,
        key=lambda incident: (incident.get("reported_at") or incident.get("date") or ""),
        reverse=True,
    )


@app.route("/incidents/export", methods=["GET"])
def export_incident_table():
    year_str = request.args.get("year")
    view_mode = request.args.get("view", "yearly")
    month_str = request.args.get("month")
    quarter_str = request.args.get("quarter")
    incident_duration_enabled = request.args.get("incident_duration") == "1"
    incident_multi_day_only = incident_duration_enabled and request.args.get(
        "incident_multi_day"
    ) == "1"

    pillar_filter = [value for value in request.args.getlist("pillar") if value]
    product_filter = [value for value in request.args.getlist("product") if value]
    rca_classification_filter = [
        value for value in request.args.getlist("rca_classification") if value
    ]
    severity_filter = [value for value in request.args.getlist("severity") if value]

    try:
        year = int(year_str) if year_str else date.today().year
    except ValueError:
        year = date.today().year

    try:
        month_selection = int(month_str) if month_str else None
    except ValueError:
        month_selection = None

    try:
        quarter_selection = int(quarter_str) if quarter_str else None
    except ValueError:
        quarter_selection = None

    if view_mode not in {"yearly", "monthly", "quarterly"}:
        view_mode = "yearly"

    current_month = date.today().month
    if view_mode == "monthly":
        month_selection = month_selection or current_month
        quarter_selection = (month_selection - 1) // 3 + 1
    elif view_mode == "quarterly":
        if quarter_selection not in {1, 2, 3, 4}:
            source_month = month_selection or current_month
            quarter_selection = (source_month - 1) // 3 + 1
        month_selection = None
    else:
        month_selection = None

    if view_mode == "monthly":
        if month_selection is None:
            today = date.today()
            month_selection = today.month if today.year == year else 1
        if not (1 <= month_selection <= 12):
            month_selection = None

    incidents = [
        event
        for event in load_events(DATA_FILE)
        if (event.get("event_type") or "Operational Incident") == "Operational Incident"
    ]

    incidents_sorted = filter_incidents_for_table_export(
        incidents,
        year=year,
        view_mode=view_mode,
        month_selection=month_selection,
        quarter_selection=quarter_selection,
        pillar_filter=pillar_filter,
        product_filter=product_filter,
        rca_classification_filter=rca_classification_filter,
        severity_filter=severity_filter,
        incident_duration_enabled=incident_duration_enabled,
        incident_multi_day_only=incident_multi_day_only,
    )

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "Reported At",
            "Incident",
            "Pillar",
            "Severity",
            "Product",
            "Title",
            "Impact Duration",
            "RCA Classification",
            "Incident Lead",
        ]
    )
    for incident in incidents_sorted:
        reported_date = parse_date(incident.get("reported_at") or incident.get("date"))
        reported_display = (
            reported_date.strftime("%m/%d/%Y") if reported_date else incident.get("date") or ""
        )
        incident_id = incident.get("inc_number") or incident.get("id") or ""
        duration_seconds = get_client_impact_duration_seconds(incident)
        duration_label = format_duration_short(duration_seconds) if duration_seconds else ""

        writer.writerow(
            [
                reported_display or "—",
                incident_id or "—",
                incident.get("pillar") or "—",
                incident.get("severity") or "—",
                incident.get("product") or "—",
                incident.get("title") or "—",
                duration_label or "—",
                incident.get("rca_classification") or "—",
                incident.get("incident_lead") or "—",
            ]
        )

    csv_data = output.getvalue()
    filename = f"incident-table-{date.today().isoformat()}.csv"
    response = Response(csv_data, mimetype="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return response


@app.route("/graphs", methods=["GET"])
def graphs_view():
    year_param = request.args.get("year")
    current_year = date.today().year

    try:
        year = int(year_param) if year_param else current_year
    except ValueError:
        year = current_year

    incidents = [
        event
        for event in load_events(DATA_FILE)
        if (event.get("event_type") or "Operational Incident") == "Operational Incident"
    ]

    month_rows = build_incident_rca_rows(incidents, year)

    legend = [
        {"status": "no-incidents", "label": "No incidents (green)"},
        {"status": "non-procedural", "label": "Non-procedural (red)"},
        {"status": "missing-task", "label": "Missing task (yellow)"},
        {"status": "deploy", "label": "Deploy (purple)"},
        {"status": "change", "label": "Change (orange)"},
        {"status": "other", "label": "Other classification"},
        {"status": "future", "label": "Future day"},
    ]

    return render_template(
        "graphs.html",
        year=year,
        month_rows=month_rows,
        legend=legend,
    )


@app.route("/", methods=["GET"])
def index():
    return render_dashboard()


@app.route("/stats", methods=["GET"])
def stats_view():
    today = date.today()
    year_param = request.args.get("year")
    view_mode = request.args.get("view", "monthly")
    month_param = request.args.get("month")
    quarter_param = request.args.get("quarter")
    pillar_filter = request.args.get("pillar") or None
    product_filter = request.args.get("product") or None

    severity_options = ["1", "2", "3", "6"]
    severity_params = [value for value in request.args.getlist("severity") if value]
    severity_filter = [value for value in severity_params if value in severity_options]
    if not severity_filter:
        severity_filter = [value for value in severity_options if value != "6"]

    try:
        year = int(year_param) if year_param else today.year
    except ValueError:
        year = today.year
    try:
        month_selection = int(month_param) if month_param else None
    except ValueError:
        month_selection = None
    try:
        quarter_selection = int(quarter_param) if quarter_param else None
    except ValueError:
        quarter_selection = None

    if view_mode not in {"yearly", "monthly", "quarterly"}:
        view_mode = "monthly"

    current_month = today.month
    if view_mode == "monthly":
        month_selection = month_selection or current_month
        quarter_selection = (month_selection - 1) // 3 + 1
    elif view_mode == "quarterly":
        if quarter_selection not in {1, 2, 3, 4}:
            source_month = month_selection or current_month
            quarter_selection = (source_month - 1) // 3 + 1
        month_selection = None
    else:
        month_selection = None

    incidents = [
        event
        for event in load_events(DATA_FILE)
        if (event.get("event_type") or "Operational Incident") == "Operational Incident"
    ]

    key_mapping = load_product_key()
    product_pillar_map, products_by_pillar, pillars = _build_product_pillar_maps(
        incidents, mapping=key_mapping
    )
    products = products_by_pillar.get("__all__", [])

    if pillar_filter:
        products = products_by_pillar.get(pillar_filter, [])
        if product_filter and product_filter not in products:
            product_filter = None

    available_years = sorted(
        {
            parsed_date.year
            for parsed_date in (
                parse_date(inc.get("date") or inc.get("reported_at")) for inc in incidents
            )
            if parsed_date
        }
    )
    if year not in available_years:
        available_years.append(year)
        available_years = sorted(available_years)

    if view_mode == "monthly" and month_selection:
        start_date = date(year, month_selection, 1)
        month_end_day = calendar.monthrange(year, month_selection)[1]
        month_end = date(year, month_selection, month_end_day)
        end_date = min(month_end, today) if year == today.year else month_end
    elif view_mode == "quarterly" and quarter_selection:
        quarter_start_month = (quarter_selection - 1) * 3 + 1
        quarter_end_month = quarter_start_month + 2
        start_date = date(year, quarter_start_month, 1)
        quarter_end_day = calendar.monthrange(year, quarter_end_month)[1]
        quarter_end = date(year, quarter_end_month, quarter_end_day)
        end_date = min(quarter_end, today) if year == today.year else quarter_end
    else:
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31) if year != today.year else today

    filtered_incidents = {}
    severity_agnostic_incidents = {}

    def _incident_unique_key(incident):
        inc_number = _normalize_incident_number(
            incident.get("inc_number")
            or incident.get("id")
            or incident.get("reference")
            or incident.get("name")
        )

        if inc_number:
            return inc_number

        return str(
            incident.get("id")
            or incident.get("name")
            or incident.get("reference")
            or incident.get("inc_number")
            or ""
        )

    for incident in incidents:
        incident_date = parse_date(incident.get("date") or incident.get("reported_at"))
        if not incident_date or incident_date < start_date or incident_date > end_date:
            continue

        product = (incident.get("product") or "").strip()
        pillar = (incident.get("pillar") or "").strip() or resolve_pillar(
            product, mapping=key_mapping
        )

        if pillar_filter and pillar != pillar_filter:
            continue
        if product_filter and product != product_filter:
            continue

        severity_value = normalize_severity_label(incident.get("severity"))
        if severity_value not in severity_options:
            continue

        entry = {
            "raw": incident,
            "date": incident_date,
            "severity": severity_value,
        }

        unique_key = _incident_unique_key(incident)

        severity_agnostic_incidents.setdefault(unique_key, entry)

        if severity_filter and severity_value not in severity_filter:
            continue

        filtered_incidents.setdefault(unique_key, entry)

    classification_counts = {"self_inflicted": 0, "non_procedural": 0, "unknown": 0}
    severity_month_counts = {value: [0] * 12 for value in severity_options}

    for entry in filtered_incidents.values():
        incident = entry["raw"]
        classification_raw = (incident.get("rca_classification") or "").strip()

        if not classification_raw:
            classification_counts["unknown"] += 1
        else:
            normalized_classification = classification_raw.casefold()
            if normalized_classification == "non-procedural incident":
                classification_counts["non_procedural"] += 1
            elif normalized_classification == "not classified":
                classification_counts["unknown"] += 1
            elif normalized_classification in PROCEDURAL_RCA_EXCLUSIONS:
                classification_counts["unknown"] += 1
            else:
                classification_counts["self_inflicted"] += 1

    for entry in severity_agnostic_incidents.values():
        sev_value = entry["severity"]
        if sev_value in severity_month_counts:
            month_index = entry["date"].month - 1
            severity_month_counts[sev_value][month_index] += 1

    if view_mode == "monthly" and month_selection:
        month_indices = [month_selection]
    elif view_mode == "quarterly" and quarter_selection:
        start_month = (quarter_selection - 1) * 3 + 1
        month_indices = [start_month, start_month + 1, start_month + 2]
    else:
        month_indices = list(range(1, 13))

    severity_totals = {
        key: sum(severity_month_counts[key][month - 1] for month in month_indices)
        for key in severity_month_counts
    }
    totals_by_month = [
        sum(
            severity_month_counts[sev][month_index - 1]
            for sev in severity_options
            if sev in severity_filter
        )
        for month_index in month_indices
    ]
    total_incidents = sum(classification_counts.values())
    percent_self_inflicted = (
        round((classification_counts["self_inflicted"] / total_incidents) * 100, 1)
        if total_incidents
        else 0
    )

    month_labels = [calendar.month_abbr[idx] for idx in month_indices]
    severity_rows = [
        {
            "label": f"Sev {value}",
            "months": [severity_month_counts[value][month - 1] for month in month_indices],
            "total": severity_totals[value],
            "included_in_totals": value in severity_filter,
            "is_total": False,
        }
        for value in severity_options
    ]

    severity_rows.append(
        {
            "label": "Total",
            "months": totals_by_month,
            "total": sum(totals_by_month),
            "included_in_totals": True,
            "is_total": True,
        }
    )

    selected_filters = []
    if pillar_filter:
        selected_filters.append({"label": "Pillar", "value": pillar_filter})
    if product_filter:
        selected_filters.append({"label": "Product", "value": product_filter})
    if severity_filter:
        selected_filters.append(
            {"label": "Severity", "value": ", ".join(f"Sev {s}" for s in severity_filter)}
        )

    def build_stats_link(target_view, target_year, target_month=None, target_quarter=None):
        params = {"view": target_view, "year": target_year}
        if target_view == "monthly" and target_month:
            params["month"] = target_month
        if target_view == "quarterly" and target_quarter:
            params["quarter"] = target_quarter
        if pillar_filter:
            params["pillar"] = pillar_filter
        if product_filter:
            params["product"] = product_filter
        if severity_filter:
            params["severity"] = severity_filter
        return url_for("stats_view", **params)

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
        prev_link = build_stats_link("monthly", prev_year, target_month=prev_month)
        next_link = build_stats_link("monthly", next_year, target_month=next_month)
    elif view_mode == "quarterly" and quarter_selection:
        current_period_label = f"Q{quarter_selection} {year}"
        prev_year = year
        prev_quarter = quarter_selection - 1
        if prev_quarter < 1:
            prev_quarter = 4
            prev_year -= 1
        next_year = year
        next_quarter = quarter_selection + 1
        if next_quarter > 4:
            next_quarter = 1
            next_year += 1
        prev_link = build_stats_link("quarterly", prev_year, target_quarter=prev_quarter)
        next_link = build_stats_link("quarterly", next_year, target_quarter=next_quarter)
    else:
        current_period_label = str(year)
        prev_link = build_stats_link("yearly", year - 1)
        next_link = build_stats_link("yearly", year + 1)

    target_month_for_toggle = month_selection or today.month
    target_quarter_for_toggle = quarter_selection or (
        (target_month_for_toggle - 1) // 3 + 1
    )
    yearly_view_link = build_stats_link("yearly", year)
    monthly_view_link = build_stats_link(
        "monthly", year, target_month=target_month_for_toggle
    )
    quarterly_view_link = build_stats_link(
        "quarterly", year, target_quarter=target_quarter_for_toggle
    )

    return render_template(
        "stats.html",
        year=year,
        available_years=available_years,
        view_mode=view_mode,
        month_selection=month_selection,
        quarter_selection=quarter_selection,
        pillars=pillars,
        products=products,
        pillar_filter=pillar_filter,
        product_filter=product_filter,
        severity_filter=severity_filter,
        severity_options=severity_options,
        classification_counts=classification_counts,
        total_incidents=total_incidents,
        percent_self_inflicted=percent_self_inflicted,
        month_labels=month_labels,
        severity_rows=severity_rows,
        selected_filters=selected_filters,
        current_period_label=current_period_label,
        prev_link=prev_link,
        next_link=next_link,
        yearly_view_link=yearly_view_link,
        monthly_view_link=monthly_view_link,
        quarterly_view_link=quarterly_view_link,
    )


@app.route("/ioadmin", methods=["GET"])
def ioadmin():
    return render_dashboard(tab_override="form", show_config_tab=True)


@app.route("/display/frame", methods=["GET"])
def display_frame_pull():
    frame_bytes = load_display_frame_bytes()

    if not frame_bytes:
        return ("No frame available", 404)

    if len(frame_bytes) != DISPLAY_FRAME_BYTES:
        return ("Invalid frame size", 500)

    etag = display_frame_etag(frame_bytes)
    if etag and request.headers.get("If-None-Match") == etag:
        return Response(status=304, headers={"ETag": etag})

    headers = {
        "Content-Type": "application/octet-stream",
        "Content-Length": str(len(frame_bytes)),
    }
    if etag:
        headers["ETag"] = etag

    return Response(frame_bytes, headers=headers)


@app.route("/display/upload", methods=["POST"])
def display_frame_upload():
    upload = request.files.get("file")
    if not upload:
        return jsonify({"status": "error", "message": "Missing file part"}), 400

    payload = upload.read()
    if len(payload) != DISPLAY_FRAME_BYTES:
        return (
            jsonify(
                {
                    "status": "error",
                    "message": f"Frame must be {DISPLAY_FRAME_BYTES} bytes",
                }
            ),
            400,
        )

    _save_display_frame(payload)
    etag = display_frame_etag(payload)

    response = jsonify({"status": "ok", "etag": etag})
    if etag:
        response.headers["ETag"] = etag

    return response


@app.route("/osha/display")
def osha_display():
    if not os.path.exists(OSHA_OUTPUT_IMAGE):
        generate_osha_sign(incidents=load_events(DATA_FILE))

    if not os.path.exists(OSHA_OUTPUT_IMAGE):
        return "No OSHA sign available", 404

    return send_file(OSHA_OUTPUT_IMAGE, mimetype="image/png")


@app.route("/api/osha/send_to_display", methods=["GET"])
def trigger_osha_display_send():
    display_ip = (request.args.get("ip") or "").strip()
    if not display_ip:
        app.logger.warning("OSHA display send requested without an IP address")
        return (
            jsonify({"status": "error", "message": "Query parameter 'ip' is required"}),
            400,
        )

    incidents = [
        event
        for event in load_events(DATA_FILE)
        if (event.get("event_type") or "Operational Incident") == "Operational Incident"
    ]
    latest_incident = latest_procedural_incident_number(incidents)

    app.logger.info(
        "Queuing OSHA sign send to %s (latest incident: %s)",
        display_ip,
        latest_incident or "none",
    )

    def worker():
        send_osha_sign_to_ip(display_ip, incidents=incidents)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

    return jsonify({"status": "queued", "message": "Send started"}), 202


@app.route("/logs", methods=["GET"])
def view_logs():
    recent = read_recent_logs()
    return render_template("logs.html", logs=recent)


@app.route("/osha/send", methods=["POST"])
def send_osha_sign():
    if not os.path.exists(OSHA_OUTPUT_IMAGE):
        generate_osha_sign(incidents=load_events(DATA_FILE))

    status = "sent" if send_osha_to_any_epaper(OSHA_OUTPUT_IMAGE) else "error"
    return redirect(url_for("index", tab="osha", osha_status=status))


@app.route("/osha/update", methods=["POST"])
def update_osha_counter():
    generate_osha_sign(incidents=load_events(DATA_FILE))

    return redirect(url_for("index", tab="osha"))


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
            "client_impact_duration_seconds": duration_seconds if event_type == "Operational Incident" else 0,
            "severity": severity,
            "status": "",
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
        status = (row.get("Status") or "").strip()
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
                "client_impact_duration_seconds": duration_seconds if event_type == "Operational Incident" else 0,
                "severity": severity,
                "status": status,
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


def build_troubleshooting_payload():
    return {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "incidents": load_events(DATA_FILE),
        "other_events": load_events(OTHER_EVENTS_FILE),
    }


@app.route("/sync/download/json", methods=["GET"])
def download_sync_json():
    if os.path.exists(DATA_FILE):
        return send_file(
            DATA_FILE,
            mimetype="application/json",
            as_attachment=True,
            download_name=os.path.basename(DATA_FILE),
        )

    payload = build_troubleshooting_payload()
    buffer = BytesIO()
    buffer.write(json.dumps(payload, indent=2).encode("utf-8"))
    buffer.seek(0)

    filename = f"incident-io-export-{date.today().isoformat()}.json"
    return send_file(
        buffer,
        mimetype="application/json",
        as_attachment=True,
        download_name=filename,
    )


@app.route("/sync/incident/<incident_id>", methods=["GET"])
def fetch_incident_payload(incident_id):
    incident_id = (incident_id or "").strip()
    if not incident_id:
        return jsonify({"error": "Incident ID is required."}), 400

    config = load_sync_config()
    token = config.get("token") or os.getenv("INCIDENT_IO_API_TOKEN")
    base_url = config.get("base_url") or None

    try:
        payload = incident_io_client.fetch_incident_details(
            incident_id,
            base_url=base_url,
            token=token,
        )
    except incident_io_client.IncidentAPIError as exc:
        return jsonify({"error": str(exc)}), 502

    return jsonify(payload)


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
        generate_osha_sign()
        stop_event = threading.Event()
        sync_thread = threading.Thread(
            target=auto_sync_loop, args=(stop_event,), daemon=True
        )
        sync_thread.start()

        try:
            app.run(host=args.host, port=args.port, debug=False)
        finally:
            stop_event.set()
            sync_thread.join(timeout=1)
