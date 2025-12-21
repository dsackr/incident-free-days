from flask import Flask, jsonify, redirect, render_template, request, send_file, url_for
import os
import json
import threading

import app as main_app

osha_app = Flask(__name__)


@osha_app.route("/")
def osha_dashboard():
    active_tab = request.args.get("tab", "osha")
    if active_tab not in {"osha", "form"}:
        active_tab = "osha"

    incidents = [
        event
        for event in main_app.load_events(main_app.DATA_FILE)
        if (event.get("event_type") or "Operational Incident") == "Operational Incident"
    ]
    other_events = [
        event
        for event in main_app.load_events(main_app.OTHER_EVENTS_FILE)
        if (event.get("event_type") or "") != "Operational Incident"
    ]

    osha_background_exists = os.path.exists(main_app.OSHA_BACKGROUND_IMAGE)
    osha_data = main_app.compute_osha_state_from_incidents(incidents)
    if osha_background_exists:
        main_app.generate_osha_sign(incidents=incidents)

    osha_image_exists = os.path.exists(main_app.OSHA_OUTPUT_IMAGE)
    osha_status = request.args.get("osha_status")

    key_mapping = main_app.load_product_key()
    key_present = bool(key_mapping)
    all_events = incidents + other_events

    product_pillar_map = {k: v for k, v in key_mapping.items() if k and v}
    for event in all_events:
        product = (event.get("product") or "").strip()
        pillar = (event.get("pillar") or "").strip()

        if not product:
            continue

        if not pillar:
            pillar = main_app.resolve_pillar(product, mapping=key_mapping)

        if pillar:
            product_pillar_map.setdefault(product, pillar)
        elif product not in product_pillar_map:
            product_pillar_map[product] = None

    products_by_pillar = {}
    for product, pillar in product_pillar_map.items():
        products_by_pillar.setdefault(pillar, set()).add(product)

    products_by_pillar = {
        pillar: sorted(values) for pillar, values in products_by_pillar.items() if pillar is not None
    }
    products_by_pillar["__all__"] = sorted(product_pillar_map.keys())

    sync_config_raw = main_app.load_sync_config()
    sync_config = main_app.build_sync_config_view(sync_config_raw)
    env_token_available = bool(os.getenv("INCIDENT_IO_API_TOKEN"))

    key_missing = request.args.get("key_missing") == "1"
    key_uploaded = request.args.get("key_uploaded") == "1"
    key_error = request.args.get("key_error")

    return render_template(
        "osha_app.html",
        active_tab=active_tab,
        osha_data=osha_data,
        osha_image_exists=osha_image_exists,
        osha_background_exists=osha_background_exists,
        osha_status=osha_status,
        sync_config=sync_config,
        env_token_available=env_token_available,
        products_by_pillar=products_by_pillar,
        product_pillar_map=product_pillar_map,
        key_missing=key_missing or (not key_present and request.args.get("tab") == "form"),
        key_uploaded=key_uploaded,
        key_error=key_error,
        incidents_by_date={},
        other_by_date={},
        client_ip=main_app.get_client_ip(request),
    )


@osha_app.route("/osha/display")
def osha_display():
    if not os.path.exists(main_app.OSHA_OUTPUT_IMAGE):
        main_app.generate_osha_sign(incidents=main_app.load_events(main_app.DATA_FILE))

    if not os.path.exists(main_app.OSHA_OUTPUT_IMAGE):
        return "No OSHA sign available", 404

    return send_file(main_app.OSHA_OUTPUT_IMAGE, mimetype="image/png")


@osha_app.route("/osha/send", methods=["POST"])
def send_osha_sign():
    if not os.path.exists(main_app.OSHA_OUTPUT_IMAGE):
        main_app.generate_osha_sign(incidents=main_app.load_events(main_app.DATA_FILE))

    status = "sent" if main_app.display_osha_on_epaper(main_app.OSHA_OUTPUT_IMAGE) else "error"
    return redirect(url_for("osha_dashboard", tab="osha", osha_status=status))


@osha_app.route("/osha/update", methods=["POST"])
def update_osha_counter():
    main_app.generate_osha_sign(incidents=main_app.load_events(main_app.DATA_FILE))
    return redirect(url_for("osha_dashboard", tab="osha"))


@osha_app.route("/sync/incidents", methods=["GET", "POST"])
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

    config = main_app.load_sync_config()
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
        main_app.save_sync_config(config)

    result = main_app.sync_incidents_from_api(
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


@osha_app.route("/sync/config", methods=["POST"])
def update_sync_config():
    payload = request.get_json(silent=True) or {}
    config = main_app.load_sync_config()

    updated_mapping = main_app.normalize_field_mapping(
        payload.get("field_mapping") or config.get("field_mapping")
    )

    updated = {
        "cadence": payload.get("cadence") or config.get("cadence", "daily"),
        "base_url": payload.get("base_url") or "",
        "token": payload.get("token") or config.get("token") or "",
        "start_date": payload.get("start_date") or "",
        "end_date": payload.get("end_date") or "",
        "last_sync": config.get("last_sync") or {},
        "field_mapping": updated_mapping,
    }

    main_app.save_sync_config(updated)
    view_config = main_app.build_sync_config_view(updated)
    return jsonify({"config": view_config})


@osha_app.route("/sync/wipe", methods=["POST"])
def wipe_local_data():
    for path in (main_app.DATA_FILE, main_app.OTHER_EVENTS_FILE):
        try:
            os.remove(path)
        except FileNotFoundError:
            continue
        except OSError:
            continue

    config = main_app.load_sync_config()
    if config:
        config["last_sync"] = {}
        main_app.save_sync_config(config)

    return jsonify({"status": "ok"})


@osha_app.route("/upload-key", methods=["POST"])
def upload_key_file():
    file = request.files.get("key_file")
    if not file or not file.filename:
        return redirect(url_for("osha_dashboard", key_error="missing", tab="form"))

    try:
        content = file.read().decode("utf-8-sig")
    except UnicodeDecodeError:
        return redirect(url_for("osha_dashboard", key_error="decode", tab="form"))

    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        return redirect(url_for("osha_dashboard", key_error="invalid", tab="form"))

    if not isinstance(data, dict):
        return redirect(url_for("osha_dashboard", key_error="format", tab="form"))

    normalized = {str(k).strip(): str(v).strip() for k, v in data.items() if str(k).strip()}

    with open(main_app.PRODUCT_KEY_FILE, "w") as f:
        json.dump(normalized, f, indent=2)

    incidents = main_app.load_events(main_app.DATA_FILE)
    other_events = main_app.load_events(main_app.OTHER_EVENTS_FILE)

    for collection, path in ((incidents, main_app.DATA_FILE), (other_events, main_app.OTHER_EVENTS_FILE)):
        if not collection:
            continue

        for incident in collection:
            incident["pillar"] = main_app.resolve_pillar(
                incident.get("product", ""), incident.get("pillar", ""), mapping=normalized
            )

        main_app.save_events(path, collection)

    return redirect(url_for("osha_dashboard", key_uploaded=1, tab="form"))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Standalone OSHA Counter")
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
    parser.add_argument("--port", default=5050, type=int, help="Port for the Flask server")

    args = parser.parse_args()

    if args.sync_incidents:
        summary = main_app.sync_incidents_from_api(dry_run=args.dry_run)
        print(json.dumps(summary, indent=2))
    else:
        main_app.generate_osha_sign()
        stop_event = threading.Event()
        sync_thread = threading.Thread(target=main_app.auto_sync_loop, args=(stop_event,), daemon=True)
        sync_thread.start()

        try:
            osha_app.run(host=args.host, port=args.port, debug=False)
        finally:
            stop_event.set()
            sync_thread.join(timeout=1)
