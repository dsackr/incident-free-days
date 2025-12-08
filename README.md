# Incident Free Days

A simple Flask app that visualizes incident-free days across a year. It renders a year-long calendar that colors each day based on whether an incident was reported and provides a form to add new incidents.

## Features
- Year view calendar with green (incident-free), red (incident), and grey (future/unknown) days.
- Tabbed interface to switch between the calendar and the incident submission form.
- Incident list showing everything recorded in `incidents.json`, sorted newest first.
- Accepts incident dates as either `YYYY-MM-DD` (native date input) or `MM/DD/YYYY`.

## Getting started
1. **Install dependencies** (Flask, Pillow, and related libraries)
   ```bash
   sudo apt-get update && sudo apt-get install -y fonts-dejavu-core
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Run the app**
   ```bash
   python app.py
   ```
   The server listens on `http://0.0.0.0:8080/`.

### Run as a system service (systemd)
1. **Create a dedicated user** (optional but recommended)
   ```bash
   sudo useradd --system --shell /usr/sbin/nologin --home /opt/incident-free-days incident
   sudo mkdir -p /opt/incident-free-days
   sudo chown -R incident:incident /opt/incident-free-days
   ```

2. **Copy the app to the service directory**
   ```bash
   sudo cp -r /workspace/incident-free-days /opt/incident-free-days/app
   sudo chown -R incident:incident /opt/incident-free-days/app
   ```

3. **Create the virtual environment and install dependencies**
   ```bash
   cd /opt/incident-free-days/app
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   deactivate
   ```

4. **Create the systemd unit file**
   ```bash
   sudo tee /etc/systemd/system/incident-free-days.service > /dev/null <<'EOF'
   [Unit]
   Description=Incident Free Days Flask app
   After=network.target

   [Service]
   Type=simple
   User=incident
   WorkingDirectory=/opt/incident-free-days/app
   ExecStart=/opt/incident-free-days/app/.venv/bin/python app.py
   Restart=on-failure
   Environment=FLASK_ENV=production

   [Install]
   WantedBy=multi-user.target
   EOF
   ```

5. **Start and enable the service**
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl start incident-free-days.service
   sudo systemctl status incident-free-days.service
   sudo systemctl enable incident-free-days.service
   ```

   The app will now start on boot and can be managed via `systemctl`.

3. **Report an incident**
- Open the app in a browser and switch to the **Configuration** tab.

## Data storage
- Incidents are stored in `incidents.json` in the project root. The file is created on first save.
- Each incident is a JSON object with keys `inc_number`, `date` (`YYYY-MM-DD`), `severity`, and `pillar`.
- Days are colored red when one or more incidents exist for that date; past days without incidents are green.

## Configuration
- The default year rendered on the calendar is controlled by `DEFAULT_YEAR` in `app.py` (currently `2025`).
- The calendar uses Sunday as the first day of the week (`calendar.Calendar(firstweekday=6)`).
- To sync incidents from incident.io, set `INCIDENT_IO_API_TOKEN` (and optionally `INCIDENT_IO_BASE_URL` if you use a non-default host).

## Syncing from incident.io
The app can pull incidents directly from incident.io so you do not need to manually upload CSV files.

### Via the Configuration tab
1. Open the app and go to **Configuration**.
2. Enter your `INCIDENT_IO_API_TOKEN` and optional base URL override. Tokens are stored locally in `sync_config.json` and are never displayed back in the UI.
3. Pick a sync cadence (once an hour, once a day [default], or once a week) and optionally set a start/end date window.
4. Click **Dry run mapping** to preview the first 10 normalized payloads and verify pillars/severities.
5. Click **Import incidents** to pull all incidents in the selected window into `incidents.json`/`others.json`.

### Automatic sync cadence
- The Flask server now runs a lightweight background worker that honors your selected cadence **as long as the app process is running**. It will trigger `/sync/incidents` with the stored token/base URL/date window and update the **Last sync** status on completion.
- To keep the worker alive between reboots, run the app under a supervisor such as the systemd unit described above; you don't need an extra cron entry.
- If you prefer to offload scheduling, you can alternatively point an external scheduler (cron, systemd timer, Kubernetes CronJob) at `POST http://<host>:8080/sync/incidents` with the token/base URL payload. In that mode, set cadence to "off" or ignore the in-app cadence.

### Via the CLI or API
1. Export your incident.io API token to the environment:
   ```bash
   export INCIDENT_IO_API_TOKEN="<token>"
   # Optional: override the API host
   # export INCIDENT_IO_BASE_URL="https://eu.api.incident.io"
   ```

2. Run a dry run from the feature branch to verify parsing and mapping without writing to disk:
   ```bash
   python app.py --sync-incidents --dry-run
   ```

3. Sync data to `incidents.json`/`others.json` once the output looks correct:
   ```bash
   python app.py --sync-incidents
   ```

You can also trigger the sync over HTTP using `GET /sync/incidents?dry_run=1` (dry run) or `POST /sync/incidents` with a JSON payload (token, optional date range) to persist results.

## Repository structure
- `app.py` — Flask application entrypoint and routing.
- `templates/index.html` — Calendar layout, tabs, and incident table.
- `static/styles.css` — Styling for the calendar, tabs, and forms.
- `static/tabs.js` — Simple tab switcher for the UI and HTML-to-image export logic.

## Importing data and pillar key mapping
- Upload a product → pillar key JSON file using the **Upload product → pillar key** form; the key is saved alongside `incidents.json`.
- Duplicate incident numbers are skipped when adding incidents via sync.

## Exporting the calendar to an image
- The calendar tab includes an **Export as Image** button that captures the visible calendar into a PNG.
- The export runs completely in the browser (no external CDN dependency), so it works even in offline environments.
- The server also provides `/calendar/eink.png`, which returns a 1600×1200 PNG calendar for the current month by default. You can override the period with `?year=YYYY&month=MM`. This endpoint is designed for consumption by external clients such as a Raspberry Pi driving a 13.3" e-ink display.
