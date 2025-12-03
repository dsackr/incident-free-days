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
   - Open the app in a browser and switch to the **Report Incident** tab.
   - Provide an incident number, date, severity, and impacted pillar, then submit.

## Data storage
- Incidents are stored in `incidents.json` in the project root. The file is created on first save.
- Each incident is a JSON object with keys `inc_number`, `date` (`YYYY-MM-DD`), `severity`, and `pillar`.
- Days are colored red when one or more incidents exist for that date; past days without incidents are green.

## Configuration
- The default year rendered on the calendar is controlled by `DEFAULT_YEAR` in `app.py` (currently `2025`).
- The calendar uses Sunday as the first day of the week (`calendar.Calendar(firstweekday=6)`).

## Repository structure
- `app.py` — Flask application entrypoint and routing.
- `templates/index.html` — Calendar layout, tabs, and incident table.
- `static/styles.css` — Styling for the calendar, tabs, and forms.
- `static/tabs.js` — Simple tab switcher for the UI and HTML-to-image export logic.

## Importing data and pillar key mapping
- CSV uploads require a product → pillar key JSON file to be present in the app directory. Upload the key first using the **Upload product → pillar key** form; the key is saved alongside `incidents.json`.
- When a CSV is imported, pillars are populated from the uploaded key using the product name. The pillar values in the CSV are ignored.
- Duplicate incident numbers are skipped when adding incidents manually or via CSV import.

## Exporting the calendar to an image
- The calendar tab includes an **Export as Image** button that captures the visible calendar into a PNG.
- The export runs completely in the browser (no external CDN dependency), so it works even in offline environments.
- The server also provides `/calendar/eink.png`, which returns a 1600×1200 PNG calendar for the current month by default. You can override the period with `?year=YYYY&month=MM`. This endpoint is designed for consumption by external clients such as a Raspberry Pi driving a 13.3" e-ink display.
