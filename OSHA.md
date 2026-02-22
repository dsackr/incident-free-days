# OSHA Counter Logic Specification (incident.io API → offense detection → day counters)

This document defines the exact logic used to sync incident.io data and compute the OSHA-style “incident-free days” counter.

Use this as implementation guidance for an agentic AI generating equivalent logic in another codebase.

## 1) Inputs and data sources

### API authentication
- Use an incident.io API token.
- Read token from:
  1. explicit runtime input (preferred), else
  2. `INCIDENT_IO_API_TOKEN` environment variable.
- If no token is available, fail fast.

### API endpoint
- Base URL defaults to `https://api.incident.io`.
- Support optional override via runtime input or `INCIDENT_IO_BASE_URL`.
- Fetch incidents from `GET /v2/incidents`.

### Local persisted state
You need two persistent stores:
1. **Normalized events** from incident.io (the source collection used for OSHA calculations).
2. **OSHA state snapshot** storing latest computed values (`incident_date`, `prior_incident_date`, `days_since`, `prior_count`, etc.) for fallback when there are no qualifying incidents.

---

## 2) Fetch incidents from incident.io

### Request details
- HTTP headers:
  - `Authorization: Bearer <token>`
  - `Accept: application/json`
  - optional `User-Agent`.
- Request params:
  - include `page_size` (default 50).
  - handle pagination cursors.

### Pagination behavior
Loop until no next cursor:
1. Request current page.
2. Parse JSON.
3. Extract array from `incidents` (or `data` fallback).
4. Append page incidents to master list.
5. Resolve next cursor from common cursor fields:
   - nested pagination object: `next_cursor`, `next`, `after`
   - top-level fallback: `next_cursor`, `next`, `after`
6. For next page request, send both cursor params for compatibility:
   - `page_after=<cursor>`
   - `after=<cursor>`

If non-200 or invalid JSON, return/raise an error.

---

## 3) Normalize incident payloads into internal events

For each API incident, emit one or more normalized events.

### Required identity/time fields
- `inc_number`: use `reference`, fallback `id`. If missing, skip incident.
- `reported_at` + `date`:
  - Prefer custom timestamp named **"Reported At"** from `incident_timestamp_values`.
  - Fallback to `created_at`.
  - Parse datetime robustly (`Z`, ISO8601, common date-time formats).
  - Convert to local business timezone (repo logic shifts UTC to EST).
  - Set:
    - `reported_at` = normalized timestamp string.
    - `date` = date-only (`YYYY-MM-DD`) derived from `reported_at`.
  - If no parseable reported date exists, skip incident.

### Field extraction
- `severity`: from `severity.name`/`severity.label` or raw value.
- `status`: from `status`/`incident_status` object name/label/summary or raw value.
- `event_type`: from `incident_type`/`type` name/label, default to `Operational Incident`.
- `title`: `name`/`title`/`incident_title`/`inc_number` fallback.
- `rca_classification`: from custom field **"RCA Classification"** (name match or known field ID), default `Not Classified`.
- `client_impact_duration_seconds`: from duration metric or custom field named **"Client Impact Duration"**, else `0`.
- `incident_lead`: resolve from likely lead fields/role assignments.
- include `permalink`, `external_issue_reference` if present.

### Product expansion (important)
- Extract Product custom field values.
- If multiple products are present, create **one normalized event per product**.
- Resolve `pillar` from product-key mapping with Solution Pillar as hint.
- If no product available, keep `Unknown` until later replacement.

### Date window filtering
After normalization, filter by configured date window using parsed `reported_at` date:
- keep only records within `[start_date, end_date]` when bounds are provided.

### Dedup/update identity
Use `(inc_number, product)` as unique key.
- Existing key + changed fields => update event.
- New key => insert event.

### Operational vs other collections
- `event_type == "Operational Incident"` → goes to operational incidents collection.
- Anything else → goes to other-events collection.

Only the operational incidents collection feeds OSHA counter calculations.

---

## 4) Determine whether an incident is an OSHA-reset offense

An incident is an **offending incident** (resets OSHA counter) if all are true:
1. It has non-empty `rca_classification`.
2. `rca_classification` is **not** in explicit exclusions:
   - `non-procedural incident`
   - `not classified`
   (case-insensitive compare)
3. Normalized RCA category is one of:
   - `deploy`
   - `change`
   - `missing-task`

### RCA normalization rules
Given case-folded `rca_classification` text:
- contains `non-procedural` → `non-procedural`
- contains (`missing` OR `missed`) AND `task` → `missing-task`
- contains `deploy` → `deploy`
- contains `change` → `change`
- non-empty unmatched text → `other`
- empty → `unknown`

Only `deploy`, `change`, and `missing-task` are OSHA-reset categories.

---

## 5) Build ordered offense timeline

From operational incidents:
1. Keep only offending incidents (rule above).
2. Compute `incident_date` from `date` or fallback `reported_at`.
3. Compute `reported_at` datetime for tie-break ordering; if absent, use `incident_date` at `00:00:00`.
4. Sort descending by `(incident_date, reported_at)` so index `0` is latest offense.

---

## 6) Compute OSHA counter values

### Core fields
If at least one offense exists:
- `latest = offenses[0]`
- `incident_date = latest.incident_date`
- `incident_number = latest.inc_number` (strip leading `INC-` if present)

#### 6.1 Days since last offense
- `days_since = max(today - incident_date in whole days, 0)`

#### 6.2 Prior count (days between latest offense and previous offense)
Goal: prior run length before the current offense.

1. Start with `previous = offenses[1]` if available.
2. If `previous.incident_date == incident_date` (multiple offenses same day), skip same-day entries and select first older offense where date `< incident_date`.
3. `prior_incident_date = selected previous date`, or empty if none.
4. `prior_count`:
   - if prior exists: `max(incident_date - prior_incident_date in days, 0)`
   - else `0`
5. Optional boolean flag: `prior_count_has_same_day_skips = true` when step 2 skipped at least one same-day offense.

#### 6.3 Reason label for display
From latest offense RCA text:
- contains `deploy` → `Deploy`
- contains `miss` → `Missed`
- contains `change` → `Change`
- default `Change`

Persist computed snapshot with timestamp (e.g., `last_reset = now`).

### Fallback mode (no qualifying offense found)
If no offending incidents exist:
- load prior saved OSHA snapshot.
- if snapshot has valid `incident_date`, recompute `days_since` from today.
- if snapshot has valid `incident_date` and `prior_incident_date`, recompute `prior_count` as date difference.
- keep other snapshot fields unchanged.

This preserves continuity when feed has no qualifying data.

---

## 7) Reference pseudocode (portable)

```pseudo
function is_offense(event):
  rca = trim(event.rca_classification)
  if rca is empty: return false

  low = casefold(rca)
  if low in {"non-procedural incident", "not classified"}: return false

  cat = normalize_rca_category(low)
  return cat in {"deploy", "change", "missing-task"}

function normalize_rca_category(low):
  if "non-procedural" in low: return "non-procedural"
  if (("missing" in low) or ("missed" in low)) and ("task" in low): return "missing-task"
  if "deploy" in low: return "deploy"
  if "change" in low: return "change"
  if low != "": return "other"
  return "unknown"

function compute_osha_state(operational_events, saved_state, today):
  offenses = []
  for e in operational_events:
    if not is_offense(e): continue
    d = parse_date(e.date) or parse_date(e.reported_at)
    if d is null: continue
    dt = parse_datetime(e.reported_at) or combine(d, 00:00:00)
    offenses.append({event:e, incident_date:d, reported_at:dt})

  sort offenses descending by (incident_date, reported_at)

  if offenses not empty:
    latest = offenses[0]
    incident_date = latest.incident_date
    prev = offenses[1] if len(offenses) > 1 else null

    skipped_same_day = false
    if prev != null and prev.incident_date == incident_date:
      prev = first entry in offenses[1:] where entry.incident_date < incident_date
      skipped_same_day = (prev != null)

    prior_date = prev.incident_date if prev else null

    return {
      incident_number: strip_prefix(latest.event.inc_number, "INC-"),
      incident_date: iso_date(incident_date),
      prior_incident_date: iso_date(prior_date) or "",
      days_since: max(days_between(today, incident_date), 0),
      prior_count: max(days_between(incident_date, prior_date), 0) if prior_date else 0,
      prior_count_has_same_day_skips: skipped_same_day,
      reason: infer_reason(latest.event.rca_classification),
      last_reset: now_iso()
    }

  # fallback if no offense
  state = copy(saved_state)
  if parse_date(state.incident_date):
    state.days_since = days_between(today, parse_date(state.incident_date))
  if parse_date(state.incident_date) and parse_date(state.prior_incident_date):
    state.prior_count = days_between(parse_date(state.incident_date), parse_date(state.prior_incident_date))
  return state
```

---

## 8) Implementation notes for AI code generation

- Treat `rca_classification` text matching as case-insensitive and whitespace-trimmed.
- Keep parsing resilient for multiple datetime formats and UTC suffix `Z`.
- Always sort by date + timestamp to ensure deterministic “latest offense” selection.
- Maintain same-day skip behavior for `prior_count`, otherwise prior count is inflated/incorrect.
- Clamp negative day values to zero.
- Store both raw normalized events and computed OSHA snapshot for repeatable behavior.
