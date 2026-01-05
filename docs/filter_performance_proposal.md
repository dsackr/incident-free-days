# Filter performance architecture proposal

The current dashboard re-renders whole pages on every filter change, which is costly when
filtering across large incident/event datasets. The following architecture reduces end-to-end
latency and network pressure while keeping the experience consistent across calendar and table
views.

## 1. Pre-compute and cache filterable aggregates
- Build a background job (Celery/cron) that materializes per-day and per-product aggregates into a
  compact store (e.g., Redis or SQLite). Cache slices by (year, quarter, month, pillar, product,
  severity) so the web layer only assembles pre-computed cells.
- When new incidents arrive, incrementally update only the affected days instead of re-hydrating the
  whole year. Track dirty ranges and refresh them asynchronously.

## 2. Move filter evaluation into an API layer
- Serve incidents/other events via a REST endpoint that accepts filter parameters and returns
  pre-aggregated daily cells plus minimal row metadata for the modal/table views.
- The Flask page renders a lightweight shell; the front end requests data via `fetch` and swaps the
  calendar/table content without a full page reload.

## 3. Introduce client-side state + optimistic UI
- Keep filter state in a single client store (e.g., Redux or a simple global store). The Save button
  persists state locally and triggers one batched API request. Use optimistic UI updates to collapse
  the filter panel immediately.
- Cache the last N filter responses in-memory keyed by the filter hash so navigating between tabs or
  toggling views reuses data without round-trips.

## 4. Pagination and virtualization for large tables
- Implement paginated/virtualized rows in the incident table. Load only the visible page from the API
  and fetch additional pages on demand. This keeps DOM sizes small and reduces serialization costs.

## 5. Background sync decoupled from requests
- Continue to fetch fresh incident feeds in the background and push updates into the aggregate cache.
  Expose a lightweight “last updated” timestamp so the client can decide when to invalidate cached
  responses.

## Rollout steps
1. Ship the new API endpoints backed by cached aggregates.
2. Port the calendar/table rendering to use the API responses while retaining the existing HTML
   structure for minimal visual change.
3. Enable client-side caching and optimistic Save interactions.
4. Add metrics (request latency, cache hit rate, render time) to validate improvements.
