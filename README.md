# Databricks Job & Task Observability Dashboard

A Lakeview dashboard for **job and task run health across all workspaces**, built
on materialized views over `system.lakeflow` tables. Operational scope only —
runs, status, duration, repairs, task-level failures. Cost is out of scope by
design (covered by a separate dashboard).

## Files

| File | What it is |
| --- | --- |
| `materialized_views.sql` | 5 `CREATE MATERIALIZED VIEW` statements. Read `system.lakeflow` / `system.access` and expose collapsed, one-row-per-run tables. Run first. |
| `dashboard_datasets.sql` | 19 reference dataset queries (parameterized), with design commentary. The source of truth for the SQL; a trimmed copy of 11 of these is embedded in the generator. |
| `unified_job_platform.lvdash.json` | The importable Lakeview dashboard — 11 datasets, 27 widgets, 5 pages. |
| `build_dashboard.py` | Generator that produces the `.lvdash.json`. Lets you regenerate the dashboard from code rather than hand-editing JSON. |

The five materialized views:

| MV | Grain |
| --- | --- |
| `synced_workspaces` | workspace id → name / url / status |
| `jobs_latest` | latest definition per job |
| `job_runs_latest` | one row per job run (final state via `max_by`, plus `repair_count`) |
| `task_runs_latest` | one row per task run *(optional — Section C)* |
| `job_tasks_latest` | latest task definition *(optional — Section C)* |

## To stand it up

1. **Set catalog/schema** (defaults `main.cost_management`) — top of
   `build_dashboard.py` and both SQL files.
2. **On Azure:** run `SHOW TABLES IN system.lakeflow`. If the task tables
   (`job_task_run_timeline`, `job_tasks`) are missing, skip **Section C** of the
   MV file and **delete the Tasks page** (the generator note flags it). The core
   job + health pages stand on their own.
3. **Run `materialized_views.sql`**, then its post-create checks (bottom of the
   file) to confirm the repair mechanism and the `max_by` fix behave as expected.
4. **Import `unified_job_platform.lvdash.json`** (Dashboards → menu → Import),
   or deploy via the API command `build_dashboard.py` prints. Pick a warehouse,
   then run through each widget once to confirm it returns data.

> The dashboard JSON is structurally validated (layout, field-name matching,
> widget versions) but has **not** been run against a live warehouse. Step 4's
> widget walk-through is the data validation.

## `build_dashboard.py` — when you'd touch it

The script defines each dataset and widget once and emits correct JSON every
time, so the dashboard is reproducible rather than a hand-maintained blob.
Run it with `python3 build_dashboard.py`; it writes `unified_job_platform.lvdash.json`
and prints the deploy command.

Reach for it when you want to:

- **Change catalog/schema** → edit the `CATALOG` / `SCHEMA` constants at the top,
  rerun.
- **Change the lookback window** → edit `D` (days) / `HOURS`, rerun.
- **Add a dataset left off the dashboard** (matrix, gantt, overlaps, anomalies,
  SLA, duration profile, inventory — all present in `dashboard_datasets.sql`) →
  add a `ds(...)` call and a widget via the helpers, rerun.
- **Re-lay-out pages** → edit the `overview` / `jobs` / `health` / `tasks` layout
  lists. Widgets sit on a 6-column grid; each row must sum to width 6.

Structure of the script:

1. **Config** — catalog, schema, window defaults.
2. **Datasets** — the SQL for each widget's data, parameters inlined as literals
   (the `.lvdash.json` format does not use `:params`).
3. **Widget helpers + layout** — `counter()`, `table()`, `bar()`, `line()`,
   `pie()`, `filt()` stamp out each widget type with the correct schema; the page
   layout lists place them on the grid.

If you prefer a static dashboard and don't want a generator in the loop, the
`.lvdash.json` stands on its own for importing — `build_dashboard.py` is for
maintainability, not a runtime dependency.
