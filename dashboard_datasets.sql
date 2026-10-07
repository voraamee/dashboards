-- =====================================================================
-- Unified Job Platform — Lakeview dashboard datasets
-- =====================================================================
-- Extracted from the Databricks App (src/backend/routers/*.py) and
-- rewritten as standalone Lakeview dataset queries.
--
-- SCOPE: operational job and task observability — did it run, did it
-- succeed, how long did it take, which task broke. NOT spend: cost by
-- job and cost by workspace already live in separate dashboards
-- (see Section 3). NOT pipelines (see Section 5).
--
-- Source tables are the materialized views built by the existing SDP
-- pipeline (src/pipeline/job_monitoring_pipeline.py). Keep that pipeline;
-- only the app layer is replaced.
--
-- All queries read MATERIALIZED VIEWS defined in materialized_views.sql.
-- The dashboard touches NO system tables directly — the MVs do, and the
-- dashboard reads only the MVs. Create the MVs first.
--
--   ${catalog}.${schema}.job_runs_latest   one row per job run, carries
--                                          result_state (final, max_by)
--                                          and repair_count
--   ${catalog}.${schema}.jobs_latest       latest state per job
--   ${catalog}.${schema}.synced_workspaces workspace id -> name/url
--   ${catalog}.${schema}.task_runs_latest  one row per task run (Sec 4)
--   ${catalog}.${schema}.job_tasks_latest  latest task definition (4.4)
--
--   system.lakeflow.job_task_run_timeline    per-task runs  (Section 4)
--   system.lakeflow.job_tasks                task definitions (Section 4)
--
-- billing_usage_enriched is NOT read by this dashboard. The SDP pipeline
-- still builds it, for the cost dashboard.
--
-- Replace ${catalog}.${schema} with your values (default main.cost_management).
--
-- =====================================================================
-- TARGET CLOUD: AZURE
-- =====================================================================
-- Everything here is standard Databricks SQL and runs unchanged on
-- Azure. Two cloud-sensitive points remain, both flagged inline:
--
--   1. WORKSPACE URLs — RESOLVED. workspace_url on this account stores
--      the full scheme (https://adb-<id>.<n>.azuredatabricks.net), so
--      the run_url columns in 1.4, 2.2 and 4.1 concatenate directly;
--      nothing needs prepending. A trailing slash is stripped
--      defensively so the result never contains '//jobs/'.
--
--      One thing still worth a single click-test: the PATH pattern.
--      These build /jobs/<job_id>/runs/<run_id>, which is the current
--      workspace UI route. Older workspaces used the hash form
--      #job/<job_id>/run/<run_id>. Open one run_url from 1.4; if it
--      lands on the run page, all three columns are correct.
--
--   2. SYSTEM TABLE AVAILABILITY (Section 4). Which system tables are
--      published, and when, varies by cloud and release. The task-level
--      tables in Section 4 need DESCRIBE-checking on this specific
--      workspace — see that section's VERIFY block. This is the main
--      open risk in the current scope.
--
-- No longer relevant now that cost is out of scope: the AWS-shaped SKU
-- region regex in the app's by-sku query (it was a silent no-op on
-- Azure), and the list_prices join. Both lived in Section 3.
-- Lakebase is irrelevant too — the dashboard reads the warehouse
-- directly.
--
-- =====================================================================
-- MULTI-WORKSPACE DESIGN — group by ID, filter by name
-- =====================================================================
-- Every dataset returns ALL workspaces by default and carries both the
-- IDs and the display names. The division of labour:
--
--   AGGREGATION GRAIN  ->  (workspace_id, job_id)
--   FILTER FIELDS      ->  workspace_name, job_name
--   CHART LABELS       ->  job_label ("job name @ workspace name")
--
-- Why the grain is the composite ID and not job_id alone:
--   job_id is assigned per workspace, so the same number is a different
--   job in each one. Grouping by job_id across workspaces merges
--   unrelated jobs and corrupts every count, cost and rate. Scoping via
--   the filter instead is only correct while exactly ONE workspace is
--   selected — it breaks in the default all-workspaces state.
--   The composite grain costs nothing: filter to one workspace and it
--   collapses to the same rows as grouping by job_id would have given.
--
-- Why names are filters and not the grain:
--   workspaces get renamed and job names duplicate freely across
--   workspaces. Names are for humans; IDs are the stable key.
--
-- The original app grouped by job_id alone in retry-stats, sla-status,
-- duration-percentiles and costs/top-jobs — correct for one workspace,
-- silently wrong across several. Preserve the composite grain if you
-- edit these, and keep it as the MV primary grain.
--
-- =====================================================================
-- FILTER WIDGETS TO CREATE
-- =====================================================================
--   Filter          Bind to field     Scope
--   -------------   ---------------   ------------------------------------
--   Workspace       workspace_name    GLOBAL — present on every dataset,
--                                     so one widget drives the whole
--                                     dashboard. Multi-select.
--   Job             job_name          Sections 1, 2 and 4. Absent from
--                                     the date-level rollups (1.1, 1.2,
--                                     1.3, 1.6) — leave those unbound.
--   Task            task_name         Section 4 only.
--   Result state    result_state      1.4, 1.5, 1.7, 4.1 (run-level).
--
-- No pipeline filter — pipelines are deferred this release (Section 5).
--
-- Use field-bound filter widgets, NOT a :workspace_id parameter:
--   - one widget drives every bound widget on the page at once
--   - multi-select (pick 3 of 12 workspaces) works; an = param does not
--   - no 'All' sentinel to special-case in each query
--   - MVs cannot hold parameters, so a param would have to be stripped
--     during conversion anyway
--
-- Every dataset here is safe to bind the workspace filter to — there is
-- no cross-workspace rollup that would collapse under it. Workspace-level
-- cost comparison lives in a separate dashboard and is out of scope.
--
-- =====================================================================
-- PARAMETERS (declare once per dataset in the Lakeview editor)
-- =====================================================================
--   :lookback_days   numeric  default 30
--   :lookback_hours  numeric  default 24  (concurrency / gantt only)
--   :min_runs        numeric  default 3   (failing jobs only)
--   :z_threshold     numeric  default 2.0 (anomalies only)
--   :baseline_days   numeric  default 90  (anomalies only — MUST exceed
--                                          :lookback_days, see 2.3)
--   :sla_multiplier  numeric  default 2.0 (sla status only)
--   :runs_per_job    numeric  default 10  (matrix only)
--   :threshold_hours numeric  default 2   (prolonged runs only)
--
-- Time windows stay parameters rather than field filters so the
-- warehouse can prune the scan. Everything else is a field filter.
--
-- NOTE on the 30-day default: there is no longer any 90-day ceiling.
-- That came from billing_usage_enriched, which is out of scope now.
-- Every remaining dataset reads job_runs_latest or the system.lakeflow
-- task tables, none of which carry a retention filter — so the window
-- is bounded only by how far back system.lakeflow.job_run_timeline and
-- job_task_run_timeline go in this workspace. 30 days is a choice, not
-- a limit; raise it if the history supports it.
--
-- Lakebase is NOT needed here — Lakeview queries the SQL warehouse
-- directly, so the dual-dialect/failover layer in the app is dropped.
-- =====================================================================


-- =====================================================================
-- SECTION 1 — JOB RUNS   (was: routers/jobs.py)
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1.1  job_run_summary  — KPI counters, per workspace
-- Was: GET /api/jobs/summary
-- Widgets: counter tiles. With a workspace filter applied the counters
--          scope to the selection; unfiltered they read account-wide.
--          Counters must use SUM aggregation over these rows, not the
--          raw first row.
-- ---------------------------------------------------------------------
SELECT
    CAST(r.workspace_id AS STRING)                                                AS workspace_id,
    COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING))                    AS workspace_name,
    COUNT(*)                                                                      AS total_runs,
    SUM(CASE WHEN r.result_state = 'SUCCEEDED' THEN 1 ELSE 0 END)                 AS succeeded,
    SUM(CASE WHEN r.result_state IN ('FAILED','ERROR','TIMED_OUT') THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN r.result_state IS NULL OR r.result_state = 'RUNNING' THEN 1 ELSE 0 END) AS running,
    COUNT(DISTINCT r.job_id)                                                      AS distinct_jobs,
    ROUND(
        100.0 * SUM(CASE WHEN r.result_state = 'SUCCEEDED' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0)
    , 2)                                                                          AS success_rate_pct
FROM ${catalog}.${schema}.job_runs_latest r
LEFT JOIN ${catalog}.${schema}.synced_workspaces w
       ON r.workspace_id = w.workspace_id
WHERE r.period_start_time >= dateadd(DAY, -:lookback_days, current_date())
GROUP BY r.workspace_id, w.workspace_name;


-- ---------------------------------------------------------------------
-- 1.2  job_runs_daily  — daily run volume by outcome, per workspace
-- Was: GET /api/jobs/daily
-- Widget: stacked bar, x = run_date, y = SUM(succeeded) / SUM(failed),
--         optionally series-split by workspace_name
-- ---------------------------------------------------------------------
SELECT
    DATE(r.period_start_time)                                                     AS run_date,
    CAST(r.workspace_id AS STRING)                                                AS workspace_id,
    COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING))                    AS workspace_name,
    COUNT(*)                                                                      AS total_runs,
    SUM(CASE WHEN r.result_state = 'SUCCEEDED' THEN 1 ELSE 0 END)                 AS succeeded,
    SUM(CASE WHEN r.result_state IN ('FAILED','ERROR','TIMED_OUT') THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN r.result_state IS NULL OR r.result_state = 'RUNNING' THEN 1 ELSE 0 END) AS running,
    ROUND(
        100.0 * SUM(CASE WHEN r.result_state = 'SUCCEEDED' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0)
    , 2)                                                                          AS success_rate_pct
FROM ${catalog}.${schema}.job_runs_latest r
LEFT JOIN ${catalog}.${schema}.synced_workspaces w
       ON r.workspace_id = w.workspace_id
WHERE r.period_start_time >= dateadd(DAY, -:lookback_days, current_date())
GROUP BY DATE(r.period_start_time), r.workspace_id, w.workspace_name
ORDER BY run_date, workspace_name;


-- ---------------------------------------------------------------------
-- 1.3  job_runs_by_type  — trigger / run type mix, per workspace
-- Was: GET /api/jobs/by-type
-- Widget: donut, or stacked bar split by workspace_name
-- ---------------------------------------------------------------------
SELECT
    CAST(r.workspace_id AS STRING)                             AS workspace_id,
    COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
    COALESCE(r.run_type, 'UNKNOWN')                            AS run_type,
    COALESCE(r.trigger_type, 'UNKNOWN')                        AS trigger_type,
    COUNT(*)                                                   AS run_count
FROM ${catalog}.${schema}.job_runs_latest r
LEFT JOIN ${catalog}.${schema}.synced_workspaces w
       ON r.workspace_id = w.workspace_id
WHERE r.period_start_time >= dateadd(DAY, -:lookback_days, current_date())
GROUP BY r.workspace_id, w.workspace_name,
         COALESCE(r.run_type, 'UNKNOWN'), COALESCE(r.trigger_type, 'UNKNOWN')
ORDER BY run_count DESC;


-- ---------------------------------------------------------------------
-- 1.4  job_runs_list  — run-level detail table
-- Was: GET /api/jobs/runs
-- Widget: table. Add field filters on workspace_name, result_state and
--         job_name; the app's free-text search becomes the table
--         widget's built-in column search.
--
-- !! At 30 days across all workspaces this is the one dataset likely to
-- hit the 10k cap. If it truncates, either narrow :lookback_days or
-- drive the table from a selected row in 1.5 / 2.1 instead of listing
-- every run.
-- ---------------------------------------------------------------------
SELECT
    CAST(r.workspace_id AS STRING)             AS workspace_id,
    COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
    CAST(r.job_id AS BIGINT)                   AS job_id,
    COALESCE(j.name, CAST(r.job_id AS STRING)) AS job_name,
    CAST(r.run_id AS BIGINT)                   AS run_id,
    COALESCE(r.result_state, 'RUNNING')        AS result_state,
    r.run_type,
    r.trigger_type,
    r.termination_code,
    r.period_start_time                        AS start_time,
    r.period_end_time                          AS end_time,
    r.execution_duration_seconds,
    ROUND(r.execution_duration_seconds / 60.0, 2) AS execution_duration_minutes,
    r.queue_duration_seconds,
    r.setup_duration_seconds,
    j.creator_user_name,
    CONCAT(REGEXP_REPLACE(w.workspace_url, '/+$', ''),
           '/jobs/', CAST(r.job_id AS STRING),
           '/runs/', CAST(r.run_id AS STRING)) AS run_url
FROM ${catalog}.${schema}.job_runs_latest r
LEFT JOIN ${catalog}.${schema}.jobs_latest j
       ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
LEFT JOIN ${catalog}.${schema}.synced_workspaces w
       ON r.workspace_id = w.workspace_id
WHERE r.period_start_time >= dateadd(DAY, -:lookback_days, current_date())
ORDER BY r.period_start_time DESC
LIMIT 10000;


-- ---------------------------------------------------------------------
-- 1.5  job_run_matrix  — last N runs per job (status grid)
-- Was: GET /api/jobs/matrix
-- Widget: heatmap, x = run_seq, y = job_label, color = result_state.
--         run_seq 1 = most recent run.
-- job_label disambiguates same-named jobs in different workspaces.
-- ---------------------------------------------------------------------
WITH ranked_runs AS (
    SELECT
        CAST(r.workspace_id AS STRING)             AS workspace_id,
        COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
        CAST(r.job_id AS BIGINT)                   AS job_id,
        COALESCE(j.name, CAST(r.job_id AS STRING)) AS job_name,
        CAST(r.run_id AS BIGINT)                   AS run_id,
        COALESCE(r.result_state, 'RUNNING')        AS result_state,
        r.period_start_time                        AS start_time,
        r.execution_duration_seconds               AS duration_seconds,
        ROW_NUMBER() OVER (
            PARTITION BY r.workspace_id, r.job_id
            ORDER BY r.period_start_time DESC
        )                                          AS run_seq
    FROM ${catalog}.${schema}.job_runs_latest r
    LEFT JOIN ${catalog}.${schema}.jobs_latest j
           ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
    LEFT JOIN ${catalog}.${schema}.synced_workspaces w
           ON r.workspace_id = w.workspace_id
    WHERE r.period_start_time >= dateadd(DAY, -:lookback_days, current_date())
)
SELECT
    workspace_id,
    workspace_name,
    job_id,
    job_name,
    CONCAT(job_name, ' @ ', workspace_name) AS job_label,
    run_id,
    result_state,
    start_time,
    duration_seconds,
    run_seq
FROM ranked_runs
WHERE run_seq <= :runs_per_job
ORDER BY workspace_name, job_name, run_seq;


-- ---------------------------------------------------------------------
-- 1.6  job_concurrency  — concurrent runs over time, per workspace
-- Was: GET /api/jobs/gantt + /api/jobs/concurrent
-- Widget: area/line, x = bucket, y = SUM(concurrent_runs),
--         series split by workspace_name
--
-- Bucket width is inlined as a CASE rather than pulled from a scalar
-- subquery: sequence()'s step argument must be a foldable expression,
-- and a subquery there fails to resolve.
--
-- Uses :lookback_hours (not :lookback_days) — a 30-day window at
-- 15-minute buckets is ~2,900 buckets per workspace, which makes an
-- unreadable chart and a slow query. Keep this view short.
-- ---------------------------------------------------------------------
WITH time_series AS (
    SELECT explode(sequence(
        date_trunc('HOUR', current_timestamp() - make_dt_interval(0, :lookback_hours)),
        current_timestamp(),
        make_dt_interval(0, 0, CASE
            WHEN :lookback_hours <= 6  THEN 5
            WHEN :lookback_hours <= 12 THEN 10
            WHEN :lookback_hours <= 24 THEN 15
            WHEN :lookback_hours <= 48 THEN 30
            ELSE 60
        END)
    )) AS bucket
)
SELECT
    ts.bucket,
    CAST(r.workspace_id AS STRING)                                                  AS workspace_id,
    COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING))                      AS workspace_name,
    COUNT(*)                                                                        AS concurrent_runs,
    COUNT(CASE WHEN r.result_state = 'SUCCEEDED' THEN 1 END)                        AS succeeded,
    COUNT(CASE WHEN r.result_state IN ('FAILED','ERROR','TIMED_OUT') THEN 1 END)    AS failed,
    COUNT(CASE WHEN r.result_state IS NULL OR r.result_state = 'RUNNING' THEN 1 END) AS running,
    COUNT(DISTINCT r.job_id)                                                        AS distinct_jobs
FROM time_series ts
JOIN ${catalog}.${schema}.job_runs_latest r
  ON ts.bucket >= r.period_start_time
 AND ts.bucket <= COALESCE(r.period_end_time, current_timestamp())
LEFT JOIN ${catalog}.${schema}.synced_workspaces w
       ON r.workspace_id = w.workspace_id
WHERE r.period_start_time <= current_timestamp()
  AND COALESCE(r.period_end_time, current_timestamp())
      >= current_timestamp() - make_dt_interval(0, :lookback_hours)
GROUP BY ts.bucket, r.workspace_id, w.workspace_name
ORDER BY ts.bucket, workspace_name;


-- ---------------------------------------------------------------------
-- 1.7  job_gantt  — per-job run spans for the timeline view
-- Was: GET /api/jobs/gantt (per-job buckets)
-- Widget: no native Gantt in Lakeview. Two workable options:
--   (a) table sorted by start_time with a duration bar column, or
--   (b) scatter/strip: x = start_time, y = job_label, size = duration.
-- Capped at 200 (workspace, job) pairs, matching the app's limit.
-- ---------------------------------------------------------------------
WITH windowed AS (
    SELECT
        r.workspace_id                             AS _ws,
        CAST(r.workspace_id AS STRING)             AS workspace_id,
        COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
        CAST(r.job_id AS BIGINT)                   AS job_id,
        COALESCE(j.name, CAST(r.job_id AS STRING)) AS job_name,
        CAST(r.run_id AS BIGINT)                   AS run_id,
        COALESCE(r.result_state, 'RUNNING')        AS result_state,
        r.period_start_time,
        COALESCE(r.period_end_time, current_timestamp()) AS period_end_time,
        r.execution_duration_seconds
    FROM ${catalog}.${schema}.job_runs_latest r
    LEFT JOIN ${catalog}.${schema}.jobs_latest j
           ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
    LEFT JOIN ${catalog}.${schema}.synced_workspaces w
           ON r.workspace_id = w.workspace_id
    WHERE COALESCE(r.period_end_time, current_timestamp())
          >= current_timestamp() - make_dt_interval(0, :lookback_hours)
      AND r.period_start_time <= current_timestamp()
),
top_jobs AS (
    SELECT _ws, job_id, MIN(period_start_time) AS first_start
    FROM windowed
    GROUP BY _ws, job_id
    ORDER BY first_start
    LIMIT 200
)
SELECT
    w.workspace_id,
    w.workspace_name,
    w.job_id,
    w.job_name,
    CONCAT(w.job_name, ' @ ', w.workspace_name) AS job_label,
    w.run_id,
    w.result_state,
    w.period_start_time,
    w.period_end_time,
    w.execution_duration_seconds,
    ROUND(w.execution_duration_seconds / 60.0, 2) AS duration_minutes
FROM windowed w
JOIN top_jobs t ON w._ws = t._ws AND w.job_id = t.job_id
ORDER BY t.first_start, w.period_start_time;


-- ---------------------------------------------------------------------
-- 1.8  job_overlaps  — pairs of runs executing at the same time
-- Was: GET /api/jobs/overlaps
-- Widget: table.
-- Pairing is constrained to runs in the SAME workspace — two runs in
-- different workspaces overlapping in wall-clock time share no compute
-- and no concurrency limit, so the pair is not meaningful.
-- Self-join is O(n^2) in the window — keep :lookback_hours <= 24.
-- ---------------------------------------------------------------------
WITH runs AS (
    SELECT
        r.workspace_id                             AS _ws,
        COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
        CAST(r.job_id AS BIGINT)                   AS job_id,
        COALESCE(j.name, CAST(r.job_id AS STRING)) AS job_name,
        CAST(r.run_id AS BIGINT)                   AS run_id,
        r.period_start_time,
        COALESCE(r.period_end_time, current_timestamp()) AS period_end_time
    FROM ${catalog}.${schema}.job_runs_latest r
    LEFT JOIN ${catalog}.${schema}.jobs_latest j
           ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
    LEFT JOIN ${catalog}.${schema}.synced_workspaces w
           ON r.workspace_id = w.workspace_id
    WHERE r.period_start_time >= current_timestamp() - make_dt_interval(0, :lookback_hours)
)
SELECT
    a.workspace_name,
    CAST(a._ws AS STRING)                                    AS workspace_id,
    a.job_name                                               AS job_name_1,
    a.run_id                                                 AS run_id_1,
    b.job_name                                               AS job_name_2,
    b.run_id                                                 AS run_id_2,
    GREATEST(a.period_start_time, b.period_start_time)       AS overlap_start,
    LEAST(a.period_end_time, b.period_end_time)              AS overlap_end,
    ROUND(
        unix_timestamp(LEAST(a.period_end_time, b.period_end_time))
      - unix_timestamp(GREATEST(a.period_start_time, b.period_start_time))
    , 0)                                                     AS overlap_seconds
FROM runs a
JOIN runs b
  ON a._ws = b._ws
 AND a.run_id < b.run_id
 AND a.period_start_time < b.period_end_time
 AND a.period_end_time   > b.period_start_time
ORDER BY overlap_seconds DESC
LIMIT 500;


-- =====================================================================
-- SECTION 2 — HEALTH   (was: routers/health.py)
-- =====================================================================

-- ---------------------------------------------------------------------
-- 2.1  failing_jobs  — jobs with failures, ranked
-- Was: GET /api/health/failed-jobs
-- Widget: table, conditional-format success_rate_pct red below 80
--
-- COUNTING SEMANTICS — this counts FAILED RUNS, one per run_id, because
-- job_runs_latest is already collapsed to one row per run. The
-- equivalent query in the Databricks docs counts failure EVENTS off raw
-- job_run_timeline, so a run that failed, was repaired and failed again
-- contributes 2 there and 1 here. Neither is wrong; they answer
-- different questions ("how many runs failed" vs "how many failures
-- occurred"). If you compare this dashboard against that query and the
-- numbers differ, this is why. 2.4 is where repair counts live.
-- ---------------------------------------------------------------------
SELECT
    CAST(r.workspace_id AS STRING)             AS workspace_id,
    COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
    CAST(r.job_id AS BIGINT)                   AS job_id,
    COALESCE(j.name, CAST(r.job_id AS STRING)) AS job_name,
    COUNT(*)                                                                        AS total_runs,
    SUM(CASE WHEN r.result_state IN ('FAILED','ERROR','TIMED_OUT') THEN 1 ELSE 0 END) AS failed_runs,
    ROUND(
        100.0 * SUM(CASE WHEN r.result_state = 'SUCCEEDED' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0)
    , 2)                                                                            AS success_rate_pct,
    MAX(CASE WHEN r.result_state IN ('FAILED','ERROR','TIMED_OUT')
             THEN r.period_start_time END)                                          AS last_failure,
    MAX(CASE WHEN r.result_state IN ('FAILED','ERROR','TIMED_OUT')
             THEN r.termination_code END)                                           AS last_termination_code
FROM ${catalog}.${schema}.job_runs_latest r
LEFT JOIN ${catalog}.${schema}.jobs_latest j
       ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
LEFT JOIN ${catalog}.${schema}.synced_workspaces w
       ON r.workspace_id = w.workspace_id
WHERE r.period_start_time >= dateadd(DAY, -:lookback_days, current_date())
GROUP BY r.workspace_id, w.workspace_name, r.job_id, j.name
HAVING COUNT(*) >= :min_runs
   AND SUM(CASE WHEN r.result_state IN ('FAILED','ERROR','TIMED_OUT') THEN 1 ELSE 0 END) > 0
ORDER BY failed_runs DESC, success_rate_pct ASC
LIMIT 200;


-- ---------------------------------------------------------------------
-- 2.2  prolonged_running_jobs  — in-flight runs past a threshold,
--      compared against each job's own historical average
-- Was: GET /api/health/prolonged-jobs
--      (the app returned avg_duration_seconds = 0 — a hardcoded stub;
--       this version computes the real baseline and a ratio)
-- Widget: table. Empty result is the healthy state.
-- Baseline is a fixed 30-day trailing window, independent of
-- :lookback_days, so the comparison stays stable as the view's window
-- changes.
-- ---------------------------------------------------------------------
WITH baseline AS (
    SELECT
        workspace_id,
        job_id,
        AVG(execution_duration_seconds) AS avg_duration_seconds,
        COUNT(*)                        AS baseline_runs
    FROM ${catalog}.${schema}.job_runs_latest
    WHERE result_state = 'SUCCEEDED'
      AND period_start_time >= dateadd(DAY, -30, current_date())
    GROUP BY workspace_id, job_id
),
in_flight AS (
    SELECT
        r.workspace_id                             AS _ws,
        r.job_id                                   AS _job,
        CAST(r.workspace_id AS STRING)             AS workspace_id,
        COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
        CAST(r.job_id AS BIGINT)                   AS job_id,
        COALESCE(j.name, CAST(r.job_id AS STRING)) AS job_name,
        CAST(r.run_id AS BIGINT)                   AS run_id,
        r.period_start_time                        AS start_time,
        unix_timestamp(current_timestamp()) - unix_timestamp(r.period_start_time)
                                                   AS current_duration_seconds,
        CONCAT(REGEXP_REPLACE(w.workspace_url, '/+$', ''),
               '/jobs/', CAST(r.job_id AS STRING),
               '/runs/', CAST(r.run_id AS STRING)) AS run_url
    FROM ${catalog}.${schema}.job_runs_latest r
    LEFT JOIN ${catalog}.${schema}.jobs_latest j
           ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
    LEFT JOIN ${catalog}.${schema}.synced_workspaces w
           ON r.workspace_id = w.workspace_id
    WHERE r.result_state IS NULL
      AND r.period_start_time >= dateadd(DAY, -:lookback_days, current_date())
)
SELECT
    f.workspace_id,
    f.workspace_name,
    f.job_id,
    f.job_name,
    f.run_id,
    f.start_time,
    f.current_duration_seconds,
    ROUND(f.current_duration_seconds / 3600.0, 2)  AS current_duration_hours,
    ROUND(b.avg_duration_seconds, 0)               AS avg_duration_seconds,
    ROUND(f.current_duration_seconds / NULLIF(b.avg_duration_seconds, 0), 2) AS vs_avg_ratio,
    CASE
        WHEN b.avg_duration_seconds IS NULL                            THEN 'no_baseline'
        WHEN f.current_duration_seconds > b.avg_duration_seconds * 3   THEN 'critical'
        WHEN f.current_duration_seconds > b.avg_duration_seconds * 1.5 THEN 'warning'
        ELSE 'normal'
    END                                            AS status,
    f.run_url
FROM in_flight f
LEFT JOIN baseline b
       ON f._ws = b.workspace_id AND f._job = b.job_id
WHERE f.current_duration_seconds > :threshold_hours * 3600
ORDER BY f.current_duration_seconds DESC
LIMIT 200;


-- ---------------------------------------------------------------------
-- 2.3  duration_anomalies  — z-score of recent duration vs baseline
-- Was: GET /api/health/anomalies
-- Widget: table, or scatter x = z_score, y = job_label
--
-- !! :baseline_days MUST be greater than :lookback_days.
-- The baseline window is [-:baseline_days, -:lookback_days), so with
-- both at 30 it collapses to zero rows and the dataset returns empty.
-- With :lookback_days = 30, set :baseline_days to 90 — that compares
-- the last 30 days against the 60 days before them.
-- ---------------------------------------------------------------------
WITH recent_stats AS (
    SELECT
        r.workspace_id,
        r.job_id,
        COALESCE(j.name, CAST(r.job_id AS STRING)) AS job_name,
        COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
        AVG(r.execution_duration_seconds)          AS recent_avg_duration,
        COUNT(*)                                   AS recent_runs
    FROM ${catalog}.${schema}.job_runs_latest r
    LEFT JOIN ${catalog}.${schema}.jobs_latest j
           ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
    LEFT JOIN ${catalog}.${schema}.synced_workspaces w
           ON r.workspace_id = w.workspace_id
    WHERE r.period_start_time >= dateadd(DAY, -:lookback_days, current_date())
      AND r.result_state IN ('SUCCEEDED','FAILED','ERROR','TIMED_OUT')
    GROUP BY r.workspace_id, r.job_id, j.name, w.workspace_name
),
baseline_stats AS (
    SELECT
        workspace_id,
        job_id,
        AVG(execution_duration_seconds)    AS baseline_avg_duration,
        STDDEV(execution_duration_seconds) AS baseline_stddev_duration,
        COUNT(*)                           AS baseline_runs
    FROM ${catalog}.${schema}.job_runs_latest
    WHERE period_start_time >= dateadd(DAY, -:baseline_days, current_date())
      AND period_start_time <  dateadd(DAY, -:lookback_days, current_date())
      AND result_state IN ('SUCCEEDED','FAILED','ERROR','TIMED_OUT')
    GROUP BY workspace_id, job_id
    HAVING COUNT(*) >= 5
)
SELECT
    CAST(r.workspace_id AS STRING)           AS workspace_id,
    r.workspace_name,
    CAST(r.job_id AS BIGINT)                 AS job_id,
    r.job_name,
    CONCAT(r.job_name, ' @ ', r.workspace_name) AS job_label,
    'duration'                               AS metric,
    ROUND(r.recent_avg_duration, 2)          AS current_value,
    ROUND(b.baseline_avg_duration, 2)        AS baseline_value,
    ROUND(b.baseline_stddev_duration, 2)     AS std_dev,
    ROUND((r.recent_avg_duration - b.baseline_avg_duration)
          / b.baseline_stddev_duration, 2)   AS z_score,
    ROUND(100.0 * (r.recent_avg_duration - b.baseline_avg_duration)
          / NULLIF(b.baseline_avg_duration, 0), 1) AS pct_change,
    r.recent_runs,
    b.baseline_runs,
    CASE WHEN ABS((r.recent_avg_duration - b.baseline_avg_duration)
                  / b.baseline_stddev_duration) >= 3.0
         THEN 'critical' ELSE 'warning' END  AS severity
FROM recent_stats r
JOIN baseline_stats b
  ON r.workspace_id = b.workspace_id AND r.job_id = b.job_id
WHERE b.baseline_stddev_duration > 0
  AND ABS((r.recent_avg_duration - b.baseline_avg_duration)
          / b.baseline_stddev_duration) >= :z_threshold
ORDER BY ABS((r.recent_avg_duration - b.baseline_avg_duration)
             / b.baseline_stddev_duration) DESC
LIMIT 200;


-- ---------------------------------------------------------------------
-- 2.4  repair_stats  — TRUE repair/retry counts per job
-- Was: GET /api/health/retry-stats
--
-- REWRITTEN. The app's version — and my first port of it — used a
-- failed-then-succeeded proxy at JOB level, on the stated belief that
-- job_run_timeline has no attempt counter. That belief is wrong, and
-- the proxy was measuring something else entirely.
--
-- HOW REPAIRS ARE ACTUALLY DETECTED:
--   job_run_timeline emits one row per reporting PERIOD, and a repaired
--   run gets an ADDITIONAL row with a terminal result_state. So for a
--   single run_id:
--       COUNT(*) WHERE result_state IS NOT NULL  >  1   =>  repaired
--       repairs = that count - 1
--   This is the mechanism the Databricks docs use, and it is a true
--   per-run attempt count rather than a proxy.
--
-- HOW REPAIRS ARE DETECTED (now baked into the MV):
--   job_run_timeline emits one row per reporting PERIOD; a repaired run
--   gets ADDITIONAL terminal rows. job_runs_latest counts those before
--   collapsing and exposes the result as repair_count (= terminal
--   attempts - 1). So this dataset is a plain roll-up of that column —
--   no raw timeline needed.
--
-- repaired_but_still_failing uses result_state, which is the max_by
-- FINAL state, so a repaired-then-succeeded run is correctly excluded.
--
-- Widget: table, sorted by total_repairs DESC. runs_with_repairs vs
--         total_runs is the headline ratio.
-- ---------------------------------------------------------------------
SELECT
    CAST(r.workspace_id AS STRING)             AS workspace_id,
    COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
    CAST(r.job_id AS BIGINT)                   AS job_id,
    COALESCE(j.name, CAST(r.job_id AS STRING)) AS job_name,
    COUNT(*)                                   AS total_runs,
    SUM(CASE WHEN r.repair_count > 0 THEN 1 ELSE 0 END) AS runs_with_repairs,
    SUM(r.repair_count)                        AS total_repairs,
    MAX(r.repair_count)                        AS max_repairs_on_one_run,
    ROUND(100.0 * SUM(CASE WHEN r.repair_count > 0 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)            AS repair_rate_pct,
    -- repaired runs whose FINAL state still was not success
    SUM(CASE WHEN r.repair_count > 0 AND r.result_state <> 'SUCCEEDED' THEN 1 ELSE 0 END)
                                               AS repaired_but_still_failing,
    MAX(r.period_end_time)                     AS last_activity
FROM ${catalog}.${schema}.job_runs_latest r
LEFT JOIN ${catalog}.${schema}.jobs_latest j
       ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
LEFT JOIN ${catalog}.${schema}.synced_workspaces w
       ON r.workspace_id = w.workspace_id
WHERE r.period_start_time >= dateadd(DAY, -:lookback_days, current_date())
GROUP BY r.workspace_id, w.workspace_name, r.job_id, j.name
HAVING SUM(r.repair_count) > 0
ORDER BY total_repairs DESC
LIMIT 200;


-- ---------------------------------------------------------------------
-- 2.5  sla_status  — runs exceeding N x the job's own average duration
-- Was: GET /api/health/sla-status
-- This is a self-referential SLA (no declared targets exist in the
-- system tables). If you have real SLA targets, replace
-- job_stats.avg_duration with a lookup table of per-job target_seconds
-- keyed on (workspace_id, job_id).
-- Widget: table + a counter for jobs at non_compliant
-- ---------------------------------------------------------------------
WITH job_durations AS (
    SELECT
        r.workspace_id                             AS _ws,
        r.job_id                                   AS _job,
        COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
        COALESCE(j.name, CAST(r.job_id AS STRING)) AS job_name,
        r.execution_duration_seconds               AS duration_seconds
    FROM ${catalog}.${schema}.job_runs_latest r
    LEFT JOIN ${catalog}.${schema}.jobs_latest j
           ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
    LEFT JOIN ${catalog}.${schema}.synced_workspaces w
           ON r.workspace_id = w.workspace_id
    WHERE r.period_start_time >= dateadd(DAY, -:lookback_days, current_date())
      AND r.result_state = 'SUCCEEDED'
),
job_stats AS (
    SELECT _ws, _job, workspace_name, job_name,
           COUNT(*) AS total_runs, AVG(duration_seconds) AS avg_duration
    FROM job_durations
    GROUP BY _ws, _job, workspace_name, job_name
),
sla_violations AS (
    SELECT d._ws, d._job, COUNT(*) AS violation_count
    FROM job_durations d
    JOIN job_stats s ON d._ws = s._ws AND d._job = s._job
    WHERE d.duration_seconds > s.avg_duration * :sla_multiplier
    GROUP BY d._ws, d._job
)
SELECT
    CAST(s._ws AS STRING)                     AS workspace_id,
    s.workspace_name,
    CAST(s._job AS BIGINT)                    AS job_id,
    s.job_name,
    s.total_runs,
    ROUND(s.avg_duration, 2)                  AS avg_duration_seconds,
    COALESCE(v.violation_count, 0)            AS sla_violations,
    ROUND(100.0 * (s.total_runs - COALESCE(v.violation_count, 0)) / s.total_runs, 2)
                                              AS compliance_rate_pct,
    CASE
        WHEN 100.0 * (s.total_runs - COALESCE(v.violation_count, 0)) / s.total_runs >= 95 THEN 'compliant'
        WHEN 100.0 * (s.total_runs - COALESCE(v.violation_count, 0)) / s.total_runs >= 80 THEN 'at_risk'
        ELSE 'non_compliant'
    END                                       AS status
FROM job_stats s
LEFT JOIN sla_violations v ON s._ws = v._ws AND s._job = v._job
ORDER BY sla_violations DESC, s.total_runs DESC
LIMIT 200;


-- ---------------------------------------------------------------------
-- 2.6  duration_percentiles  — p50/p90/p95/p99 per job
-- Was: GET /api/health/duration-percentiles
-- Widget: table, or grouped bar comparing p50 vs p95 per job.
-- p95_p50_spread is the useful sort key — a high spread means the job's
-- runtime is erratic, which matters more than it simply being slow.
-- ---------------------------------------------------------------------
SELECT
    CAST(r.workspace_id AS STRING)             AS workspace_id,
    COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
    CAST(r.job_id AS BIGINT)                   AS job_id,
    COALESCE(j.name, CAST(r.job_id AS STRING)) AS job_name,
    COUNT(*)                                   AS run_count,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY r.execution_duration_seconds), 2) AS p50_seconds,
    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY r.execution_duration_seconds), 2) AS p90_seconds,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY r.execution_duration_seconds), 2) AS p95_seconds,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY r.execution_duration_seconds), 2) AS p99_seconds,
    ROUND(MAX(r.execution_duration_seconds), 2) AS max_seconds,
    ROUND(
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY r.execution_duration_seconds)
        / NULLIF(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY r.execution_duration_seconds), 0)
    , 2)                                        AS p95_p50_spread
FROM ${catalog}.${schema}.job_runs_latest r
LEFT JOIN ${catalog}.${schema}.jobs_latest j
       ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
LEFT JOIN ${catalog}.${schema}.synced_workspaces w
       ON r.workspace_id = w.workspace_id
WHERE r.period_start_time >= dateadd(DAY, -:lookback_days, current_date())
  AND r.result_state = 'SUCCEEDED'
GROUP BY r.workspace_id, w.workspace_name, r.job_id, j.name
HAVING COUNT(*) >= 5
ORDER BY p50_seconds DESC
LIMIT 200;


-- =====================================================================
-- =====================================================================
-- SECTION 3 — COST   (REMOVED — covered by an existing dashboard)
-- =====================================================================
-- Cost is deliberately NOT in this dashboard. Spend by job, and spend
-- by workspace, are already covered by separate existing dashboards.
-- Duplicating them here would mean two places to maintain and two
-- numbers to reconcile when they disagree.
--
-- What was removed (ported from routers/costs.py, then dropped):
--   cost_summary, cost_daily, cost_top_jobs, cost_top_runs,
--   cost_by_sku, cost_by_workspace
--
-- This dashboard is OPERATIONAL observability: did it run, did it
-- succeed, how long did it take, which task broke. Not spend.
--
-- Two consequences worth knowing:
--
--   1. billing_usage_enriched is no longer read by this dashboard at
--      all. The only sources now are job_runs_latest, jobs_latest,
--      synced_workspaces and the system.lakeflow task tables. One
--      fewer MV to maintain on this side — the SDP pipeline still
--      builds it, presumably for the cost dashboard.
--
--   2. The :lookback_days <= 90 ceiling is GONE. It came from
--      billing_usage_enriched's 90-day filter. Sections 1, 2 and 4 read
--      job_runs_latest, which has no retention filter, so the window is
--      now bounded only by how far back
--      system.lakeflow.job_run_timeline goes in this workspace.
--
-- IF YOU RECONSIDER ONE THING, make it per-RUN spend (was 3.4). A
-- job-grain cost dashboard may stop at "job X cost $N this month" and
-- not expose "run 12345 cost $N". Per-run cost is what lets you say a
-- single run was both slow AND expensive, which is the one place cost
-- genuinely belongs in an operational view. Ask whether the existing
-- dashboard goes to run grain before ruling it out.
-- =====================================================================


-- SECTION 4 — TASK-LEVEL STATUS   (NEW — no equivalent in the app)
-- =====================================================================
-- Per-task outcomes for every job run, scoped by workspace.
-- Answers "which STEP failed", not just "the job failed".
--
-- SOURCE MVs (materialized_views.sql, Section C):
--   ${catalog}.${schema}.task_runs_latest   one row per task run
--   ${catalog}.${schema}.job_tasks_latest   latest task definition
--
-- The per-period collapse, max_by(result_state) fix and repair_count all
-- live in task_runs_latest, so the datasets below are plain reads of it —
-- no inline aggregation of the raw timeline.
--
-- !! VERIFY BEFORE CREATING THE MVs — ESPECIALLY ON AZURE !!
-- The two MVs read system.lakeflow.job_task_run_timeline and job_tasks.
-- System table publication varies by cloud and release, so confirm they
-- exist before creating the MVs:
--   SHOW TABLES IN system.lakeflow;
--   DESCRIBE system.lakeflow.job_task_run_timeline;
--   DESCRIBE system.lakeflow.job_tasks;
-- If either is absent, skip Section C of the MV file and this whole
-- section; Sections 1-2 stand on their own. The MV file notes the two
-- column caveats (parent_run_id, depends_on_keys).
--
-- !! GRAIN: (workspace_id, job_id, task_run_id, task_key) !!
-- task_key is unique only WITHIN a job, job_id only within a workspace.
-- Same principle as the rest of this file: aggregate on IDs, filter on
-- names. parent_run_id links a task run back to job_runs_latest.run_id.
--
-- SIZING NOTE: task_runs_latest is rows_per_job x tasks_per_job — a
-- 10-task job is 10x job_runs_latest. It is the largest MV in the set;
-- size the warehouse and refresh schedule accordingly.
--
-- SCOPE: NO SPEND AT TASK GRAIN — decided, not an omission. Billing
-- attributes usage to job_id / job_run_id, never task_key. Do not add
-- cost columns here.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 4.1  task_runs_list  — status of every task, per job run
-- THE CORE DATASET. One row per task run.
-- Widget: table. Pair it with 1.4 job_runs_list — filter 1.4 to a job
--         run, and this shows that run's task breakdown. task_seq
--         orders tasks by when they started, so the table reads as the
--         execution sequence.
-- ---------------------------------------------------------------------
SELECT
    CAST(t.workspace_id AS STRING)             AS workspace_id,
    COALESCE(w.workspace_name, CAST(t.workspace_id AS STRING)) AS workspace_name,
    CAST(t.job_id AS BIGINT)                   AS job_id,
    COALESCE(j.name, CAST(t.job_id AS STRING)) AS job_name,
    CAST(t.parent_run_id AS BIGINT)            AS job_run_id,
    CAST(t.task_run_id AS BIGINT)              AS task_run_id,
    t.task_key                                 AS task_name,
    COALESCE(t.result_state, 'RUNNING')        AS result_state,
    t.termination_code,
    t.repair_count,
    t.period_start_time                        AS start_time,
    t.period_end_time                          AS end_time,
    t.execution_duration_seconds,
    ROUND(t.execution_duration_seconds / 60.0, 2) AS duration_minutes,
    t.queue_duration_seconds,
    ROW_NUMBER() OVER (
        PARTITION BY t.workspace_id, t.job_id, t.parent_run_id
        ORDER BY t.period_start_time
    )                                          AS task_seq,
    ROUND(100.0 * t.execution_duration_seconds / NULLIF(SUM(t.execution_duration_seconds) OVER (
        PARTITION BY t.workspace_id, t.job_id, t.parent_run_id
    ), 0), 1)                                  AS pct_of_run_duration,
    CONCAT(REGEXP_REPLACE(w.workspace_url, '/+$', ''),
           '/jobs/', CAST(t.job_id AS STRING),
           '/runs/', CAST(t.parent_run_id AS STRING)) AS job_run_url
FROM ${catalog}.${schema}.task_runs_latest t
LEFT JOIN ${catalog}.${schema}.jobs_latest j
       ON t.job_id = j.job_id AND t.workspace_id = j.workspace_id
LEFT JOIN ${catalog}.${schema}.synced_workspaces w
       ON t.workspace_id = w.workspace_id
WHERE t.period_start_time >= dateadd(DAY, -:lookback_days, current_date())
ORDER BY t.period_start_time DESC, task_seq
LIMIT 10000;


-- ---------------------------------------------------------------------
-- 4.2  task_failure_breakdown  — which tasks actually break
-- The companion to 2.1 failing_jobs: 2.1 says a job fails 40% of the
-- time, this says which step is responsible.
-- Widget: table, conditional-format success_rate_pct red below 80.
--         Sort by failed_runs to get the worst offenders first.
-- ---------------------------------------------------------------------
SELECT
    CAST(t.workspace_id AS STRING)             AS workspace_id,
    COALESCE(w.workspace_name, CAST(t.workspace_id AS STRING)) AS workspace_name,
    CAST(t.job_id AS BIGINT)                   AS job_id,
    COALESCE(j.name, CAST(t.job_id AS STRING)) AS job_name,
    t.task_key                                 AS task_name,
    CONCAT(COALESCE(j.name, CAST(t.job_id AS STRING)), ' / ', t.task_key) AS task_label,
    COUNT(*)                                                                        AS total_runs,
    SUM(CASE WHEN t.result_state IN ('FAILED','ERROR','TIMED_OUT') THEN 1 ELSE 0 END) AS failed_runs,
    SUM(CASE WHEN t.result_state = 'SUCCEEDED' THEN 1 ELSE 0 END)                   AS succeeded_runs,
    SUM(CASE WHEN t.result_state = 'SKIPPED' THEN 1 ELSE 0 END)                     AS skipped_runs,
    SUM(t.repair_count)                                                             AS total_repairs,
    ROUND(
        100.0 * SUM(CASE WHEN t.result_state = 'SUCCEEDED' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0)
    , 2)                                                                            AS success_rate_pct,
    MAX(CASE WHEN t.result_state IN ('FAILED','ERROR','TIMED_OUT')
             THEN t.period_start_time END)                                          AS last_failure,
    MAX(CASE WHEN t.result_state IN ('FAILED','ERROR','TIMED_OUT')
             THEN t.termination_code END)                                           AS last_termination_code
FROM ${catalog}.${schema}.task_runs_latest t
LEFT JOIN ${catalog}.${schema}.jobs_latest j
       ON t.job_id = j.job_id AND t.workspace_id = j.workspace_id
LEFT JOIN ${catalog}.${schema}.synced_workspaces w
       ON t.workspace_id = w.workspace_id
WHERE t.period_start_time >= dateadd(DAY, -:lookback_days, current_date())
GROUP BY t.workspace_id, w.workspace_name, t.job_id, j.name, t.task_key
HAVING COUNT(*) >= :min_runs
   AND SUM(CASE WHEN t.result_state IN ('FAILED','ERROR','TIMED_OUT') THEN 1 ELSE 0 END) > 0
ORDER BY failed_runs DESC, success_rate_pct ASC
LIMIT 200;


-- ---------------------------------------------------------------------
-- 4.3  task_duration_profile  — p50/p95 per task + bottleneck share
-- The companion to 2.6: 2.6 says a job takes 90 min at p95, this says
-- which task owns 70 of them.
-- Widget: table sorted by avg_pct_of_run DESC — that column is the
--         bottleneck ranking. Or stacked bar of avg duration per task
--         within a job.
-- ---------------------------------------------------------------------
WITH with_share AS (
    SELECT
        workspace_id,
        job_id,
        parent_run_id,
        task_key,
        execution_duration_seconds,
        100.0 * execution_duration_seconds / NULLIF(SUM(execution_duration_seconds) OVER (
            PARTITION BY workspace_id, job_id, parent_run_id
        ), 0) AS pct_of_run
    FROM ${catalog}.${schema}.task_runs_latest
    WHERE result_state = 'SUCCEEDED'
      AND period_start_time >= dateadd(DAY, -:lookback_days, current_date())
)
SELECT
    CAST(t.workspace_id AS STRING)             AS workspace_id,
    COALESCE(w.workspace_name, CAST(t.workspace_id AS STRING)) AS workspace_name,
    CAST(t.job_id AS BIGINT)                   AS job_id,
    COALESCE(j.name, CAST(t.job_id AS STRING)) AS job_name,
    t.task_key                                 AS task_name,
    CONCAT(COALESCE(j.name, CAST(t.job_id AS STRING)), ' / ', t.task_key) AS task_label,
    COUNT(*)                                   AS run_count,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY t.execution_duration_seconds), 2) AS p50_seconds,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY t.execution_duration_seconds), 2) AS p95_seconds,
    ROUND(MAX(t.execution_duration_seconds), 2) AS max_seconds,
    ROUND(AVG(t.pct_of_run), 1)                AS avg_pct_of_run,
    ROUND(
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY t.execution_duration_seconds)
        / NULLIF(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY t.execution_duration_seconds), 0)
    , 2)                                        AS p95_p50_spread
FROM with_share t
LEFT JOIN ${catalog}.${schema}.jobs_latest j
       ON t.job_id = j.job_id AND t.workspace_id = j.workspace_id
LEFT JOIN ${catalog}.${schema}.synced_workspaces w
       ON t.workspace_id = w.workspace_id
GROUP BY t.workspace_id, w.workspace_name, t.job_id, j.name, t.task_key
HAVING COUNT(*) >= 5
ORDER BY avg_pct_of_run DESC
LIMIT 200;


-- ---------------------------------------------------------------------
-- 4.4  task_inventory  — task definitions per job
-- SCD1 latest row per (workspace, job, task), same pattern as
-- jobs_latest. Useful for spotting tasks that exist but never ran in
-- the window — they appear here and in none of 4.1-4.3.
-- Widget: table
-- !! depends_on_keys is the least certain column here (VERIFY 3).
--    If job_tasks_latest was created without it, drop the two lines
--    that reference it; the rest stands.
-- ---------------------------------------------------------------------
SELECT
    CAST(t.workspace_id AS STRING)             AS workspace_id,
    COALESCE(w.workspace_name, CAST(t.workspace_id AS STRING)) AS workspace_name,
    CAST(t.job_id AS BIGINT)                   AS job_id,
    COALESCE(j.name, CAST(t.job_id AS STRING)) AS job_name,
    t.task_key                                 AS task_name,
    t.depends_on_keys,
    CASE WHEN t.depends_on_keys IS NULL OR size(t.depends_on_keys) = 0
         THEN TRUE ELSE FALSE END              AS is_entry_task,
    t.change_time                              AS last_changed
FROM ${catalog}.${schema}.job_tasks_latest t
LEFT JOIN ${catalog}.${schema}.jobs_latest j
       ON t.job_id = j.job_id AND t.workspace_id = j.workspace_id
LEFT JOIN ${catalog}.${schema}.synced_workspaces w
       ON t.workspace_id = w.workspace_id
WHERE t.delete_time IS NULL
ORDER BY workspace_name, job_name, task_name;


-- =====================================================================
-- SECTION 5 — PIPELINES   (DEFERRED — out of scope for this release)
-- =====================================================================
-- Pipeline observability is deliberately NOT in this release.
--
-- What it would have required, for whoever picks this up:
--   1. system.lakeflow.pipelines         -> pipeline inventory
--   2. system.lakeflow.job_tasks         -> which job tasks ARE pipelines
--   3. system.lakeflow.job_task_run_timeline -> per-task outcomes
--   4. billing usage on usage_metadata.dlt_pipeline_id -> pipeline cost
--      (NOTE: billing_usage_enriched currently filters to
--       `usage_metadata.job_id IS NOT NULL`, which drops every pipeline
--       usage row. Pipeline cost needs that filter relaxed or a parallel
--       view — an ingestion change, not just a dashboard query.)
--   5. the pipeline event log, published to UC per pipeline, for
--      standalone/continuous pipeline update outcomes — which exist in
--      no system table today.
--
-- WHAT THIS RELEASE DOES AND DOES NOT SHOW
--
-- A job whose task happens to be a pipeline appears in Sections 1-3
-- like any other job: its runs, status, duration and cost are all
-- there, under the JOB's name.
--
-- But that is job observability, not pipeline observability. Two limits
-- to be clear about:
--
--   a) GRAIN IS THE JOB, NOT THE TASK. job_run_timeline carries one row
--      per job run, with the job's overall result_state. If the job's
--      only task is the pipeline, that status is effectively the
--      pipeline's. If the job has several tasks, a FAILED job does not
--      tell you the pipeline was the cause. Per-task status lives in
--      system.lakeflow.job_task_run_timeline, which this repo does not
--      ingest.
--
--   b) PIPELINE RUNS ARE NOT IDENTIFIABLE AS SUCH. Nothing in
--      job_run_timeline marks a run as having involved a pipeline. That
--      mapping lives in system.lakeflow.job_tasks, also not ingested.
--      So these runs cannot be filtered, counted or labelled as
--      pipeline runs — only as the jobs they are.
--
-- Net: pipeline-backed jobs are COVERED as jobs, and INVISIBLE as
-- pipelines. Don't describe this dashboard as covering pipelines.
-- =====================================================================


-- SECTION 6 — FILTER SOURCE DATASETS
-- =====================================================================
-- Only needed if you drive filters from a dedicated dataset rather than
-- binding a filter widget directly to the workspace_name / job_name
-- field on the datasets above. Field binding is simpler and keeps the
-- filter in sync with what the data actually contains — prefer it.
--
-- 6.1 is still useful as a reference list of active workspaces, which
-- surfaces workspaces that have no job activity in the window at all
-- (they appear here but in none of the datasets above).
-- =====================================================================

-- 6.1  workspace_reference
SELECT
    CAST(workspace_id AS STRING) AS workspace_id,
    workspace_name,
    workspace_url,
    status
FROM ${catalog}.${schema}.synced_workspaces
WHERE status = 'RUNNING'
ORDER BY workspace_name;

-- 6.2  job_reference
SELECT
    CAST(j.workspace_id AS STRING) AS workspace_id,
    COALESCE(w.workspace_name, CAST(j.workspace_id AS STRING)) AS workspace_name,
    CAST(j.job_id AS STRING)       AS job_id,
    j.name                         AS job_name,
    CONCAT(j.name, ' @ ', COALESCE(w.workspace_name, CAST(j.workspace_id AS STRING))) AS job_label,
    j.creator_user_name,
    j.run_as
FROM ${catalog}.${schema}.jobs_latest j
LEFT JOIN ${catalog}.${schema}.synced_workspaces w
       ON j.workspace_id = w.workspace_id
WHERE j.delete_time IS NULL
ORDER BY workspace_name, job_name;
