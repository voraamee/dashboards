-- =====================================================================
-- Unified Job Platform — Materialized Views
-- =====================================================================
-- Standalone CREATE MATERIALIZED VIEW DDL for the job/task observability
-- dashboard. Run these once in the customer's Unity Catalog; the
-- dashboard datasets in dashboard_datasets.sql read from them.
--
-- These are self-contained: they read Databricks system tables directly
-- and do NOT depend on the repo's DLT/SDP pipeline. You are creating and
-- owning them, so there is nothing from that repo to deploy.
--
-- Both correctness fixes we discussed are baked in:
--   1. max_by(result_state, period_end_time) — the FINAL period's
--      outcome, not the alphabetical MAX. (A repaired run that went
--      TIMED_OUT then SUCCEEDED now reads SUCCEEDED.)
--   2. repair_count — computed BEFORE the per-run collapse, so repairs
--      survive. This is why 2.4 can read job_runs_latest directly and
--      no longer needs the raw timeline.
--
-- PREREQUISITES
--   - Unity Catalog, with the target catalog.schema already created.
--   - A serverless or pro SQL warehouse (MVs require it).
--   - SELECT on the system schemas these read:
--       system.lakeflow.jobs
--       system.lakeflow.job_run_timeline
--       system.access.workspaces_latest
--       system.lakeflow.job_task_run_timeline   (task MVs only)
--       system.lakeflow.job_tasks               (task MVs only)
--     Grant with: GRANT SELECT ON SCHEMA system.lakeflow TO `<you>`;
--     system.access is enabled per-metastore by an account admin.
--
-- REPLACE ${catalog}.${schema} with your real values before running
-- (sed -i '' 's/${catalog}/main/g; s/${schema}/cost_management/g' this file,
-- or set them in your SQL editor).
--
-- REFRESH: each MV below carries SCHEDULE EVERY 1 HOUR. Drop that clause
-- to refresh manually (REFRESH MATERIALIZED VIEW ...). Hourly matches
-- how fresh job-monitoring data needs to be; tighten if you want.
--
-- !! AZURE — VERIFY FIRST !!
-- Section C (task-level) reads system.lakeflow.job_task_run_timeline and
-- system.lakeflow.job_tasks. Confirm they exist on this workspace before
-- running Section C:
--     SHOW TABLES IN system.lakeflow;
-- If absent, skip Section C and the task-level dashboard page; A and B
-- stand on their own.
-- =====================================================================


-- =====================================================================
-- SECTION A — CORE JOB MVs  (required)
-- =====================================================================

-- ---------------------------------------------------------------------
-- A.1  synced_workspaces  — workspace id -> name / url / status
-- A straight pass-through of the already-latest system view. Kept as an
-- MV so the dashboard joins stay inside the catalog.schema and do not
-- each hit system.access at query time.
-- ---------------------------------------------------------------------
CREATE OR REPLACE MATERIALIZED VIEW ${catalog}.${schema}.synced_workspaces
SCHEDULE EVERY 1 HOUR
AS
SELECT
    workspace_id,
    workspace_name,
    workspace_url,
    status
FROM system.access.workspaces_latest;


-- ---------------------------------------------------------------------
-- A.2  jobs_latest  — latest definition per job (SCD1)
-- One row per (workspace_id, job_id): the most recent change row.
-- ---------------------------------------------------------------------
CREATE OR REPLACE MATERIALIZED VIEW ${catalog}.${schema}.jobs_latest
SCHEDULE EVERY 1 HOUR
AS
SELECT * EXCEPT (_rn)
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY workspace_id, job_id
            ORDER BY change_time DESC
        ) AS _rn
    FROM system.lakeflow.jobs
)
WHERE _rn = 1;


-- ---------------------------------------------------------------------
-- A.3  job_runs_latest  — ONE ROW PER JOB RUN  (the central table)
--
-- system.lakeflow.job_run_timeline emits one row per reporting PERIOD,
-- so a run spans several rows and a repaired run gets extra terminal
-- rows. This collapses to one row per run and, in the same pass:
--   - takes the FINAL period's result_state / termination_code via
--     max_by (not alphabetical MAX)
--   - counts repairs BEFORE collapsing, exposing them as repair_count
--   - reproduces the pipeline's duration logic: summed execution
--     seconds if present, else end-minus-start.
--
-- Because repair_count lives here, dataset 2.4 reads this MV and the raw
-- timeline is NOT needed as a separate MV.
-- ---------------------------------------------------------------------
CREATE OR REPLACE MATERIALIZED VIEW ${catalog}.${schema}.job_runs_latest
SCHEDULE EVERY 1 HOUR
AS
SELECT
    account_id,
    workspace_id,
    job_id,
    run_id,
    MIN(period_start_time)                      AS period_start_time,
    MAX(period_end_time)                        AS period_end_time,
    -- final period wins, not the lexically-largest string
    max_by(result_state, period_end_time)       AS result_state,
    max_by(termination_code, period_end_time)   AS termination_code,
    max_by(run_name, period_end_time)           AS run_name,
    max_by(run_type, period_end_time)           AS run_type,
    max_by(trigger_type, period_end_time)       AS trigger_type,
    -- repair detection, computed before the collapse:
    -- a clean run has 1 terminal row; each extra terminal row is a repair
    COUNT(*) FILTER (WHERE result_state IS NOT NULL)              AS terminal_attempts,
    GREATEST(COUNT(*) FILTER (WHERE result_state IS NOT NULL) - 1, 0) AS repair_count,
    SUM(run_duration_seconds)                   AS run_duration_seconds,
    COALESCE(
        NULLIF(SUM(execution_duration_seconds), 0),
        unix_timestamp(MAX(period_end_time)) - unix_timestamp(MIN(period_start_time))
    )                                           AS execution_duration_seconds,
    SUM(setup_duration_seconds)                 AS setup_duration_seconds,
    SUM(queue_duration_seconds)                 AS queue_duration_seconds,
    SUM(cleanup_duration_seconds)               AS cleanup_duration_seconds
FROM system.lakeflow.job_run_timeline
GROUP BY account_id, workspace_id, job_id, run_id;


-- =====================================================================
-- SECTION B — (reserved)
-- Cost MVs intentionally omitted — spend is covered by a separate
-- existing dashboard. See dashboard_datasets.sql Section 3.
-- =====================================================================


-- =====================================================================
-- SECTION C — TASK-LEVEL MVs  (optional — VERIFY source tables first)
-- Only create these if SHOW TABLES IN system.lakeflow lists
-- job_task_run_timeline and job_tasks on this workspace.
-- =====================================================================

-- ---------------------------------------------------------------------
-- C.1  task_runs_latest  — ONE ROW PER TASK RUN
-- Same collapse + max_by + repair logic as job_runs_latest, one level
-- down. parent_run_id is the JOB run id (links back to
-- job_runs_latest.run_id); task_run_id is the task's own run id.
--
-- !! VERIFY: that parent_run_id exists and carries the job run id. If it
-- does not in this release, drop it from the GROUP BY and the SELECT and
-- join tasks to runs on (workspace_id, job_id, run_id) instead.
-- ---------------------------------------------------------------------
CREATE OR REPLACE MATERIALIZED VIEW ${catalog}.${schema}.task_runs_latest
SCHEDULE EVERY 1 HOUR
AS
SELECT
    workspace_id,
    job_id,
    parent_run_id,                              -- job run id
    run_id                                      AS task_run_id,
    task_key,
    MIN(period_start_time)                      AS period_start_time,
    MAX(period_end_time)                        AS period_end_time,
    max_by(result_state, period_end_time)       AS result_state,
    max_by(termination_code, period_end_time)   AS termination_code,
    GREATEST(COUNT(*) FILTER (WHERE result_state IS NOT NULL) - 1, 0) AS repair_count,
    SUM(execution_duration_seconds)             AS execution_duration_seconds,
    SUM(queue_duration_seconds)                 AS queue_duration_seconds
FROM system.lakeflow.job_task_run_timeline
GROUP BY workspace_id, job_id, parent_run_id, run_id, task_key;


-- ---------------------------------------------------------------------
-- C.2  job_tasks_latest  — latest task definition per (job, task) SCD1
-- Backs dataset 4.4 (task inventory).
-- !! VERIFY: depends_on_keys may not exist in every release — drop that
-- column if DESCRIBE does not show it.
-- ---------------------------------------------------------------------
CREATE OR REPLACE MATERIALIZED VIEW ${catalog}.${schema}.job_tasks_latest
SCHEDULE EVERY 1 HOUR
AS
SELECT * EXCEPT (_rn)
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY workspace_id, job_id, task_key
            ORDER BY change_time DESC
        ) AS _rn
    FROM system.lakeflow.job_tasks
)
WHERE _rn = 1;


-- =====================================================================
-- POST-CREATE CHECKS
-- =====================================================================
-- 1. Repair mechanism holds (expect mostly terminal_attempts = 1, a
--    smaller tail at 2+). If everything is 1, no run was repaired in
--    range; if a big spike at high counts, tighten the FILTER in A.3/C.1
--    to result_state IN ('SUCCEEDED','FAILED','ERROR','TIMED_OUT','CANCELLED').
--      SELECT terminal_attempts, COUNT(*)
--      FROM ${catalog}.${schema}.job_runs_latest
--      GROUP BY terminal_attempts ORDER BY terminal_attempts;
--
-- 2. The max_by fix is live (TIMED_OUT no longer masks a later success).
--    Spot-check a known repaired run against the Jobs UI.
--
-- 3. Row counts look sane:
--      SELECT COUNT(*) FROM ${catalog}.${schema}.job_runs_latest;
--      SELECT COUNT(*) FROM ${catalog}.${schema}.task_runs_latest;
-- =====================================================================
