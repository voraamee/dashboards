#!/usr/bin/env python3
"""
Generate the Unified Job Platform observability dashboard as a .lvdash.json.

Reads from the 5 MVs created by materialized_views.sql. Parameter defaults
from dashboard_datasets.sql are inlined as literals (30-day window, etc.);
workspace_name / job_name / result_state are wired as global filter widgets.

Output: unified_job_platform.lvdash.json
  - import via the Databricks UI (Dashboards > New > import), or
  - deploy via the Lakeview API (see the printed command).

Edit CATALOG / SCHEMA below before running.
"""
import json
import uuid

CATALOG = "main"
SCHEMA = "cost_management"
T = f"{CATALOG}.{SCHEMA}"

# Default lookback window (days). Inlined because MV-backed Lakeview
# datasets here use literals, not :params.
D = 30
BASELINE_D = 90
HOURS = 24

def wid(prefix): return f"{prefix}-{uuid.uuid4().hex[:8]}"

datasets = []
def ds(name, sql):
    datasets.append({"name": name, "displayName": name,
                     "queryLines": [sql if sql.endswith(" ") else sql + " "]})
    return name

# ---------------------------------------------------------------- datasets
ds("job_run_summary", f"""
SELECT CAST(r.workspace_id AS STRING) AS workspace_id,
  COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
  r.result_state,
  COUNT(*) AS total_runs,
  SUM(CASE WHEN r.result_state='SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
  SUM(CASE WHEN r.result_state IN ('FAILED','ERROR','TIMED_OUT') THEN 1 ELSE 0 END) AS failed,
  SUM(CASE WHEN r.result_state IS NULL OR r.result_state='RUNNING' THEN 1 ELSE 0 END) AS running
FROM {T}.job_runs_latest r
LEFT JOIN {T}.synced_workspaces w ON r.workspace_id=w.workspace_id
WHERE r.period_start_time >= date_sub(current_date(), {D})
GROUP BY r.workspace_id, w.workspace_name, r.result_state """)

ds("job_runs_daily", f"""
SELECT DATE(r.period_start_time) AS run_date,
  COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
  COUNT(*) AS total_runs,
  SUM(CASE WHEN r.result_state='SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
  SUM(CASE WHEN r.result_state IN ('FAILED','ERROR','TIMED_OUT') THEN 1 ELSE 0 END) AS failed
FROM {T}.job_runs_latest r
LEFT JOIN {T}.synced_workspaces w ON r.workspace_id=w.workspace_id
WHERE r.period_start_time >= date_sub(current_date(), {D})
GROUP BY DATE(r.period_start_time), w.workspace_name """)

ds("job_runs_by_type", f"""
SELECT COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
  COALESCE(r.run_type,'UNKNOWN') AS run_type, COUNT(*) AS run_count
FROM {T}.job_runs_latest r
LEFT JOIN {T}.synced_workspaces w ON r.workspace_id=w.workspace_id
WHERE r.period_start_time >= date_sub(current_date(), {D})
GROUP BY w.workspace_name, COALESCE(r.run_type,'UNKNOWN') """)

ds("job_runs_list", f"""
SELECT CAST(r.workspace_id AS STRING) AS workspace_id,
  COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
  CAST(r.job_id AS BIGINT) AS job_id,
  COALESCE(j.name, CAST(r.job_id AS STRING)) AS job_name,
  CAST(r.run_id AS BIGINT) AS run_id,
  COALESCE(r.result_state,'RUNNING') AS result_state,
  r.run_type, r.trigger_type, r.termination_code, r.repair_count,
  r.period_start_time AS start_time, r.period_end_time AS end_time,
  ROUND(r.execution_duration_seconds/60.0,2) AS duration_minutes,
  j.creator_user_name,
  CONCAT(REGEXP_REPLACE(w.workspace_url,'/+$',''),'/jobs/',CAST(r.job_id AS STRING),'/runs/',CAST(r.run_id AS STRING)) AS run_url
FROM {T}.job_runs_latest r
LEFT JOIN {T}.jobs_latest j ON r.job_id=j.job_id AND r.workspace_id=j.workspace_id
LEFT JOIN {T}.synced_workspaces w ON r.workspace_id=w.workspace_id
WHERE r.period_start_time >= date_sub(current_date(), {D})
ORDER BY r.period_start_time DESC LIMIT 10000 """)

ds("job_concurrency", f"""
WITH time_series AS (
  SELECT explode(sequence(date_trunc('HOUR', current_timestamp() - make_dt_interval(0,{HOURS})),
    current_timestamp(), make_dt_interval(0,0,15))) AS bucket)
SELECT ts.bucket,
  COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
  COUNT(*) AS concurrent_runs
FROM time_series ts
JOIN {T}.job_runs_latest r ON ts.bucket >= r.period_start_time
  AND ts.bucket <= COALESCE(r.period_end_time, current_timestamp())
LEFT JOIN {T}.synced_workspaces w ON r.workspace_id=w.workspace_id
WHERE r.period_start_time <= current_timestamp()
  AND COALESCE(r.period_end_time, current_timestamp()) >= current_timestamp() - make_dt_interval(0,{HOURS})
GROUP BY ts.bucket, w.workspace_name """)

ds("failing_jobs", f"""
SELECT COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
  CAST(r.job_id AS BIGINT) AS job_id,
  COALESCE(j.name, CAST(r.job_id AS STRING)) AS job_name,
  CONCAT(COALESCE(j.name, CAST(r.job_id AS STRING)),' @ ',COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING))) AS job_label,
  COUNT(*) AS total_runs,
  SUM(CASE WHEN r.result_state IN ('FAILED','ERROR','TIMED_OUT') THEN 1 ELSE 0 END) AS failed_runs,
  ROUND(100.0*SUM(CASE WHEN r.result_state='SUCCEEDED' THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),2) AS success_rate_pct,
  MAX(CASE WHEN r.result_state IN ('FAILED','ERROR','TIMED_OUT') THEN r.period_start_time END) AS last_failure
FROM {T}.job_runs_latest r
LEFT JOIN {T}.jobs_latest j ON r.job_id=j.job_id AND r.workspace_id=j.workspace_id
LEFT JOIN {T}.synced_workspaces w ON r.workspace_id=w.workspace_id
WHERE r.period_start_time >= date_sub(current_date(), {D})
GROUP BY w.workspace_name, r.job_id, j.name, r.workspace_id
HAVING COUNT(*) >= 3 AND SUM(CASE WHEN r.result_state IN ('FAILED','ERROR','TIMED_OUT') THEN 1 ELSE 0 END) > 0
ORDER BY failed_runs DESC LIMIT 100 """)

ds("prolonged_jobs", f"""
WITH baseline AS (
  SELECT workspace_id, job_id, AVG(execution_duration_seconds) AS avg_dur
  FROM {T}.job_runs_latest WHERE result_state='SUCCEEDED'
    AND period_start_time >= date_sub(current_date(),30)
  GROUP BY workspace_id, job_id)
SELECT COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
  CAST(r.job_id AS BIGINT) AS job_id,
  COALESCE(j.name, CAST(r.job_id AS STRING)) AS job_name,
  CAST(r.run_id AS BIGINT) AS run_id, r.period_start_time AS start_time,
  ROUND((unix_timestamp(current_timestamp())-unix_timestamp(r.period_start_time))/3600.0,2) AS current_hours,
  ROUND(b.avg_dur,0) AS avg_duration_seconds,
  ROUND((unix_timestamp(current_timestamp())-unix_timestamp(r.period_start_time))/NULLIF(b.avg_dur,0),2) AS vs_avg_ratio
FROM {T}.job_runs_latest r
LEFT JOIN {T}.jobs_latest j ON r.job_id=j.job_id AND r.workspace_id=j.workspace_id
LEFT JOIN {T}.synced_workspaces w ON r.workspace_id=w.workspace_id
LEFT JOIN baseline b ON r.workspace_id=b.workspace_id AND r.job_id=b.job_id
WHERE r.result_state IS NULL AND r.period_start_time >= date_sub(current_date(),7)
  AND (unix_timestamp(current_timestamp())-unix_timestamp(r.period_start_time)) > 2*3600
ORDER BY current_hours DESC LIMIT 100 """)

ds("repair_stats", f"""
SELECT COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
  CAST(r.job_id AS BIGINT) AS job_id,
  COALESCE(j.name, CAST(r.job_id AS STRING)) AS job_name,
  COUNT(*) AS total_runs,
  SUM(CASE WHEN r.repair_count>0 THEN 1 ELSE 0 END) AS runs_with_repairs,
  SUM(r.repair_count) AS total_repairs, MAX(r.repair_count) AS max_repairs_on_one_run,
  ROUND(100.0*SUM(CASE WHEN r.repair_count>0 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),2) AS repair_rate_pct,
  SUM(CASE WHEN r.repair_count>0 AND r.result_state<>'SUCCEEDED' THEN 1 ELSE 0 END) AS repaired_but_still_failing
FROM {T}.job_runs_latest r
LEFT JOIN {T}.jobs_latest j ON r.job_id=j.job_id AND r.workspace_id=j.workspace_id
LEFT JOIN {T}.synced_workspaces w ON r.workspace_id=w.workspace_id
WHERE r.period_start_time >= date_sub(current_date(), {D})
GROUP BY w.workspace_name, r.job_id, j.name
HAVING SUM(r.repair_count) > 0
ORDER BY total_repairs DESC LIMIT 100 """)

ds("duration_percentiles", f"""
SELECT COALESCE(w.workspace_name, CAST(r.workspace_id AS STRING)) AS workspace_name,
  CAST(r.job_id AS BIGINT) AS job_id,
  COALESCE(j.name, CAST(r.job_id AS STRING)) AS job_name,
  COUNT(*) AS run_count,
  ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY r.execution_duration_seconds),2) AS p50_seconds,
  ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY r.execution_duration_seconds),2) AS p95_seconds,
  ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY r.execution_duration_seconds),2) AS p99_seconds
FROM {T}.job_runs_latest r
LEFT JOIN {T}.jobs_latest j ON r.job_id=j.job_id AND r.workspace_id=j.workspace_id
LEFT JOIN {T}.synced_workspaces w ON r.workspace_id=w.workspace_id
WHERE r.period_start_time >= date_sub(current_date(), {D}) AND r.result_state='SUCCEEDED'
GROUP BY w.workspace_name, r.job_id, j.name
HAVING COUNT(*) >= 5 ORDER BY p50_seconds DESC LIMIT 100 """)

ds("task_runs_list", f"""
SELECT COALESCE(w.workspace_name, CAST(t.workspace_id AS STRING)) AS workspace_name,
  CAST(t.job_id AS BIGINT) AS job_id,
  COALESCE(j.name, CAST(t.job_id AS STRING)) AS job_name,
  CAST(t.parent_run_id AS BIGINT) AS job_run_id, t.task_key AS task_name,
  COALESCE(t.result_state,'RUNNING') AS result_state, t.repair_count,
  t.period_start_time AS start_time,
  ROUND(t.execution_duration_seconds/60.0,2) AS duration_minutes
FROM {T}.task_runs_latest t
LEFT JOIN {T}.jobs_latest j ON t.job_id=j.job_id AND t.workspace_id=j.workspace_id
LEFT JOIN {T}.synced_workspaces w ON t.workspace_id=w.workspace_id
WHERE t.period_start_time >= date_sub(current_date(), {D})
ORDER BY t.period_start_time DESC LIMIT 10000 """)

ds("task_failures", f"""
SELECT COALESCE(w.workspace_name, CAST(t.workspace_id AS STRING)) AS workspace_name,
  CONCAT(COALESCE(j.name, CAST(t.job_id AS STRING)),' / ',t.task_key) AS task_label,
  COUNT(*) AS total_runs,
  SUM(CASE WHEN t.result_state IN ('FAILED','ERROR','TIMED_OUT') THEN 1 ELSE 0 END) AS failed_runs,
  ROUND(100.0*SUM(CASE WHEN t.result_state='SUCCEEDED' THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),2) AS success_rate_pct
FROM {T}.task_runs_latest t
LEFT JOIN {T}.jobs_latest j ON t.job_id=j.job_id AND t.workspace_id=j.workspace_id
LEFT JOIN {T}.synced_workspaces w ON t.workspace_id=w.workspace_id
WHERE t.period_start_time >= date_sub(current_date(), {D})
GROUP BY w.workspace_name, j.name, t.job_id, t.task_key
HAVING COUNT(*) >= 3 AND SUM(CASE WHEN t.result_state IN ('FAILED','ERROR','TIMED_OUT') THEN 1 ELSE 0 END) > 0
ORDER BY failed_runs DESC LIMIT 100 """)

# ---------------------------------------------------------------- widgets
def text(name, md, x, y, w, h):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": [md]}},
            "position": {"x": x, "y": y, "width": w, "height": h}}

def counter(name, dataset, field, title, x, y):
    nm = f"sum({field})"
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {
        "datasetName": dataset, "fields": [{"name": nm, "expression": f"SUM(`{field}`)"}],
        "disaggregated": False}}],
        "spec": {"version": 2, "widgetType": "counter",
                 "encodings": {"value": {"fieldName": nm, "displayName": title}},
                 "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": 2, "height": 3}}

def table(name, dataset, cols, title, x, y, w, h):
    fields = [{"name": c, "expression": f"`{c}`"} for c, _ in cols]
    enc = [{"fieldName": c, "displayName": d} for c, d in cols]
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {
        "datasetName": dataset, "fields": fields, "disaggregated": True}}],
        "spec": {"version": 2, "widgetType": "table", "encodings": {"columns": enc},
                 "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

def bar(name, dataset, xf, yf, yexpr, title, x, y, w, h, sort_desc=True, color=None):
    fields = [{"name": xf, "expression": f"`{xf}`"}, {"name": yf, "expression": yexpr}]
    xscale = {"type": "categorical"}
    if sort_desc: xscale["sort"] = {"by": "y-reversed"}
    enc = {"x": {"fieldName": xf, "scale": xscale, "displayName": xf},
           "y": {"fieldName": yf, "scale": {"type": "quantitative"}, "displayName": title},
           "label": {"show": True}}
    if color:
        fields.append({"name": color, "expression": f"`{color}`"})
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color}
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {
        "datasetName": dataset, "fields": fields, "disaggregated": False}}],
        "spec": {"version": 3, "widgetType": "bar", "encodings": enc,
                 "frame": {"showTitle": True, "title": title},
                 "mark": {"colors": ["#FF3621", "#FFAB00", "#00A972", "#8BCAE7"]}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

def line(name, dataset, xf, xexpr, yfields, title, x, y, w, h, color=None, temporal=True):
    fields = [{"name": xf, "expression": xexpr}]
    for n, e in yfields: fields.append({"name": n, "expression": e})
    enc = {"x": {"fieldName": xf, "scale": {"type": "temporal" if temporal else "categorical"}, "displayName": xf}}
    if len(yfields) == 1 and not color:
        enc["y"] = {"fieldName": yfields[0][0], "scale": {"type": "quantitative"}, "displayName": title}
    elif color:
        enc["y"] = {"fieldName": yfields[0][0], "scale": {"type": "quantitative"}, "displayName": title}
    else:
        enc["y"] = {"scale": {"type": "quantitative"},
                    "fields": [{"fieldName": n, "displayName": n} for n, _ in yfields]}
    if color:
        fields.append({"name": color, "expression": f"`{color}`"})
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color}
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {
        "datasetName": dataset, "fields": fields, "disaggregated": False}}],
        "spec": {"version": 3, "widgetType": "line", "encodings": enc,
                 "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

def pie(name, dataset, cat, valexpr, valname, title, x, y, w, h):
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {
        "datasetName": dataset, "fields": [{"name": cat, "expression": f"`{cat}`"},
            {"name": valname, "expression": valexpr}], "disaggregated": False}}],
        "spec": {"version": 3, "widgetType": "pie",
                 "encodings": {"angle": {"fieldName": valname, "scale": {"type": "quantitative"}, "displayName": title},
                               "color": {"fieldName": cat, "scale": {"type": "categorical"}, "displayName": cat}},
                 "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

def filt(name, dataset, field, title, wtype, x, y):
    qn = f"{dataset}_{field}"
    return {"widget": {"name": name, "queries": [{"name": qn, "query": {
        "datasetName": dataset, "fields": [{"name": field, "expression": f"`{field}`"}],
        "disaggregated": False}}],
        "spec": {"version": 2, "widgetType": wtype,
                 "encodings": {"fields": [{"fieldName": field, "displayName": title, "queryName": qn}]},
                 "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": 2, "height": 2}}

# ---------------------------------------------------------------- pages
overview = {"name": wid("page"), "displayName": "Overview", "pageType": "PAGE_TYPE_CANVAS", "layout": [
    text("ov-title", "## Unified Job Platform — Operational Observability", 0, 0, 6, 1),
    text("ov-sub", "Job and task run health across all workspaces. Filter by workspace, job or status. 30-day window.", 0, 1, 6, 1),
    counter("ov-total", "job_run_summary", "total_runs", "Total Runs", 0, 2),
    counter("ov-succ", "job_run_summary", "succeeded", "Succeeded", 2, 2),
    counter("ov-fail", "job_run_summary", "failed", "Failed", 4, 2),
    text("ov-trend-h", "### Daily run volume", 0, 5, 6, 1),
    line("ov-daily", "job_runs_daily", "run_date", "`run_date`",
         [("sum(succeeded)", "SUM(`succeeded`)"), ("sum(failed)", "SUM(`failed`)")],
         "Runs by day", 0, 6, 4, 5),
    pie("ov-type", "job_runs_by_type", "run_type", "SUM(`run_count`)", "sum(run_count)", "Run type mix", 4, 6, 2, 5),
]}

jobs = {"name": wid("page"), "displayName": "Jobs", "pageType": "PAGE_TYPE_CANVAS", "layout": [
    text("jb-h", "### Concurrent runs (last 24h)", 0, 0, 6, 1),
    line("jb-conc", "job_concurrency", "bucket", "`bucket`",
         [("sum(concurrent_runs)", "SUM(`concurrent_runs`)")], "Concurrent runs", 0, 1, 6, 5,
         color="workspace_name"),
    text("jb-th", "### Recent job runs", 0, 6, 6, 1),
    table("jb-runs", "job_runs_list", [
        ("workspace_name", "Workspace"), ("job_name", "Job"), ("run_id", "Run"),
        ("result_state", "Status"), ("run_type", "Type"), ("start_time", "Start"),
        ("duration_minutes", "Duration (min)"), ("repair_count", "Repairs"),
        ("termination_code", "Term code"), ("creator_user_name", "Creator"), ("run_url", "Link")],
        "Job runs", 0, 7, 6, 8),
]}

health = {"name": wid("page"), "displayName": "Health", "pageType": "PAGE_TYPE_CANVAS", "layout": [
    text("hl-h1", "### Jobs by failure count", 0, 0, 6, 1),
    bar("hl-fail-bar", "failing_jobs", "job_label", "sum(failed_runs)", "SUM(`failed_runs`)",
        "Failing jobs", 0, 1, 6, 5),
    text("hl-h2", "### Failing jobs detail", 0, 6, 6, 1),
    table("hl-fail", "failing_jobs", [
        ("workspace_name", "Workspace"), ("job_name", "Job"), ("total_runs", "Runs"),
        ("failed_runs", "Failed"), ("success_rate_pct", "Success %"), ("last_failure", "Last failure")],
        "Failing jobs", 0, 7, 3, 6),
    table("hl-repair", "repair_stats", [
        ("workspace_name", "Workspace"), ("job_name", "Job"), ("total_runs", "Runs"),
        ("runs_with_repairs", "Repaired runs"), ("total_repairs", "Repairs"),
        ("repair_rate_pct", "Repair %"), ("repaired_but_still_failing", "Still failing")],
        "Repairs", 3, 7, 3, 6),
    text("hl-h3", "### Long-running & slow jobs", 0, 13, 6, 1),
    table("hl-prolong", "prolonged_jobs", [
        ("workspace_name", "Workspace"), ("job_name", "Job"), ("run_id", "Run"),
        ("start_time", "Started"), ("current_hours", "Running (h)"),
        ("avg_duration_seconds", "Avg (s)"), ("vs_avg_ratio", "x Avg")],
        "In-flight past threshold", 0, 14, 3, 6),
    table("hl-pct", "duration_percentiles", [
        ("workspace_name", "Workspace"), ("job_name", "Job"), ("run_count", "Runs"),
        ("p50_seconds", "p50 (s)"), ("p95_seconds", "p95 (s)"), ("p99_seconds", "p99 (s)")],
        "Duration percentiles", 3, 14, 3, 6),
]}

tasks = {"name": wid("page"), "displayName": "Tasks", "pageType": "PAGE_TYPE_CANVAS", "layout": [
    text("tk-note", "### Task-level status  —  requires the task MVs (materialized_views.sql Section C). If those were not created, these widgets will error; remove this page.", 0, 0, 6, 1),
    bar("tk-fail-bar", "task_failures", "task_label", "sum(failed_runs)", "SUM(`failed_runs`)",
        "Tasks by failure count", 0, 1, 6, 5),
    text("tk-h", "### Task runs", 0, 6, 6, 1),
    table("tk-runs", "task_runs_list", [
        ("workspace_name", "Workspace"), ("job_name", "Job"), ("job_run_id", "Job run"),
        ("task_name", "Task"), ("result_state", "Status"), ("repair_count", "Repairs"),
        ("start_time", "Start"), ("duration_minutes", "Duration (min)")],
        "Task runs", 0, 7, 6, 8),
]}

global_filters = {"name": wid("page"), "displayName": "Filters", "pageType": "PAGE_TYPE_GLOBAL_FILTERS", "layout": [
    filt("f-ws", "job_runs_list", "workspace_name", "Workspace", "filter-multi-select", 0, 0),
    filt("f-job", "job_runs_list", "job_name", "Job", "filter-multi-select", 2, 0),
    filt("f-state", "job_runs_list", "result_state", "Status", "filter-multi-select", 4, 0),
]}

dashboard = {"datasets": datasets,
             "pages": [overview, jobs, health, tasks, global_filters],
             "uiSettings": {"theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"}}}

out = "unified_job_platform.lvdash.json"
with open(out, "w") as f:
    json.dump(dashboard, f, indent=2)

print(f"Wrote {out}")
print(f"  datasets: {len(datasets)}  pages: {len(dashboard['pages'])}")
print(f"  catalog.schema: {T}")
print()
print("Deploy via API (fill in <profile>, <warehouse_id>, <email>):")
print(f"  databricks api post /api/2.0/lakeview/dashboards --profile <profile> --json \"$(jq -n \\")
print(f"    --arg sd \"$(cat {out})\" \\")
print("    '{display_name:\"Unified Job Platform\", warehouse_id:\"<warehouse_id>\",")
print("      parent_path:\"/Users/<email>\", serialized_dashboard:$sd}')\"")
