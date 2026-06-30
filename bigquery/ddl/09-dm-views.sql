-- ----------------------------------------------------------------------------
-- 09-dm-views: 13 regular views + 3 materialized views = 16 objects
-- Migrated from Hive/Impala dm layer on nbcs-cdh-prod (CDH 6.3.4).
--
-- Hand-translated Impala/Hive SQL to BigQuery Standard SQL:
--   NDV()            -> APPROX_COUNT_DISTINCT()
--   GROUPING__ID     -> GROUPING(col1)*2 + GROUPING(col2)
--   WITH ROLLUP      -> GROUP BY ROLLUP(...)
--   RLIKE            -> REGEXP_CONTAINS()
--   regexp_extract() -> REGEXP_EXTRACT()
--   unix_timestamp() -> UNIX_SECONDS()
--   from_unixtime()  -> TIMESTAMP_SECONDS() / TIMESTAMP_MILLIS()
--   date_add(ts, n)  -> TIMESTAMP_ADD(ts, INTERVAL n DAY)
--   CAST(x AS BIGINT)-> CAST(x AS INT64)
--
-- All table references are unqualified (harness default-dataset redirection).
-- Cross-dataset references (staging/ods/dm) all land in one build dataset.
-- Materialized views: vw_agent_scorecard, vw_csat_rollup, mv_agg_queue_hourly.
-- ----------------------------------------------------------------------------

-- 1. Org hierarchy (recursive CTE — BQ native support)
CREATE VIEW IF NOT EXISTS vw_org_hierarchy AS
WITH RECURSIVE org_tree AS (
  SELECT o.org_unit_id, o.unit_code, o.unit_name, o.unit_type,
         o.site_code, o.org_unit_id AS root_unit_id, 0 AS depth,
         o.unit_name AS path_names
  FROM   ods_org_unit o
  WHERE  o.parent_unit_id IS NULL
  UNION ALL
  SELECT c.org_unit_id, c.unit_code, c.unit_name, c.unit_type,
         c.site_code, p.root_unit_id, p.depth + 1,
         CONCAT(p.path_names, ' > ', c.unit_name)
  FROM   ods_org_unit c
  JOIN   org_tree p ON c.parent_unit_id = p.org_unit_id
  WHERE  p.depth < 6
)
SELECT org_unit_id, unit_code, unit_name, unit_type, site_code,
       root_unit_id, depth, path_names
FROM   org_tree;

-- 2. Active-agent panel (NDV -> APPROX_COUNT_DISTINCT)
CREATE VIEW IF NOT EXISTS vw_active_agents_ndv AS
SELECT f.date_key,
       a.site_code,
       APPROX_COUNT_DISTINCT(f.agent_sk)                                     AS approx_active_agents,
       APPROX_COUNT_DISTINCT(CONCAT(CAST(f.agent_sk AS STRING), '|', f.channel)) AS approx_agent_channel_pairs,
       COUNT(*)                                                              AS interactions
FROM   fact_interaction f
JOIN   dim_agent a ON a.agent_sk = f.agent_sk
GROUP  BY f.date_key, a.site_code;

-- 3. CSAT rollup — MATERIALIZED VIEW
-- BQ MVs do not support ROLLUP/GROUPING or scalar transforms on aggregates.
-- Raw aggregates materialized; ROLLUP + derived metrics layered via query.
CREATE MATERIALIZED VIEW IF NOT EXISTS vw_csat_rollup AS
SELECT p.client_id,
       p.program_code,
       COUNT(*)                                           AS surveys,
       SUM(s.csat_score)                                  AS sum_csat,
       SUM(CASE WHEN s.nps_score >= 9 THEN 1 ELSE 0 END) AS promoter_count
FROM   fact_csat_survey s
JOIN   dim_program p ON p.program_sk = s.program_sk
GROUP  BY p.client_id, p.program_code;

-- 4. Call-driver classification (RLIKE -> REGEXP_CONTAINS, regexp_extract -> REGEXP_EXTRACT)
CREATE VIEW IF NOT EXISTS vw_call_driver_regex AS
SELECT c.call_date,
       c.queue_id,
       CASE
         WHEN REGEXP_CONTAINS(d.disposition_desc, r'(?i)(bill|invoice|charge|refund)')  THEN 'BILLING'
         WHEN REGEXP_CONTAINS(d.disposition_desc, r'(?i)(password|login|locked|reset)')  THEN 'ACCESS'
         WHEN REGEXP_CONTAINS(d.disposition_desc, r'(?i)(cancel|churn|retention)')       THEN 'RETENTION'
         WHEN REGEXP_EXTRACT(d.disposition_desc, r'^\[([A-Z]{2,5})\]') IS NOT NULL
              THEN REGEXP_EXTRACT(d.disposition_desc, r'^\[([A-Z]{2,5})\]')
         ELSE 'OTHER'
       END                                              AS call_driver,
       REGEXP_EXTRACT(d.disposition_desc, r'ref#(\d+)') AS embedded_ref_no,
       COUNT(*)                                         AS calls,
       AVG(c.talk_seconds)                              AS avg_talk_seconds
FROM   ods_call c
JOIN   dim_disposition d ON d.disposition_code = c.disposition_code
GROUP  BY c.call_date, c.queue_id,
       CASE
         WHEN REGEXP_CONTAINS(d.disposition_desc, r'(?i)(bill|invoice|charge|refund)')  THEN 'BILLING'
         WHEN REGEXP_CONTAINS(d.disposition_desc, r'(?i)(password|login|locked|reset)')  THEN 'ACCESS'
         WHEN REGEXP_CONTAINS(d.disposition_desc, r'(?i)(cancel|churn|retention)')       THEN 'RETENTION'
         WHEN REGEXP_EXTRACT(d.disposition_desc, r'^\[([A-Z]{2,5})\]') IS NOT NULL
              THEN REGEXP_EXTRACT(d.disposition_desc, r'^\[([A-Z]{2,5})\]')
         ELSE 'OTHER'
       END,
       REGEXP_EXTRACT(d.disposition_desc, r'ref#(\d+)');

-- 5. Repeat contacts within 72h (unix_timestamp -> UNIX_SECONDS)
CREATE VIEW IF NOT EXISTS vw_repeat_contact_window AS
SELECT i.interaction_id,
       i.customer_ref,
       i.channel,
       i.start_ts,
       LAG(i.start_ts) OVER (PARTITION BY i.customer_ref ORDER BY i.start_ts) AS prev_contact_ts,
       CASE WHEN UNIX_SECONDS(i.start_ts)
               - UNIX_SECONDS(LAG(i.start_ts) OVER (PARTITION BY i.customer_ref
                                                     ORDER BY i.start_ts)) <= 259200
            THEN 1 ELSE 0 END                            AS repeat_within_72h
FROM   ods_interaction i
WHERE  i.customer_ref IS NOT NULL AND i.customer_ref <> '';

-- 6. Billing reconciliation (from_unixtime/unix_timestamp -> TIMESTAMP_SECONDS/UNIX_SECONDS;
--    CAST(x/1000 AS BIGINT) -> CAST(x/1000 AS INT64))
-- Cross-dataset: staging.stg_fin_invoice + ods.ods_invoice_acid (both unqualified)
CREATE VIEW IF NOT EXISTS vw_billing_reconciliation AS
SELECT s.invoice_no,
       s.total_amount                                    AS staged_amount,
       a.total_amount                                    AS ods_amount,
       TIMESTAMP_SECONDS(CAST(s.issued_ts_sec / 1000 AS INT64)) AS staged_issued_ts,
       a.issued_ts                                       AS ods_issued_ts,
       (UNIX_SECONDS(a.issued_ts) - CAST(s.issued_ts_sec / 1000 AS INT64)) AS drift_seconds,
       CASE WHEN ABS(s.total_amount - a.total_amount) > 0.01 THEN 'AMOUNT_MISMATCH'
            WHEN UNIX_SECONDS(a.issued_ts) <> CAST(s.issued_ts_sec / 1000 AS INT64) THEN 'TS_MISMATCH'
            ELSE 'OK' END                                AS recon_status
FROM   stg_fin_invoice s
JOIN   ods_invoice_acid a ON a.invoice_id = s.invoice_id;

-- 7. Current agent roster (row_number — identical BQ syntax)
CREATE VIEW IF NOT EXISTS vw_agent_roster_current AS
SELECT latest.agent_id, latest.employee_no, latest.org_unit_id, latest.job_grade,
       latest.employment_type, latest.status, latest.eff_from_ts,
       asg.program_id AS current_program_id,
       asg.queue_id   AS current_queue_id,
       asg.role_on_program
FROM (
  SELECT h.*,
         ROW_NUMBER() OVER (PARTITION BY h.agent_id ORDER BY h.eff_from_ts DESC) AS rn
  FROM   ods_agent_scd2 h
) latest
LEFT   JOIN ods_agent_assignment_scd2 asg
       ON asg.agent_id = latest.agent_id AND asg.is_current = TRUE
WHERE  latest.rn = 1;

-- 8. Agent scorecard — MATERIALIZED VIEW
-- BQ MVs do not support CTEs, window functions, or scalar transforms.
-- Raw aggregates materialized; PERCENT_RANK/NTILE/skills layered via query.
CREATE MATERIALIZED VIEW IF NOT EXISTS vw_agent_scorecard AS
SELECT a.agent_sk,
       a.full_name,
       a.site_code,
       a.job_grade,
       SUM(d.interactions_handled)                  AS total_interactions,
       COUNT(d.date_key)                            AS daily_records,
       SUM(CAST(d.adherence_pct AS FLOAT64))        AS sum_adherence,
       SUM(CAST(d.avg_handle_seconds AS FLOAT64))   AS sum_aht
FROM   dim_agent a
JOIN   agg_agent_daily d ON d.agent_sk = a.agent_sk
WHERE  a.is_current = TRUE
GROUP  BY a.agent_sk, a.full_name, a.site_code, a.job_grade;

-- 9. Attrition risk (nested CTEs + NTILE banding — BQ native support)
CREATE VIEW IF NOT EXISTS vw_attrition_risk AS
WITH adh AS (
  SELECT f.agent_sk, AVG(f.adherence_pct) AS adherence_90d
  FROM   fact_adherence_daily f
  GROUP  BY f.agent_sk
),
notice AS (
  SELECT e.agent_id, COUNT(*) AS notice_events
  FROM   ods_attrition_event e
  GROUP  BY e.agent_id
),
wk AS (
  SELECT w.agent_sk, AVG(w.interactions_handled) AS weekly_volume
  FROM   agg_agent_weekly w
  GROUP  BY w.agent_sk
),
banded AS (
  SELECT a.agent_sk, a.agent_id, a.full_name, a.site_code,
         adh.adherence_90d,
         COALESCE(n.notice_events, 0) AS notice_events,
         NTILE(5) OVER (ORDER BY adh.adherence_90d ASC) AS adherence_band
  FROM   dim_agent a
  JOIN   adh    ON adh.agent_sk = a.agent_sk
  LEFT   JOIN notice n ON n.agent_id = a.agent_id
  LEFT   JOIN wk ON wk.agent_sk = a.agent_sk
  WHERE  a.is_current = TRUE AND a.status = 'ACTIVE'
)
SELECT agent_sk, agent_id, full_name, site_code, adherence_90d, notice_events,
       CASE WHEN adherence_band = 1 OR notice_events > 0 THEN 'HIGH'
            WHEN adherence_band = 2 THEN 'MEDIUM' ELSE 'LOW' END AS attrition_risk
FROM   banded;

-- 10. Queue SLA attainment (layer-skip: reads staging.stg_crm_sla_target — unqualified)
CREATE VIEW IF NOT EXISTS vw_queue_sla_attainment AS
SELECT q.queue_code,
       q.media_type,
       f.date_key,
       SAFE_DIVIDE(
         CAST(SUM(f.answered_in_sl) AS FLOAT64),
         CAST(NULLIF(SUM(f.answered), 0) AS FLOAT64)
       ) * 100                                           AS sl_pct,
       MAX(t.target_value)                               AS sl_target,
       CASE WHEN SAFE_DIVIDE(
              CAST(SUM(f.answered_in_sl) AS FLOAT64),
              CAST(NULLIF(SUM(f.answered), 0) AS FLOAT64)
            ) * 100 >= MAX(t.target_value) THEN 'MET'
            ELSE 'MISSED' END                            AS attainment
FROM   fact_queue_interval f
JOIN   dim_queue q          ON q.queue_sk = f.queue_sk
LEFT   JOIN stg_crm_sla_target t
       ON t.queue_id = q.queue_id AND t.metric_code = 'SL_20_80'
GROUP  BY q.queue_code, q.media_type, f.date_key;

-- 11. First-contact resolution (date_add(ts, 7) -> TIMESTAMP_ADD(ts, INTERVAL 7 DAY))
CREATE VIEW IF NOT EXISTS vw_first_contact_resolution AS
SELECT f.date_key,
       f.program_sk,
       COUNT(*)                                          AS resolved_interactions,
       SUM(CASE WHEN rpt.interaction_id IS NULL THEN 1 ELSE 0 END) AS fcr_count,
       SAFE_DIVIDE(
         CAST(SUM(CASE WHEN rpt.interaction_id IS NULL THEN 1 ELSE 0 END) AS FLOAT64),
         CAST(COUNT(*) AS FLOAT64)
       ) * 100                                           AS fcr_pct
FROM   fact_interaction f
LEFT   JOIN fact_interaction rpt
       ON  rpt.customer_ref = f.customer_ref
       AND rpt.start_ts > f.end_ts
       AND rpt.start_ts <= TIMESTAMP_ADD(f.end_ts, INTERVAL 7 DAY)
WHERE  f.resolved_flag = TRUE
GROUP  BY f.date_key, f.program_sk;

-- 12. Occupancy / utilization (LIKE 'AUX%' — same BQ syntax)
CREATE VIEW IF NOT EXISTS vw_occupancy_utilization AS
SELECT f.date_key,
       a.site_code,
       f.agent_sk,
       SUM(CASE WHEN f.state_code IN ('TALK','HOLD','ACW') THEN f.state_seconds ELSE 0 END) AS handle_seconds,
       SUM(CASE WHEN f.state_code = 'READY'                THEN f.state_seconds ELSE 0 END) AS ready_seconds,
       SUM(CASE WHEN f.state_code LIKE 'AUX%'              THEN f.state_seconds ELSE 0 END) AS aux_seconds,
       SAFE_DIVIDE(
         CAST(SUM(CASE WHEN f.state_code IN ('TALK','HOLD','ACW') THEN f.state_seconds ELSE 0 END) AS FLOAT64),
         CAST(NULLIF(SUM(CASE WHEN f.state_code IN ('TALK','HOLD','ACW','READY')
                              THEN f.state_seconds ELSE 0 END), 0) AS FLOAT64)
       ) * 100                                           AS occupancy_pct
FROM   fact_agent_activity f
JOIN   dim_agent a ON a.agent_sk = f.agent_sk
GROUP  BY f.date_key, a.site_code, f.agent_sk;

-- 13. Shrinkage analysis
-- (from_unixtime(unix_timestamp(CAST(date_key AS STRING),'yyyyMMdd'),'yyyy-MM-dd')
--  -> PARSE_DATE('%Y%m%d', CAST(date_key AS STRING)))
-- Cross-dataset: ods.ods_schedule, dm.dim_agent, dm.dim_shift (all unqualified)
CREATE VIEW IF NOT EXISTS vw_shrinkage_analysis AS
SELECT f.date_key,
       a.site_code,
       SUM(f.scheduled_minutes)                          AS scheduled_minutes,
       SUM(f.worked_minutes)                             AS worked_minutes,
       SUM(f.exception_minutes + f.timeoff_minutes)      AS shrinkage_minutes,
       SAFE_DIVIDE(
         CAST(SUM(f.exception_minutes + f.timeoff_minutes) AS FLOAT64),
         CAST(NULLIF(SUM(f.scheduled_minutes), 0) AS FLOAT64)
       ) * 100                                           AS shrinkage_pct,
       COUNT(DISTINCT s.schedule_id)                     AS schedules,
       COUNT(DISTINCT sh.shift_sk)                       AS overnight_shifts
FROM   fact_adherence_daily f
JOIN   dim_agent a ON a.agent_sk = f.agent_sk
LEFT   JOIN ods_schedule s
       ON s.agent_id = a.agent_id
      AND s.sched_date = PARSE_DATE('%Y%m%d', CAST(f.date_key AS STRING))
LEFT   JOIN dim_shift sh ON sh.shift_id = s.shift_id AND sh.overnight_flag = TRUE
GROUP  BY f.date_key, a.site_code;

-- 14. Program margin (cross-dataset reads: ods.ods_timesheet, ods_payroll_adjustment,
--     ods_contract_line — all unqualified)
CREATE VIEW IF NOT EXISTS vw_program_margin AS
SELECT b.period_month,
       b.client_sk,
       b.program_sk,
       b.billed_amount,
       b.net_revenue,
       lab.billable_cost_minutes / 60.0 * 18.50          AS est_labor_cost,
       adj.total_adjustments,
       b.net_revenue - (lab.billable_cost_minutes / 60.0 * 18.50)
                     - COALESCE(adj.total_adjustments, 0) AS est_margin,
       cmt.committed_min
FROM   agg_billing_monthly b
LEFT   JOIN (
  SELECT t.program_id, t.work_month, SUM(t.billable_minutes) AS billable_cost_minutes
  FROM   ods_timesheet t GROUP BY t.program_id, t.work_month
) lab ON lab.work_month = b.period_month
LEFT   JOIN (
  SELECT p.period_month, SUM(p.amount) AS total_adjustments
  FROM   ods_payroll_adjustment p GROUP BY p.period_month
) adj ON adj.period_month = b.period_month
LEFT   JOIN (
  SELECT cl.contract_id, SUM(cl.min_commit) AS committed_min
  FROM   ods_contract_line cl GROUP BY cl.contract_id
) cmt ON 1 = 1;

-- 15. Client executive summary (HUB view — wide multi-fact join)
CREATE VIEW IF NOT EXISTS vw_client_executive_summary AS
SELECT c.client_code,
       c.client_name,
       pm.period_month,
       pr.program_code,
       pm.interactions,
       pm.avg_handle_seconds,
       pm.avg_csat,
       cs.pct_promoters,
       cs.pct_detractors,
       bm.billed_amount,
       bm.sla_credit_amount,
       bm.net_revenue,
       tk.open_tickets,
       tk.sla_breached_tickets
FROM   dim_client c
JOIN   dim_program pr            ON pr.client_id = c.client_id
JOIN   agg_program_monthly pm    ON pm.program_sk = pr.program_sk AND pm.grouping_level = 0
LEFT   JOIN agg_csat_rollup_monthly cs
       ON cs.program_sk = pr.program_sk AND cs.period_month = pm.period_month
LEFT   JOIN agg_billing_monthly bm
       ON bm.program_sk = pr.program_sk AND bm.period_month = pm.period_month
LEFT   JOIN (
  SELECT t.program_sk,
         SUM(CASE WHEN t.status IN ('OPEN','PENDING') THEN 1 ELSE 0 END) AS open_tickets,
         SUM(CASE WHEN t.sla_breached_flag THEN 1 ELSE 0 END)            AS sla_breached_tickets
  FROM   fact_ticket t GROUP BY t.program_sk
) tk ON tk.program_sk = pr.program_sk;

-- 16. Intraday queue hourly aggregate — MATERIALIZED VIEW
-- BQ MVs do not support scalar transforms on aggregates.
-- Raw aggregates materialized; sl_pct derived via query.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_queue_hourly AS
SELECT fact_queue_interval.queue_sk,
       EXTRACT(HOUR FROM fact_queue_interval.interval_start_ts) AS hour_of_day,
       SUM(fact_queue_interval.offered)                         AS offered,
       SUM(fact_queue_interval.answered)                        AS answered,
       SUM(fact_queue_interval.abandoned)                       AS abandoned,
       SUM(fact_queue_interval.answered_in_sl)                  AS answered_in_sl,
       fact_queue_interval.date_key
FROM   fact_queue_interval
GROUP  BY fact_queue_interval.queue_sk,
       EXTRACT(HOUR FROM fact_queue_interval.interval_start_ts),
       fact_queue_interval.date_key;
