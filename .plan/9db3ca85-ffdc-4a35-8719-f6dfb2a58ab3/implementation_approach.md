# Implementation Approach

## Implementation Approach — Static Hand-Authored BigQuery DDL

### Strategy
Write all 116 DDL objects as static, hand-authored BigQuery Standard SQL across 9 files mirroring the source `hive/ddl/01-09` structure. No code generation — every CREATE statement is explicitly written with all type mappings, partitioning, clustering, descriptions, and options baked in. The authoritative source for column names, types, and ordinal positions is `manifests/tables.yaml`, cross-checked against `hive/ddl/01-08`.

### Output File Structure
All DDL files written to `/workspace/project/bigquery/ddl/`:

| File | Contents | Object Count |
|------|----------|-------------|
| `01-create-datasets.sql` | `CREATE SCHEMA` for `nbcs_staging`, `nbcs_ods`, `nbcs_dm` | 3 datasets |
| `02-staging-sqoop-mirrors.sql` | 27 sqoop mirror tables | 27 tables |
| `03-staging-delta-feeds.sql` | 8 CDC delta feed tables | 8 tables |
| `04-staging-file-feeds.sql` | 10 SFTP/file feed tables (JsonSerDe, RegexSerDe, SequenceFile, RCFile → standard) | 10 tables |
| `05-ods-cleanse.sql` | 15 cleansed/conformed tables | 15 tables |
| `06-ods-delta-scd2.sql` | 8 delta-merged + 3 SCD-2 history tables | 11 tables |
| `07-ods-acid.sql` | 4 former ACID tables → standard BQ tables with CLUSTER BY | 4 tables |
| `08-dm-tables.sql` | 9 dimensions + 9 facts + 7 aggregates | 25 tables |
| `09-dm-views.sql` | 13 regular views + 3 materialized views | 16 views |
| **Total** | | **116 + 3 datasets** |

### Key Technical Parameters

**Partitioning:**
- **Staging sqoop mirrors (27)**: `PARTITION BY DATE(load_date)` where `load_date` is a `DATE` column; `partition_expiration_days = 90`
- **Staging delta feeds (8)**: `PARTITION BY DATE(extract_ts)` where `extract_ts` is a `DATE` column; `partition_expiration_days = 90`
- **Staging file feeds (10)**: `PARTITION BY DATE(feed_date)` — Hive's `(client_code, feed_date)` dual-partition becomes single-column `feed_date` partition + `CLUSTER BY client_code`; `partition_expiration_days = 90`
- **`stg_wfm_schedule`**: Special case — Hive's `(load_date, site_code)` becomes `PARTITION BY DATE(load_date)` + `CLUSTER BY site_code`
- **ODS cleanse (15)**: `PARTITION BY DATE(snapshot_date/sched_date/event_date/call_date)` as appropriate per table
- **ODS delta-merged (8)**: `PARTITION BY DATE(work_month/period_month/event_date/swap_month/event_month/snapshot_date)` — STRING month columns like `work_month` ('YYYY-MM') are stored as `DATE` type (first of month) in BQ
- **ODS SCD-2 (3)**: `PARTITION BY RANGE_BUCKET(eff_from_year, GENERATE_ARRAY(2018, 2031, 1))`
- **ODS ACID (4)**: Unpartitioned, `CLUSTER BY` former bucketing key
- **DM dimensions (9)**: Unpartitioned (small tables)
- **DM facts with date_key (7)**: `PARTITION BY RANGE_BUCKET(date_key, GENERATE_ARRAY(20200101, 20301201, 100))`
- **`fact_billing_line`**: `PARTITION BY DATE(period_month)` + `CLUSTER BY (client_sk, program_sk)`
- **DM aggs**: Partitioned by their respective period column using same RANGE_BUCKET or DATE pattern

**Clustering (per locked Performance Optimization):**

| Table | CLUSTER BY |
|-------|-----------|
| `fact_interaction` | `channel, agent_sk, client_sk` |
| `fact_agent_activity` | `agent_sk, state_code` |
| `fact_queue_interval` | `queue_sk` |
| `fact_billing_line` | `client_sk, program_sk` |
| `fact_csat_survey` | `program_sk` |
| `agg_agent_daily` | `agent_sk` |
| `agg_billing_monthly` | `client_sk, program_sk` |
| File feed staging (10) | `client_code` |
| `stg_wfm_schedule` | `site_code` |
| `stg_tel_call` | `call_id` (preserves source bucketing intent) |
| ACID tables (4) | Former bucketing key (`client_id`, `agent_id`, `ticket_id`, `invoice_id`) |

### View Translation Strategy
All 15 views + 3 materialized views are hand-translated from Impala/Hive SQL to BigQuery Standard SQL. Key translations:

| Source Pattern | BigQuery Translation |
|---------------|---------------------|
| `NDV(x)` | `APPROX_COUNT_DISTINCT(x)` |
| `GROUPING__ID` + `WITH ROLLUP` | `GROUPING(col1, col2)` + `GROUP BY ROLLUP(...)` |
| `RLIKE 'pattern'` | `REGEXP_CONTAINS(col, r'pattern')` |
| `regexp_extract(s, p, 1)` | `REGEXP_EXTRACT(s, r'pattern')` with capture group |
| `unix_timestamp(ts)` | `UNIX_SECONDS(ts)` |
| `from_unixtime(x)` | `TIMESTAMP_SECONDS(x)` |
| `from_unixtime(x/1000)` | `TIMESTAMP_MILLIS(x)` |
| `date_add(ts, 7)` | `DATE_ADD(ts, INTERVAL 7 DAY)` |
| `WITH RECURSIVE` | `WITH RECURSIVE` (BQ native) |
| `PERCENT_RANK()`, `NTILE()` | Direct port (same syntax) |
| `staging.stg_crm_sla_target` (layer-skip) | `nbcs_staging.stg_crm_sla_target` (preserved as-is) |
| `CAST(s.issued_ts_sec / 1000 AS BIGINT)` (lie column) | `CAST(s.issued_ts_sec / 1000 AS INT64)` (preserved exactly) |

**Materialized views (3):**
- `vw_agent_scorecard` — materialized (complex multi-source join, frequent ops dashboard use)
- `vw_csat_rollup` — materialized (ROLLUP cube computation)
- `mv_agg_queue_hourly` — materialized (intraday-refreshed aggregate)

### Cross-Dataset References
All views use fully qualified `dataset.table` references. Cross-dataset reads:
- `vw_billing_reconciliation`: `nbcs_staging.stg_fin_invoice` JOIN `nbcs_ods.ods_invoice_acid`
- `vw_queue_sla_attainment`: reads `nbcs_staging.stg_crm_sla_target` (layer-skip preserved)
- `vw_agent_roster_current`: `nbcs_ods.ods_agent_scd2` + `nbcs_ods.ods_agent_assignment_scd2`
- `vw_shrinkage_analysis`: `nbcs_ods.ods_schedule` + `nbcs_dm.dim_agent`
- `vw_program_margin`: `nbcs_ods.ods_timesheet` + `nbcs_ods.ods_payroll_adjustment` + `nbcs_ods.ods_contract_line`

### ACID to Standard Table Conversion
The 4 ACID tables lose all transactional Hive properties:
- No `STORED AS ORC`, no `TBLPROPERTIES ('transactional'='true')`, no `INTO N BUCKETS`
- Each becomes a standard `CREATE TABLE` with `CLUSTER BY` on the former bucketing key
- BigQuery natively supports MERGE/UPDATE/DELETE on all tables

### SerDe Elimination
6 tables with non-standard storage:
- 3 JsonSerDe (`stg_file_qa_forms`, `stg_file_chat_transcripts`, `stg_file_speech_analytics`) → standard BQ tables with native `ARRAY`, `STRUCT`, `REPEATED` types
- 1 RegexSerDe (`stg_file_ivr_logs`) → standard BQ table with clean columns
- 1 SequenceFile (`stg_file_telco_invoice`) → standard BQ table
- 1 RCFile (`stg_file_dialer_result`) → standard BQ table
No format, SerDe, or TBLPROPERTIES references in any BQ DDL.

### Partition Column Type Handling
Hive partition columns defined as `STRING` in the source are converted to proper BQ types:
- `load_date STRING` → `load_date DATE` (BQ partition column)
- `extract_ts STRING` → `extract_ts DATE` (BQ partition column)
- `feed_date STRING` → `feed_date DATE` (BQ partition column)
- `client_code STRING` → `client_code STRING` (demoted from partition to `CLUSTER BY` column)
- `site_code STRING` → `site_code STRING` (demoted from partition to `CLUSTER BY` column)
- `snapshot_date STRING` → `snapshot_date DATE`
- `work_month STRING` → `work_month DATE` (stored as first-of-month)
- `period_month STRING` → `period_month DATE` (stored as first-of-month for DM tables using DATE partition; remains STRING where it is a regular column in staging)
- `eff_from_year INT` → `eff_from_year INT64` (RANGE partition)
- `date_key INT` → `date_key INT64` (RANGE partition)
- `channel STRING` → `channel STRING` (demoted from partition to `CLUSTER BY` column on `fact_interaction`)
