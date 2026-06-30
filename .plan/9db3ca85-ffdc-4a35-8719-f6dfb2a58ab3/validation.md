# Validation

## Validation — Live BigQuery Catalog Verification (9 Acceptance Criteria)

### Validation Environment
All validation runs against **live BigQuery scratch datasets** — never offline parse, dry-run, or static analysis. The scratch datasets are created by the DDL scripts themselves (`nbcs_staging`, `nbcs_ods`, `nbcs_dm`). Every check queries `INFORMATION_SCHEMA` or executes `SELECT` against the actual catalog.

### Validation Script
A single validation SQL script (`/workspace/project/bigquery/validation/validate-schema.sql`) executes all checks sequentially after DDL application. Results are reported per acceptance criterion with PASS/FAIL and specific failure details.

### AC1 — DDL Error-Free Application (116/116 objects)
**Method:** Execute each of the 9 DDL files in order (01 through 09) against the scratch project. Each `CREATE` statement must succeed with 0 errors.
- 3 `CREATE SCHEMA` statements
- 100 `CREATE TABLE` statements
- 13 `CREATE VIEW` statements
- 3 `CREATE MATERIALIZED VIEW` statements
- Any BQ error message on any statement is a HARD FAIL naming the object and error.
- The 4 former ACID tables must create as standard tables with no transactional syntax.
- The 6 former SerDe tables must create with no format/SerDe references.

**Pass criterion:** 116/116 objects created, 0 DDL errors.

### AC2 — Per-Column Fidelity (916/916 columns)
**Method:** Query `INFORMATION_SCHEMA.COLUMNS` for all 100 tables across 3 datasets and compare each column against `manifests/tables.yaml`.

Checks per column:
1. **Name** matches exactly (case-sensitive)
2. **Ordinal position** matches source order (regular columns first, then partition columns promoted to end)
3. **Mapped type** matches the type mapping rules:
   - BIGINT/INT → INT64
   - STRING → STRING
   - BOOLEAN → BOOL
   - TIMESTAMP → TIMESTAMP
   - DOUBLE → FLOAT64
   - DECIMAL(p,s) → NUMERIC(p,s) with exact precision/scale
4. **Nested sub-fields** verified recursively for all 5 complex columns:
   - `stg_file_qa_forms.sections`: 3 sub-fields, INT→INT64
   - `stg_file_chat_transcripts.messages`: 3 sub-fields, BIGINT→INT64
   - `stg_file_chat_transcripts.metadata`: 2 sub-fields (MAP→ARRAY of STRUCT)
   - `stg_file_speech_analytics.keywords`: REPEATED STRING mode
5. **Column descriptions** present and matching for all 68 annotated columns + 4 Oracle string columns + 2 lie columns
6. **Epoch columns** (56 total) remain INT64, not TIMESTAMP
7. **DECIMAL precision/scale** exact for all 53 DECIMAL columns across 7 signatures

**Pass criterion:** 916/916 columns across 100/100 tables, 0 mismatches.

### AC3 — Object-Type Fidelity (116/116 correct type)
**Method:** Query `INFORMATION_SCHEMA.TABLES` for all 3 datasets. Verify:
- 100 objects have `table_type = 'BASE TABLE'` (including 4 former ACID tables)
- 13 objects have `table_type = 'VIEW'`
- 3 objects have `table_type = 'MATERIALIZED VIEW'` (`vw_agent_scorecard`, `vw_csat_rollup`, `mv_agg_queue_hourly`)
- No silent type flips (table created as view or view created as table)

**Pass criterion:** 100 BASE TABLE + 13 VIEW + 3 MATERIALIZED VIEW = 116 correct types.

### AC4 — Partition + Cluster + Key Intent (100/100 tables)
**Method:** Query `INFORMATION_SCHEMA.COLUMNS` (for `is_partitioning_column`) and `INFORMATION_SCHEMA.TABLE_OPTIONS` (for `partition_expiration_days`) to verify each table's partition column, partition type, clustering columns, and expiration.

Key verifications:
- 27 staging sqoop mirrors: partitioned by `load_date` DATE, expiration = 90 days
- `stg_wfm_schedule`: partitioned by `load_date`, clustered by `site_code`
- 8 staging deltas: partitioned by `extract_ts` DATE, expiration = 90 days
- 10 staging file feeds: partitioned by `feed_date` DATE, clustered by `client_code`, expiration = 90 days
- 15 ODS cleanse: partitioned by respective date columns
- 8 ODS delta-merged: partitioned by period/date column
- 3 ODS SCD-2: RANGE partitioned by `eff_from_year`
- 4 ODS ACID: unpartitioned, clustered by former bucketing key
- 9 DM dims: unpartitioned
- 7 DM facts with date_key: RANGE partitioned with monthly boundaries
- `fact_billing_line`: DATE partitioned by `period_month`, clustered by `client_sk, program_sk`
- 7 DM aggs: partitioned by respective period column
- Clustering verified on all 7 hot-path tables per locked Performance Optimization

**Pass criterion:** 100/100 tables match the partition/cluster matrix.

### AC5 — Cross-Dataset FK/PK Type Consistency
**Method:** Query `INFORMATION_SCHEMA.COLUMNS` for join-path column pairs and verify data_type matches on both sides.

Join paths to verify:
- **staging to ods:** `client_id` (INT64=INT64), `agent_id`, `program_id`, `ticket_id`, `invoice_id`, `queue_id`, `org_unit_id` — 7 pairs
- **ods to dm:** `agent_id`, `program_id`, `queue_id`, `client_id` — 4 pairs
- **dm surrogate keys:** `agent_sk`, `client_sk`, `program_sk`, `queue_sk` across fact to dim — 8+ pairs
- **Cross-layer view joins:** `vw_queue_sla_attainment` (dm to staging), `vw_billing_reconciliation` (staging to ods), `vw_shrinkage_analysis` (dm to ods) — 3 pairs

**Pass criterion:** All documented FK/PK join paths have matching types, 0 mismatches.

### AC6 — Queryability Smoke (115/115 + 3/3 cross-joins)
**Method:** Execute `SELECT * FROM <object> LIMIT 0` for all 100 tables and 15 views/MVs. This validates schema correctness and view SQL compilation without requiring data.

Plus 3 representative cross-join queries:
1. `nbcs_staging`: `stg_fin_invoice` JOIN `stg_fin_invoice_line` ON `invoice_id`
2. `nbcs_ods`: `ods_interaction` JOIN `ods_call` using CAST pattern
3. `nbcs_dm`: `fact_interaction` JOIN `dim_agent` JOIN `dim_program` on surrogate keys

**Pass criterion:** 115 SELECT * succeed, 3 cross-join queries execute with 0 errors.

### AC7 — Integrity Guards (116/116 catalog-readable)
**Method:** After all DDL applied, verify every object appears in `INFORMATION_SCHEMA.TABLES` and every column appears in `INFORMATION_SCHEMA.COLUMNS`. Absence of an object or column is a HARD FAIL.

Anti-pattern enforcement: a violation-finding query returning 0 rows is NOT proof of no violation if the object itself is absent. Both sides absent = HARD FAIL for required structural objects.

**Pass criterion:** 116/116 objects catalog-readable, 916/916 columns catalog-readable, 0 integrity violations.

### AC8 — No-Silent-Skip (116/116 live-checked)
**Method:** Every criterion (AC1-AC7, AC9) is proven by executing BQ SQL against the live catalog. No offline parse, no dry-run, no sampling, no representative subset.

The validation script outputs a manifest of all 116 objects checked and all 916 columns verified, with per-object status. Any object not individually checked is a FAIL.

**Pass criterion:** 116/116 objects and 916/916 columns verified via live catalog queries, 0 skipped.

### AC9 — Physical-Access Performance (scan reduction)
**Method:** Load fixture data from `data/parquet/` and `data/text/` into 7 hot-path clustered tables + 1 staging table. For each, run a filtered query AND unfiltered equivalent, capturing `totalBytesProcessed` from BQ job metadata.

Tables and filter patterns:
1. `fact_interaction` — filter by `date_key` range + `channel` vs unfiltered
2. `fact_agent_activity` — filter by `agent_sk` vs unfiltered
3. `fact_queue_interval` — filter by `queue_sk` vs unfiltered
4. `fact_billing_line` — filter by `client_sk` + `program_sk` vs unfiltered
5. `agg_agent_daily` — filter by `agent_sk` vs unfiltered
6. `agg_billing_monthly` — filter by `client_sk` + `program_sk` vs unfiltered
7. `stg_tel_call` — filter by `load_date` partition vs unfiltered

All bytes-scanned figures from actual BQ job metadata (`totalBytesProcessed`), never invented. Filtered queries must scan materially fewer bytes than unfiltered.

**Pass criterion:** 7/7 hot-path + 1 staging — every filtered query scans fewer bytes.

### Validation Execution Order
1. Run DDL files 01-09 in sequence (AC1)
2. Run catalog verification queries (AC2, AC3, AC4, AC5, AC7)
3. Run queryability smoke tests (AC6)
4. Load fixture data and run performance benchmarks (AC9)
5. Generate summary report with per-AC pass/fail status (AC8 — ensures no skips)
