-- ----------------------------------------------------------------------------
-- 07-ods-acid: 4 former Hive ACID transactional tables -> standard BQ tables
-- Migrated from Hive ODS layer on nbcs-cdh-prod (CDH 6.3.4).
--
-- Hive ACID conversion: no ORC format, no transactional TBLPROPERTIES,
--   no INTO N BUCKETS. Each table gets CLUSTER BY on its former bucketing key.
-- BigQuery natively supports MERGE/UPDATE/DELETE on all tables.
-- Type mapping: BIGINT->INT64, STRING->STRING, TIMESTAMP->TIMESTAMP,
--               DECIMAL(p,s)->NUMERIC(p,s).
-- These tables are unpartitioned; CLUSTER BY preserves the bucketing intent.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ods_client_acid (
  client_id                   INT64,
  client_code                 STRING,
  client_name                 STRING,
  industry                    STRING,
  hq_country                  STRING,
  status                      STRING,
  created_ts                  TIMESTAMP,
  updated_ts                  TIMESTAMP
)
CLUSTER BY client_id;

CREATE TABLE IF NOT EXISTS ods_agent_acid (
  agent_id                    INT64,
  employee_no                 STRING,
  full_name                   STRING,
  email                       STRING,
  org_unit_id                 INT64,
  job_grade                   STRING,
  employment_type             STRING,
  hire_ts                     TIMESTAMP,
  term_ts                     TIMESTAMP,
  status                      STRING
)
CLUSTER BY agent_id;

CREATE TABLE IF NOT EXISTS ods_ticket_acid (
  ticket_id                   INT64,
  ticket_no                   STRING,
  program_id                  INT64,
  category_id                 INT64,
  assigned_agent_id           INT64,
  priority                    STRING,
  status                      STRING,
  created_ts                  TIMESTAMP,
  updated_ts                  TIMESTAMP,
  resolved_ts                 TIMESTAMP
)
CLUSTER BY ticket_id;

CREATE TABLE IF NOT EXISTS ods_invoice_acid (
  invoice_id                  INT64,
  invoice_no                  STRING,
  client_id                   INT64,
  program_id                  INT64,
  period_month                STRING,
  issued_ts                   TIMESTAMP,
  due_ts                      TIMESTAMP,
  currency                    STRING,
  total_amount                NUMERIC(14,2),
  status                      STRING
)
CLUSTER BY invoice_id;
