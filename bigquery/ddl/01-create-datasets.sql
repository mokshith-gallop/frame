-- ----------------------------------------------------------------------------
-- 01: BigQuery datasets (staging -> ods -> dm)
-- Migrated from Hive databases on nbcs-cdh-prod (CDH 6.3.4).
-- ----------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS nbcs_staging
  OPTIONS(description='Sqoop + SFTP landing mirrors (epoch dates live here)');

CREATE SCHEMA IF NOT EXISTS nbcs_ods
  OPTIONS(description='Cleansed / conformed / merged (all TIMESTAMPs)');

CREATE SCHEMA IF NOT EXISTS nbcs_dm
  OPTIONS(description='Dimensional marts + all views');
