# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,FPL Bronze Ingestion
# MAGIC %md
# MAGIC # FPL Bronze Ingestion
# MAGIC
# MAGIC Reads raw JSON files landed by the GitHub Actions `fpl_ingest.py` script from
# MAGIC `/Volumes/fpl/bronze/landing/` and loads them into the bronze Delta tables.
# MAGIC
# MAGIC **Tables populated:**
# MAGIC - `fpl.bronze.bootstrap_static` — players, teams, gameweeks, positions
# MAGIC - `fpl.bronze.fixtures` — fixture snapshots
# MAGIC - `fpl.bronze.player_gameweek_history` — per-player element-summary (batched)
# MAGIC
# MAGIC Uses `MERGE INTO` keyed on `source_file` so re-running is idempotent (no duplicates).

# COMMAND ----------

# DBTITLE 1,Ingest bootstrap_static
# MAGIC %sql
# MAGIC -- bootstrap_static: players, teams, gameweeks, positions
# MAGIC MERGE INTO fpl.bronze.bootstrap_static AS t
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     to_timestamp(
# MAGIC       regexp_extract(_metadata.file_path, '([0-9]{8}T[0-9]{6}Z)', 1),
# MAGIC       "yyyyMMdd'T'HHmmss'Z'"
# MAGIC     ) AS ingest_ts,
# MAGIC     _metadata.file_path AS source_file,
# MAGIC     value AS payload
# MAGIC   FROM read_files(
# MAGIC     '/Volumes/fpl/bronze/landing/bootstrap_static/',
# MAGIC     format => 'text'
# MAGIC   )
# MAGIC ) AS s
# MAGIC ON t.source_file = s.source_file
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (ingest_ts, source_file, payload)
# MAGIC   VALUES (s.ingest_ts, s.source_file, s.payload);

# COMMAND ----------

# DBTITLE 1,Ingest fixtures
# MAGIC %sql
# MAGIC -- fixtures: match schedule and results
# MAGIC MERGE INTO fpl.bronze.fixtures AS t
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     to_timestamp(
# MAGIC       regexp_extract(_metadata.file_path, '([0-9]{8}T[0-9]{6}Z)', 1),
# MAGIC       "yyyyMMdd'T'HHmmss'Z'"
# MAGIC     ) AS ingest_ts,
# MAGIC     _metadata.file_path AS source_file,
# MAGIC     value AS payload
# MAGIC   FROM read_files(
# MAGIC     '/Volumes/fpl/bronze/landing/fixtures/',
# MAGIC     format => 'text'
# MAGIC   )
# MAGIC ) AS s
# MAGIC ON t.source_file = s.source_file
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (ingest_ts, source_file, payload)
# MAGIC   VALUES (s.ingest_ts, s.source_file, s.payload);

# COMMAND ----------

# DBTITLE 1,Ingest player_gameweek_history
# MAGIC %sql
# MAGIC -- player_gameweek_history: batched element-summary for all players
# MAGIC MERGE INTO fpl.bronze.player_gameweek_history AS t
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     to_timestamp(
# MAGIC       regexp_extract(_metadata.file_path, '([0-9]{8}T[0-9]{6}Z)', 1),
# MAGIC       "yyyyMMdd'T'HHmmss'Z'"
# MAGIC     ) AS ingest_ts,
# MAGIC     _metadata.file_path AS source_file,
# MAGIC     cast(get_json_object(value, '$.gameweek') AS INT) AS gameweek,
# MAGIC     value AS payload
# MAGIC   FROM read_files(
# MAGIC     '/Volumes/fpl/bronze/landing/player_gameweek_history/',
# MAGIC     format => 'text'
# MAGIC   )
# MAGIC ) AS s
# MAGIC ON t.source_file = s.source_file
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (ingest_ts, source_file, gameweek, payload)
# MAGIC   VALUES (s.ingest_ts, s.source_file, s.gameweek, s.payload);

# COMMAND ----------

# DBTITLE 1,Verify bronze table counts
# MAGIC %sql
# MAGIC -- Verify row counts after ingestion
# MAGIC SELECT 'bootstrap_static'        AS table_name, COUNT(*) AS row_count FROM fpl.bronze.bootstrap_static
# MAGIC UNION ALL
# MAGIC SELECT 'fixtures'                 AS table_name, COUNT(*)           FROM fpl.bronze.fixtures
# MAGIC UNION ALL
# MAGIC SELECT 'player_gameweek_history'   AS table_name, COUNT(*)           FROM fpl.bronze.player_gameweek_history;

# COMMAND ----------

