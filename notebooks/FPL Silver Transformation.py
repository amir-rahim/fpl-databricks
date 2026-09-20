# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,FPL Silver Transformation
# MAGIC %md
# MAGIC # FPL Silver Transformation
# MAGIC
# MAGIC Parses raw JSON payloads from the bronze Delta tables and loads them into typed,
# MAGIC conformed silver tables. Reads only the latest bronze snapshot per run.
# MAGIC
# MAGIC **Tables populated:**
# MAGIC - `fpl.silver.dim_teams` — team dimension (upsert)
# MAGIC - `fpl.silver.dim_gameweeks` — gameweek dimension (upsert)
# MAGIC - `fpl.silver.dim_players` — SCD2 player dimension (close + insert on change)
# MAGIC - `fpl.silver.fact_fixtures` — fixture facts (upsert)
# MAGIC - `fpl.silver.fact_player_gameweek_stats` — core per-player-per-gameweek facts (upsert)
# MAGIC
# MAGIC Run after the [FPL Bronze Ingestion](#notebook-423499055476427) notebook.

# COMMAND ----------

# DBTITLE 1,dim_teams
# MAGIC %sql
# MAGIC -- dim_teams: upsert from latest bootstrap_static snapshot
# MAGIC MERGE INTO fpl.silver.dim_teams AS t
# MAGIC USING (
# MAGIC   WITH latest AS (
# MAGIC     SELECT payload, ingest_ts
# MAGIC     FROM fpl.bronze.bootstrap_static
# MAGIC     ORDER BY ingest_ts DESC
# MAGIC     LIMIT 1
# MAGIC   )
# MAGIC   SELECT
# MAGIC     team.id                                             AS team_id,
# MAGIC     team.name                                           AS name,
# MAGIC     team.short_name                                     AS short_name,
# MAGIC     team.strength_overall_home                          AS strength_overall,
# MAGIC     team.strength_attack_home                           AS strength_attack_home,
# MAGIC     team.strength_attack_away                           AS strength_attack_away,
# MAGIC     team.strength_defence_home                          AS strength_defence_home,
# MAGIC     team.strength_defence_away                          AS strength_defence_away,
# MAGIC     latest.ingest_ts                                    AS updated_ts
# MAGIC   FROM latest
# MAGIC   LATERAL VIEW explode(from_json(
# MAGIC     get_json_object(payload, '$.teams'),
# MAGIC     'ARRAY<STRUCT<id: INT, name: STRING, short_name: STRING, strength_overall_home: INT, strength_attack_home: INT, strength_attack_away: INT, strength_defence_home: INT, strength_defence_away: INT>>'
# MAGIC   )) AS team
# MAGIC ) AS s
# MAGIC ON t.team_id = s.team_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   t.name                 = s.name,
# MAGIC   t.short_name           = s.short_name,
# MAGIC   t.strength_overall     = s.strength_overall,
# MAGIC   t.strength_attack_home = s.strength_attack_home,
# MAGIC   t.strength_attack_away = s.strength_attack_away,
# MAGIC   t.strength_defence_home = s.strength_defence_home,
# MAGIC   t.strength_defence_away = s.strength_defence_away,
# MAGIC   t.updated_ts           = s.updated_ts
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   team_id, name, short_name, strength_overall,
# MAGIC   strength_attack_home, strength_attack_away,
# MAGIC   strength_defence_home, strength_defence_away, updated_ts
# MAGIC )
# MAGIC VALUES (
# MAGIC   s.team_id, s.name, s.short_name, s.strength_overall,
# MAGIC   s.strength_attack_home, s.strength_attack_away,
# MAGIC   s.strength_defence_home, s.strength_defence_away, s.updated_ts
# MAGIC );

# COMMAND ----------

# DBTITLE 1,dim_gameweeks
# MAGIC %sql
# MAGIC -- dim_gameweeks: upsert from latest bootstrap_static snapshot
# MAGIC MERGE INTO fpl.silver.dim_gameweeks AS t
# MAGIC USING (
# MAGIC   WITH latest AS (
# MAGIC     SELECT payload, ingest_ts
# MAGIC     FROM fpl.bronze.bootstrap_static
# MAGIC     ORDER BY ingest_ts DESC
# MAGIC     LIMIT 1
# MAGIC   )
# MAGIC   SELECT
# MAGIC     event.id                  AS event_id,
# MAGIC     event.name               AS name,
# MAGIC     to_timestamp(event.deadline_time) AS deadline_time,
# MAGIC     event.is_current          AS is_current,
# MAGIC     event.is_next             AS is_next,
# MAGIC     event.finished            AS finished
# MAGIC   FROM latest
# MAGIC   LATERAL VIEW explode(from_json(
# MAGIC     get_json_object(payload, '$.events'),
# MAGIC     'ARRAY<STRUCT<id: INT, name: STRING, deadline_time: STRING, is_current: BOOLEAN, is_next: BOOLEAN, finished: BOOLEAN>>'
# MAGIC   )) AS event
# MAGIC ) AS s
# MAGIC ON t.event_id = s.event_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   t.name         = s.name,
# MAGIC   t.deadline_time = s.deadline_time,
# MAGIC   t.is_current   = s.is_current,
# MAGIC   t.is_next      = s.is_next,
# MAGIC   t.finished     = s.finished
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   event_id, name, deadline_time, is_current, is_next, finished
# MAGIC )
# MAGIC VALUES (
# MAGIC   s.event_id, s.name, s.deadline_time, s.is_current, s.is_next, s.finished
# MAGIC );

# COMMAND ----------

# DBTITLE 1,dim_players SCD2 Step 1: close changed rows
# MAGIC %sql
# MAGIC -- dim_players SCD2 Step 1: close existing current rows where team, position, or price changed
# MAGIC MERGE INTO fpl.silver.dim_players AS t
# MAGIC USING (
# MAGIC   WITH latest AS (
# MAGIC     SELECT payload, ingest_ts
# MAGIC     FROM fpl.bronze.bootstrap_static
# MAGIC     ORDER BY ingest_ts DESC
# MAGIC     LIMIT 1
# MAGIC   ),
# MAGIC   players AS (
# MAGIC     SELECT
# MAGIC       p.id               AS player_id,
# MAGIC       p.team             AS team_id,
# MAGIC       CASE p.element_type
# MAGIC         WHEN 1 THEN 'GKP'
# MAGIC         WHEN 2 THEN 'DEF'
# MAGIC         WHEN 3 THEN 'MID'
# MAGIC         WHEN 4 THEN 'FWD'
# MAGIC       END                AS position,
# MAGIC       cast(p.now_cost AS DOUBLE) / 10 AS price,
# MAGIC       latest.ingest_ts   AS valid_from
# MAGIC     FROM latest
# MAGIC     LATERAL VIEW explode(from_json(
# MAGIC       get_json_object(payload, '$.elements'),
# MAGIC       'ARRAY<STRUCT<id: INT, team: INT, element_type: INT, now_cost: INT>>'
# MAGIC     )) AS p
# MAGIC   )
# MAGIC   SELECT pl.player_id, pl.valid_from
# MAGIC   FROM players pl
# MAGIC   JOIN fpl.silver.dim_players d
# MAGIC     ON pl.player_id = d.player_id AND d.is_current = true
# MAGIC   WHERE COALESCE(pl.team_id, -1)  <> COALESCE(d.team_id, -1)
# MAGIC      OR COALESCE(pl.position, '') <> COALESCE(d.position, '')
# MAGIC      OR COALESCE(pl.price, -1)   <> COALESCE(d.price, -1)
# MAGIC ) AS s
# MAGIC ON t.player_id = s.player_id AND t.is_current = true
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   t.valid_to   = s.valid_from,
# MAGIC   t.is_current = false;

# COMMAND ----------

# DBTITLE 1,dim_players SCD2 Step 2: insert new/changed
# MAGIC %sql
# MAGIC -- dim_players SCD2 Step 2: insert new rows for new and changed players
# MAGIC MERGE INTO fpl.silver.dim_players AS t
# MAGIC USING (
# MAGIC   WITH latest AS (
# MAGIC     SELECT payload, ingest_ts
# MAGIC     FROM fpl.bronze.bootstrap_static
# MAGIC     ORDER BY ingest_ts DESC
# MAGIC     LIMIT 1
# MAGIC   ),
# MAGIC   players AS (
# MAGIC     SELECT
# MAGIC       p.id               AS player_id,
# MAGIC       p.web_name         AS web_name,
# MAGIC       concat(p.first_name, ' ', p.second_name) AS full_name,
# MAGIC       p.team             AS team_id,
# MAGIC       CASE p.element_type
# MAGIC         WHEN 1 THEN 'GKP'
# MAGIC         WHEN 2 THEN 'DEF'
# MAGIC         WHEN 3 THEN 'MID'
# MAGIC         WHEN 4 THEN 'FWD'
# MAGIC       END                AS position,
# MAGIC       cast(p.now_cost AS DOUBLE) / 10 AS price,
# MAGIC       latest.ingest_ts   AS valid_from
# MAGIC     FROM latest
# MAGIC     LATERAL VIEW explode(from_json(
# MAGIC       get_json_object(payload, '$.elements'),
# MAGIC       'ARRAY<STRUCT<id: INT, web_name: STRING, first_name: STRING, second_name: STRING, team: INT, element_type: INT, now_cost: INT>>'
# MAGIC     )) AS p
# MAGIC   )
# MAGIC   SELECT pl.player_id, pl.web_name, pl.full_name, pl.team_id,
# MAGIC          pl.position, pl.price, pl.valid_from
# MAGIC   FROM players pl
# MAGIC   LEFT JOIN fpl.silver.dim_players d
# MAGIC     ON pl.player_id = d.player_id AND d.is_current = true
# MAGIC   WHERE d.player_id IS NULL
# MAGIC ) AS s
# MAGIC ON t.player_id = s.player_id AND t.is_current = true
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   player_id, web_name, full_name, team_id, position, price,
# MAGIC   valid_from, valid_to, is_current
# MAGIC )
# MAGIC VALUES (
# MAGIC   s.player_id, s.web_name, s.full_name, s.team_id, s.position, s.price,
# MAGIC   s.valid_from, NULL, true
# MAGIC );

# COMMAND ----------

# DBTITLE 1,fact_fixtures
# MAGIC %sql
# MAGIC -- fact_fixtures: upsert from latest fixtures snapshot
# MAGIC MERGE INTO fpl.silver.fact_fixtures AS t
# MAGIC USING (
# MAGIC   WITH latest AS (
# MAGIC     SELECT payload, ingest_ts
# MAGIC     FROM fpl.bronze.fixtures
# MAGIC     ORDER BY ingest_ts DESC
# MAGIC     LIMIT 1
# MAGIC   )
# MAGIC   SELECT
# MAGIC     fx.id                       AS fixture_id,
# MAGIC     fx.event                    AS event_id,
# MAGIC     fx.team_h                   AS home_team_id,
# MAGIC     fx.team_a                   AS away_team_id,
# MAGIC     to_timestamp(fx.kickoff_time) AS kickoff_time,
# MAGIC     fx.team_h_difficulty        AS home_difficulty,
# MAGIC     fx.team_a_difficulty        AS away_difficulty,
# MAGIC     fx.finished                 AS finished,
# MAGIC     fx.team_h_score             AS home_score,
# MAGIC     fx.team_a_score             AS away_score
# MAGIC   FROM latest
# MAGIC   LATERAL VIEW explode(from_json(
# MAGIC     payload,
# MAGIC     'ARRAY<STRUCT<id: INT, event: INT, team_h: INT, team_a: INT, kickoff_time: STRING, team_h_difficulty: INT, team_a_difficulty: INT, finished: BOOLEAN, team_h_score: INT, team_a_score: INT>>'
# MAGIC   )) AS fx
# MAGIC ) AS s
# MAGIC ON t.fixture_id = s.fixture_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   t.event_id         = s.event_id,
# MAGIC   t.home_team_id     = s.home_team_id,
# MAGIC   t.away_team_id     = s.away_team_id,
# MAGIC   t.kickoff_time     = s.kickoff_time,
# MAGIC   t.home_difficulty  = s.home_difficulty,
# MAGIC   t.away_difficulty  = s.away_difficulty,
# MAGIC   t.finished         = s.finished,
# MAGIC   t.home_score       = s.home_score,
# MAGIC   t.away_score       = s.away_score
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   fixture_id, event_id, home_team_id, away_team_id,
# MAGIC   kickoff_time, home_difficulty, away_difficulty,
# MAGIC   finished, home_score, away_score
# MAGIC )
# MAGIC VALUES (
# MAGIC   s.fixture_id, s.event_id, s.home_team_id, s.away_team_id,
# MAGIC   s.kickoff_time, s.home_difficulty, s.away_difficulty,
# MAGIC   s.finished, s.home_score, s.away_score
# MAGIC );

# COMMAND ----------

# DBTITLE 1,fact_player_gameweek_stats
# MAGIC %sql
# MAGIC -- fact_player_gameweek_stats: upsert from latest player_gameweek_history snapshot
# MAGIC -- Parses nested JSON: payload -> players[] -> history[] -> one row per player per fixture
# MAGIC MERGE INTO fpl.silver.fact_player_gameweek_stats AS t
# MAGIC USING (
# MAGIC   WITH latest AS (
# MAGIC     SELECT payload, ingest_ts
# MAGIC     FROM fpl.bronze.player_gameweek_history
# MAGIC     ORDER BY ingest_ts DESC
# MAGIC     LIMIT 1
# MAGIC   ),
# MAGIC   parsed AS (
# MAGIC     SELECT from_json(payload,
# MAGIC       'STRUCT<gameweek: INT, players: ARRAY<STRUCT<player_id: INT, history: ARRAY<STRUCT<element: INT, fixture: INT, round: INT, minutes: INT, goals_scored: INT, assists: INT, clean_sheets: INT, goals_conceded: INT, own_goals: INT, penalties_saved: INT, penalties_missed: INT, yellow_cards: INT, red_cards: INT, saves: INT, bonus: INT, bps: INT, influence: STRING, creativity: STRING, threat: STRING, ict_index: STRING, total_points: INT, value: INT, selected_by_percent: STRING, transfers_in: INT, transfers_out: INT>>>>>'
# MAGIC     ) AS data
# MAGIC     FROM latest
# MAGIC   ),
# MAGIC   exploded_players AS (
# MAGIC     SELECT player
# MAGIC     FROM parsed
# MAGIC     LATERAL VIEW explode(data.players) AS player
# MAGIC   ),
# MAGIC   stats AS (
# MAGIC     SELECT
# MAGIC       hist.element                           AS player_id,
# MAGIC       hist.round                             AS event_id,
# MAGIC       hist.fixture                           AS fixture_id,
# MAGIC       hist.minutes                           AS minutes,
# MAGIC       hist.goals_scored                      AS goals_scored,
# MAGIC       hist.assists                           AS assists,
# MAGIC       hist.clean_sheets                      AS clean_sheets,
# MAGIC       hist.goals_conceded                    AS goals_conceded,
# MAGIC       hist.own_goals                         AS own_goals,
# MAGIC       hist.penalties_saved                   AS penalties_saved,
# MAGIC       hist.penalties_missed                  AS penalties_missed,
# MAGIC       hist.yellow_cards                      AS yellow_cards,
# MAGIC       hist.red_cards                         AS red_cards,
# MAGIC       hist.saves                             AS saves,
# MAGIC       hist.bonus                             AS bonus,
# MAGIC       hist.bps                               AS bps,
# MAGIC       cast(hist.influence AS DECIMAL(6,1))   AS influence,
# MAGIC       cast(hist.creativity AS DECIMAL(6,1))  AS creativity,
# MAGIC       cast(hist.threat AS DECIMAL(6,1))       AS threat,
# MAGIC       cast(hist.ict_index AS DECIMAL(6,1))   AS ict_index,
# MAGIC       hist.total_points                      AS total_points,
# MAGIC       cast(hist.value AS DECIMAL(4,1)) / 10  AS value,
# MAGIC       cast(hist.selected_by_percent AS DECIMAL(5,1)) AS selected_by_percent,
# MAGIC       hist.transfers_in                      AS transfers_in,
# MAGIC       hist.transfers_out                     AS transfers_out
# MAGIC     FROM exploded_players
# MAGIC     LATERAL VIEW explode(player.history) AS hist
# MAGIC   )
# MAGIC   SELECT * FROM stats
# MAGIC ) AS s
# MAGIC ON t.player_id = s.player_id
# MAGIC   AND t.event_id = s.event_id
# MAGIC   AND COALESCE(t.fixture_id, -1) = COALESCE(s.fixture_id, -1)
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   t.minutes              = s.minutes,
# MAGIC   t.goals_scored         = s.goals_scored,
# MAGIC   t.assists              = s.assists,
# MAGIC   t.clean_sheets         = s.clean_sheets,
# MAGIC   t.goals_conceded       = s.goals_conceded,
# MAGIC   t.own_goals            = s.own_goals,
# MAGIC   t.penalties_saved      = s.penalties_saved,
# MAGIC   t.penalties_missed     = s.penalties_missed,
# MAGIC   t.yellow_cards         = s.yellow_cards,
# MAGIC   t.red_cards            = s.red_cards,
# MAGIC   t.saves                = s.saves,
# MAGIC   t.bonus                = s.bonus,
# MAGIC   t.bps                  = s.bps,
# MAGIC   t.influence            = s.influence,
# MAGIC   t.creativity           = s.creativity,
# MAGIC   t.threat               = s.threat,
# MAGIC   t.ict_index            = s.ict_index,
# MAGIC   t.total_points         = s.total_points,
# MAGIC   t.value                = s.value,
# MAGIC   t.selected_by_percent  = s.selected_by_percent,
# MAGIC   t.transfers_in         = s.transfers_in,
# MAGIC   t.transfers_out       = s.transfers_out
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   player_id, event_id, fixture_id, minutes, goals_scored, assists,
# MAGIC   clean_sheets, goals_conceded, own_goals, penalties_saved, penalties_missed,
# MAGIC   yellow_cards, red_cards, saves, bonus, bps, influence, creativity, threat,
# MAGIC   ict_index, total_points, value, selected_by_percent, transfers_in, transfers_out
# MAGIC )
# MAGIC VALUES (
# MAGIC   s.player_id, s.event_id, s.fixture_id, s.minutes, s.goals_scored, s.assists,
# MAGIC   s.clean_sheets, s.goals_conceded, s.own_goals, s.penalties_saved, s.penalties_missed,
# MAGIC   s.yellow_cards, s.red_cards, s.saves, s.bonus, s.bps, s.influence, s.creativity, s.threat,
# MAGIC   s.ict_index, s.total_points, s.value, s.selected_by_percent, s.transfers_in, s.transfers_out
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Verify silver table counts
# MAGIC %sql
# MAGIC -- Verify silver table row counts after transformation
# MAGIC SELECT 'dim_teams'                  AS table_name, COUNT(*) AS row_count FROM fpl.silver.dim_teams
# MAGIC UNION ALL
# MAGIC SELECT 'dim_gameweeks',              COUNT(*)           FROM fpl.silver.dim_gameweeks
# MAGIC UNION ALL
# MAGIC SELECT 'dim_players',                COUNT(*)           FROM fpl.silver.dim_players
# MAGIC UNION ALL
# MAGIC SELECT 'fact_fixtures',              COUNT(*)           FROM fpl.silver.fact_fixtures
# MAGIC UNION ALL
# MAGIC SELECT 'fact_player_gameweek_stats',  COUNT(*)           FROM fpl.silver.fact_player_gameweek_stats;

# COMMAND ----------

