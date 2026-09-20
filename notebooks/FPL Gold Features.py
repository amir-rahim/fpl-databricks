# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,FPL Gold Features
# MAGIC %md
# MAGIC # FPL Gold Features
# MAGIC
# MAGIC Builds comprehensive ML-ready feature tables from the silver tier. Run after the
# MAGIC [FPL Silver Transformation](#notebook-423499055476428) notebook.
# MAGIC
# MAGIC **Tables populated:**
# MAGIC - `fpl.gold.team_strength_rolling` — rolling team performance metrics (3/5 GW windows)
# MAGIC - `fpl.gold.player_form_features` — one row per player per gameweek with 45+ engineered features
# MAGIC
# MAGIC **Feature categories:**
# MAGIC - Rolling player stats (points, goals, assists, minutes, ICT, BPS, bonus, cards)
# MAGIC - Season-to-date totals and per-90 rates
# MAGIC - Fixture context (opponent, difficulty, home/away)
# MAGIC - Team strength (rolling goals, clean sheets, form)
# MAGIC - Transfer activity and ownership
# MAGIC - `actual_points` included as ML target variable

# COMMAND ----------

# DBTITLE 1,Gold DDL
# MAGIC %sql
# MAGIC -- Gold tier DDL: drop and recreate with expanded schemas for ML
# MAGIC DROP TABLE IF EXISTS fpl.gold.team_strength_rolling;
# MAGIC DROP TABLE IF EXISTS fpl.gold.player_form_features;
# MAGIC
# MAGIC CREATE TABLE fpl.gold.team_strength_rolling (
# MAGIC     team_id                        INT     NOT NULL,
# MAGIC     event_id                       INT     NOT NULL,
# MAGIC     rolling_3gw_goals_scored       DECIMAL(6,2),
# MAGIC     rolling_5gw_goals_scored       DECIMAL(6,2),
# MAGIC     rolling_3gw_goals_conceded     DECIMAL(6,2),
# MAGIC     rolling_5gw_goals_conceded     DECIMAL(6,2),
# MAGIC     rolling_3gw_clean_sheets       DECIMAL(6,2)  COMMENT 'Fraction of last 3 games with clean sheet',
# MAGIC     rolling_5gw_clean_sheets       DECIMAL(6,2),
# MAGIC     rolling_3gw_points             DECIMAL(6,2)   COMMENT 'Avg team points (W=3,D=1,L=0) over last 3',
# MAGIC     rolling_5gw_points             DECIMAL(6,2),
# MAGIC     rolling_3gw_home_goals_scored  DECIMAL(6,2),
# MAGIC     rolling_3gw_away_goals_scored  DECIMAL(6,2),
# MAGIC     rolling_3gw_home_goals_conceded DECIMAL(6,2),
# MAGIC     rolling_3gw_away_goals_conceded DECIMAL(6,2),
# MAGIC     games_played                   INT,
# MAGIC     total_goals_scored             INT,
# MAGIC     total_goals_conceded           INT,
# MAGIC     updated_ts                     TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Rolling team-level strength features from finished fixtures'
# MAGIC PARTITIONED BY (event_id);
# MAGIC
# MAGIC CREATE TABLE fpl.gold.player_form_features (
# MAGIC     player_id                      INT     NOT NULL,
# MAGIC     event_id                       INT     NOT NULL COMMENT 'Target gameweek',
# MAGIC     web_name                       STRING,
# MAGIC     position                       STRING,
# MAGIC     team_id                        INT,
# MAGIC     price                          DECIMAL(4,1),
# MAGIC     rolling_3gw_points_avg         DECIMAL(6,2),
# MAGIC     rolling_5gw_points_avg         DECIMAL(6,2),
# MAGIC     rolling_10gw_points_avg        DECIMAL(6,2),
# MAGIC     points_per_90                  DECIMAL(6,2),
# MAGIC     points_stddev_5gw              DECIMAL(6,2)   COMMENT 'Consistency metric',
# MAGIC     rolling_3gw_goals_avg          DECIMAL(6,2),
# MAGIC     rolling_5gw_goals_avg          DECIMAL(6,2),
# MAGIC     rolling_3gw_assists_avg        DECIMAL(6,2),
# MAGIC     rolling_5gw_assists_avg        DECIMAL(6,2),
# MAGIC     goals_assists_per_90           DECIMAL(6,2),
# MAGIC     rolling_3gw_minutes_avg        DECIMAL(6,2),
# MAGIC     rolling_5gw_minutes_avg        DECIMAL(6,2),
# MAGIC     minutes_pct_5gw                DECIMAL(5,2)   COMMENT 'Avg pct of 90 mins over last 5 GW',
# MAGIC     rolling_3gw_influence_avg      DECIMAL(6,2),
# MAGIC     rolling_3gw_creativity_avg     DECIMAL(6,2),
# MAGIC     rolling_3gw_threat_avg         DECIMAL(6,2),
# MAGIC     rolling_3gw_ict_index_avg      DECIMAL(6,2),
# MAGIC     rolling_3gw_bps_avg            DECIMAL(6,2),
# MAGIC     rolling_3gw_bonus_avg          DECIMAL(6,2),
# MAGIC     rolling_3gw_clean_sheets       DECIMAL(6,2),
# MAGIC     rolling_3gw_goals_conceded     DECIMAL(6,2),
# MAGIC     rolling_3gw_saves              DECIMAL(6,2)   COMMENT 'GKP specific',
# MAGIC     rolling_5gw_yellow_cards       DECIMAL(6,2),
# MAGIC     rolling_5gw_red_cards          DECIMAL(6,2),
# MAGIC     season_total_points            INT,
# MAGIC     season_total_minutes           INT,
# MAGIC     season_total_goals             INT,
# MAGIC     season_total_assists           INT,
# MAGIC     season_total_bps               INT,
# MAGIC     season_avg_points              DECIMAL(6,2),
# MAGIC     games_played                   INT,
# MAGIC     rolling_3gw_transfers_in       DECIMAL(10,1),
# MAGIC     rolling_3gw_transfers_out     DECIMAL(10,1),
# MAGIC     net_transfers_5gw              DECIMAL(10,1),
# MAGIC     selected_by_percent            DECIMAL(5,1),
# MAGIC     next_fixture_id                INT,
# MAGIC     next_opponent_team_id          INT,
# MAGIC     fixture_difficulty_next        INT,
# MAGIC     is_home_next                   BOOLEAN,
# MAGIC     next_opponent_conceding        DECIMAL(6,2)  COMMENT 'Opponent rolling goals conceded (higher = easier fixture)',
# MAGIC     team_rolling_3gw_goals_scored  DECIMAL(6,2),
# MAGIC     team_rolling_3gw_goals_conceded DECIMAL(6,2),
# MAGIC     team_rolling_3gw_points        DECIMAL(6,2),
# MAGIC     actual_points                  INT           COMMENT 'Actual FPL points scored this gameweek (ML target)',
# MAGIC     days_since_last_game           INT,
# MAGIC     updated_ts                     TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Comprehensive ML-ready player features — one row per player per gameweek'
# MAGIC PARTITIONED BY (event_id);

# COMMAND ----------

# DBTITLE 1,team_strength_rolling
# MAGIC %sql
# MAGIC -- team_strength_rolling: rolling team performance from finished fixtures
# MAGIC -- One row per team per gameweek where the team has a finished fixture
# MAGIC -- Rolling windows exclude current GW to prevent leakage (ROWS BETWEEN N PRECEDING AND 1 PRECEDING)
# MAGIC
# MAGIC MERGE INTO fpl.gold.team_strength_rolling AS t
# MAGIC USING (
# MAGIC   WITH team_fixtures AS (
# MAGIC     SELECT
# MAGIC       home_team_id AS team_id,
# MAGIC       event_id,
# MAGIC       home_score AS goals_scored,
# MAGIC       away_score AS goals_conceded,
# MAGIC       CASE WHEN away_score = 0 THEN 1 ELSE 0 END AS clean_sheet,
# MAGIC       CASE WHEN home_score > away_score THEN 3 WHEN home_score = away_score THEN 1 ELSE 0 END AS result_points,
# MAGIC       true AS is_home
# MAGIC     FROM fpl.silver.fact_fixtures
# MAGIC     WHERE finished = true
# MAGIC     UNION ALL
# MAGIC     SELECT
# MAGIC       away_team_id AS team_id,
# MAGIC       event_id,
# MAGIC       away_score AS goals_scored,
# MAGIC       home_score AS goals_conceded,
# MAGIC       CASE WHEN home_score = 0 THEN 1 ELSE 0 END AS clean_sheet,
# MAGIC       CASE WHEN away_score > home_score THEN 3 WHEN away_score = home_score THEN 1 ELSE 0 END AS result_points,
# MAGIC       false AS is_home
# MAGIC     FROM fpl.silver.fact_fixtures
# MAGIC     WHERE finished = true
# MAGIC   )
# MAGIC   SELECT
# MAGIC     team_id,
# MAGIC     event_id,
# MAGIC     AVG(goals_scored)   OVER w3 AS rolling_3gw_goals_scored,
# MAGIC     AVG(goals_scored)   OVER w5 AS rolling_5gw_goals_scored,
# MAGIC     AVG(goals_conceded) OVER w3 AS rolling_3gw_goals_conceded,
# MAGIC     AVG(goals_conceded) OVER w5 AS rolling_5gw_goals_conceded,
# MAGIC     AVG(clean_sheet)    OVER w3 AS rolling_3gw_clean_sheets,
# MAGIC     AVG(clean_sheet)    OVER w5 AS rolling_5gw_clean_sheets,
# MAGIC     AVG(result_points)  OVER w3 AS rolling_3gw_points,
# MAGIC     AVG(result_points)  OVER w5 AS rolling_5gw_points,
# MAGIC     AVG(CASE WHEN is_home THEN goals_scored ELSE NULL END)     OVER w3 AS rolling_3gw_home_goals_scored,
# MAGIC     AVG(CASE WHEN NOT is_home THEN goals_scored ELSE NULL END) OVER w3 AS rolling_3gw_away_goals_scored,
# MAGIC     AVG(CASE WHEN is_home THEN goals_conceded ELSE NULL END)   OVER w3 AS rolling_3gw_home_goals_conceded,
# MAGIC     AVG(CASE WHEN NOT is_home THEN goals_conceded ELSE NULL END) OVER w3 AS rolling_3gw_away_goals_conceded,
# MAGIC     COUNT(*) OVER (PARTITION BY team_id ORDER BY event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS games_played,
# MAGIC     SUM(goals_scored)   OVER (PARTITION BY team_id ORDER BY event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_goals_scored,
# MAGIC     SUM(goals_conceded) OVER (PARTITION BY team_id ORDER BY event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_goals_conceded,
# MAGIC     current_timestamp() AS updated_ts
# MAGIC   FROM team_fixtures
# MAGIC   WINDOW
# MAGIC     w3 AS (PARTITION BY team_id ORDER BY event_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING),
# MAGIC     w5 AS (PARTITION BY team_id ORDER BY event_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
# MAGIC ) AS s
# MAGIC ON t.team_id = s.team_id AND t.event_id = s.event_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# DBTITLE 1,player_form_features
# MAGIC %sql
# MAGIC -- player_form_features: comprehensive ML-ready features
# MAGIC -- Rolling windows exclude current GW to prevent leakage (ROWS BETWEEN N PRECEDING AND 1 PRECEDING)
# MAGIC -- Season-to-date uses all prior GWs (UNBOUNDED PRECEDING AND 1 PRECEDING)
# MAGIC -- actual_points = the ML target variable
# MAGIC -- CTEs placed BEFORE MERGE (CTEs inside USING subquery cause parse errors with complex queries)
# MAGIC
# MAGIC DELETE FROM fpl.gold.player_form_features;
# MAGIC
# MAGIC WITH
# MAGIC -- 1. Aggregate player stats to one row per player per gameweek
# MAGIC player_gw AS (
# MAGIC   SELECT
# MAGIC     player_id,
# MAGIC     event_id,
# MAGIC     SUM(minutes)          AS minutes,
# MAGIC     SUM(goals_scored)     AS goals_scored,
# MAGIC     SUM(assists)          AS assists,
# MAGIC     SUM(clean_sheets)     AS clean_sheets,
# MAGIC     SUM(goals_conceded)   AS goals_conceded,
# MAGIC     SUM(saves)            AS saves,
# MAGIC     SUM(bonus)            AS bonus,
# MAGIC     SUM(bps)              AS bps,
# MAGIC     SUM(yellow_cards)     AS yellow_cards,
# MAGIC     SUM(red_cards)        AS red_cards,
# MAGIC     AVG(influence)        AS influence,
# MAGIC     AVG(creativity)       AS creativity,
# MAGIC     AVG(threat)           AS threat,
# MAGIC     AVG(ict_index)        AS ict_index,
# MAGIC     SUM(total_points)     AS total_points,
# MAGIC     MAX(value)            AS value,
# MAGIC     MAX(selected_by_percent) AS selected_by_percent,
# MAGIC     SUM(transfers_in)     AS transfers_in,
# MAGIC     SUM(transfers_out)    AS transfers_out,
# MAGIC     MIN(fixture_id)       AS fixture_id
# MAGIC   FROM fpl.silver.fact_player_gameweek_stats
# MAGIC   GROUP BY player_id, event_id
# MAGIC ),
# MAGIC
# MAGIC -- 2. Player info from current SCD2 record
# MAGIC player_info AS (
# MAGIC   SELECT player_id, web_name, team_id, position, price
# MAGIC   FROM fpl.silver.dim_players
# MAGIC   WHERE is_current = true
# MAGIC ),
# MAGIC
# MAGIC -- 3. Rolling features via window functions (exclude current GW)
# MAGIC player_rolling AS (
# MAGIC   SELECT
# MAGIC     pg.player_id,
# MAGIC     pg.event_id,
# MAGIC     pi.web_name,
# MAGIC     pi.team_id,
# MAGIC     pi.position,
# MAGIC     pi.price,
# MAGIC     pg.total_points,
# MAGIC     pg.minutes,
# MAGIC     AVG(pg.total_points)   OVER w3 AS rolling_3gw_points_avg,
# MAGIC     AVG(pg.goals_scored)   OVER w3 AS rolling_3gw_goals_avg,
# MAGIC     AVG(pg.assists)        OVER w3 AS rolling_3gw_assists_avg,
# MAGIC     AVG(pg.minutes)        OVER w3 AS rolling_3gw_minutes_avg,
# MAGIC     AVG(pg.influence)      OVER w3 AS rolling_3gw_influence_avg,
# MAGIC     AVG(pg.creativity)     OVER w3 AS rolling_3gw_creativity_avg,
# MAGIC     AVG(pg.threat)         OVER w3 AS rolling_3gw_threat_avg,
# MAGIC     AVG(pg.ict_index)      OVER w3 AS rolling_3gw_ict_index_avg,
# MAGIC     AVG(pg.bps)            OVER w3 AS rolling_3gw_bps_avg,
# MAGIC     AVG(pg.bonus)          OVER w3 AS rolling_3gw_bonus_avg,
# MAGIC     AVG(pg.clean_sheets)   OVER w3 AS rolling_3gw_clean_sheets,
# MAGIC     AVG(pg.goals_conceded) OVER w3 AS rolling_3gw_goals_conceded,
# MAGIC     AVG(pg.saves)          OVER w3 AS rolling_3gw_saves,
# MAGIC     AVG(pg.transfers_in)   OVER w3 AS rolling_3gw_transfers_in,
# MAGIC     AVG(pg.transfers_out)  OVER w3 AS rolling_3gw_transfers_out,
# MAGIC     AVG(pg.total_points)   OVER w5 AS rolling_5gw_points_avg,
# MAGIC     AVG(pg.goals_scored)   OVER w5 AS rolling_5gw_goals_avg,
# MAGIC     AVG(pg.assists)        OVER w5 AS rolling_5gw_assists_avg,
# MAGIC     AVG(pg.minutes)        OVER w5 AS rolling_5gw_minutes_avg,
# MAGIC     AVG(pg.yellow_cards)   OVER w5 AS rolling_5gw_yellow_cards,
# MAGIC     AVG(pg.red_cards)      OVER w5 AS rolling_5gw_red_cards,
# MAGIC     SUM(pg.transfers_in - pg.transfers_out) OVER w5 AS net_transfers_5gw,
# MAGIC     AVG(pg.total_points)   OVER w10 AS rolling_10gw_points_avg,
# MAGIC     STDDEV(pg.total_points) OVER w5 AS points_stddev_5gw,
# MAGIC     SUM(pg.total_points)   OVER season AS season_total_points,
# MAGIC     SUM(pg.minutes)        OVER season AS season_total_minutes,
# MAGIC     SUM(pg.goals_scored)   OVER season AS season_total_goals,
# MAGIC     SUM(pg.assists)        OVER season AS season_total_assists,
# MAGIC     SUM(pg.bps)            OVER season AS season_total_bps,
# MAGIC     COUNT(*)               OVER season AS games_played,
# MAGIC     pg.selected_by_percent
# MAGIC   FROM player_gw pg
# MAGIC   JOIN player_info pi ON pg.player_id = pi.player_id
# MAGIC   WINDOW
# MAGIC     w3     AS (PARTITION BY pg.player_id ORDER BY pg.event_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING),
# MAGIC     w5     AS (PARTITION BY pg.player_id ORDER BY pg.event_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),
# MAGIC     w10    AS (PARTITION BY pg.player_id ORDER BY pg.event_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING),
# MAGIC     season AS (PARTITION BY pg.player_id ORDER BY pg.event_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
# MAGIC ),
# MAGIC
# MAGIC -- 4. Fixture context: each team's fixture per gameweek (home + away perspectives)
# MAGIC fixture_ctx AS (
# MAGIC   SELECT
# MAGIC     team_id,
# MAGIC     event_id,
# MAGIC     fixture_id,
# MAGIC     opponent_team_id,
# MAGIC     fixture_difficulty,
# MAGIC     is_home,
# MAGIC     DATEDIFF(
# MAGIC       kickoff_time,
# MAGIC       LAG(kickoff_time) OVER (PARTITION BY team_id ORDER BY event_id)
# MAGIC     ) AS days_since_last_game
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       home_team_id AS team_id,
# MAGIC       event_id,
# MAGIC       fixture_id,
# MAGIC       away_team_id AS opponent_team_id,
# MAGIC       home_difficulty AS fixture_difficulty,
# MAGIC       true AS is_home,
# MAGIC       kickoff_time
# MAGIC     FROM fpl.silver.fact_fixtures
# MAGIC     UNION ALL
# MAGIC     SELECT
# MAGIC       away_team_id AS team_id,
# MAGIC       event_id,
# MAGIC       fixture_id,
# MAGIC       home_team_id AS opponent_team_id,
# MAGIC       away_difficulty AS fixture_difficulty,
# MAGIC       false AS is_home,
# MAGIC       kickoff_time
# MAGIC     FROM fpl.silver.fact_fixtures
# MAGIC   )
# MAGIC ),
# MAGIC
# MAGIC -- 5. Team strength "as of" each event_id (latest team_strength at or before)
# MAGIC team_events AS (
# MAGIC   SELECT DISTINCT team_id, event_id FROM player_rolling
# MAGIC ),
# MAGIC team_strength_asof AS (
# MAGIC   SELECT
# MAGIC     te.team_id,
# MAGIC     te.event_id,
# MAGIC     ts.rolling_3gw_goals_scored,
# MAGIC     ts.rolling_3gw_goals_conceded,
# MAGIC     ts.rolling_3gw_points,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY te.team_id, te.event_id ORDER BY ts.event_id DESC) AS rn
# MAGIC   FROM team_events te
# MAGIC   LEFT JOIN fpl.gold.team_strength_rolling ts
# MAGIC     ON te.team_id = ts.team_id AND ts.event_id <= te.event_id
# MAGIC ),
# MAGIC
# MAGIC -- 6. Final SELECT joining all context
# MAGIC final_source AS (
# MAGIC   SELECT
# MAGIC     pr.player_id,
# MAGIC     pr.event_id,
# MAGIC     pr.web_name,
# MAGIC     pr.position,
# MAGIC     pr.team_id,
# MAGIC     pr.price,
# MAGIC     pr.rolling_3gw_points_avg,
# MAGIC     pr.rolling_5gw_points_avg,
# MAGIC     pr.rolling_10gw_points_avg,
# MAGIC     CASE WHEN pr.season_total_minutes > 0
# MAGIC          THEN ROUND(pr.season_total_points * 90.0 / pr.season_total_minutes, 2)
# MAGIC          ELSE NULL END AS points_per_90,
# MAGIC     pr.points_stddev_5gw,
# MAGIC     pr.rolling_3gw_goals_avg,
# MAGIC     pr.rolling_5gw_goals_avg,
# MAGIC     pr.rolling_3gw_assists_avg,
# MAGIC     pr.rolling_5gw_assists_avg,
# MAGIC     CASE WHEN pr.season_total_minutes > 0
# MAGIC          THEN ROUND((pr.season_total_goals + pr.season_total_assists) * 90.0 / pr.season_total_minutes, 2)
# MAGIC          ELSE NULL END AS goals_assists_per_90,
# MAGIC     pr.rolling_3gw_minutes_avg,
# MAGIC     pr.rolling_5gw_minutes_avg,
# MAGIC     CASE WHEN pr.rolling_5gw_minutes_avg IS NOT NULL
# MAGIC          THEN ROUND(pr.rolling_5gw_minutes_avg / 90.0 * 100, 2)
# MAGIC          ELSE NULL END AS minutes_pct_5gw,
# MAGIC     pr.rolling_3gw_influence_avg,
# MAGIC     pr.rolling_3gw_creativity_avg,
# MAGIC     pr.rolling_3gw_threat_avg,
# MAGIC     pr.rolling_3gw_ict_index_avg,
# MAGIC     pr.rolling_3gw_bps_avg,
# MAGIC     pr.rolling_3gw_bonus_avg,
# MAGIC     pr.rolling_3gw_clean_sheets,
# MAGIC     pr.rolling_3gw_goals_conceded,
# MAGIC     pr.rolling_3gw_saves,
# MAGIC     pr.rolling_5gw_yellow_cards,
# MAGIC     pr.rolling_5gw_red_cards,
# MAGIC     pr.season_total_points,
# MAGIC     pr.season_total_minutes,
# MAGIC     pr.season_total_goals,
# MAGIC     pr.season_total_assists,
# MAGIC     pr.season_total_bps,
# MAGIC     CASE WHEN pr.games_played > 0
# MAGIC          THEN ROUND(pr.season_total_points * 1.0 / pr.games_played, 2)
# MAGIC          ELSE NULL END AS season_avg_points,
# MAGIC     pr.games_played,
# MAGIC     pr.rolling_3gw_transfers_in,
# MAGIC     pr.rolling_3gw_transfers_out,
# MAGIC     pr.net_transfers_5gw,
# MAGIC     pr.selected_by_percent,
# MAGIC     fc.fixture_id           AS next_fixture_id,
# MAGIC     fc.opponent_team_id     AS next_opponent_team_id,
# MAGIC     fc.fixture_difficulty   AS fixture_difficulty_next,
# MAGIC     fc.is_home              AS is_home_next,
# MAGIC     ots.rolling_3gw_goals_conceded AS next_opponent_conceding,
# MAGIC     ts_asof.rolling_3gw_goals_scored  AS team_rolling_3gw_goals_scored,
# MAGIC     ts_asof.rolling_3gw_goals_conceded AS team_rolling_3gw_goals_conceded,
# MAGIC     ts_asof.rolling_3gw_points         AS team_rolling_3gw_points,
# MAGIC     pr.total_points         AS actual_points,
# MAGIC     fc.days_since_last_game,
# MAGIC     current_timestamp()     AS updated_ts
# MAGIC   FROM player_rolling pr
# MAGIC   LEFT JOIN fixture_ctx fc ON pr.team_id = fc.team_id AND pr.event_id = fc.event_id
# MAGIC   LEFT JOIN team_strength_asof ts_asof ON pr.team_id = ts_asof.team_id AND pr.event_id = ts_asof.event_id AND ts_asof.rn = 1
# MAGIC   LEFT JOIN team_strength_asof ots ON fc.opponent_team_id = ots.team_id AND pr.event_id = ots.event_id AND ots.rn = 1
# MAGIC )
# MAGIC
# MAGIC MERGE INTO fpl.gold.player_form_features AS t
# MAGIC USING final_source AS s
# MAGIC ON t.player_id = s.player_id AND t.event_id = s.event_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# DBTITLE 1,Verify gold tables
# MAGIC %sql
# MAGIC -- Verify gold table row counts
# MAGIC SELECT 'team_strength_rolling'   AS table_name, COUNT(*) AS row_count FROM fpl.gold.team_strength_rolling
# MAGIC UNION ALL
# MAGIC SELECT 'player_form_features',    COUNT(*)        FROM fpl.gold.player_form_features;
# MAGIC
# MAGIC -- Sample: top 10 players by rolling 3gw points avg for latest finished gameweek
# MAGIC SELECT player_id, web_name, position, price,
# MAGIC        rolling_3gw_points_avg, rolling_5gw_points_avg, points_per_90,
# MAGIC        rolling_3gw_goals_avg, rolling_3gw_assists_avg, rolling_3gw_minutes_avg,
# MAGIC        fixture_difficulty_next, is_home_next, actual_points,
# MAGIC        team_rolling_3gw_points
# MAGIC FROM fpl.gold.player_form_features
# MAGIC WHERE event_id = 5
# MAGIC ORDER BY rolling_3gw_points_avg DESC NULLS LAST
# MAGIC LIMIT 10;