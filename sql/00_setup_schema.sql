-- =====================================================================
-- FPL Databricks project — schema setup
-- Run this once (or via a Databricks Asset Bundle deploy step later)
-- in a Free Edition SQL editor / notebook against your metastore.
-- =====================================================================

-- ---------------------------------------------------------------------
-- Catalog & schemas
-- ---------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS fpl;

CREATE SCHEMA IF NOT EXISTS fpl.bronze;
CREATE SCHEMA IF NOT EXISTS fpl.silver;
CREATE SCHEMA IF NOT EXISTS fpl.gold;

-- Landing volume: GitHub Actions drops raw JSON here from outside Databricks,
-- sidestepping the Free Edition outbound-internet restriction on compute.
CREATE VOLUME IF NOT EXISTS fpl.bronze.landing;

-- =====================================================================
-- BRONZE — raw, append-only, minimal typing. One row per ingestion run.
-- =====================================================================

CREATE TABLE IF NOT EXISTS fpl.bronze.bootstrap_static (
    ingest_ts        TIMESTAMP   COMMENT 'When this run was ingested',
    source_file       STRING      COMMENT 'Path in the landing volume',
    payload           STRING      COMMENT 'Raw JSON payload as text'
) USING DELTA
COMMENT 'Raw bootstrap-static snapshots (players, teams, events, positions)';

CREATE TABLE IF NOT EXISTS fpl.bronze.fixtures (
    ingest_ts        TIMESTAMP,
    source_file       STRING,
    payload           STRING
) USING DELTA
COMMENT 'Raw /fixtures snapshots';

CREATE TABLE IF NOT EXISTS fpl.bronze.player_gameweek_history (
    ingest_ts        TIMESTAMP,
    source_file       STRING,
    gameweek          INT         COMMENT 'Current event id at ingestion time',
    payload           STRING      COMMENT 'Raw JSON: one blob containing all element-summary responses'
) USING DELTA
COMMENT 'Raw per-player element-summary history, batched per ingestion run';

CREATE TABLE IF NOT EXISTS fpl.bronze.xg_match_stats (
    ingest_ts        TIMESTAMP,
    source_file       STRING,
    payload           STRING      COMMENT 'Raw JSON from your xG source'
) USING DELTA
COMMENT 'Raw xG/xA data landed from whatever external source you are using';

-- =====================================================================
-- SILVER — typed, conformed, deduplicated
-- =====================================================================

CREATE TABLE IF NOT EXISTS fpl.silver.dim_teams (
    team_id           INT         NOT NULL,
    name              STRING,
    short_name        STRING,
    strength_overall  INT,
    strength_attack_home INT,
    strength_attack_away INT,
    strength_defence_home INT,
    strength_defence_away INT,
    updated_ts        TIMESTAMP
) USING DELTA
COMMENT 'Conformed team dimension';

CREATE TABLE IF NOT EXISTS fpl.silver.dim_players (
    player_id         INT         NOT NULL,
    web_name          STRING,
    full_name         STRING,
    team_id           INT,
    position          STRING      COMMENT 'GKP / DEF / MID / FWD',
    price             DECIMAL(4,1),
    valid_from        TIMESTAMP,
    valid_to          TIMESTAMP,
    is_current        BOOLEAN
) USING DELTA
COMMENT 'SCD2 player dimension — new row whenever team, position, or price changes';

CREATE TABLE IF NOT EXISTS fpl.silver.dim_gameweeks (
    event_id          INT         NOT NULL,
    name              STRING,
    deadline_time     TIMESTAMP,
    is_current        BOOLEAN,
    is_next           BOOLEAN,
    finished          BOOLEAN
) USING DELTA
COMMENT 'Gameweek dimension';

CREATE TABLE IF NOT EXISTS fpl.silver.fact_fixtures (
    fixture_id        INT         NOT NULL,
    event_id          INT,
    home_team_id      INT,
    away_team_id      INT,
    kickoff_time      TIMESTAMP,
    home_difficulty   INT,
    away_difficulty   INT,
    finished          BOOLEAN,
    home_score        INT,
    away_score        INT
) USING DELTA
COMMENT 'Fixture-level facts';

CREATE TABLE IF NOT EXISTS fpl.silver.fact_player_gameweek_stats (
    player_id         INT         NOT NULL,
    event_id          INT         NOT NULL,
    fixture_id        INT,
    minutes           INT,
    goals_scored      INT,
    assists           INT,
    clean_sheets      INT,
    goals_conceded    INT,
    own_goals         INT,
    penalties_saved   INT,
    penalties_missed  INT,
    yellow_cards      INT,
    red_cards         INT,
    saves             INT,
    bonus             INT,
    bps               INT,
    influence         DECIMAL(6,1),
    creativity        DECIMAL(6,1),
    threat            DECIMAL(6,1),
    ict_index         DECIMAL(6,1),
    total_points      INT,
    value             DECIMAL(4,1)  COMMENT 'Player price at time of this gameweek',
    selected_by_percent DECIMAL(5,1),
    transfers_in      INT,
    transfers_out     INT
) USING DELTA
COMMENT 'Core fact table — one row per player per gameweek'
PARTITIONED BY (event_id);

CREATE TABLE IF NOT EXISTS fpl.silver.map_player_external_ids (
    player_id         INT         NOT NULL COMMENT 'FPL element id',
    external_id       STRING      COMMENT 'Id in your xG source (e.g. Understat/FBref)',
    external_source   STRING
) USING DELTA
COMMENT 'Bridge table between FPL player ids and your xG source ids';

CREATE TABLE IF NOT EXISTS fpl.silver.fact_player_xg_match (
    player_id         INT         NOT NULL,
    fixture_id        INT,
    match_date        DATE,
    xg                DECIMAL(6,3),
    xa                DECIMAL(6,3),
    shots             INT,
    key_passes        INT
) USING DELTA
COMMENT 'xG/xA facts, joined to FPL player_id via map_player_external_ids'
PARTITIONED BY (fixture_id);

-- =====================================================================
-- GOLD — wide, feature-ready tables
-- =====================================================================

CREATE TABLE IF NOT EXISTS fpl.gold.player_form_features (
    player_id             INT     NOT NULL,
    event_id              INT     NOT NULL COMMENT 'Target gameweek being predicted',
    rolling_3gw_points_avg DECIMAL(6,2),
    rolling_5gw_points_avg DECIMAL(6,2),
    rolling_3gw_xg_avg     DECIMAL(6,3),
    rolling_3gw_xa_avg     DECIMAL(6,3),
    minutes_last_3gw       INT,
    price                  DECIMAL(4,1),
    selected_by_percent    DECIMAL(5,1),
    fixture_difficulty_next INT,
    is_home_next            BOOLEAN,
    days_since_last_game    INT,
    position                 STRING
) USING DELTA
COMMENT 'Feature table consumed directly by the prediction model'
PARTITIONED BY (event_id);

CREATE TABLE IF NOT EXISTS fpl.gold.team_strength_rolling (
    team_id                  INT   NOT NULL,
    event_id                 INT   NOT NULL,
    rolling_goals_scored     DECIMAL(6,2),
    rolling_goals_conceded   DECIMAL(6,2),
    rolling_xg_for           DECIMAL(6,3),
    rolling_xg_against       DECIMAL(6,3)
) USING DELTA
COMMENT 'Rolling team-level strength features'
PARTITIONED BY (event_id);

CREATE TABLE IF NOT EXISTS fpl.gold.predictions (
    player_id         INT       NOT NULL,
    event_id          INT       NOT NULL,
    predicted_points  DECIMAL(6,2),
    actual_points     INT       COMMENT 'Backfilled once the gameweek finishes',
    model_run_id      STRING    COMMENT 'MLflow run id',
    predicted_at      TIMESTAMP
) USING DELTA
COMMENT 'Model output, backtestable against actuals once populated'
PARTITIONED BY (event_id);