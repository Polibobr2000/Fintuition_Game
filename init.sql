-- =========================================================================
-- 1. ASSET REFERENCE DIRECTORY (Dimension Table)
-- =========================================================================
CREATE TABLE assets (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL,      
    name VARCHAR(50) NOT NULL  
);

-- Seed internal product asset identifiers immediately
INSERT INTO assets (ticker, name) VALUES 
('BTC/USD', 'Bitcoin'), -- BTC/USD!
('USD/RUB', 'US Dollar / Russian Ruble'),
('EUR/RUB', 'Euro / Russian Ruble')
ON CONFLICT (ticker) DO NOTHING;

-- =========================================================================
-- 2. HISTORICAL PRICE RECORD (Fact Table)
-- =========================================================================
CREATE TABLE price_history (
    id SERIAL PRIMARY KEY,
    asset_id INT REFERENCES assets(id) ON DELETE CASCADE,
    price NUMERIC(12, 4) NOT NULL,
    price_date DATE NOT NULL,
    CONSTRAINT unique_asset_date UNIQUE (asset_id, price_date) 
);

-- =========================================================================
-- 3. USER BASE REGISTRY (Dimension Table / Current Profile State)
-- =========================================================================
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT UNIQUE NOT NULL,
    username VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    is_subscribed BOOLEAN DEFAULT FALSE,
    last_poll_sent_date DATE
);

-- =========================================================================
-- 4. FORECAST HISTORY TRACKER (High-Frequency Transactional Table)
-- =========================================================================
CREATE TABLE forecasts (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id) ON DELETE CASCADE,
    asset_id INT REFERENCES assets(id) ON DELETE CASCADE,
    forecast_direction SMALLINT,     -- 1 (UP), 0 (DOWN), NULL (SKIPPED)
    forecast_ts TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, 
    target_date DATE NOT NULL,       
    is_correct BOOLEAN DEFAULT NULL, 
    CONSTRAINT unique_user_asset_target UNIQUE (user_id, asset_id, target_date)
);

-- =========================================================================
-- 5. HISTORICAL USER SUBSCRIPTION EVENT LOGS (Append-Only Log Table)
-- =========================================================================
CREATE TABLE action_log (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id) ON DELETE CASCADE, -- Fixed context mapping to 'users'
    action_type TEXT NOT NULL, 
    action_ts TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- =========================================================================
-- 6. PERFORMANCE TUNING INDEX LAYER
-- =========================================================================
CREATE INDEX idx_forecasts_target_date ON forecasts(target_date);
CREATE INDEX idx_price_history_date ON price_history(price_date);

-- =========================================================================
-- 7. MATERIALIZED VIEWS LAYER
-- =========================================================================

-- View A: Feed the 3 Line Graphs
CREATE MATERIALIZED VIEW mv_datalens_prices AS
SELECT 
    p.price_date AS dashboard_date,
    a.ticker,
    a.name AS asset_name,
    p.price AS asset_close_price
FROM price_history p
JOIN assets a ON p.asset_id = a.id;

CREATE UNIQUE INDEX idx_mv_prices_unique ON mv_datalens_prices (dashboard_date, ticker);

-- =========================================================================
-- My Dashboard
-- =========================================================================

-- View C: Daily Turnout Analytics
CREATE MATERIALIZED VIEW mv_bi_daily_turnout AS
WITH voting_dates AS (
    SELECT DISTINCT forecast_ts::DATE AS vote_date 
    FROM forecasts
),
numbered_toggles AS (
    SELECT 
        user_id,
        action_ts,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY action_ts ASC) AS toggle_sequence
    FROM action_log
    WHERE action_type = 'Button: toggle_subscription'
),
daily_subscriber_state AS (
    SELECT 
        vd.vote_date,
        u.id AS user_id,
        (
            SELECT t.toggle_sequence 
            FROM numbered_toggles t 
            WHERE t.user_id = u.id 
              AND t.action_ts <= (vd.vote_date + TIME '21:00:00')::TIMESTAMP WITH TIME ZONE
            ORDER BY t.action_ts DESC 
            LIMIT 1
        ) AS last_toggle_sequence
    FROM voting_dates vd
    CROSS JOIN users u
    WHERE u.created_at::DATE <= vd.vote_date
),
historical_subscriber_counts AS (
    SELECT 
        dss.vote_date,
        COUNT(DISTINCT dss.user_id) AS total_subscribed_users
    FROM daily_subscriber_state dss
    JOIN users u ON dss.user_id = u.id
    WHERE 
        (dss.last_toggle_sequence IS NOT NULL AND dss.last_toggle_sequence % 2 != 0)
        OR 
        (dss.last_toggle_sequence IS NULL AND u.is_subscribed = TRUE)
    GROUP BY dss.vote_date
)
SELECT 
    f.forecast_ts::DATE AS dashboard_date,
    COUNT(DISTINCT f.user_id) AS unique_voters,
    COALESCE(hsc.total_subscribed_users, 0) AS total_subscribed_users,
    ROUND(
        (COUNT(DISTINCT f.user_id)::NUMERIC / 
         NULLIF(COALESCE(hsc.total_subscribed_users, 0), 0)) * 100, 
        2
    ) AS turnout_percentage
FROM forecasts f
LEFT JOIN historical_subscriber_counts hsc ON f.forecast_ts::DATE = hsc.vote_date
GROUP BY f.forecast_ts::DATE, hsc.total_subscribed_users;

CREATE UNIQUE INDEX idx_mv_turnout_date ON mv_bi_daily_turnout (dashboard_date);

-- View D: Daily Feature adoption
CREATE MATERIALIZED VIEW mv_bi_feature_adoption AS
WITH daily_totals AS (
    SELECT 
        action_ts::DATE AS dashboard_date,
        COUNT(DISTINCT user_id) AS total_active_users_today
    FROM action_log
    GROUP BY action_ts::DATE
)
SELECT 
    al.action_ts::DATE AS dashboard_date,
    al.action_type,
    COUNT(DISTINCT al.user_id) AS unique_feature_users,
    dt.total_active_users_today,
    ROUND((COUNT(DISTINCT al.user_id)::NUMERIC / NULLIF(dt.total_active_users_today, 0)) * 100, 2) AS adoption_depth_percentage
FROM action_log al
JOIN daily_totals dt ON al.action_ts::DATE = dt.dashboard_date
GROUP BY al.action_ts::DATE, al.action_type, dt.total_active_users_today;

CREATE UNIQUE INDEX idx_mv_adoption_unique ON mv_bi_feature_adoption (dashboard_date, action_type);

-- View E: Response Velocity
CREATE MATERIALIZED VIEW mv_bi_response_velocity AS
SELECT 
    f.id AS forecast_id,
    f.user_id,
    f.forecast_ts,
    a.ticker
FROM forecasts f
JOIN assets a ON f.asset_id = a.id;

CREATE UNIQUE INDEX idx_mv_velocity_unique ON mv_bi_response_velocity (forecast_id);
CREATE INDEX idx_mv_velocity_query ON mv_bi_response_velocity (forecast_ts, ticker);

-- View F: Duplicate Interaction Rate
CREATE MATERIALIZED VIEW mv_bi_interface_spam AS
WITH labeled_logs AS (
    SELECT 
        user_id,
        action_ts::DATE AS dashboard_date,
        CASE 
            WHEN action_type = LAG(action_type) OVER (PARTITION BY user_id ORDER BY action_ts)
             AND action_ts - LAG(action_ts) OVER (PARTITION BY user_id ORDER BY action_ts) <= INTERVAL '2 seconds'
            THEN 1 
            ELSE 0 
        END AS is_spam
    FROM action_log
)
SELECT 
    dashboard_date,
    SUM(is_spam) AS total_daily_spam_events,
    COUNT(*) AS total_daily_actions, -- 🌟 Added to capture total click volume
    ROUND(
        (SUM(is_spam)::NUMERIC / NULLIF(COUNT(*), 0)) * 100, 
        2
    ) AS spam_interaction_rate_percentage -- 🌟 This is your true Friction Rate!
FROM labeled_logs
GROUP BY dashboard_date;

CREATE UNIQUE INDEX idx_mv_spam_date ON mv_bi_interface_spam (dashboard_date);

-- View G: Weekly user forcasts/accuracy
CREATE MATERIALIZED VIEW mv_datalens_forecasts_accuracy AS
SELECT 
    DATE_TRUNC('week', f.target_date)::DATE AS evaluation_week,
    f.user_id,
    u.username,
    u.telegram_id,
    COUNT(f.id) AS total_records,
    COUNT(CASE WHEN f.is_correct = TRUE THEN 1 END) AS correct_count,
    COUNT(CASE WHEN f.is_correct = FALSE THEN 1 END) AS incorrect_count,
    COUNT(CASE WHEN f.forecast_direction IS NULL THEN 1 END) AS skipped_count
FROM forecasts f
JOIN users u ON f.user_id = u.id
GROUP BY DATE_TRUNC('week', f.target_date)::DATE, f.user_id, u.username, u.telegram_id;

CREATE UNIQUE INDEX idx_mv_dl_forecasts_acc_uid ON mv_datalens_forecasts_accuracy (evaluation_week, user_id);

-- =========================================================================
-- 8. AIRFLOW METADATA STORAGE SYSTEM ENGINE
-- =========================================================================
SELECT 'CREATE DATABASE airflow_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db');
\gexec
\r

-- =========================================================================
-- 9. SUPERSET METADATA STORAGE SYSTEM ENGINE
-- =========================================================================
SELECT 'CREATE DATABASE superset_metadata'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset_metadata');
\gexec
