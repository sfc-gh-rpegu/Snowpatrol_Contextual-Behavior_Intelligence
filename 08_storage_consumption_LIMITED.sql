-- =====================================================
-- Data Write Activity by Role (LIMITED ACCESS VERSION)
-- Purpose: Track data write activity as proxy for storage growth
-- Note: Without INFORMATION_SCHEMA or TABLE_STORAGE_METRICS access
-- =====================================================

USE DATABASE GRP17_LAB_DB;
USE SCHEMA ps_user_behavior;
USE WAREHOUSE grp17_cortex_wh;

-- =====================================================
-- View 1: Data Write Activity by Role
-- This is the BEST we can do without storage metadata access
-- =====================================================
CREATE OR REPLACE VIEW vw_data_write_activity_by_role AS
WITH write_operations AS (
    SELECT 
        role_name,
        user_name,
        database_name,
        DATE(start_time) AS activity_date,
        query_type,
        COUNT(DISTINCT query_id) AS query_count,
        SUM(COALESCE(bytes_written, 0)) AS bytes_written,
        SUM(COALESCE(rows_inserted, 0)) AS rows_inserted,
        SUM(COALESCE(rows_updated, 0)) AS rows_updated,
        SUM(COALESCE(rows_deleted, 0)) AS rows_deleted
    FROM ps_user_behavior.query_history
    WHERE query_type IN (
            'INSERT', 
            'UPDATE', 
            'MERGE',
            'CREATE_TABLE', 
            'CREATE_TABLE_AS_SELECT',
            'COPY',
            'CREATE_MATERIALIZED_VIEW'
        )
        AND role_name IS NOT NULL
    GROUP BY 
        role_name,
        user_name,
        database_name,
        DATE(start_time),
        query_type
),
daily_role_writes AS (
    SELECT 
        role_name,
        activity_date,
        database_name,
        COUNT(DISTINCT user_name) AS active_users,
        SUM(query_count) AS total_write_queries,
        SUM(bytes_written) / (1024.0 * 1024.0 * 1024.0) AS gb_written,
        SUM(rows_inserted) AS total_rows_inserted,
        SUM(rows_updated) AS total_rows_updated,
        SUM(rows_deleted) AS total_rows_deleted
    FROM write_operations
    GROUP BY role_name, activity_date, database_name
),
trend_calculation AS (
    SELECT 
        *,
        LAG(gb_written, 1) OVER (PARTITION BY role_name ORDER BY activity_date) AS prev_day_gb_written,
        LAG(gb_written, 7) OVER (PARTITION BY role_name ORDER BY activity_date) AS week_ago_gb_written,
        AVG(gb_written) OVER (
            PARTITION BY role_name 
            ORDER BY activity_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS gb_written_7day_avg,
        AVG(gb_written) OVER (
            PARTITION BY role_name 
            ORDER BY activity_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS gb_written_30day_avg
    FROM daily_role_writes
)
SELECT 
    role_name,
    activity_date,
    database_name,
    active_users,
    total_write_queries,
    ROUND(gb_written, 2) AS gb_written_daily,
    total_rows_inserted,
    total_rows_updated,
    total_rows_deleted,
    ROUND(gb_written_7day_avg, 2) AS gb_written_7day_avg,
    ROUND(gb_written_30day_avg, 2) AS gb_written_30day_avg,
    -- Calculate growth percentages
    CASE 
        WHEN prev_day_gb_written > 0 
        THEN ROUND(((gb_written - prev_day_gb_written) / prev_day_gb_written * 100), 1)
        ELSE 0
    END AS day_over_day_change_pct,
    CASE 
        WHEN week_ago_gb_written > 0 
        THEN ROUND(((gb_written - week_ago_gb_written) / week_ago_gb_written * 100), 1)
        ELSE 0
    END AS week_over_week_change_pct,
    CASE 
        WHEN gb_written_7day_avg > 0
        THEN ROUND(((gb_written - gb_written_7day_avg) / gb_written_7day_avg * 100), 1)
        ELSE 0
    END AS deviation_from_7day_avg_pct,
    -- Growth classification
    CASE 
        WHEN gb_written > gb_written_7day_avg * 3 THEN 'Extreme Write Growth'
        WHEN gb_written > gb_written_7day_avg * 2 THEN 'Significant Write Growth'
        WHEN gb_written > gb_written_7day_avg * 1.5 THEN 'Moderate Write Growth'
        WHEN gb_written < gb_written_7day_avg * 0.5 THEN 'Significant Decrease'
        ELSE 'Normal'
    END AS write_activity_classification,
    -- Recommendations
    CASE 
        WHEN gb_written > gb_written_7day_avg * 3 THEN 'URGENT: Extreme data write activity - investigate immediately'
        WHEN gb_written > gb_written_7day_avg * 2 THEN 'HIGH: Significant increase in data writes - review data pipelines'
        WHEN gb_written > gb_written_7day_avg * 1.5 THEN 'MEDIUM: Monitor continued growth'
        ELSE 'LOW: Normal write activity'
    END AS recommendation
FROM trend_calculation
ORDER BY activity_date DESC, gb_written_daily DESC;

GRANT SELECT ON VIEW vw_data_write_activity_by_role TO ROLE grp17_lab_role;

-- =====================================================
-- View 2: Role Write Summary (for agent queries)
-- =====================================================
CREATE OR REPLACE VIEW vw_role_write_summary AS
SELECT 
    role_name,
    SUM(gb_written_daily) AS total_gb_written_last_7days,
    AVG(gb_written_daily) AS avg_gb_per_day,
    SUM(total_write_queries) AS total_write_queries,
    COUNT(DISTINCT database_name) AS databases_affected,
    AVG(week_over_week_change_pct) AS avg_weekly_growth_pct,
    MAX(write_activity_classification) AS highest_classification,
    -- Rank roles
    RANK() OVER (ORDER BY SUM(gb_written_daily) DESC) AS write_volume_rank,
    RANK() OVER (ORDER BY AVG(week_over_week_change_pct) DESC) AS growth_rate_rank
FROM vw_data_write_activity_by_role
WHERE activity_date >= CURRENT_DATE() - 7
GROUP BY role_name
ORDER BY total_gb_written_last_7days DESC;

GRANT SELECT ON VIEW vw_role_write_summary TO ROLE grp17_lab_role;

-- =====================================================
-- View 3: Data Consumption (Scanned) by Role
-- Additional context - who's READING the most data
-- =====================================================
CREATE OR REPLACE VIEW vw_data_consumption_by_role AS
WITH role_data_usage AS (
    SELECT 
        role_name,
        user_name,
        database_name,
        DATE(start_time) AS usage_date,
        COUNT(DISTINCT query_id) AS query_count,
        SUM(bytes_scanned) AS total_bytes_scanned,
        AVG(bytes_scanned) AS avg_bytes_per_query
    FROM ps_user_behavior.query_history
    WHERE role_name IS NOT NULL
    GROUP BY 
        role_name,
        user_name,
        database_name,
        DATE(start_time)
),
role_aggregation AS (
    SELECT 
        role_name,
        usage_date,
        COUNT(DISTINCT user_name) AS active_users,
        COUNT(DISTINCT database_name) AS databases_accessed,
        SUM(query_count) AS total_queries,
        SUM(total_bytes_scanned) / (1024.0 * 1024.0 * 1024.0) AS gb_scanned
    FROM role_data_usage
    GROUP BY role_name, usage_date
)
SELECT 
    role_name,
    usage_date,
    active_users,
    databases_accessed,
    total_queries,
    ROUND(gb_scanned, 2) AS gb_data_scanned,
    AVG(gb_scanned) OVER (
        PARTITION BY role_name 
        ORDER BY usage_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS gb_scanned_7day_avg
FROM role_aggregation
ORDER BY usage_date DESC, gb_data_scanned DESC;

GRANT SELECT ON VIEW vw_data_consumption_by_role TO ROLE grp17_lab_role;

-- =====================================================
-- Test Queries
-- =====================================================

-- Test 1: Which role is writing the most data (proxy for storage growth)?
SELECT * FROM vw_role_write_summary 
ORDER BY total_gb_written_last_7days DESC 
LIMIT 10;

-- Test 2: Daily write activity trends
SELECT * FROM vw_data_write_activity_by_role
WHERE activity_date >= CURRENT_DATE() - 7
ORDER BY gb_written_daily DESC
LIMIT 20;

-- Test 3: Answer the question: "Which role is contributing to storage increase?"
SELECT 
    role_name,
    total_gb_written_last_7days AS gb_added_last_week,
    avg_weekly_growth_pct AS growth_rate,
    highest_classification,
    write_volume_rank,
    growth_rate_rank
FROM vw_role_write_summary
WHERE highest_classification IN ('Extreme Write Growth', 'Significant Write Growth')
ORDER BY total_gb_written_last_7days DESC;

-- =====================================================
-- IMPORTANT NOTE FOR AGENT
-- =====================================================
/*
LIMITATION: Without access to INFORMATION_SCHEMA.TABLES or TABLE_STORAGE_METRICS,
we cannot determine:
- Actual current storage size by role
- Historical storage growth in absolute terms

WHAT WE CAN DETERMINE:
- Which roles are WRITING the most data (bytes_written)
- Trends in write activity (increasing/decreasing)
- Relative comparison of data write activity across roles

INTERPRETATION:
- High bytes_written typically correlates with storage growth
- Roles with increasing write activity likely contributing to storage increase
- But actual storage impact depends on factors we can't see:
  * Data retention policies
  * Table drops/truncates
  * Time travel and fail-safe settings

AGENT SHOULD SAY:
"Based on data write activity, ROLE_X is likely contributing most to storage 
growth with N GB written this week. However, actual storage size confirmation 
requires database metadata access."
*/

