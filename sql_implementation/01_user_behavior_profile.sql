-- =====================================================
-- User Behavior Profile View
-- Purpose: Baseline user behavior metrics with z-scores
-- =====================================================

USE DATABASE GRP17_LAB_DB;
USE SCHEMA ps_user_behavior;
USE WAREHOUSE grp17_cortex_wh;

CREATE OR REPLACE VIEW vw_user_behavior_profile AS
WITH user_metrics AS (
    SELECT 
        user_name,
        role_name,
        DATE(start_time) AS activity_date,
        COUNT(DISTINCT query_id) AS query_count,
        SUM(execution_time) AS total_execution_time,
        SUM(total_elapsed_time) AS total_elapsed_time,
        COUNT(DISTINCT warehouse_name) AS warehouses_used,
        SUM(bytes_scanned) AS total_bytes_scanned,
        SUM(rows_produced) AS total_rows_produced,
        COUNT(CASE WHEN error_code IS NOT NULL THEN 1 END) AS error_count,
        AVG(execution_time) AS avg_execution_time,
        MAX(execution_time) AS max_execution_time,
        COUNT(DISTINCT database_name) AS databases_accessed,
        COUNT(DISTINCT schema_name) AS schemas_accessed
    FROM query_history
    WHERE user_name IS NOT NULL
    GROUP BY user_name, role_name, DATE(start_time)
    
),
baseline_metrics AS (
    SELECT 
        user_name,
        role_name,
        AVG(query_count) AS avg_daily_queries,
        STDDEV(query_count) AS stddev_daily_queries,
        AVG(total_execution_time) AS avg_daily_exec_time,
        STDDEV(total_execution_time) AS stddev_daily_exec_time,
        AVG(total_bytes_scanned) AS avg_daily_bytes_scanned,
        STDDEV(total_bytes_scanned) AS stddev_daily_bytes_scanned,
        AVG(error_count) AS avg_daily_errors,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY query_count) AS p95_daily_queries,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_execution_time) AS p95_daily_exec_time
    FROM user_metrics
    WHERE activity_date IS NOT NULL
    GROUP BY user_name, role_name
)
--select * from baseline_metrics;
SELECT 
    um.*,
    bm.avg_daily_queries,
    bm.stddev_daily_queries,
    bm.avg_daily_exec_time,
    bm.stddev_daily_exec_time,
    bm.avg_daily_bytes_scanned,
    bm.stddev_daily_bytes_scanned,
    bm.avg_daily_errors,
    bm.p95_daily_queries,
    bm.p95_daily_exec_time,
    -- Calculate Z-scores for anomaly detection
    CASE 
        WHEN bm.stddev_daily_queries > 0 
        THEN (um.query_count - bm.avg_daily_queries) / bm.stddev_daily_queries 
        ELSE 0 
    END AS query_count_zscore,
    CASE 
        WHEN bm.stddev_daily_exec_time > 0 
        THEN (um.total_execution_time - bm.avg_daily_exec_time) / bm.stddev_daily_exec_time 
        ELSE 0 
    END AS exec_time_zscore,
    CASE 
        WHEN bm.stddev_daily_bytes_scanned > 0 
        THEN (um.total_bytes_scanned - bm.avg_daily_bytes_scanned) / bm.stddev_daily_bytes_scanned 
        ELSE 0 
    END AS bytes_scanned_zscore,
    -- Behavior classification
    CASE 
        WHEN ABS((um.query_count - bm.avg_daily_queries) / NULLIF(bm.stddev_daily_queries, 0)) > 3 THEN 'Highly Anomalous'
        WHEN ABS((um.query_count - bm.avg_daily_queries) / NULLIF(bm.stddev_daily_queries, 0)) > 2 THEN 'Anomalous'
        WHEN ABS((um.query_count - bm.avg_daily_queries) / NULLIF(bm.stddev_daily_queries, 0)) > 1 THEN 'Unusual'
        ELSE 'Normal'
    END AS behavior_classification
FROM user_metrics um
LEFT JOIN baseline_metrics bm 
    ON um.user_name = bm.user_name 
    AND um.role_name = bm.role_name;

-- Grant permissions
GRANT SELECT ON VIEW vw_user_behavior_profile TO ROLE grp17_lab_role;

-- Test query
SELECT * FROM vw_user_behavior_profile 
--WHERE activity_date >= CURRENT_DATE() - 7
ORDER BY query_count_zscore DESC
LIMIT 10;

