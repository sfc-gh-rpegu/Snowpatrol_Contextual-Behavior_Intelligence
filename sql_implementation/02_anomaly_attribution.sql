-- =====================================================
-- Anomaly Attribution View
-- Purpose: Link anomalies to specific users with contribution analysis
-- =====================================================

USE DATABASE GRP17_LAB_DB;
USE SCHEMA ps_user_behavior;
USE WAREHOUSE grp17_cortex_wh;

CREATE OR REPLACE VIEW vw_anomaly_attribution AS
WITH anomaly_window AS (
    SELECT 
        DATE AS anomaly_date,
        ANOMALY_ID,
        IS_ANOMALY,
        ACTUAL_VALUE,
        UPPER_BOUND,
        LOWER_BOUND,
        FORECASTED_VALUE,
        -- Calculate anomaly score based on deviation
        CASE 
            WHEN IS_ANOMALY = TRUE AND UPPER_BOUND > 0 
            THEN ((ACTUAL_VALUE - UPPER_BOUND) / UPPER_BOUND * 100)
            ELSE 0 
        END AS anomaly_score,
        CASE 
            WHEN IS_ANOMALY = TRUE THEN 'Statistical Anomaly'
            ELSE 'Normal Activity'
        END AS anomaly_type,
        'Query Activity' AS metric_name,
        UPPER_BOUND AS threshold_value,
        ACTUAL_VALUE AS actual_value_metric
    FROM anomalies_daily
    -- Removed WHERE IS_ANOMALY = TRUE to show both anomalies and normal days
)
,
daily_aggregates AS (
    SELECT 
        DATE(start_time) AS query_date,
        SUM(execution_time) AS total_daily_exec_time,
        SUM(bytes_scanned) AS total_daily_bytes_scanned,
        COUNT(DISTINCT query_id) AS total_daily_queries
    FROM query_history
    GROUP BY DATE(start_time)
)
,

contributing_users AS (
    SELECT 
        aw.anomaly_date,
        aw.IS_ANOMALY,
        aw.anomaly_type,
        aw.metric_name,
        aw.anomaly_score,
        aw.threshold_value,
        aw.actual_value_metric AS actual_value,
        qh.user_name,
        qh.role_name,
        qh.warehouse_name,
        qh.database_name,
        COUNT(DISTINCT qh.query_id) AS query_count,
        SUM(qh.execution_time) AS total_execution_time,
        SUM(qh.total_elapsed_time) AS total_elapsed_time,
        SUM(qh.bytes_scanned) AS total_bytes_scanned,
        SUM(qh.rows_produced) AS total_rows_produced,
        COUNT(CASE WHEN qh.error_code IS NOT NULL THEN 1 END) AS error_count,
        AVG(qh.execution_time) AS avg_execution_time,
        MAX(qh.execution_time) AS max_execution_time,
        COUNT(DISTINCT qh.query_hash) AS unique_query_patterns
    FROM anomaly_window aw
    INNER JOIN query_history qh
        ON DATE(qh.start_time) = aw.anomaly_date
    WHERE qh.user_name IS NOT NULL
    GROUP BY 
        aw.anomaly_date,
        aw.IS_ANOMALY,
        aw.anomaly_type,
        aw.metric_name,
        aw.anomaly_score,
        aw.threshold_value,
        aw.actual_value_metric,
        qh.user_name,
        qh.role_name,
        qh.warehouse_name,
        qh.database_name
),
user_contribution AS (
    SELECT 
        cu.*,
        da.total_daily_exec_time,
        da.total_daily_queries,
        da.total_daily_bytes_scanned,
        -- Calculate contribution percentages
        cu.total_execution_time / NULLIF(da.total_daily_exec_time, 0) * 100 AS execution_time_contribution_pct,
        cu.query_count / NULLIF(da.total_daily_queries, 0) * 100 AS query_count_contribution_pct,
        cu.total_bytes_scanned / NULLIF(da.total_daily_bytes_scanned, 0) * 100 AS bytes_scanned_contribution_pct,
        -- Calculate deviation from anomaly threshold
        (cu.total_execution_time - cu.threshold_value) / NULLIF(cu.threshold_value, 0) * 100 AS deviation_from_threshold_pct
    FROM contributing_users cu
    LEFT JOIN daily_aggregates da
        ON cu.anomaly_date = da.query_date
)
SELECT 
    uc.IS_ANOMALY,
    uc.anomaly_date,
    uc.anomaly_type,
    uc.user_name,
    uc.role_name,
    uc.warehouse_name,
    uc.database_name,
    uc.metric_name,
    uc.anomaly_score,
    uc.threshold_value,
    uc.actual_value,
    uc.query_count,
    uc.total_execution_time,
    uc.total_elapsed_time,
    uc.total_bytes_scanned,
    uc.total_rows_produced,
    uc.error_count,
    uc.avg_execution_time,
    uc.max_execution_time,
    uc.unique_query_patterns,
    uc.total_daily_exec_time,
    uc.total_daily_queries,
    uc.total_daily_bytes_scanned,
    uc.execution_time_contribution_pct,
    uc.query_count_contribution_pct,
    uc.bytes_scanned_contribution_pct,
    uc.deviation_from_threshold_pct,
    -- Classification logic
    CASE 
        WHEN uc.IS_ANOMALY = FALSE THEN 'Normal Activity'
        WHEN uc.execution_time_contribution_pct > 30 THEN 'Major Contributor'
        WHEN uc.execution_time_contribution_pct > 10 THEN 'Moderate Contributor'
        WHEN uc.execution_time_contribution_pct > 1 THEN 'Minor Contributor'
        ELSE 'Negligible'
    END AS contribution_level,
    -- Risk assessment
    CASE 
        WHEN uc.IS_ANOMALY = FALSE THEN 'Normal - No Anomaly'
        WHEN uc.error_count > 10 AND uc.execution_time_contribution_pct > 20 THEN 'Critical Risk - Major Contributor with High Errors'
        WHEN uc.error_count > 10 THEN 'High Risk - Multiple Errors'
        WHEN uc.query_count > 1000 AND uc.execution_time_contribution_pct > 30 THEN 'High Risk - Excessive Queries & Major Contributor'
        WHEN uc.query_count > 1000 THEN 'High Risk - Excessive Queries'
        WHEN uc.execution_time_contribution_pct > 50 THEN 'High Risk - Dominant User'
        WHEN uc.execution_time_contribution_pct > 30 THEN 'Medium Risk - Major Contributor'
        WHEN uc.error_count > 5 THEN 'Medium Risk - Some Errors'
        ELSE 'Low Risk'
    END AS risk_level,
    -- Behavior pattern
    CASE 
        WHEN uc.IS_ANOMALY = FALSE THEN 'Normal Activity'
        WHEN uc.unique_query_patterns = 1 AND uc.query_count > 100 THEN 'Repetitive Query Pattern'
        WHEN uc.error_count / NULLIF(uc.query_count, 0) > 0.2 THEN 'High Error Rate'
        WHEN uc.avg_execution_time > 60000 THEN 'Long Running Queries'
        WHEN uc.query_count > 500 THEN 'High Query Volume'
        ELSE 'Normal Pattern'
    END AS behavior_pattern,
    -- Recommended action
    CASE 
        WHEN uc.IS_ANOMALY = FALSE THEN 'No action needed - normal activity'
        WHEN uc.error_count > 10 THEN 'Investigate query errors immediately'
        WHEN uc.execution_time_contribution_pct > 50 THEN 'Review user activity and implement resource limits'
        WHEN uc.query_count > 1000 THEN 'Analyze query patterns and optimize'
        WHEN uc.avg_execution_time > 60000 THEN 'Optimize long-running queries'
        ELSE 'Monitor activity'
    END AS recommended_action
FROM user_contribution uc
-- Removed filter to show both anomalies and normal activity
ORDER BY uc.anomaly_date DESC, uc.IS_ANOMALY DESC, uc.execution_time_contribution_pct DESC;

-- Grant permissions
GRANT SELECT ON VIEW vw_anomaly_attribution TO ROLE grp17_lab_role;

-- Test query - Show both anomalies and normal days
SELECT 
    IS_ANOMALY,
    anomaly_date,
    anomaly_type,
    user_name,
    role_name,
    warehouse_name,
    contribution_level,
    risk_level,
    execution_time_contribution_pct,
    query_count,
    error_count,
    recommended_action
FROM vw_anomaly_attribution 
--WHERE anomaly_date >= CURRENT_DATE() - 7
ORDER BY anomaly_date DESC, IS_ANOMALY DESC, execution_time_contribution_pct DESC
LIMIT 20;

-- Test query - Show only anomalies
SELECT 
    IS_ANOMALY,
    anomaly_date,
    user_name,
    contribution_level,
    risk_level,
    execution_time_contribution_pct,
    recommended_action
FROM vw_anomaly_attribution 
WHERE IS_ANOMALY = TRUE
ORDER BY anomaly_date DESC, execution_time_contribution_pct DESC
LIMIT 10;

