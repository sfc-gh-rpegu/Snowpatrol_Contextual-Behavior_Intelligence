-- =====================================================
-- Cost Attribution View
-- Purpose: Attribute warehouse costs to specific users and roles
-- =====================================================

USE DATABASE GRP17_LAB_DB;
USE SCHEMA ps_user_behavior;
USE WAREHOUSE grp17_cortex_wh;

CREATE OR REPLACE VIEW vw_cost_attribution AS
WITH warehouse_costs AS (
    SELECT 
        DATE_TRUNC('hour', TO_TIMESTAMP(start_time)) AS cost_hour,
        warehouse_name,
        SUM(credits_used) AS credits_used,
        SUM(credits_used_compute) AS credits_used_compute,
        SUM(credits_used_cloud_services) AS credits_used_cloud_services
    FROM warehouse_metering_history
    GROUP BY DATE_TRUNC('hour', TO_TIMESTAMP(start_time)), warehouse_name

),
query_attribution AS (
    SELECT 
        DATE_TRUNC('hour', TO_TIMESTAMP(qh.start_time)) AS cost_hour,
        qh.warehouse_name,
        qh.user_name,
        qh.role_name,
        qh.database_name,
        qh.schema_name,
        COUNT(DISTINCT qh.query_id) AS query_count,
        SUM(qh.execution_time) AS total_execution_time,
        SUM(qh.total_elapsed_time) AS total_elapsed_time,
        SUM(qh.bytes_scanned) AS total_bytes_scanned,
        AVG(qh.execution_time) AS avg_execution_time
    FROM query_history qh
    WHERE qh.warehouse_name IS NOT NULL
        AND qh.user_name IS NOT NULL
    GROUP BY 
        DATE_TRUNC('hour', TO_TIMESTAMP(qh.start_time)),
        qh.warehouse_name,
        qh.user_name,
        qh.role_name,
        qh.database_name,
        qh.schema_name
),
hourly_warehouse_totals AS (
    SELECT 
        cost_hour,
        warehouse_name,
        SUM(total_execution_time) AS total_warehouse_exec_time,
        SUM(query_count) AS total_warehouse_queries
    FROM query_attribution
    GROUP BY cost_hour, warehouse_name
),
user_cost_share AS (
    SELECT 
        qa.cost_hour,
        qa.warehouse_name,
        qa.user_name,
        qa.role_name,
        qa.database_name,
        qa.schema_name,
        qa.query_count,
        qa.total_execution_time,
        qa.avg_execution_time,
        qa.total_bytes_scanned,
        wc.credits_used,
        wc.credits_used_compute,
        wc.credits_used_cloud_services,
        hwt.total_warehouse_exec_time,
        hwt.total_warehouse_queries,
        -- Allocate compute costs based on execution time proportion
        (qa.total_execution_time / NULLIF(hwt.total_warehouse_exec_time, 0)) 
            * wc.credits_used_compute AS attributed_credits_compute,
        -- Allocate cloud services costs based on query count proportion
        (qa.query_count / NULLIF(hwt.total_warehouse_queries, 0)) 
            * wc.credits_used_cloud_services AS attributed_credits_cloud_services
    FROM query_attribution qa
    LEFT JOIN warehouse_costs wc
        ON qa.cost_hour = wc.cost_hour
        AND qa.warehouse_name = wc.warehouse_name
    LEFT JOIN hourly_warehouse_totals hwt
        ON qa.cost_hour = hwt.cost_hour
        AND qa.warehouse_name = hwt.warehouse_name
),
daily_aggregation AS (
    SELECT 
        DATE(cost_hour) AS cost_date,
        warehouse_name,
        user_name,
        role_name,
        database_name,
        schema_name,
        SUM(query_count) AS daily_query_count,
        SUM(total_execution_time) AS daily_execution_time,
        AVG(avg_execution_time) AS avg_query_execution_time,
        SUM(total_bytes_scanned) AS daily_bytes_scanned,
        SUM(attributed_credits_compute) AS daily_credits_compute,
        SUM(attributed_credits_cloud_services) AS daily_credits_cloud_services,
        SUM(attributed_credits_compute + attributed_credits_cloud_services) AS total_daily_credits
FROM user_cost_share
GROUP BY 
    DATE(cost_hour),
        warehouse_name,
        user_name,
        role_name,
        database_name,
        schema_name
),
cost_trends AS (
    SELECT 
        *,
        -- Assuming $2 per credit (adjust based on your pricing)
        total_daily_credits * 2 AS estimated_daily_cost_usd,
        -- Calculate 7-day moving average
        AVG(total_daily_credits) OVER (
            PARTITION BY user_name, warehouse_name 
            ORDER BY cost_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS credits_7day_avg,
        -- Calculate previous day cost
        LAG(total_daily_credits, 1) OVER (
            PARTITION BY user_name, warehouse_name 
            ORDER BY cost_date
        ) AS prev_day_credits,
        -- Calculate same day last week
        LAG(total_daily_credits, 7) OVER (
            PARTITION BY user_name, warehouse_name 
            ORDER BY cost_date
        ) AS same_day_last_week_credits
    FROM daily_aggregation
)
SELECT 
    ct.*,
    -- Cost change indicators
    CASE 
        WHEN ct.prev_day_credits IS NOT NULL AND ct.prev_day_credits > 0
        THEN ((ct.total_daily_credits - ct.prev_day_credits) / ct.prev_day_credits * 100)
        ELSE NULL
    END AS day_over_day_change_pct,
    CASE 
        WHEN ct.same_day_last_week_credits IS NOT NULL AND ct.same_day_last_week_credits > 0
        THEN ((ct.total_daily_credits - ct.same_day_last_week_credits) / ct.same_day_last_week_credits * 100)
        ELSE NULL
    END AS week_over_week_change_pct,
    CASE 
        WHEN ct.credits_7day_avg > 0
        THEN ((ct.total_daily_credits - ct.credits_7day_avg) / ct.credits_7day_avg * 100)
        ELSE NULL
    END AS deviation_from_7day_avg_pct,
    -- Cost classification
    CASE 
        WHEN ct.total_daily_credits > ct.credits_7day_avg * 3 THEN 'Extreme Spike'
        WHEN ct.total_daily_credits > ct.credits_7day_avg * 2 THEN 'Significant Spike'
        WHEN ct.total_daily_credits > ct.credits_7day_avg * 1.5 THEN 'Moderate Increase'
        WHEN ct.total_daily_credits < ct.credits_7day_avg * 0.5 THEN 'Significant Decrease'
        ELSE 'Normal'
    END AS cost_classification,
    -- Recommended action based on cost trends
    CASE 
        WHEN ct.total_daily_credits > ct.credits_7day_avg * 3 THEN 'URGENT: Investigate immediately - extreme cost spike'
        WHEN ct.total_daily_credits > ct.credits_7day_avg * 2 THEN 'HIGH: Review user queries and consider resource limits'
        WHEN ct.total_daily_credits > ct.credits_7day_avg * 1.5 THEN 'MEDIUM: Monitor for continued increase'
        ELSE 'LOW: Normal activity'
    END AS cost_alert_level
FROM cost_trends ct
ORDER BY ct.cost_date DESC, ct.total_daily_credits DESC;

-- Grant permissions
GRANT SELECT ON VIEW vw_cost_attribution TO ROLE grp17_lab_role;

-- Test queries

-- Top 10 users by cost today
SELECT 
    user_name,
    warehouse_name,
    SUM(total_daily_credits) AS total_credits,
    SUM(estimated_daily_cost_usd) AS total_cost_usd,
    SUM(daily_query_count) AS total_queries
FROM vw_cost_attribution 
WHERE cost_date = CURRENT_DATE()
GROUP BY user_name, warehouse_name
ORDER BY total_credits DESC
LIMIT 10;

-- Cost spikes in last 7 days
SELECT 
    cost_date,
    user_name,
    warehouse_name,
    total_daily_credits,
    estimated_daily_cost_usd,
    day_over_day_change_pct,
    cost_classification,
    cost_alert_level
FROM vw_cost_attribution 
WHERE cost_date >= CURRENT_DATE() - 7
    AND cost_classification IN ('Extreme Spike', 'Significant Spike')
ORDER BY cost_date DESC, total_daily_credits DESC;

