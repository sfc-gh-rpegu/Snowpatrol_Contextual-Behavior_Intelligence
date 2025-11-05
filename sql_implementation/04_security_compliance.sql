-- =====================================================
-- Security & MFA Compliance View
-- Purpose: Track authentication methods, MFA usage, and security risks
-- =====================================================

USE DATABASE GRP17_LAB_DB;
USE SCHEMA ps_user_behavior;
USE WAREHOUSE grp17_cortex_wh;

CREATE OR REPLACE VIEW vw_security_compliance AS
WITH login_analysis AS (
    SELECT 
        user_name,
        DATE(event_timestamp) AS login_date,
        first_authentication_factor,
        second_authentication_factor,
        is_success,
        error_code,
        error_message,
        client_ip,
        reported_client_type,
        reported_client_version,
        event_id,
        event_type
    FROM login_history
    WHERE event_type = 'LOGIN'
),
daily_login_summary AS (
    SELECT 
        user_name,
        login_date,
        COUNT(*) AS login_attempts,
        COUNT(CASE WHEN is_success = 'YES' THEN 1 END) AS successful_logins,
        COUNT(CASE WHEN is_success = 'NO' THEN 1 END) AS failed_logins,
        COUNT(CASE 
            WHEN second_authentication_factor IS NULL 
                AND is_success = 'YES' 
                AND first_authentication_factor = 'PASSWORD'
            THEN 1 
        END) AS password_only_logins,
        COUNT(CASE 
            WHEN second_authentication_factor IS NOT NULL 
                AND is_success = 'YES' 
            THEN 1 
        END) AS mfa_logins,
        COUNT(CASE 
            WHEN is_success = 'NO' 
                AND error_message LIKE '%MFA%' 
            THEN 1 
        END) AS mfa_failed_logins,
        COUNT(DISTINCT client_ip) AS unique_ip_addresses,
        COUNT(DISTINCT reported_client_type) AS unique_client_types,
        LISTAGG(DISTINCT client_ip, ', ') WITHIN GROUP (ORDER BY client_ip) AS ip_addresses_used,
        LISTAGG(DISTINCT reported_client_type, ', ') WITHIN GROUP (ORDER BY reported_client_type) AS client_types_used
    FROM login_analysis
    GROUP BY user_name, login_date
),
user_security_baseline AS (
    SELECT 
        user_name,
        AVG(successful_logins) AS avg_daily_successful_logins,
        AVG(failed_logins) AS avg_daily_failed_logins,
        AVG(password_only_logins) AS avg_daily_password_only,
        AVG(mfa_logins) AS avg_daily_mfa_logins,
        AVG(unique_ip_addresses) AS avg_unique_ips
    FROM daily_login_summary
    GROUP BY user_name
),
user_security_profile AS (
    SELECT 
        dls.user_name,
        dls.login_date,
        dls.login_attempts,
        dls.successful_logins,
        dls.failed_logins,
        dls.password_only_logins,
        dls.mfa_logins,
        dls.mfa_failed_logins,
        dls.unique_ip_addresses,
        dls.unique_client_types,
        dls.ip_addresses_used,
        dls.client_types_used,
        usb.avg_daily_successful_logins,
        usb.avg_daily_failed_logins,
        usb.avg_daily_password_only,
        usb.avg_daily_mfa_logins,
        usb.avg_unique_ips,
        -- Calculate MFA compliance rate
        CASE 
            WHEN dls.successful_logins > 0 
            THEN (dls.mfa_logins / dls.successful_logins * 100)
            ELSE 0
        END AS mfa_compliance_rate,
        -- MFA compliance status
        CASE 
            WHEN dls.mfa_logins = 0 AND dls.password_only_logins > 0 THEN 'MFA Never Used'
            WHEN dls.successful_logins > 0 AND (dls.mfa_logins / dls.successful_logins) < 0.3 THEN 'MFA Rarely Used'
            WHEN dls.successful_logins > 0 AND (dls.mfa_logins / dls.successful_logins) BETWEEN 0.3 AND 0.7 THEN 'MFA Inconsistent'
            WHEN dls.successful_logins > 0 AND (dls.mfa_logins / dls.successful_logins) > 0.9 THEN 'MFA Compliant'
            WHEN dls.successful_logins > 0 AND (dls.mfa_logins / dls.successful_logins) > 0.7 THEN 'MFA Mostly Used'
            ELSE 'No Login Activity'
        END AS mfa_compliance_status,
        -- Security risk assessment
        CASE 
            WHEN dls.failed_logins > 20 THEN 'Critical Risk - Excessive Failed Attempts'
            WHEN dls.failed_logins > 10 THEN 'High Risk - Multiple Failed Attempts'
            WHEN dls.password_only_logins > 5 AND dls.mfa_logins = 0 THEN 'High Risk - No MFA Usage'
            WHEN dls.unique_ip_addresses > 5 THEN 'Medium Risk - Multiple IP Addresses'
            WHEN dls.password_only_logins > 3 THEN 'Medium Risk - Limited MFA Usage'
            WHEN dls.failed_logins BETWEEN 3 AND 10 THEN 'Medium Risk - Some Failed Attempts'
            WHEN dls.password_only_logins > 0 THEN 'Low Risk - Some Password-Only Logins'
            ELSE 'Low Risk'
        END AS security_risk_level,
        -- Anomaly detection
        CASE 
            WHEN usb.avg_daily_failed_logins > 0 
                AND dls.failed_logins > usb.avg_daily_failed_logins * 3 
            THEN 'Anomalous - Unusual Failed Login Spike'
            WHEN usb.avg_unique_ips > 0 
                AND dls.unique_ip_addresses > usb.avg_unique_ips * 2 
            THEN 'Anomalous - Unusual IP Address Pattern'
            WHEN usb.avg_daily_password_only > 0 
                AND dls.password_only_logins > usb.avg_daily_password_only * 2 
            THEN 'Anomalous - Increased Password-Only Usage'
            ELSE 'Normal Pattern'
        END AS security_anomaly_flag
    FROM daily_login_summary dls
    LEFT JOIN user_security_baseline usb
        ON dls.user_name = usb.user_name
)
SELECT 
    usp.*,
    -- Previous day metrics for trend analysis
    LAG(password_only_logins, 1) OVER (PARTITION BY user_name ORDER BY login_date) AS prev_day_password_only,
    LAG(failed_logins, 1) OVER (PARTITION BY user_name ORDER BY login_date) AS prev_day_failed_logins,
    LAG(mfa_compliance_rate, 1) OVER (PARTITION BY user_name ORDER BY login_date) AS prev_day_mfa_compliance_rate,
    -- 7-day trends
    AVG(mfa_compliance_rate) OVER (
        PARTITION BY user_name 
        ORDER BY login_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS mfa_compliance_7day_avg,
    SUM(failed_logins) OVER (
        PARTITION BY user_name 
        ORDER BY login_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS failed_logins_7day_total,
    -- Recommended actions
    CASE 
        WHEN failed_logins > 20 THEN 'CRITICAL: Lock account and investigate immediately'
        WHEN failed_logins > 10 THEN 'HIGH: Contact user and review access logs'
        WHEN password_only_logins > 5 AND mfa_logins = 0 THEN 'HIGH: Mandate MFA enrollment immediately'
        WHEN unique_ip_addresses > 5 THEN 'MEDIUM: Review login locations and implement network policy'
        WHEN password_only_logins > 3 THEN 'MEDIUM: Send MFA reminder and schedule training'
        WHEN failed_logins BETWEEN 3 AND 10 THEN 'MEDIUM: Monitor for continued issues'
        WHEN password_only_logins > 0 THEN 'LOW: Encourage MFA adoption'
        ELSE 'LOW: Continue monitoring'
    END AS recommended_action
FROM user_security_profile usp
ORDER BY login_date DESC, security_risk_level DESC;

-- Grant permissions
GRANT SELECT ON VIEW vw_security_compliance TO ROLE grp17_lab_role;

-- Test queries

-- Users with MFA compliance issues in last 7 days
SELECT 
    user_name,
    COUNT(DISTINCT login_date) AS days_active,
    SUM(password_only_logins) AS total_password_only,
    SUM(mfa_logins) AS total_mfa_logins,
    AVG(mfa_compliance_rate) AS avg_mfa_compliance_rate,
    MAX(security_risk_level) AS highest_risk_level,
    ARRAY_AGG(DISTINCT mfa_compliance_status) AS compliance_statuses
FROM vw_security_compliance 
WHERE login_date >= CURRENT_DATE() - 7
GROUP BY user_name
HAVING SUM(password_only_logins) > 0
ORDER BY total_password_only DESC;

-- Security incidents in last 7 days
SELECT 
    login_date,
    user_name,
    failed_logins,
    password_only_logins,
    unique_ip_addresses,
    security_risk_level,
    security_anomaly_flag,
    recommended_action,
    ip_addresses_used
FROM vw_security_compliance 
WHERE login_date >= CURRENT_DATE() - 7
    AND (security_risk_level LIKE '%High Risk%' 
         OR security_risk_level LIKE '%Critical Risk%'
         OR security_anomaly_flag LIKE 'Anomalous%')
ORDER BY login_date DESC, security_risk_level;

-- MFA compliance summary by user
SELECT 
    user_name,
    MAX(login_date) AS last_login_date,
    SUM(successful_logins) AS total_logins,
    SUM(password_only_logins) AS total_password_only,
    SUM(mfa_logins) AS total_mfa_logins,
    ROUND(AVG(mfa_compliance_rate), 2) AS avg_compliance_rate,
    MAX(mfa_compliance_status) AS current_status
FROM vw_security_compliance 
WHERE login_date >= CURRENT_DATE() - 30
GROUP BY user_name
ORDER BY avg_compliance_rate ASC, total_logins DESC;

