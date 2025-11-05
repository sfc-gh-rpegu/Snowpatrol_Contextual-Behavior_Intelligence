-- =====================================================
-- Role-Level Security Compliance View
-- Purpose: Aggregate security metrics by role and team
-- =====================================================

USE DATABASE GRP17_LAB_DB;
USE SCHEMA ps_user_behavior;
USE WAREHOUSE grp17_cortex_wh;

CREATE OR REPLACE VIEW vw_role_security_compliance AS
WITH role_login_summary AS (
    SELECT 
        lh.user_name,
        COALESCE(qh.role_name, 'UNKNOWN_ROLE') AS role_name,
        DATE(lh.event_timestamp) AS login_date,
        COUNT(*) AS login_attempts,
        COUNT(CASE WHEN lh.is_success = 'YES' THEN 1 END) AS successful_logins,
        COUNT(CASE WHEN lh.is_success = 'NO' THEN 1 END) AS failed_logins,
        COUNT(CASE 
            WHEN lh.second_authentication_factor IS NULL 
                AND lh.is_success = 'YES' 
                AND lh.first_authentication_factor = 'PASSWORD'
            THEN 1 
        END) AS password_only_logins,
        COUNT(CASE 
            WHEN lh.second_authentication_factor IS NOT NULL 
                AND lh.is_success = 'YES' 
            THEN 1 
        END) AS mfa_logins
    FROM login_history lh
    LEFT JOIN (
        -- Get the most commonly used role per user
        SELECT 
            user_name,
            role_name,
            ROW_NUMBER() OVER (PARTITION BY user_name ORDER BY COUNT(*) DESC) AS rn
        FROM query_history
        GROUP BY user_name, role_name
    ) qh ON lh.user_name = qh.user_name AND qh.rn = 1
    WHERE lh.event_type = 'LOGIN'
    GROUP BY 
        lh.user_name,
        qh.role_name,
        DATE(lh.event_timestamp)
),
role_aggregation AS (
    SELECT 
        role_name,
        login_date,
        COUNT(DISTINCT user_name) AS unique_users,
        SUM(login_attempts) AS total_login_attempts,
        SUM(successful_logins) AS total_successful_logins,
        SUM(failed_logins) AS total_failed_logins,
        SUM(password_only_logins) AS total_password_only_logins,
        SUM(mfa_logins) AS total_mfa_logins
    FROM role_login_summary
    GROUP BY role_name, login_date
)
SELECT 
    role_name,
    login_date,
    unique_users,
    total_login_attempts,
    total_successful_logins,
    total_failed_logins,
    total_password_only_logins,
    total_mfa_logins,
    -- Calculate MFA compliance rate
    CASE 
        WHEN total_successful_logins > 0 
        THEN (total_mfa_logins * 100.0 / total_successful_logins)
        ELSE 0
    END AS mfa_compliance_rate,
    -- MFA compliance status
    CASE 
        WHEN total_mfa_logins = 0 AND total_password_only_logins > 0 THEN 'MFA Never Used'
        WHEN total_successful_logins > 0 AND (total_mfa_logins / total_successful_logins) < 0.3 THEN 'MFA Rarely Used'
        WHEN total_successful_logins > 0 AND (total_mfa_logins / total_successful_logins) BETWEEN 0.3 AND 0.7 THEN 'MFA Inconsistent'
        WHEN total_successful_logins > 0 AND (total_mfa_logins / total_successful_logins) > 0.9 THEN 'MFA Compliant'
        WHEN total_successful_logins > 0 AND (total_mfa_logins / total_successful_logins) > 0.7 THEN 'MFA Mostly Used'
        ELSE 'No Login Activity'
    END AS role_mfa_compliance_status,
    -- Risk assessment
    CASE 
        WHEN total_failed_logins > 50 THEN 'High Risk - Multiple Failed Attempts Across Role'
        WHEN total_password_only_logins > unique_users * 5 THEN 'High Risk - Widespread Password-Only Usage'
        WHEN total_failed_logins > 20 THEN 'Medium Risk - Some Failed Attempts'
        WHEN total_password_only_logins > unique_users * 2 THEN 'Medium Risk - Moderate Password-Only Usage'
        ELSE 'Low Risk'
    END AS role_security_risk_level
FROM role_aggregation
ORDER BY login_date DESC, total_password_only_logins DESC;

-- Grant permissions
GRANT SELECT ON VIEW vw_role_security_compliance TO ROLE grp17_lab_role;

-- Test query
SELECT 
    role_name,
    SUM(total_password_only_logins) AS total_password_only,
    SUM(total_mfa_logins) AS total_mfa,
    AVG(mfa_compliance_rate) AS avg_compliance_rate,
    COUNT(DISTINCT login_date) AS days_tracked
FROM vw_role_security_compliance
WHERE login_date >= CURRENT_DATE() - 7
GROUP BY role_name
ORDER BY total_password_only DESC;

