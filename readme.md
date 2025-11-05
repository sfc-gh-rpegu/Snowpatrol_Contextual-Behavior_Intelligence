# Snowflake User Behavior Analytics with Cortex AI

Enhanced anomaly detection system for Snowflake with AI-powered natural language queries using Cortex Analyst.

## Overview

This solution analyzes user behavior, detects anomalies, tracks costs, and monitors security compliance in Snowflake. It uses **Snowflake Cortex Analyst** to enable natural language queries over structured analytical views.

## Features

- **User Behavior Tracking** - Query patterns, execution times, and anomaly detection
- **Cost Attribution** - Compute costs by user, role, and warehouse
- **Security Monitoring** - MFA compliance, login failures, and security risks
- **Anomaly Detection** - Identifies unusual patterns and links them to users
- **AI-Powered Queries** - Ask questions in plain English using Cortex Analyst

## Architecture

```
MySQL (Source) 
    ↓
Openflow (Ingestion)
    ↓
Snowflake Raw Tables (Bronze)
    ↓
Feature Views (Silver) - 6 SQL Views
    ↓
Semantic View
    ↓
Cortex Analyst (AI)
    ↓
Streamlit + Snowsight (UI)
```

## 📁 Project Structure

```
├── sql_implementation/
│   ├── 01_user_behavior_profile.sql      # User activity metrics
│   ├── 02_anomaly_attribution.sql        # Anomaly + normal day tracking
│   ├── 03_cost_attribution.sql           # Cost by user/role/warehouse
│   ├── 04_security_compliance.sql        # User-level security
│   ├── 07_role_level_security.sql        # Role-level security
│   ├── 08_storage_consumption_LIMITED.sql # Storage tracking by role
│   └── 00_deploy_all_views.sql           # Deploy all views
├── cortex_analyst_semantic_view         # AI semantic model
├── app_with_cortex_agent.py              # Streamlit dashboard
└── ARCHITECTURE_DATAFLOW.md              # Detailed architecture
```

## Quick Start

### 1. Deploy Views

```bash
# Connect to Snowflake
snowsql -a <account> -u <username>

# Deploy all 6 feature views
snowsql -f sql_implementation/00_deploy_all_views.sql
```

### 2. Setup Cortex Analyst & Cortex Agent

```bash
# Deploy semantic model
snowsql -f cortex_analyst_semantic_view.sql
```

### 3. Create & Run Streamlit App for Viz & Chat with  Cortex Agent
Upload the native python streamlit app code app_with_cortex_agent.py in snowflake

## Example Questions

Ask Cortex Analyst in plain English:

- "Who are the top 10 users by compute cost?"
- "Which roles have poor MFA compliance?"
- "Show me all anomalies detected"
- "Which users logged in without MFA?"
- "What's the storage contribution by role?"

##  Core Views

| View | Purpose |
|------|---------|
| `vw_user_behavior_profile` | User query patterns and anomaly detection |
| `vw_anomaly_attribution` | Anomaly tracking with user contributions |
| `vw_cost_attribution` | Compute cost analysis |
| `vw_security_compliance` | User-level MFA and login security |
| `vw_role_security_compliance` | Role-level security metrics |
| `vw_data_write_activity_by_role` | Storage growth by role |

## Requirements

- Snowflake account with Cortex Analyst enabled
- Access to Snowflake query history and login tables
- Python 3.10+ (for Streamlit app)



## Use Cases

- **Cost Optimization** - Identify high-cost users and optimize warehouse usage
- **Security Compliance** - Monitor MFA adoption and detect security risks
- **Anomaly Detection** - Catch unusual behavior patterns early
- **Capacity Planning** - Track storage growth and query patterns

## Contributing

This is a professional services solution template. Customize the views and semantic model based on your specific requirements.

## License

Internal use only - Professional Services solution

## Resources

- [Snowflake Cortex Analyst Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst)
- [Snowflake Query History](https://docs.snowflake.com/en/sql-reference/account-usage/query_history)

---

**Version:** 1.0  
**Last Updated:** November 2025  
**Status:** Production Ready
