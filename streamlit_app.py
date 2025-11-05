import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, date
from snowflake.snowpark.context import get_active_session
import json
from collections import defaultdict
import _snowflake
from io import BytesIO
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
import plotly.io as pio
import base64



st.set_page_config(
    page_title="Behavior Intelligence Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_resource
def get_session():
    return get_active_session()

session = get_session()

DB_SCHEMA = "GRP17_LAB_DB.PS_USER_BEHAVIOR"
AGENT_NAME = "SNOWPATROL_AGENT"
AGENT_DATABASE = "SNOWFLAKE_INTELLIGENCE"
AGENT_SCHEMA = "AGENTS"

DATA_MIN_DATE = date(2025, 8, 18)
DATA_MAX_DATE = date(2025, 9, 18)



st.sidebar.title("📊 Navigation")
page = st.sidebar.radio(
    "Select Page:",
    ["🏠 Home", "⚠️ Anomalies", "💰 Cost Analysis", "👤 Behavioral Patterns", 
     "🔐 Security Compliance", "💡 Recommendations", "📊 Data Activity"]
)

st.sidebar.markdown("---")
st.sidebar.markdown("### 📅 Date Range Filter")
st.sidebar.info("**Available Data:** Aug 18 - Sep 18, 2025 (32 days)")

col1, col2 = st.sidebar.columns(2)
with col1:
    start_date = st.date_input(
        "Start Date",
        value=DATA_MIN_DATE,
        min_value=DATA_MIN_DATE,
        max_value=DATA_MAX_DATE,
        help="Select the start date for analysis"
    )

with col2:
    end_date = st.date_input(
        "End Date", 
        value=DATA_MAX_DATE,
        min_value=DATA_MIN_DATE,
        max_value=DATA_MAX_DATE,
        help="Select the end date for analysis"
    )

if start_date > end_date:
    st.sidebar.error("⚠️ Start date must be before end date")
    start_date = DATA_MIN_DATE
    end_date = DATA_MAX_DATE

days_selected = (end_date - start_date).days + 1
st.sidebar.success(f"✅ {days_selected} days selected")

st.sidebar.markdown("---")
st.sidebar.info(f"""
**Connected to:**  
Database: GRP17_LAB_DB  
Schema: PS_USER_BEHAVIOR
""")

def inject_context_into_message(user_message, page_context, data_summary):
    context_prefix = f"""**Context**: You are viewing the {page_context} dashboard page.
**Date Range**: {start_date.strftime('%b %d, %Y')} to {end_date.strftime('%b %d, %Y')} ({days_selected} days selected)

**Current Data Summary**:
{data_summary}

**User Question**: {user_message}
"""
    return context_prefix

def agent_run():
    api_endpoint = f"/api/v2/databases/{AGENT_DATABASE}/schemas/{AGENT_SCHEMA}/agents/{AGENT_NAME}:run"
    request_headers = {"Accept": "application/json, text/event-stream"}
    
    request_body = {
        "messages": st.session_state.agent_messages,
        "tool_choice": {"type": "auto"}
    }
    
    resp = _snowflake.send_snow_api_request(
        "POST",
        api_endpoint,
        request_headers,
        {},
        request_body,
        None,
        60_000
    )
    
    if resp["status"] != 200:
        error_details = f"Status: {resp['status']}, Reason: {resp.get('reason')}, Content: {resp.get('content', '')[:500]}"
        raise Exception(f"Cortex HTTP {resp['status']} – {error_details}")
    
    return resp

def parse_and_display_response(response_data, container):
    try:
        raw_content = response_data["content"]
        events = []
        
        # Parse Server-Sent Events format
        for chunk in raw_content.split("\n\n"):
            if not chunk.startswith("data:"):
                continue
            payload = chunk[len("data:"):].strip()
            if payload and payload != "[DONE]":
                try:
                    event = json.loads(payload)
                    events.append(event)
                except json.JSONDecodeError:
                    continue
        
        # If no events were parsed, try parsing as JSON array
        if not events:
            try:
                events = json.loads(raw_content)
                if not isinstance(events, list):
                    events = [events]
            except json.JSONDecodeError:
                container.error("Failed to parse response content")
                return
        
        # Extract text content from events (matching cortex_agent_service.py logic)
        full_text = ""
        for ev in events:
            if not isinstance(ev, dict):
                continue
            
            # Handle response.text.delta events
            if ev.get("event") == "response.text.delta":
                data = ev.get("data", {})
                if isinstance(data, dict):
                    text = data.get("text", "")
                    if text:
                        full_text += text
            
            # Handle response.text events (complete text)
            elif ev.get("event") == "response.text":
                data = ev.get("data", {})
                if isinstance(data, dict):
                    text = data.get("text", "")
                    if text:
                        full_text = text
            
            # Handle message.delta events (new Cortex Agent API format)
            elif ev.get("event") == "message.delta":
                data = ev.get("data", {})
                if isinstance(data, dict):
                    delta = data.get("delta", {})
                    if isinstance(delta, dict):
                        content_array = delta.get("content", [])
                        if isinstance(content_array, list):
                            for content_item in content_array:
                                if isinstance(content_item, dict):
                                    # Extract text content
                                    if content_item.get("type") == "text":
                                        text_content = content_item.get("text", "")
                                        if text_content:
                                            full_text += text_content
                                    # Skip tool_use and tool_results
                                    elif content_item.get("type") in ["tool_use", "tool_results"]:
                                        continue
            
            # Handle 'response' event type
            elif ev.get("event") == "response":
                data = ev.get("data", {})
                if isinstance(data, dict):
                    content = data.get("content", [])
                    if isinstance(content, list):
                        for item in content:
                            if isinstance(item, dict) and item.get("type") == "text":
                                text = item.get("text", "")
                                if text:
                                    full_text = text
            
            # Handle legacy format
            else:
                choices = ev.get("data", ev).get("choices", []) if isinstance(ev.get("data", ev), dict) else []
                for choice in choices:
                    if not isinstance(choice, dict):
                        continue
                    delta = choice.get("delta", {})
                    if isinstance(delta, dict) and "content" in delta:
                        full_text += delta["content"]
                    elif "content" in choice:
                        full_text += choice["content"]
                
                # Also check for direct content in event
                if "content" in ev:
                    content = ev["content"]
                    if isinstance(content, str):
                        full_text += content
        
        if full_text:
            container.markdown(full_text)
            # Store assistant response in session state
            assistant_message = {
                "role": "assistant",
                "content": [{"type": "text", "text": full_text}]
            }
            st.session_state.agent_messages.append(assistant_message)
        else:
            container.warning("No text response received from agent.")
            # Debug: show first few events
            if events:
                container.info(f"Received {len(events)} events. First event: {str(events[0])[:200]}")
            
    except Exception as e:
        container.error(f"Error parsing response: {str(e)}")

def render_chat_interface(page_name, data_summary):
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🤖 AI Assistant")
    
    if 'agent_messages' not in st.session_state:
        st.session_state.agent_messages = []
    
    with st.sidebar.expander("💬 Chat with SnowPatrol Agent", expanded=False):
        st.markdown("Ask questions about the data you're viewing")
        
        user_input = st.text_area(
            "Your question:",
            placeholder="e.g., What are the top security risks in this period?",
            height=80,
            key=f"agent_input_{page_name}"
        )
        
        col1, col2 = st.columns([1, 1])
        with col1:
            send_button = st.button("Send", key=f"send_{page_name}", type="primary")
        with col2:
            if st.button("Clear", key=f"clear_{page_name}"):
                st.session_state.agent_messages = []
                st.rerun()
        
        if send_button and user_input:
            context_message = inject_context_into_message(user_input, page_name, data_summary)
            
            message = {
                "role": "user",
                "content": [{"type": "text", "text": context_message}]
            }
            st.session_state.agent_messages.append(message)
            
            with st.spinner("Calling agent..."):
                try:
                    response = agent_run()
                    response_container = st.container()
                    parse_and_display_response(response, response_container)
                except Exception as e:
                    st.error(f"Error calling agent: {str(e)}")
                    if st.session_state.agent_messages:
                        st.session_state.agent_messages.pop()
        
        if st.session_state.agent_messages:
            st.markdown("**Recent Conversation:**")
            for msg in st.session_state.agent_messages[-3:]:
                if msg.get("role") == "user":
                    text = msg.get("content", [{}])[0].get("text", "")
                    st.markdown(f"**You:** {text[:200]}...")
                else:
                    for content_item in msg.get("content", []):
                        if content_item.get("type") == "text":
                            st.markdown(f"**🤖 Agent:** {content_item.get('text', '')[:200]}...")


if page == "🏠 Home":
    st.title("🏠 Behavior Intelligence Dashboard")
    st.markdown(f"### Executive Summary ({start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')})")
    
    col1, col2, col3, col4 = st.columns(4)
    
    anomaly_count = session.sql(f"SELECT COUNT(*) as cnt FROM {DB_SCHEMA}.VW_ANOMALY_ATTRIBUTION WHERE ANOMALY_DATE BETWEEN '{start_date}' AND '{end_date}'").collect()[0]['CNT']
    recommendations_count = session.sql(f"SELECT COUNT(*) as cnt FROM {DB_SCHEMA}.VW_CONTEXTUAL_RECOMMENDATIONS WHERE PRIORITY = 'High Priority' AND EVENT_DATE BETWEEN '{start_date}' AND '{end_date}'").collect()[0]['CNT']
    total_cost_df = session.sql(f"SELECT SUM(TOTAL_DAILY_CREDITS * 2) as total_cost FROM {DB_SCHEMA}.VW_COST_ATTRIBUTION WHERE COST_DATE BETWEEN '{start_date}' AND '{end_date}'").to_pandas()
    security_issues = session.sql(f"SELECT COUNT(*) as cnt FROM {DB_SCHEMA}.VW_SECURITY_COMPLIANCE WHERE SECURITY_RISK_LEVEL NOT IN ('Low Risk') AND LOGIN_DATE BETWEEN '{start_date}' AND '{end_date}'").collect()[0]['CNT']
    
    with col1:
        st.metric("Active Anomalies", f"{anomaly_count:,}", help="Users contributing to detected anomalies")
    
    with col2:
        st.metric("High Priority Actions", f"{recommendations_count:,}", help="Critical recommendations requiring attention")
    
    with col3:
        total_cost = total_cost_df['TOTAL_COST'].iloc[0] if not total_cost_df.empty else 0
        st.metric("Total Cost (USD)", f"${total_cost:,.2f}", help="Estimated total compute cost")
    
    with col4:
        st.metric("Security Issues", f"{security_issues:,}", help="Users with medium/high security risk")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📈 Top Cost Contributors")
        top_cost_df = session.sql(f"""
            SELECT 
                USER_NAME,
                ROLE_NAME,
                SUM(TOTAL_DAILY_CREDITS) as TOTAL_CREDITS,
                SUM(DAILY_QUERY_COUNT) as TOTAL_QUERIES
            FROM {DB_SCHEMA}.VW_COST_ATTRIBUTION
            WHERE COST_DATE BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY USER_NAME, ROLE_NAME
            ORDER BY TOTAL_CREDITS DESC
            LIMIT 10
        """).to_pandas()
        
        if not top_cost_df.empty:
            fig = px.bar(top_cost_df, x='TOTAL_CREDITS', y='USER_NAME', 
                        orientation='h', 
                        color='TOTAL_CREDITS',
                        color_continuous_scale='Reds',
                        labels={'TOTAL_CREDITS': 'Total Credits', 'USER_NAME': 'User'})
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No cost data available for the selected period")
    
    with col2:
        st.markdown("#### 🔐 Security Risk Distribution")
        security_df = session.sql(f"""
            SELECT 
                SECURITY_RISK_LEVEL,
                COUNT(*) as USER_COUNT
            FROM {DB_SCHEMA}.VW_SECURITY_COMPLIANCE
            WHERE LOGIN_DATE BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY SECURITY_RISK_LEVEL
        """).to_pandas()
        
        if not security_df.empty:
            fig = px.pie(security_df, names='SECURITY_RISK_LEVEL', values='USER_COUNT',
                        color_discrete_map={'Low Risk': 'green', 'Medium Risk - Some Failed Attempts': 'orange', 'High Risk - Multiple Failed Logins': 'red'})
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No security data available")
    
    st.markdown("---")
    st.markdown("#### 💡 Recent High-Priority Recommendations")
    recent_recs = session.sql(f"""
        SELECT 
            EVENT_DATE,
            RECOMMENDATION_TYPE,
            USER_NAME,
            ROLE_NAME,
            ISSUE_DESCRIPTION,
            RECOMMENDED_ACTIONS
        FROM {DB_SCHEMA}.VW_CONTEXTUAL_RECOMMENDATIONS
        WHERE PRIORITY = 'High Priority'
            AND EVENT_DATE BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY EVENT_DATE DESC
        LIMIT 5
    """).to_pandas()
    
    if not recent_recs.empty:
        for idx, row in recent_recs.iterrows():
            with st.expander(f"🔴 {row['RECOMMENDATION_TYPE']} - {row['USER_NAME']} ({row['EVENT_DATE']})"):
                st.markdown(f"**Issue:** {row['ISSUE_DESCRIPTION']}")
                st.markdown(f"**Actions:** {row['RECOMMENDED_ACTIONS']}")
    else:
        st.success("✅ No high-priority issues detected!")
    
    data_summary = f"""
    Key Metrics:
    - Active Anomalies: {anomaly_count}
    - High Priority Actions: {recommendations_count}
    - Total Cost: ${total_cost:,.2f} USD
    - Security Issues: {security_issues}
    """
    render_chat_interface("Home Dashboard", data_summary)


elif page == "⚠️ Anomalies":
    st.title("⚠️ Anomaly Attribution Analysis")
    st.markdown(f"**Date Range:** {start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}")
    
    st.markdown("""
    This view shows users contributing to detected statistical anomalies in query activity.
    Anomalies indicate unusual spikes in compute usage that exceed forecasted thresholds.
    """)
    
    anomaly_df = session.sql(f"""
        SELECT * FROM {DB_SCHEMA}.VW_ANOMALY_ATTRIBUTION
        WHERE ANOMALY_DATE BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY ANOMALY_DATE DESC, EXECUTION_TIME_CONTRIBUTION_PCT DESC
    """).to_pandas()
    
    if not anomaly_df.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Anomaly Contributors", len(anomaly_df))
        
        with col2:
            high_risk_count = len(anomaly_df[anomaly_df['RISK_LEVEL'].str.contains('High Risk', na=False)])
            st.metric("High Risk Contributors", high_risk_count)
        
        with col3:
            avg_contribution = anomaly_df['EXECUTION_TIME_CONTRIBUTION_PCT'].mean()
            st.metric("Avg Contribution %", f"{avg_contribution:.1f}%")
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Risk Level Distribution")
            risk_dist = anomaly_df['RISK_LEVEL'].value_counts().reset_index()
            risk_dist.columns = ['Risk Level', 'Count']
            fig = px.bar(risk_dist, x='Risk Level', y='Count', color='Risk Level',
                        color_discrete_map={'Low Risk': 'green', 'Medium Risk': 'orange', 'High Risk': 'red'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### Behavior Patterns")
            behavior_dist = anomaly_df['BEHAVIOR_PATTERN'].value_counts().reset_index()
            behavior_dist.columns = ['Pattern', 'Count']
            fig = px.pie(behavior_dist, names='Pattern', values='Count')
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        st.markdown("#### Detailed Anomaly Contributors")
        
        risk_filter = st.multiselect("Filter by Risk Level:", 
                                     options=anomaly_df['RISK_LEVEL'].unique(),
                                     default=anomaly_df['RISK_LEVEL'].unique())
        
        filtered_df = anomaly_df[anomaly_df['RISK_LEVEL'].isin(risk_filter)]
        
        st.dataframe(
            filtered_df[[
                'ANOMALY_DATE', 'USER_NAME', 'ROLE_NAME', 'WAREHOUSE_NAME',
                'QUERY_COUNT', 'EXECUTION_TIME_CONTRIBUTION_PCT', 'RISK_LEVEL',
                'BEHAVIOR_PATTERN', 'RECOMMENDED_ACTION'
            ]],
            use_container_width=True,
            height=400
        )
        
        st.download_button(
            "📥 Download Anomaly Data",
            filtered_df.to_csv(index=False),
            "anomaly_attribution.csv",
            "text/csv"
        )
    else:
        st.success("✅ No anomalies detected!")
    
    if not anomaly_df.empty:
        data_summary = f"""
        Anomaly Statistics:
        - Total Contributors: {len(anomaly_df)}
        - High Risk Contributors: {len(anomaly_df[anomaly_df['RISK_LEVEL'].str.contains('High Risk', na=False)])}
        - Average Contribution: {anomaly_df['EXECUTION_TIME_CONTRIBUTION_PCT'].mean():.1f}%
        - Top 3 Users: {', '.join(anomaly_df.nlargest(3, 'EXECUTION_TIME_CONTRIBUTION_PCT')['USER_NAME'].tolist())}
        """
    else:
        data_summary = "No anomalies detected in the selected date range."
    render_chat_interface("Anomaly Analysis", data_summary)


elif page == "💰 Cost Analysis":
    st.title("💰 Cost Attribution Analysis")
    st.markdown(f"**Date Range:** {start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}")
    
    cost_df = session.sql(f"""
        SELECT * FROM {DB_SCHEMA}.VW_COST_ATTRIBUTION
        WHERE COST_DATE BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY COST_DATE DESC, TOTAL_DAILY_CREDITS DESC
    """).to_pandas()
    
    if not cost_df.empty:
        total_credits = cost_df['TOTAL_DAILY_CREDITS'].sum()
        total_queries = cost_df['DAILY_QUERY_COUNT'].sum()
        avg_cost_per_query = total_credits / total_queries if total_queries > 0 else 0
        unique_users = cost_df['USER_NAME'].nunique()
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Credits", f"{total_credits:,.2f}")
        with col2:
            st.metric("Estimated Cost", f"${total_credits * 2:,.2f}")
        with col3:
            st.metric("Total Queries", f"{total_queries:,}")
        with col4:
            st.metric("Active Users", unique_users)
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Top 10 Users by Cost")
            top_users = cost_df.groupby('USER_NAME')['TOTAL_DAILY_CREDITS'].sum().nlargest(10).reset_index()
            fig = px.bar(top_users, x='TOTAL_DAILY_CREDITS', y='USER_NAME', orientation='h',
                        color='TOTAL_DAILY_CREDITS', color_continuous_scale='Oranges')
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### Cost Trend Over Time")
            daily_cost = cost_df.groupby('COST_DATE')['TOTAL_DAILY_CREDITS'].sum().reset_index()
            fig = px.line(daily_cost, x='COST_DATE', y='TOTAL_DAILY_CREDITS',
                         labels={'TOTAL_DAILY_CREDITS': 'Daily Credits', 'COST_DATE': 'Date'})
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        st.markdown("#### Detailed Cost Breakdown")
        st.dataframe(cost_df[['COST_DATE', 'USER_NAME', 'ROLE_NAME', 'WAREHOUSE_NAME', 
                               'TOTAL_DAILY_CREDITS', 'DAILY_QUERY_COUNT', 'AVG_QUERY_EXECUTION_TIME']],
                    use_container_width=True, height=400)
        
        data_summary = f"""
        Cost Analysis Summary:
        - Total Credits: {total_credits:,.2f}
        - Estimated Cost: ${total_credits * 2:,.2f}
        - Total Queries: {total_queries:,}
        - Active Users: {unique_users}
        - Top Cost User: {top_users.iloc[0]['USER_NAME']} ({top_users.iloc[0]['TOTAL_DAILY_CREDITS']:.2f} credits)
        """
    else:
        st.info("No cost data available for the selected period")
        data_summary = "No cost data available in the selected date range."
    
    render_chat_interface("Cost Analysis", data_summary)


elif page == "👤 Behavioral Patterns":
    st.title("👤 Behavioral Pattern Analysis")
    st.markdown(f"**Date Range:** {start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}")
    
    patterns_df = session.sql(f"""
        SELECT * FROM {DB_SCHEMA}.VW_BEHAVIORAL_PATTERNS
        WHERE ACTIVITY_DATE BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY ACTIVITY_DATE DESC, ABS(QUERY_DEVIATION_SCORE) DESC
        LIMIT 1000
    """).to_pandas()
    
    if not patterns_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            anomalous_count = len(patterns_df[patterns_df['BEHAVIOR_CLASSIFICATION'].isin(['Highly Anomalous', 'Anomalous'])])
            st.metric("Anomalous Patterns", anomalous_count)
        
        with col2:
            high_risk = len(patterns_df[patterns_df['RISK_LEVEL'] == 'High Risk'])
            st.metric("High Risk Behaviors", high_risk)
        
        with col3:
            off_hours = len(patterns_df[patterns_df['TIME_CLASSIFICATION'] == 'Off Hours'])
            st.metric("Off-Hours Activity", off_hours)
        
        with col4:
            unique_users = patterns_df['USER_NAME'].nunique()
            st.metric("Users with Patterns", unique_users)
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Behavior Classification")
            behavior_dist = patterns_df['BEHAVIOR_CLASSIFICATION'].value_counts().reset_index()
            behavior_dist.columns = ['Classification', 'Count']
            fig = px.pie(behavior_dist, names='Classification', values='Count')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### Pattern Types")
            pattern_types = patterns_df['PATTERN_TYPE'].value_counts().head(10).reset_index()
            pattern_types.columns = ['Pattern', 'Count']
            fig = px.bar(pattern_types, x='Count', y='Pattern', orientation='h')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        st.markdown("#### Detailed Behavioral Patterns")
        
        classification_filter = st.multiselect("Filter by Classification:",
                                               options=patterns_df['BEHAVIOR_CLASSIFICATION'].unique(),
                                               default=patterns_df['BEHAVIOR_CLASSIFICATION'].unique())
        
        filtered_df = patterns_df[patterns_df['BEHAVIOR_CLASSIFICATION'].isin(classification_filter)]
        
        st.dataframe(filtered_df[['ACTIVITY_DATE', 'USER_NAME', 'ROLE_NAME', 'ACTIVITY_HOUR',
                                   'QUERY_COUNT', 'BEHAVIOR_CLASSIFICATION', 'PATTERN_TYPE',
                                   'RISK_LEVEL', 'RECOMMENDED_ACTION']],
                    use_container_width=True, height=400)
        
        data_summary = f"""
        Behavioral Pattern Summary:
        - Anomalous Patterns: {anomalous_count}
        - High Risk Behaviors: {high_risk}
        - Off-Hours Activity: {off_hours}
        - Users with Patterns: {unique_users}
        - Most Common Pattern: {patterns_df['PATTERN_TYPE'].value_counts().index[0]}
        """
    else:
        st.info("No behavioral patterns detected for the selected period")
        data_summary = "No behavioral patterns detected in the selected date range."
    
    render_chat_interface("Behavioral Patterns", data_summary)


elif page == "🔐 Security Compliance":
    st.title("🔐 Security & Compliance Analysis")
    st.markdown(f"**Date Range:** {start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}")
    
    security_df = session.sql(f"""
        SELECT * FROM {DB_SCHEMA}.VW_SECURITY_COMPLIANCE
        WHERE LOGIN_DATE BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY LOGIN_DATE DESC, FAILED_LOGINS DESC
    """).to_pandas()
    
    if not security_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            high_risk = len(security_df[security_df['SECURITY_RISK_LEVEL'].str.contains('High Risk', na=False)])
            st.metric("High Risk Users", high_risk)
        
        with col2:
            total_failed = security_df['FAILED_LOGINS'].sum()
            st.metric("Failed Login Attempts", int(total_failed))
        
        with col3:
            mfa_disabled = len(security_df[security_df['MFA_COMPLIANCE_STATUS'] != 'MFA Compliant'])
            st.metric("Users without MFA", mfa_disabled)
        
        with col4:
            password_issues = len(security_df[security_df['PASSWORD_ONLY_LOGINS'] > 0])
            st.metric("Users with Password-Only Logins", password_issues)
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Security Risk Distribution")
            risk_dist = security_df['SECURITY_RISK_LEVEL'].value_counts().reset_index()
            risk_dist.columns = ['Risk Level', 'Count']
            fig = px.pie(risk_dist, names='Risk Level', values='Count',
                        color='Risk Level',
                        color_discrete_map={'Low Risk': 'green', 
                                          'Medium Risk - Some Failed Attempts': 'orange',
                                          'High Risk - Multiple Failed Logins': 'red'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### Failed Logins Over Time")
            daily_failed = security_df.groupby('LOGIN_DATE')['FAILED_LOGINS'].sum().reset_index()
            fig = px.line(daily_failed, x='LOGIN_DATE', y='FAILED_LOGINS')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        st.markdown("#### High-Risk Users")
        
        high_risk_df = security_df[security_df['SECURITY_RISK_LEVEL'].str.contains('High Risk', na=False)]
        
        if not high_risk_df.empty:
            st.dataframe(high_risk_df[['LOGIN_DATE', 'USER_NAME', 'FAILED_LOGINS',
                                       'MFA_COMPLIANCE_STATUS', 'PASSWORD_ONLY_LOGINS', 'SECURITY_RISK_LEVEL']],
                        use_container_width=True, height=300)
        else:
            st.success("✅ No high-risk users detected!")
        
        data_summary = f"""
        Security Summary:
        - High Risk Users: {high_risk}
        - Total Failed Login Attempts: {int(total_failed)}
        - Users without MFA: {mfa_disabled}
        - Password Issues: {password_issues}
        """
    else:
        st.info("No security data available for the selected period")
        data_summary = "No security data available in the selected date range."
    
    render_chat_interface("Security Compliance", data_summary)


elif page == "💡 Recommendations":
    st.title("💡 Contextual Recommendations")
    st.markdown(f"**Date Range:** {start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}")
    
    recs_df = session.sql(f"""
        SELECT * FROM {DB_SCHEMA}.VW_CONTEXTUAL_RECOMMENDATIONS
        WHERE EVENT_DATE BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY 
            CASE PRIORITY 
                WHEN 'High Priority' THEN 1 
                WHEN 'Medium Priority' THEN 2 
                ELSE 3 
            END,
            EVENT_DATE DESC
    """).to_pandas()
    
    if not recs_df.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            high_priority = len(recs_df[recs_df['PRIORITY'] == 'High Priority'])
            st.metric("High Priority", high_priority, delta="Urgent" if high_priority > 0 else None)
        
        with col2:
            medium_priority = len(recs_df[recs_df['PRIORITY'] == 'Medium Priority'])
            st.metric("Medium Priority", medium_priority)
        
        with col3:
            low_priority = len(recs_df[recs_df['PRIORITY'] == 'Low Priority'])
            st.metric("Low Priority", low_priority)
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Recommendations by Type")
            type_dist = recs_df['RECOMMENDATION_TYPE'].value_counts().reset_index()
            type_dist.columns = ['Type', 'Count']
            fig = px.bar(type_dist, x='Count', y='Type', orientation='h')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### Priority Distribution")
            priority_dist = recs_df['PRIORITY'].value_counts().reset_index()
            priority_dist.columns = ['Priority', 'Count']
            fig = px.pie(priority_dist, names='Priority', values='Count',
                        color='Priority',
                        color_discrete_map={'High Priority': 'red',
                                          'Medium Priority': 'orange',
                                          'Low Priority': 'yellow'})
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        st.markdown("#### All Recommendations")
        
        priority_filter = st.multiselect("Filter by Priority:",
                                        options=recs_df['PRIORITY'].unique(),
                                        default=recs_df['PRIORITY'].unique())
        
        filtered_recs = recs_df[recs_df['PRIORITY'].isin(priority_filter)]
        
        for idx, row in filtered_recs.iterrows():
            priority_icon = "🔴" if row['PRIORITY'] == 'High Priority' else "🟡" if row['PRIORITY'] == 'Medium Priority' else "🟢"
            with st.expander(f"{priority_icon} {row['RECOMMENDATION_TYPE']} - {row['USER_NAME']} ({row['EVENT_DATE']})"):
                st.markdown(f"**Priority:** {row['PRIORITY']}")
                st.markdown(f"**Issue:** {row['ISSUE_DESCRIPTION']}")
                st.markdown(f"**Recommended Actions:** {row['RECOMMENDED_ACTIONS']}")
        
        data_summary = f"""
        Recommendations Summary:
        - High Priority: {high_priority}
        - Medium Priority: {medium_priority}
        - Low Priority: {low_priority}
        - Most Common Type: {recs_df['RECOMMENDATION_TYPE'].value_counts().index[0]}
        """
    else:
        st.success("✅ No recommendations - all systems optimal!")
        data_summary = "No recommendations in the selected date range."
    
    render_chat_interface("Recommendations", data_summary)


elif page == "📊 Data Activity":
    st.title("📊 Data Activity Tracking")
    st.markdown(f"**Date Range:** {start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Data Consumption by Role")
        consumption_df = session.sql(f"""
            SELECT * FROM {DB_SCHEMA}.VW_DATA_CONSUMPTION_BY_ROLE
            WHERE USAGE_DATE BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY GB_DATA_SCANNED DESC
            LIMIT 20
        """).to_pandas()
        
        if not consumption_df.empty:
            fig = px.bar(consumption_df, x='GB_DATA_SCANNED', y='ROLE_NAME',
                        orientation='h', color='GB_DATA_SCANNED',
                        color_continuous_scale='Blues')
            fig.update_layout(height=500, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
            
            st.dataframe(consumption_df[['ROLE_NAME', 'TOTAL_QUERIES', 'GB_DATA_SCANNED',
                                        'ACTIVE_USERS', 'DATABASES_ACCESSED']],
                        use_container_width=True, height=300)
        else:
            st.info("No data consumption data available")
    
    with col2:
        st.markdown("#### Data Write Activity by Role")
        write_df = session.sql(f"""
            SELECT * FROM {DB_SCHEMA}.VW_DATA_WRITE_ACTIVITY_BY_ROLE
            WHERE ACTIVITY_DATE BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY TOTAL_ROWS_INSERTED DESC
            LIMIT 20
        """).to_pandas()
        
        if not write_df.empty:
            fig = px.bar(write_df, x='TOTAL_ROWS_INSERTED', y='ROLE_NAME',
                        orientation='h', color='TOTAL_ROWS_INSERTED',
                        color_continuous_scale='Greens')
            fig.update_layout(height=500, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
            
            st.dataframe(write_df[['ROLE_NAME', 'TOTAL_WRITE_QUERIES', 'TOTAL_ROWS_INSERTED',
                                  'TOTAL_ROWS_UPDATED', 'GB_WRITTEN_DAILY']],
                        use_container_width=True, height=300)
        else:
            st.info("No write activity data available")
    
    data_summary = f"""
    Data Activity Summary:
    - Top Consumer Role: {consumption_df.iloc[0]['ROLE_NAME'] if not consumption_df.empty else 'N/A'}
    - Total GB Scanned: {consumption_df['GB_DATA_SCANNED'].sum() if not consumption_df.empty else 0:,.2f}
    - Top Write Role: {write_df.iloc[0]['ROLE_NAME'] if not write_df.empty else 'N/A'}
    - Total Rows Inserted: {write_df['TOTAL_ROWS_INSERTED'].sum() if not write_df.empty else 0:,.0f}
    """
    
    render_chat_interface("Data Activity", data_summary)


st.sidebar.markdown("---")
st.sidebar.markdown("### 📄 Generate Business Report")

with st.sidebar.expander("📊 Create PDF Report", expanded=False):
    st.markdown("**Select sections to include:**")
    
    report_sections = st.multiselect(
        "Report Sections:",
        ["Executive Summary", "Unusual Activity", "Resource Costs", "Security & Access", "Action Items"],
        default=["Executive Summary", "Unusual Activity", "Resource Costs", "Security & Access", "Action Items"],
        help="Choose which sections to include in your business report"
    )
    
    if st.button("📥 Generate PDF Report", type="primary", use_container_width=True):
        if not report_sections:
            st.error("Please select at least one section")
        else:
            with st.spinner("Generating your business report..."):
                try:
                    pdf_buffer = generate_business_report(report_sections, start_date, end_date, session)
                    
                    st.download_button(
                        label="📥 Download Business Report",
                        data=pdf_buffer,
                        file_name=f"Business_Report_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )
                    st.success("✅ Report generated successfully!")
                except Exception as e:
                    st.error(f"Error generating report: {str(e)}")

st.sidebar.markdown("---")
st.sidebar.markdown("### 📖 About")
st.sidebar.info("""
**Behavior Intelligence Dashboard**

Comprehensive monitoring of:
- Anomaly detection & attribution
- Cost analysis & optimization
- User behavior patterns
- Security & compliance
- Contextual recommendations
- Data activity tracking

Built with Snowflake + Streamlit
""")