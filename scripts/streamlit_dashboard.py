import streamlit as st
import pandas as pd
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import time

# Configure the page
st.set_page_config(
    page_title="Real-Time Patient Monitoring",
    page_icon="🏥",
    layout="wide"
)

# Connect to Elasticsearch
@st.cache_resource
def get_es_connection():
    return Elasticsearch('http://localhost:9200')

es = get_es_connection()

def get_recent_patient_data(minutes=10):
    """Get patient data from the last N minutes"""
    try:
        # Calculate time range
        time_threshold = datetime.utcnow() - timedelta(minutes=minutes)
        
        query = {
            "query": {
                "range": {
                    "timestamp": {
                        "gte": time_threshold.isoformat()
                    }
                }
            },
            "sort": [{"timestamp": {"order": "desc"}}],
            "size": 1000
        }
        
        response = es.search(index="patient-vitals", body=query)
        hits = response['hits']['hits']
        
        if not hits:
            return pd.DataFrame()
        
        # Convert to DataFrame
        data = [hit['_source'] for hit in hits]
        df = pd.DataFrame(data)
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        return df
    
    except Exception as e:
        st.error(f"Error fetching data from Elasticsearch: {e}")
        return pd.DataFrame()

def calculate_risk_score(row):
    """Simple risk score based on vital signs"""
    risk = 0
    
    # Heart rate risk
    if row['heart_rate'] > 100 or row['heart_rate'] < 60:
        risk += 1
    if row['heart_rate'] > 120 or row['heart_rate'] < 50:
        risk += 2
    
    # Oxygen saturation risk
    if row['oxygen_saturation'] < 95:
        risk += 1
    if row['oxygen_saturation'] < 90:
        risk += 2
    
    # Blood pressure risk
    if row['blood_pressure_systolic'] > 140 or row['blood_pressure_diastolic'] > 90:
        risk += 1
    
    return risk

def main():
    # Title and description
    st.title("🏥 Real-Time Patient Monitoring Dashboard")
    st.markdown("Live monitoring of patient vital signs with risk assessment")
    
    # Auto-refresh checkbox
    auto_refresh = st.sidebar.checkbox("Auto-refresh every 10 seconds", value=True)
    refresh_minutes = st.sidebar.slider("Data time window (minutes)", 1, 60, 10)
    
    if auto_refresh:
        st.sidebar.write("Next refresh in: 10 seconds")
        time.sleep(10)
        st.rerun()
    
    # Get data
    df = get_recent_patient_data(minutes=refresh_minutes)
    
    if df.empty:
        st.warning("No patient data found in the selected time window.")
        st.info("Make sure your patient simulator and Elasticsearch connector are running.")
        return
    
    # Calculate risk scores
    df['risk_score'] = df.apply(calculate_risk_score, axis=1)
    
    # Get latest reading for each patient
    latest_readings = df.sort_values('timestamp').groupby('patient_id').last().reset_index()
    
    # Display overview metrics
    st.subheader("📊 Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_patients = latest_readings['patient_id'].nunique()
        st.metric("Total Patients", total_patients)
    
    with col2:
        high_risk = len(latest_readings[latest_readings['risk_score'] >= 3])
        st.metric("High Risk Patients", high_risk)
    
    with col3:
        avg_heart_rate = latest_readings['heart_rate'].mean()
        st.metric("Avg Heart Rate", f"{avg_heart_rate:.1f} bpm")
    
    with col4:
        avg_oxygen = latest_readings['oxygen_saturation'].mean()
        st.metric("Avg Oxygen Sat", f"{avg_oxygen:.1f}%")
    
    # Patient details
    st.subheader("👥 Patient Details")
    
    # Risk level coloring
    def color_risk(val):
        if val >= 3:
            return 'color: red; font-weight: bold'
        elif val >= 1:
            return 'color: orange'
        else:
            return 'color: green'
    
    # Display patient table
    display_cols = ['patient_id', 'heart_rate', 'blood_pressure_systolic', 
                   'blood_pressure_diastolic', 'oxygen_saturation', 'risk_score']
    
    styled_df = latest_readings[display_cols].style.applymap(
        color_risk, subset=['risk_score']
    )
    
    st.dataframe(styled_df, use_container_width=True)
    
    # Detailed charts
    st.subheader("📈 Vital Signs Trends")
    
    # Let user select a patient for detailed view
    selected_patient = st.selectbox(
        "Select patient for detailed view:",
        options=sorted(df['patient_id'].unique())
    )
    
    if selected_patient:
        patient_data = df[df['patient_id'] == selected_patient]
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.line_chart(patient_data.set_index('timestamp')['heart_rate'], 
                         use_container_width=True)
            st.caption(f"Heart Rate Trend - {selected_patient}")
        
        with col2:
            st.line_chart(patient_data.set_index('timestamp')['oxygen_saturation'], 
                         use_container_width=True)
            st.caption(f"Oxygen Saturation Trend - {selected_patient}")
    # Raw data expander
    with st.expander("View Raw Data"):
        st.dataframe(df.sort_values('timestamp', ascending=False), use_container_width=True)

if __name__ == "__main__":
    main()