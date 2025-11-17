import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text
from typing import Optional


st.set_page_config(page_title="Real-Time Rides Dashboard", layout="wide")
st.title("🚗 Real-Time Ride-Sharing Dashboard")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"

@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)

engine = get_engine(DATABASE_URL)

def load_data(status_filter: Optional[str] = None, limit: int = 200) -> pd.DataFrame:
    base_query = "SELECT * FROM rides"
    params = {}
    if status_filter and status_filter != "All":
        base_query += " WHERE status = :status"
        params["status"] = status_filter
    base_query += " ORDER BY ride_id DESC LIMIT :limit"
    params["limit"] = limit

    try:
        df = pd.read_sql_query(text(base_query), con=engine.connect(), params=params)
        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        return pd.DataFrame()

# Sidebar controls
status_options = ["All", "ongoing", "completed"]
selected_status = st.sidebar.selectbox("Filter by Status", status_options)
update_interval = st.sidebar.slider("Update Interval (seconds)", min_value=2, max_value=20, value=5)
limit_records = st.sidebar.number_input("Number of records to load", min_value=50, max_value=2000, value=200, step=50)

if st.sidebar.button("Refresh now"):
    st.rerun()

placeholder = st.empty()

while True:
    df_rides = load_data(selected_status, limit=int(limit_records))

    with placeholder.container():
        if df_rides.empty:
            st.warning("No records found. Waiting for data...")
            time.sleep(update_interval)
            continue

        if "ts_utc" in df_rides.columns:
            df_rides["ts_utc"] = pd.to_datetime(df_rides["ts_utc"])

        # KPIs
        total_rides = len(df_rides)
        total_revenue = df_rides["fare_usd"].sum()
        average_fare = total_revenue / total_rides if total_rides > 0 else 0.0
        completed = len(df_rides[df_rides["status"] == "completed"])
        ongoing = len(df_rides[df_rides["status"] == "ongoing"])
        completion_rate = (completed / total_rides * 100) if total_rides > 0 else 0.0
        total_distance = df_rides["distance_km"].sum()
        avg_distance = total_distance / total_rides if total_rides > 0 else 0.0

        st.subheader(f"Displaying {total_rides} rides (Filter: {selected_status})")

        # KPI Metrics
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total Rides", total_rides)
        k2.metric("Total Revenue", f"${total_revenue:,.2f}")
        k3.metric("Average Fare", f"${average_fare:,.2f}")
        k4.metric("Completion Rate", f"{completion_rate:,.1f}%")
        k5.metric("Avg Distance", f"{avg_distance:,.2f} km")

        # Additional metrics row
        k6, k7, k8, k9, k10 = st.columns(5)
        k6.metric("Completed", completed)
        k7.metric("Ongoing", ongoing)
        k8.metric("Total Distance", f"{total_distance:,.1f} km")
        active_drivers = df_rides["driver_id"].nunique()
        k9.metric("Active Drivers", active_drivers)
        rides_per_driver = total_rides / active_drivers if active_drivers > 0 else 0
        k10.metric("Rides/Driver", f"{rides_per_driver:,.1f}")

        st.markdown("### Raw Data (Top 10)")
        st.dataframe(df_rides.head(10), use_container_width=True)

        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            # Revenue by Status
            status_revenue = df_rides.groupby("status")["fare_usd"].sum().reset_index()
            fig_status = px.bar(
                status_revenue, 
                x="status", 
                y="fare_usd", 
                title="Revenue by Ride Status",
                labels={"fare_usd": "Revenue (USD)", "status": "Status"},
                color="status",
                color_discrete_map={"completed": "#2ecc71", "ongoing": "#f39c12"}
            )
            st.plotly_chart(fig_status, use_container_width=True)

        with col2:
            # Rides by Driver (Top 10)
            driver_counts = df_rides["driver_id"].value_counts().head(10).reset_index()
            driver_counts.columns = ["driver_id", "ride_count"]
            fig_drivers = px.bar(
                driver_counts, 
                x="driver_id", 
                y="ride_count", 
                title="Top 10 Most Active Drivers",
                labels={"driver_id": "Driver ID", "ride_count": "Number of Rides"}
            )
            st.plotly_chart(fig_drivers, use_container_width=True)

        col3, col4 = st.columns(2)

        with col3:
            # Distance distribution
            fig_distance = px.histogram(
                df_rides, 
                x="distance_km", 
                nbins=20,
                title="Distance Distribution",
                labels={"distance_km": "Distance (km)", "count": "Number of Rides"}
            )
            st.plotly_chart(fig_distance, use_container_width=True)

        with col4:
            # Fare distribution
            fig_fare = px.histogram(
                df_rides, 
                x="fare_usd", 
                nbins=20,
                title="Fare Distribution",
                labels={"fare_usd": "Fare (USD)", "count": "Number of Rides"}
            )
            st.plotly_chart(fig_fare, use_container_width=True)

        # Map visualization (pickup locations)
        if not df_rides.empty and "pickup_lat" in df_rides.columns:
            st.markdown("### Pickup Locations Map")
            fig_map = px.scatter_mapbox(
                df_rides,
                lat="pickup_lat",
                lon="pickup_lng",
                color="status",
                size="fare_usd",
                hover_data=["ride_id", "driver_id", "distance_km", "fare_usd"],
                color_discrete_map={"completed": "#2ecc71", "ongoing": "#f39c12"},
                zoom=10,
                height=500,
                title="Ride Pickup Locations"
            )
            fig_map.update_layout(mapbox_style="open-street-map")
            st.plotly_chart(fig_map, use_container_width=True)

        # Time series analysis (if we have enough data)
        if len(df_rides) > 10 and "ts_utc" in df_rides.columns:
            st.markdown("### Rides Over Time")
            df_rides_sorted = df_rides.sort_values("ts_utc")
            df_rides_sorted["cumulative_rides"] = range(1, len(df_rides_sorted) + 1)
            
            fig_time = px.line(
                df_rides_sorted,
                x="ts_utc",
                y="cumulative_rides",
                title="Cumulative Rides Over Time",
                labels={"ts_utc": "Timestamp", "cumulative_rides": "Total Rides"}
            )
            st.plotly_chart(fig_time, use_container_width=True)

        st.markdown("---")
        st.caption(f"Last updated: {datetime.now().isoformat()} • Auto-refresh: {update_interval}s")

    time.sleep(update_interval)