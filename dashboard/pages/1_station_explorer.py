# dashboard/pages/1_station_explorer.py
import datetime
import time
import streamlit as st
import plotly.express as px
import folium
from streamlit_folium import st_folium
from db import run_query, RER_COLORS

st.set_page_config(page_title="Station Explorer", layout="wide")
st.title("📍 Station Explorer (Q7)")

with st.sidebar:
    st.header("Filters")
    line = st.selectbox("Select Line", ["RER A", "RER B", "RER C", "RER D", "RER E"])
    MIN_DATE, MAX_DATE = datetime.date(2025, 11, 12), datetime.date(2026, 4, 10)
    df = st.date_input("From", value=datetime.date(2026, 3, 1), min_value=MIN_DATE, max_value=MAX_DATE)
    dt = st.date_input("To", value=datetime.date(2026, 3, 8), min_value=MIN_DATE, max_value=MAX_DATE)

Q7_SQL = """
SELECT ds.stop_name, ds.stop_lat, ds.stop_lon, round(avg(f.mean_lateness_s)::numeric, 2) AS lateness
FROM core.fact_delay_events f
JOIN core.dim_stop s ON s.stop_id = f.stop_id
JOIN core.dim_monomodal_stop ms ON ms.monomodal_code = s.monomodal_code
JOIN core.dim_station ds ON ds.station_id = ms.station_id
WHERE f.line_code = %(line)s AND f.poll_at_utc BETWEEN %(df)s AND %(dt)s
GROUP BY ds.stop_name, ds.stop_lat, ds.stop_lon HAVING count(*) >= 5;
"""

start_q7 = time.time()
df_map = run_query(Q7_SQL, {"line": line, "df": str(df), "dt": str(dt)})
dur_q7 = time.time() - start_q7

if not df_map.empty:
    df_map[["stop_lat", "stop_lon", "lateness"]] = df_map[["stop_lat", "stop_lon", "lateness"]].astype(float)
    m = folium.Map(location=[df_map["stop_lat"].mean(), df_map["stop_lon"].mean()], zoom_start=11)
    for _, r in df_map.iterrows():
        color = "#e74c3c" if r['lateness'] > 60 else "#f1c40f" if r['lateness'] > 30 else "#2ecc71"
        folium.CircleMarker([r['stop_lat'], r['stop_lon']], radius=7, color=color, fill=True, popup=r['stop_name']).add_to(m)
    st_folium(m, width=900, height=500, returned_objects=[])

    with st.expander("🛠️ Database Technical Details (Map)"):
        st.write(f"**Execution Time:** {dur_q7:.4f}s")
        st.write("**Strategy:** Index Scan using Composite Index (`idx_fact_line_time`)")
        st.code(Q7_SQL)

    st.markdown("---")
    # Drill-down (Q5)
    sel_station = st.selectbox("Drill-down: Pick a station from the map results", sorted(df_map["stop_name"].tolist()))
    Q5_SQL = """SELECT date_trunc('day', f.poll_at_utc)::date AS day, round(avg(f.mean_lateness_s)::numeric, 2) AS lateness
                FROM core.fact_delay_events f JOIN core.dim_stop s ON s.stop_id = f.stop_id 
                JOIN core.dim_station ds ON ds.station_id = (SELECT station_id FROM core.dim_monomodal_stop WHERE monomodal_code = s.monomodal_code)
                WHERE ds.stop_name = %(name)s GROUP BY 1 ORDER BY 1;"""
    
    start_q5 = time.time()
    df_t = run_query(Q5_SQL, {"name": sel_station})
    dur_q5 = time.time() - start_q5

    if not df_t.empty:
        df_t["lateness"] = df_t["lateness"].astype(float)
        st.plotly_chart(px.line(df_t, x="day", y="lateness", title=f"Trend: {sel_station}", color_discrete_sequence=[RER_COLORS.get(line)]))
        with st.expander("🛠️ Database Technical Details (Drill-down)"):
            st.write(f"**Execution Time:** {dur_q5:.4f}s")
            st.write("**Strategy:** Index Scan using `idx_fact_stop` (B-Tree)")
            st.code(Q5_SQL)