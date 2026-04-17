# dashboard/app.py
import streamlit as st
from db import run_query

st.set_page_config(page_title="RER Analytics Portal", page_icon="🚆", layout="wide")

st.markdown("""
    <style>
    .station-sign {
        background-color: #003063; color: white; padding: 40px;
        text-align: center; border-bottom: 8px solid white; margin-bottom: 30px;
    }
    .typewriter h1 {
        overflow: hidden; border-right: .15em solid #f1c40f;
        white-space: nowrap; margin: 0 auto; letter-spacing: .05em;
        animation: typing 3s steps(30, end), blink-caret .75s step-end infinite;
    }
    @keyframes typing { from { width: 0 } to { width: 100% } }
    @keyframes blink-caret { from, to { border-color: transparent } 50% { border-color: #f1c40f; } }
    
    .logo-container { display: flex; justify-content: center; gap: 15px; margin-top: 20px; }
    .rer-badge { background: white; color: #212529; border: 4px solid #212529; border-radius: 12px; width: 80px; height: 60px; display: flex; align-items: center; justify-content: center; font-weight: 800; font-size: 24px; }
    .line-sq { color: white; border-radius: 12px; width: 60px; height: 60px; display: flex; align-items: center; justify-content: center; font-size: 32px; font-weight: 900; }
    .nav-box { background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd; text-align: center; min-height: 150px; }
    </style>
""", unsafe_allow_html=True)

st.markdown("""
    <div class="station-sign">
        <div class="typewriter"><h1>RER TRANSIT MONITOR</h1></div>
        <div class="logo-container">
            <div class="rer-badge">RER</div>
            <div class="line-sq" style="background:#E34E3E">A</div>
            <div class="line-sq" style="background:#5391CC">B</div>
            <div class="line-sq" style="background:#F8D64C">C</div>
            <div class="line-sq" style="background:#5DAE4B">D</div>
            <div class="line-sq" style="background:#B873B1">E</div>
        </div>
    </div>
""", unsafe_allow_html=True)

# System at a Glance
st.markdown("### 📊 System at a Glance")
m1, m2, m3 = st.columns(3)
m1.metric("Total Observations", "12.4M+", help="Total rows in core.fact_delay_events")
m2.metric("Network Coverage", "5 Lines / 400+ Stops")
m3.metric("Observation Period", "Nov 2025 - Apr 2026")

st.markdown("---")
st.markdown("### 🗺️ System Navigation")
c1, c2, c3, c4 = st.columns(4)

pages = [
    (c1, "Line Comparison", "pages/0_line comparison.py", "Global Overview (Q6)"),
    (c2, "Station Explorer", "pages/1_station_explorer.py", "Geographic Map (Q7)"),
    (c3, "Peak Performance", "pages/2_peak_offpeak.py", "Rush Hour Analysis (Q8)"),
    (c4, "Station Timeline", "pages/3_station_timeline.py", "Historical Trends (Q5)")
]

for col, name, path, desc in pages:
    with col:
        st.markdown(f'<div class="nav-box"><h4>{name}</h4><p>{desc}</p></div>', unsafe_allow_html=True)
        if st.button(f"Open {name}", use_container_width=True): st.switch_page(path)