# dashboard/pages/2_peak_offpeak.py
import time
import streamlit as st
import plotly.express as px
from db import run_query

st.set_page_config(page_title="Peak Analysis", layout="wide")
st.title("⚡ Peak vs Off-Peak (Q8)")

Q8_SQL = """
SELECT f.line_code, 
       CASE WHEN extract(hour FROM t.poll_at_local) IN (7,8,9,17,18,19) THEN 'Peak' ELSE 'Off-Peak' END AS period,
       round(avg(f.mean_lateness_s)::numeric, 2) AS lateness
FROM core.fact_delay_events f
JOIN core.dim_time t ON t.poll_at_utc = f.poll_at_utc
GROUP BY 1, 2;
"""

start = time.time()
res = run_query(Q8_SQL)
duration = time.time() - start

if not res.empty:
    res['lateness'] = res['lateness'].astype(float)
    fig = px.bar(res, x="line_code", y="lateness", color="period", barmode="group",
                 color_discrete_map={"Peak": "#e74c3c", "Off-Peak": "#2ecc71"}, title="Peak Hour Impact")
    st.plotly_chart(fig, use_container_width=True)
    
    with st.expander("🛠️ Database Technical Details"):
        st.write(f"**Execution Time:** {duration:.4f}s")
        st.write("**Strategy:** Sequential Scan (Required for full-table categorization via CASE statement)")
        st.code(Q8_SQL)