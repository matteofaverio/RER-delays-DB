# dashboard/pages/0_line comparison.py
import datetime
import time
import streamlit as st
import plotly.express as px
from db import run_query, RER_COLORS

st.set_page_config(page_title="Line Comparison", layout="wide")
st.title("📊 Global Line Comparison (Q6)")

with st.sidebar:
    st.header("Filters")
    MIN_DATE, MAX_DATE = datetime.date(2025, 11, 12), datetime.date(2026, 4, 10)
    df = st.date_input("From", value=datetime.date(2026, 3, 1), min_value=MIN_DATE, max_value=MAX_DATE)
    dt = st.date_input("To", value=datetime.date(2026, 3, 8), min_value=MIN_DATE, max_value=MAX_DATE)

Q6_SQL = """
SELECT line_code, round(avg(mean_lateness_s)::numeric, 2) AS avg_lateness_s, sum(n) AS total_trains
FROM core.fact_delay_events
WHERE poll_at_utc >= %(df)s AND poll_at_utc < %(dt)s
GROUP BY line_code ORDER BY avg_lateness_s DESC;
"""

start = time.time()
res = run_query(Q6_SQL, {"df": str(df), "dt": str(dt)})
duration = time.time() - start

if not res.empty:
    fig = px.bar(res, x="avg_lateness_s", y="line_code", orientation='h', color="line_code",
                 color_discrete_map=RER_COLORS, title="Avg Lateness by Line")
    st.plotly_chart(fig, use_container_width=True)
    
    with st.expander("🛠️ Database Technical Details"):
        st.write(f"**Execution Time:** {duration:.4f}s")
        st.write("**Strategy:** Dynamic (Bitmap Index Scan for narrow ranges, Sequential Scan for wide ranges)")
        st.code(Q6_SQL)