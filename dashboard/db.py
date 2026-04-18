# dashboard/db.py
import os
import pandas as pd
import psycopg
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
load_dotenv(Path(__file__).parent.parent / ".env")

# Official RER Color Palette for consistency
RER_COLORS = {
    "RER A": "#E34E3E", "RER B": "#5391CC", "RER C": "#F8D64C",
    "RER D": "#5DAE4B", "RER E": "#B873B1"
}

@st.cache_resource
def get_connection():
    try:
        conn = psycopg.connect(
            dbname=os.environ["PGDATABASE"],
            user=os.environ["PGUSER"],
            password=os.environ.get("PGPASSWORD", ""),
            host=os.environ.get("PGHOST", "localhost"),
            port=int(os.environ.get("PGPORT", 5432)),
            autocommit=True
        )
        return conn
    except Exception as e:
        st.error(f"Database Connection Error: {e}")
        st.stop()

def run_query(sql: str, params: dict = None) -> pd.DataFrame:
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql, params)
        if cur.description:
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)
        return pd.DataFrame()