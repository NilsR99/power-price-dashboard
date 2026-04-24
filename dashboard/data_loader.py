import streamlit as st
import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

@st.cache_resource
def get_engine():
    db_user = os.getenv("POSTGRES_USER")
    db_pass = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    return create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")

@st.cache_data(ttl=3600)
def load_data():
    """Lädt die Master-Tabelle aus der PostgreSQL-Datenbank."""
    engine = get_engine()
    # Wir laden die wichtigsten Spalten für das Dashboard
    query = "SELECT * FROM v_power_dashboard"
    
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
        
    df['datetime_local'] = pd.to_datetime(df['datetime_local'])
    # Daten chronologisch sortieren (wichtig für Liniendiagramme)
    df = df.sort_values('datetime_local').reset_index(drop=True)
    return df