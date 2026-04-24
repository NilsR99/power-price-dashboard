import streamlit as st
import pandas as pd
import os
import urllib.parse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Lädt lokale Umgebungsvariablen (für lokale Entwicklung)
load_dotenv()

@st.cache_resource
def get_engine():
    # 1. Cloud- vs. Lokal-Weiche
    if "POSTGRES_HOST" in st.secrets:
        db_user = st.secrets["POSTGRES_USER"]
        db_pass = st.secrets["POSTGRES_PASSWORD"]
        db_host = st.secrets["POSTGRES_HOST"]
        db_port = st.secrets["POSTGRES_PORT"]
        db_name = st.secrets["POSTGRES_DB"]
    else:
        db_user = os.getenv("POSTGRES_USER")
        db_pass = os.getenv("POSTGRES_PASSWORD")
        db_host = os.getenv("POSTGRES_HOST", "localhost")
        db_port = os.getenv("POSTGRES_PORT", "5432")
        db_name = os.getenv("POSTGRES_DB")

    # 2. Sicheres URL-Encoding für Sonderzeichen im Passwort
    db_pass_encoded = urllib.parse.quote_plus(db_pass)

    # 3. Zwingende SSL-Verschlüsselung anhängen
    connection_string = f"postgresql+psycopg2://{db_user}:{db_pass_encoded}@{db_host}:{db_port}/{db_name}?sslmode=require"
    
    return create_engine(connection_string)

@st.cache_data(ttl=3600)
def load_data():
    """Lädt die Master-Tabelle aus der PostgreSQL-Datenbank."""
    engine = get_engine()
    
    query = "SELECT * FROM v_power_dashboard"
    
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
        
    df['datetime_local'] = pd.to_datetime(df['datetime_local'])
    df = df.sort_values('datetime_local').reset_index(drop=True)
    
    return df