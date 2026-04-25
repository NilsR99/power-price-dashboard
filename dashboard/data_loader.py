import os
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Lokale .env nur für lokale Entwicklung laden.
load_dotenv()


_REQUIRED_DB_KEYS = (
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
)


def _resolve_db_credentials() -> dict[str, str]:
    """Liest Credentials zuerst aus st.secrets, danach aus .env / os.environ."""
    credentials: dict[str, str] = {}

    use_streamlit_secrets = bool(getattr(st, "secrets", {})) and all(
        key in st.secrets for key in _REQUIRED_DB_KEYS
    )

    if use_streamlit_secrets:
        credentials = {key: str(st.secrets[key]) for key in _REQUIRED_DB_KEYS}
    else:
        credentials = {
            "POSTGRES_USER": os.getenv("POSTGRES_USER", ""),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", ""),
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "localhost"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB", ""),
        }

    missing = [k for k, value in credentials.items() if not value]
    if missing:
        missing_str = ", ".join(missing)
        raise ValueError(
            "Unvollständige DB-Konfiguration. Fehlende Werte: "
            f"{missing_str}. Bitte st.secrets (Cloud) oder .env (lokal) prüfen."
        )

    return credentials


@st.cache_resource
def get_engine():
    """Erzeugt eine robuste SQLAlchemy-Engine mit kleinem, kontrolliertem Pool."""
    creds = _resolve_db_credentials()
    db_pass_encoded = quote_plus(creds["POSTGRES_PASSWORD"])

    connection_string = (
        "postgresql+psycopg2://"
        f"{creds['POSTGRES_USER']}:{db_pass_encoded}"
        f"@{creds['POSTGRES_HOST']}:{creds['POSTGRES_PORT']}/{creds['POSTGRES_DB']}"
        "?sslmode=require"
    )

    # Free-Tier-safe Pooling: wenige persistente Verbindungen, kurze Overflow-Phase.
    return create_engine(
        connection_string,
        pool_size=2,
        max_overflow=1,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
    )


@st.cache_data(ttl=3600)
def load_data():
    """Lädt die Master-Tabelle aus der PostgreSQL-Datenbank."""
    engine = get_engine()

    query = "SELECT * FROM v_power_dashboard"

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    df["datetime_local"] = pd.to_datetime(df["datetime_local"])
    df = df.sort_values("datetime_local").reset_index(drop=True)

    return df
