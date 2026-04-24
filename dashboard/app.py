import streamlit as st
import datetime
import pandas as pd

# Import der Architekturschichten
from data_loader import load_data
from utils import add_derived_columns

# Import der View-Module (Zentrales Routing)
from views import (
    imbalance_analysis,
    heatmap_negative_preise,
    merit_order,
    weather_sensitivity,
    correlation_matrix,
    energy_mix,
    standard_load_profile
)

# Seitenkonfiguration (Muss die erste Streamlit-Anweisung sein)
st.set_page_config(
    page_title="Power Price Dashboard", 
    layout="wide", 
)

def main():
    st.title("Power Price Dashboard")
    
    # 1. Daten laden (Zentrale Datenquelle mit Caching)
    with st.spinner("Verbindung zum Data Warehouse wird hergestellt..."):
        df_raw = load_data()
        
    if df_raw.empty:
        st.error("Kritischer Fehler: Keine Daten in der Datenbank gefunden.")
        st.stop()
        
    # Ergänzung um berechnete Spalten/Features (Logic Layer)
    df = add_derived_columns(df_raw)

    # --- SIDEBAR: Globales Kontrollzentrum ---
    st.sidebar.header("Globales Kontrollzentrum")
    
    # Zeitliche Eingrenzung (Projektrahmen 2015 - 2025)
    MIN_VAL = datetime.date(2015, 1, 1)
    MAX_VAL = datetime.date(2025, 12, 31)
    
    # Dynamische Grenzen basierend auf den tatsächlich vorhandenen Daten
    data_min = df['datetime_local'].min().date()
    data_max = df['datetime_local'].max().date()
    
    default_start = max(data_min, MIN_VAL)
    default_end = min(data_max, MAX_VAL)

    # Kalender-Filter zur Definition des Analysefensters
    date_range = st.sidebar.date_input(
        "Analysezeitraum (Limit: 2015-2025)",
        value=(default_start, default_end),
        min_value=MIN_VAL,
        max_value=MAX_VAL,
        help="Eingrenzung des Datensatzes auf ein spezifisches Zeitfenster."
    )

    # Anwendung des Zeitfilters
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_sel, end_sel = date_range
        df_filtered = df[(df['datetime_local'].dt.date >= start_sel) & (df['datetime_local'].dt.date <= end_sel)]
    else:
        df_filtered = df

    st.sidebar.divider()
    st.sidebar.subheader("Erweiterte Marktfilter")

    # Preis-Filter zur Isolierung von Extremereignissen
    price_min = float(df['price_day_ahead'].min())
    price_max = float(df['price_day_ahead'].max())
    price_limit = st.sidebar.slider(
        "Day-Ahead Preisbereich (€/MWh)",
        price_min, price_max, (price_min, price_max),
        step=0.5
    )
    
    # Last-Filter zur Analyse von Lastspitzen
    load_min = float(df['actual_total_load'].min())
    load_max = float(df['actual_total_load'].max())
    load_limit = st.sidebar.slider(
        "Netzlast Bereich (MW)",
        load_min, load_max, (load_min, load_max),
        step=100.0
    )

    # Anwendung der kombinierten Marktfilter
    df_filtered = df_filtered[
        (df_filtered['price_day_ahead'] >= price_limit[0]) & 
        (df_filtered['price_day_ahead'] <= price_limit[1]) &
        (df_filtered['actual_total_load'] >= load_limit[0]) &
        (df_filtered['actual_total_load'] <= load_limit[1])
    ]

    # --- NAVIGATION ---
    st.sidebar.divider()
    st.sidebar.subheader("Analytische Perspektiven")
    menu = st.sidebar.radio(
        "Wähle ein Modul:",
        [
            "Energiemix (Pie-Analyse)",
            "Merit-Order & Strommix",
            "Tageslastprofil (SLP)",
            "Netzstabilität & Prognosefehler",
            "Heatmap: Negative Preise",
            "Wetter-Sensitivität",
            "Korrelations-Matrix"
        ]
    )
    
    st.sidebar.divider()
    st.sidebar.info(f"Aktive Stunden im Filter: {len(df_filtered):,}")

    # --- ROUTING LOGIK ---
    # Übergabe des gefilterten DataFrames an die render-Funktionen der Module
    if df_filtered.empty:
        st.warning("Keine Daten für die gewählte Filterkombination vorhanden. Bitte Filter anpassen.")
    else:
        if menu == "Energiemix (Pie-Analyse)":
            energy_mix.render(df_filtered)
            
        elif menu == "Merit-Order & Strommix":
            merit_order.render(df_filtered)
            
        elif menu == "Tageslastprofil (SLP)":
            standard_load_profile.render(df_filtered)
            
        elif menu == "Netzstabilität & Prognosefehler":
            imbalance_analysis.render(df_filtered)
            
        elif menu == "Heatmap: Negative Preise":
            heatmap_negative_preise.render(df_filtered)
            
        elif menu == "Wetter-Sensitivität":
            weather_sensitivity.render(df_filtered)
            
        elif menu == "Korrelations-Matrix":
            correlation_matrix.render(df_filtered)

if __name__ == "__main__":
    main()