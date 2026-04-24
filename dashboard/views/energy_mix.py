import streamlit as st
import plotly.express as px
import pandas as pd

def render(df):
    st.header("Struktur-Analyse: Der selektive Energiemix")
    st.markdown(
        "Diese Analyse aggregiert die absolute Stromerzeugung (MWh) im global ausgewählten Zeitraum "
        "und visualisiert die prozentualen Marktanteile der selektierten Energieträger."
    )

    # Mapping der Datenbank-Spalten auf lesbare Namen
    all_possible_cols = {
        "actual_wind_onshore": "Wind Onshore",
        "actual_wind_offshore": "Wind Offshore",
        "actual_pv": "Solar (PV)",
        "actual_brown_coal": "Braunkohle",
        "actual_gas": "Erdgas",
        "actual_nuclear": "Kernenergie",
        "actual_hydro": "Wasserkraft",
        "actual_biomass": "Biomasse",
        "actual_hard_coal": "Steinkohle",
        "actual_pumped_storage": "Pumpspeicher",
        "actual_other_conventional": "Sonstige Konventionelle",
        "actual_other_renewables": "Sonstige Erneuerbare"
    }

    # Nur Spalten anbieten, die auch im geladenen Data Warehouse View existieren
    available_cols = {k: v for k, v in all_possible_cols.items() if k in df.columns}

    if not available_cols:
        st.error("Analytischer Fehler: Keine Erzeugungsdaten im Datensatz gefunden.")
        return

    # Standardauswahl definieren
    default_db_cols = ["actual_wind_onshore", "actual_pv", "actual_brown_coal", "actual_gas", "actual_hard_coal"]
    default_selection = [available_cols[k] for k in default_db_cols if k in available_cols]
    
    selected_friendly_names = st.multiselect(
        "Welche Energieträger sollen verglichen werden?",
        options=list(available_cols.values()),
        default=default_selection if default_selection else list(available_cols.values())[:3]
    )

    if not selected_friendly_names:
        st.warning("Bitte mindestens eine Energiequelle auswählen.")
        return

    # Reverse Mapping für die Berechnung
    selected_db_cols = [k for k, v in available_cols.items() if v in selected_friendly_names]

    # Aggregation der MWh für den ausgewählten Zeitraum
    total_generation_series = df[selected_db_cols].sum().fillna(0)
    
    # DataFrame für Plotly vorbereiten
    plot_data = [
        {"Energiequelle": available_cols[col_name], "Erzeugung (MWh)": val}
        for col_name in selected_db_cols if (val := total_generation_series[col_name]) > 0
    ]

    if not plot_data:
        st.warning("Im gewählten Zeitraum wurde von den ausgewählten Quellen 0 MWh erzeugt.")
        return

    df_pie = pd.DataFrame(plot_data)
    total_selected_sum = df_pie["Erzeugung (MWh)"].sum()

    # Feste Farben für visuelle Konsistenz
    color_map = {
        "Wind Onshore": "#1f77b4", "Wind Offshore": "#17becf", "Solar (PV)": "#ff7f0e",
        "Braunkohle": "#8c564b", "Erdgas": "#d62728", "Kernenergie": "#e377c2",
        "Wasserkraft": "#2ca02c", "Biomasse": "#8c564b", "Steinkohle": "#7f7f7f",
        "Pumpspeicher": "#bcbd22", "Sonstige Konventionelle": "#c7c7c7", "Sonstige Erneuerbare": "#dbdb8d"
    }

    fig = px.pie(
        df_pie, 
        values="Erzeugung (MWh)", 
        names="Energiequelle",
        color="Energiequelle",
        color_discrete_map=color_map,
        hole=0.4,
        title=f"Verhältnis der Auswahl (Aggregiertes Volumen: {total_selected_sum:,.0f} MWh)"
    )
    
    fig.update_traces(textposition="inside", textinfo="percent+label")
    fig.update_layout(margin=dict(l=0, r=0, b=0, t=40))

    st.plotly_chart(fig, use_container_width=True)