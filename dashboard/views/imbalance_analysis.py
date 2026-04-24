import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

def render(df):
    """
    Rendert die Analyse der Prognosefehler und Pönalen.
    Erwartet ein DataFrame, das bereits vom Master-Orchestrator gefiltert wurde.
    """
    st.header("Diagnose: Prognosefehler vs. Ausgleichsenergie")
    st.markdown(
        "**Forensische Analyse:** Dieses Diagramm vergleicht die stündliche Abweichung "
        "zwischen der SMARD Prognose und der physikalischen Realität. "
        "Die rote Linie (Imbalance Price) zeigt die finanziellen Netz-Pönalen für diese Fehlprognosen."
    )

    # Architektonischer Sicherheitscheck
    required_cols = ['forecast_pv', 'forecast_wind_onshore', 'forecast_wind_offshore', 'price_imbalance_short']
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        st.warning(f"Der Datenbank-View fehlen benötigte Spalten: {missing_cols}. Ist der Backfill gelaufen?")
        return

    # --- Dynamische UI (Lokale Steuerung) ---
    st.write("###Konfiguration der Kausalanalyse")
    
    focus = st.selectbox(
        "Wähle die Fehlerquelle (Verursacher):",
        ("Gesamtes Wetter-Portfolio (Wind + PV)", "Nur Photovoltaik", "Nur Wind Onshore", "Nur Wind Offshore")
    )
        
    # Feature Engineering (temporär für diesen Plot berechnet)
    df_plot = df.copy()
    
    if focus == "Nur Photovoltaik":
        df_plot['Error_MWh'] = df_plot['actual_pv'] - df_plot['forecast_pv']
    elif focus == "Nur Wind Onshore":
        df_plot['Error_MWh'] = df_plot['actual_wind_onshore'] - df_plot['forecast_wind_onshore']
    elif focus == "Nur Wind Offshore":
        df_plot['Error_MWh'] = df_plot['actual_wind_offshore'] - df_plot['forecast_wind_offshore']
    else:
        df_plot['actual_weather'] = df_plot['actual_pv'] + df_plot['actual_wind_onshore'] + df_plot['actual_wind_offshore']
        df_plot['forecast_weather'] = df_plot['forecast_pv'] + df_plot['forecast_wind_onshore'] + df_plot['forecast_wind_offshore']
        df_plot['Error_MWh'] = df_plot['actual_weather'] - df_plot['forecast_weather']

    # Farbe: Rot für Defizit (Short/Unterdeckung), Grün für Überschuss (Long/Überdeckung)
    df_plot['Error_Color'] = df_plot['Error_MWh'].apply(lambda x: '#d62728' if pd.notnull(x) and x < 0 else '#2ca02c')

    # Optionaler lokaler Zoom-Slider (innerhalb des globalen Zeitfensters)
    min_dt = df_plot["datetime_local"].min().to_pydatetime()
    max_dt = df_plot["datetime_local"].max().to_pydatetime()

    selected_range = st.slider(
        "Detail-Zoom (Suche nach extremen Ausschlägen):",
        min_value=min_dt, max_value=max_dt, value=(min_dt, max_dt),
        format="DD.MM.YY - HH:mm", step=pd.Timedelta(hours=1), key="prog_slider"
    )
    
    df_plot = df_plot[(df_plot['datetime_local'] >= selected_range[0]) & (df_plot['datetime_local'] <= selected_range[1])]

    # --- Plotly Rendering ---
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Trace 1: Balken (Volumenfehler)
    fig.add_trace(
        go.Bar(
            x=df_plot['datetime_local'], 
            y=df_plot['Error_MWh'],
            marker_color=df_plot['Error_Color'],
            name=f"Prognosefehler {focus} (MWh)",
            hovertemplate="Uhrzeit: %{x|%d.%m. %H:%M}<br>Error: %{y:.1f} MWh<extra></extra>",
            opacity=0.7
        ),
        secondary_y=False,
    )

    # Trace 2: Linie (Pönale) - Nutzt den Short-Preis als Indikator für Systemstress
    fig.add_trace(
        go.Scatter(
            x=df_plot['datetime_local'], 
            y=df_plot['price_imbalance_short'],
            mode='lines',
            name="Ausgleichsenergie-Preis (€)",
            line=dict(color='#d62728', width=2), 
            hovertemplate="Uhrzeit: %{x|%d.%m. %H:%M}<br>Strafpreis: %{y:.1f} €/MWh<extra></extra>"
        ),
        secondary_y=True,
    )

    fig.update_layout(
        title=f"Kausalanalyse: Einfluss von '{focus}' auf Ausgleichsenergiekosten",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1)
    )

    fig.update_yaxes(title_text="Fehlmenge (MWh)", secondary_y=False, showgrid=True, gridcolor="rgba(211, 211, 211, 0.2)")
    fig.update_yaxes(title_text="Ausgleichsenergie-Preis (€/MWh)", secondary_y=True, showgrid=False)

    st.plotly_chart(fig, use_container_width=True)
    
    st.info(
        "**Methodischer Hinweis zur Kausalität:**\n\n"
        "Wenn ein massiver Einbruch bei der Erzeugung zu sehen ist (rote Balken nach unten), aber der Ausgleichsenergiepreis (rote Linie) stabil bleibt, "
        "haben sich Prognosefehler im Stromnetz gegenseitig aufgehoben. Schlägt die Linie jedoch gemeinsam mit den Balken aus, "
        "war die ausgewählte Erzeugungsart der dominante Treiber für das Defizit im Übertragungsnetz."
    )