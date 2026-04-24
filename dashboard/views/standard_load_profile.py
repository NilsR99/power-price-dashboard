import streamlit as st
import plotly.express as px
import pandas as pd

def render(df):
    st.header("Tageslastprofil (SLP) Deutschland")
    st.markdown(
        "**Verbrauchsmuster:** Dieses Diagramm aggregiert die absolute Netzlast (Stromverbrauch) "
        "zu einem durchschnittlichen 24-Stunden-Profil. Es berechnet den Durchschnitt exakt für das in der Sidebar "
        "gewählte globale Zeitfenster und trennt dabei in Werktage und Wochenenden."
    )
    
    if "actual_total_load" not in df.columns or "datetime_local" not in df.columns:
        st.error("Analytischer Fehler: Benötigte Spalten fehlen im Datensatz.")
        return

    # Datenbereinigung
    df_slp = df.dropna(subset=["actual_total_load", "datetime_local"]).copy()
    
    if df_slp.empty:
        st.warning("Keine Last-Daten im ausgewählten Zeitraum verfügbar.")
        return

    # Feature Engineering direkt aus datetime_local
    df_slp['hour_local'] = df_slp['datetime_local'].dt.hour
    df_slp['weekday'] = df_slp['datetime_local'].dt.dayofweek
    df_slp['Tagesart'] = df_slp['weekday'].apply(lambda x: 'Wochenende (Sa/So)' if x >= 5 else 'Werktag (Mo-Fr)')
    
    # Aggregation: Gruppierung nach Stunde und Tagesart
    df_agg = df_slp.groupby(['hour_local', 'Tagesart'])['actual_total_load'].mean().reset_index()

    # Plotly Rendering
    color_map = {
        'Werktag (Mo-Fr)': '#1f77b4',
        'Wochenende (Sa/So)': '#ff7f0e'      
    }
    dash_map = {
        'Werktag (Mo-Fr)': 'solid',
        'Wochenende (Sa/So)': 'dash' 
    }

    fig_slp = px.line(
        df_agg, 
        x="hour_local", 
        y="actual_total_load", 
        color="Tagesart",           
        line_dash="Tagesart",      
        color_discrete_map=color_map, 
        line_dash_map=dash_map,
        labels={
            "hour_local": "Uhrzeit (Lokalzeit)", 
            "actual_total_load": "Ø Gesamtlast (MWh)"
        },
        markers=True,
        title="Durchschnittliches Lastprofil im ausgewählten Zeitraum"
    )
    
    fig_slp.update_layout(
        height=550, 
        xaxis=dict(tickmode='linear', tick0=0, dtick=1), # Zwingt Plotly, jede Stunde anzuzeigen
        legend=dict(title=None, orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified"
    )
    
    st.plotly_chart(fig_slp, use_container_width=True)