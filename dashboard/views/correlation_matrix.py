import streamlit as st
import plotly.express as px

def render(df):
    st.header("Korrelations-Matrix")
    st.markdown(
        "**Statistische Untersuchung:** Diese Matrix berechnet den linearen Zusammenhang (Pearson-Korrelation) "
        "zwischen den gewählten Metriken. Werte nahe 1 bedeuten einen starken positiven, Werte nahe -1 einen starken negativen Zusammenhang."
    )
    
    # Automatische Extraktion aller numerischen Spalten aus dem Data Warehouse
    df_numeric = df.select_dtypes(include=['number']).dropna(axis=1, how='all')
    all_available_columns = df_numeric.columns.tolist()
    
    # Sinnvolle Standardauswahl
    default_selection = ["price_day_ahead", "temperature_2m", "actual_total_load", "actual_wind_onshore", "actual_pv"]
    valid_defaults = [col for col in default_selection if col in all_available_columns]
    
    selected_columns = st.multiselect(
        "Metriken für die Korrelation auswählen (Mindestens 2):",
        options=all_available_columns,
        default=valid_defaults if valid_defaults else all_available_columns[:2]
    )

    if len(selected_columns) < 2:
        st.warning("Analytischer Fehler: Es müssen mindestens 2 Variablen ausgewählt werden, um eine Korrelation zu berechnen.")
        return

    # Datenbasis auf Auswahl reduzieren und berechnen
    df_filtered = df_numeric[selected_columns]
    
    with st.expander("Datenqualität (Missing Values) der Auswahl anzeigen"):
        missing_data_ratio = df_filtered.isna().sum() / len(df_filtered) * 100
        st.dataframe(missing_data_ratio.round(2).astype(str) + " %")

    corr_matrix = df_filtered.corr()

    # Plotly Rendering
    fig = px.imshow(
        corr_matrix, 
        text_auto=".2f", 
        aspect="auto",
        color_continuous_scale="RdBu_r", 
        zmin=-1, 
        zmax=1,
        labels=dict(color="Korrelation")
    )
    
    fig.update_layout(
        height=max(450, len(selected_columns) * 60), 
        margin=dict(l=0, r=0, b=0, t=30)
    )
    
    st.plotly_chart(fig, use_container_width=True)  