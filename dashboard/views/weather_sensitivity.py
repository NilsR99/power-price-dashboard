import streamlit as st
import plotly.express as px

def render(df):
    st.header("Wetter-Sensitivität (Hitze führt zu höheren Preisen)")
    
    required_cols = ["temperature_2m", "price_day_ahead"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        st.warning(f"Für diese Analyse fehlen folgende Spalten im Datensatz: {missing_cols}")
        return

    df_clean = df.dropna(subset=required_cols).copy()
    
    # Runden der Temperatur für die Aggregation
    df_clean["temp_rounded"] = df_clean["temperature_2m"].round()
    
    df_agg = df_clean.groupby("temp_rounded").agg(
        mean_price=("price_day_ahead", "mean"),
        hour_count=("price_day_ahead", "count") 
    ).reset_index()

    fig_scatter = px.scatter(
        df_agg, x="temp_rounded", y="mean_price", size="hour_count",
        color_discrete_sequence=["#1f77b4"], 
        labels={
            "temp_rounded": "Temperatur (°C)", 
            "mean_price": "Ø Preis (€/MWh)",
            "hour_count": "Anzahl Stunden"
        },
        hover_data=["hour_count"] 
    )
    
    fig_scatter.update_layout(height=650)
    fig_scatter.add_hline(y=0, line_dash="dash", line_color="red")
    fig_scatter.add_vline(x=30, line_dash="solid", line_color="firebrick", line_width=2, annotation_text="Hitzegrenze (>30°C)")
    fig_scatter.add_vrect(x0=30, x1=df_agg["temp_rounded"].max(), fillcolor="lightsalmon", opacity=0.3, layer="below")
    
    st.plotly_chart(fig_scatter, use_container_width=True)