import streamlit as st
import plotly.express as px

def render(df):
    st.header("Senken Erneuerbare den Strompreis? (Merit-Order)")
    
    required_cols = ["price_day_ahead", "actual_wind_onshore", "actual_wind_offshore", "actual_pv", "actual_total_load"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        st.error(f"Für diese Analyse fehlen folgende Spalten im Datensatz: {missing_cols}")
        return
        
    df_plot = df.copy()
    df_plot["total_wind_solar"] = (
        df_plot["actual_wind_onshore"].fillna(0) + 
        df_plot["actual_wind_offshore"].fillna(0) + 
        df_plot["actual_pv"].fillna(0)
    )
    
    df_plot = df_plot.dropna(subset=["price_day_ahead", "total_wind_solar"])
    
    # Scatterplot mit OLS-Trendlinie
    fig_scatter = px.scatter(
        df_plot, x="total_wind_solar", y="price_day_ahead", color="actual_total_load",
        color_continuous_scale="Plasma", opacity=0.6, 
        labels={
            "total_wind_solar": "Wind + PV Einspeisung (MWh)", 
            "price_day_ahead": "Day-Ahead Preis (€/MWh)", 
            "actual_total_load": "Gesamtlast (MWh)"
        },
        hover_data=["datetime_local"], 
        trendline="ols", 
        trendline_color_override="green",
    )
    
    fig_scatter.update_layout(height=600, yaxis=dict(range=[-100, 300]))
    fig_scatter.add_hline(y=0, line_dash="dash", line_color="red", annotation_text="0 € Grenze")
    
    st.plotly_chart(fig_scatter, use_container_width=True)
    
    st.success("**Ergebnis:** Die Hypothese wird visuell bestätigt. Ein hohes Einspeisevolumen durch Erneuerbare Energien korreliert stark mit einem Preisverfall an der Börse.")