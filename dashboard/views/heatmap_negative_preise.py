import streamlit as st
import plotly.express as px
import pandas as pd

def render(df):
    st.header("Heatmap: Verteilung negativer Strompreise")
    st.markdown(
        "**Ursachenforschung:** Dieses Diagramm visualisiert, dass negative Preise nicht nur von der absoluten "
        "Einspeisemenge (MWh) abhängen, sondern massiv vom **Zeitpunkt der Einspeisung**. "
        "Es zeigt die absolute Häufigkeit (Anzahl der Stunden) negativer Strompreise (< 0 €) im gewählten Zeitfenster."
    )

    if "price_day_ahead" not in df.columns or "datetime_local" not in df.columns:
        st.error("Analytischer Fehler: Benötigte Spalten fehlen im Datensatz.")
        return

    # Datenbereinigung und Kopie zur Vermeidung von SettingWithCopy-Warnungen
    df_plot = df.dropna(subset=["price_day_ahead", "datetime_local"]).copy()
    
    # Feature Engineering (Wochentag und Stunde aus der lokalen Zeit extrahieren)
    df_plot['Stunde'] = df_plot['datetime_local'].dt.hour
    
    # Wochentage fest als kategoriale Variable definieren (für chronologische Sortierung)
    wochentage = ['Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag', 'Samstag', 'Sonntag']
    df_plot['Wochentag'] = df_plot['datetime_local'].dt.dayofweek.map(lambda x: wochentage[x])
    df_plot['Wochentag'] = pd.Categorical(df_plot['Wochentag'], categories=wochentage, ordered=True)

    # Filterung auf negative Preise
    df_neg = df_plot[df_plot['price_day_ahead'] < 0]

    if df_neg.empty:
        st.success("Im analysierten Datensatz gab es **keine einzige Stunde** mit negativen Strompreisen!")
        return

    # Mathematische Aggregation zur 7x24 Matrix
    df_agg = df_neg.groupby(['Wochentag', 'Stunde'], observed=False).size().unstack(fill_value=0)
    df_agg = df_agg.reindex(columns=range(24), fill_value=0)

    # Plotly Rendering
    fig = px.imshow(
        df_agg,
        labels=dict(x="Uhrzeit (Lokalzeit)", y="Wochentag", color="Anzahl Negativ-Stunden"),
        x=df_agg.columns,
        y=df_agg.index,
        color_continuous_scale="Reds", 
        aspect="auto",
        text_auto=True 
    )

    fig.update_layout(
        title="Verteilung negativer Strompreise nach Wochentag und Uhrzeit",
        xaxis=dict(tickmode='linear', tick0=0, dtick=1),
        height=500
    )
    
    # Die Y-Achse umdrehen, damit Montag oben steht
    fig.update_yaxes(autorange="reversed")

    st.plotly_chart(fig, use_container_width=True)

    # Energiewirtschaftliche Auswertung
    st.info(
        "**Energiewirtschaftliche Auswertung:**\n\n"
        "Die Heatmap offenbart häufig einen tiefroten Kern im Bereich **Sonntagmittag (12:00 - 15:00 Uhr)**. "
        "Dieses Muster markiert den sogenannten *'Inflexibilitäts-Kollaps'*:\n"
        "* **Minimale Last:** Die Industrie ruht, der Verbrauch ist auf dem wöchentlichen Tiefststand.\n"
        "* **Maximale Einspeisung:** Die Photovoltaik erreicht ihren Leistungszenit.\n"
        "* **Must-Run Blockade:** Konventionelle Großkraftwerke können aus technischen und wirtschaftlichen Gründen (Anfahrtskosten) nicht kurzfristig abgeschaltet werden. Sie speisen weiterhin ein und bieten zu negativen Preisen an, um Netzstabilität zu wahren.\n\n"
        "Die Preisstruktur kollabiert somit durch das Zusammentreffen mangelnder Nachfrage, hoher Erneuerbarer Einspeisung und unflexibler Alt-Kraftwerke."
    )