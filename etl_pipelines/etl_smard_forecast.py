import pandas as pd
import logging
import os

# Import der In-Memory-Extraktion
from api_response_scripts.fetch_smard_data import fetch_smard_data

# Import der etablierten Datenbank-Infrastruktur
from src.warehouse.db.connector import get_db_engine
from src.warehouse.db.operations import idempotent_upsert

# Korrekte Konfiguration für Prognosen
SMARD_FORECAST_CONFIG = [
    ("123",  "forecast_wind_onshore"),      # Prognostizierte Erzeugung: Onshore
    ("125",  "forecast_pv"),                # Prognostizierte Erzeugung: Photovoltaik
    ("3791", "forecast_wind_offshore"),     # Prognostizierte Erzeugung: Offshore
    ("715",  "forecast_other"),             # Prognostizierte Erzeugung: Sonstige
    ("122",  "forecast_total_load")         # Prognostizierte Erzeugung: Gesamt
]

# Logging Setup
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def in_memory_merge(dataframes: list) -> pd.DataFrame:
    """Nimmt eine Liste von DataFrames und merged sie über die 'date' Spalte."""
    if not dataframes:
        return pd.DataFrame()
        
    df_merged = dataframes[0]
    for df_temp in dataframes[1:]:
        df_merged = pd.merge(df_merged, df_temp, on='date', how='outer')
        
    return df_merged.sort_values('date').reset_index(drop=True)

def extract_and_transform_forecasts(start_date: str, end_date: str) -> pd.DataFrame:
    """
    EXTRACT & TRANSFORM: Lädt und transformiert die Prognosedaten In-Memory.
    """
    logger.info(f"Starte Extraktion der SMARD-Prognosen von {start_date} bis {end_date}...")
    
    # --- 1. EXTRACT ---
    dataframes = []
    # KORREKTUR: Die richtige Config-Liste verwenden
    for filter_id, metric_name in SMARD_FORECAST_CONFIG:
        logger.info(f"Lade API-Daten für: {metric_name}...")
        df_metric = fetch_smard_data(
            filter_id=filter_id, 
            start_date=start_date, 
            end_date=end_date, 
            metric_name=metric_name
        )
        if not df_metric.empty:
            dataframes.append(df_metric)

    df_raw = in_memory_merge(dataframes)

    if df_raw.empty:
        raise ValueError("Keine SMARD-Prognosedaten gefunden. API offline oder fehlerhafter Zeitraum?")

    # --- 2. TRANSFORM ---
    logger.info("Transformiere Prognosedaten für das Data src.warehouse..")
    df_transformed = df_raw.copy()

    # time_id generieren und Aufräumen
    df_transformed['time_id'] = df_transformed['date'].dt.strftime('%Y%m%d%H').astype('int64')
    df_transformed = df_transformed.drop(columns=['date'])

    # Duplikats-Prüfung
    anzahl_vorher = len(df_transformed)
    df_transformed = df_transformed.drop_duplicates(subset=['time_id'], keep='last')
    if anzahl_vorher != len(df_transformed):
        logger.warning(f"{anzahl_vorher - len(df_transformed)} Duplikate entfernt!")
    
    # Schema-Schutz: Sicherstellen, dass nur die prognostizierten Spalten in die DB fließen
    expected_columns = [
        'time_id', 'forecast_total_load', 'forecast_wind_onshore', 
        'forecast_wind_offshore', 'forecast_pv', 'forecast_other'
    ]
    df_transformed = df_transformed[[col for col in expected_columns if col in df_transformed.columns]]
    
    return df_transformed


def run_forecasts_etl(start_date: str, end_date: str):
    """
    ORCHESTRATOR: Steuert den Datenfluss der Prognosen von der API bis in die Datenbank.
    """
    try:
        df_final = extract_and_transform_forecasts(start_date, end_date)
        engine = get_db_engine()

        # KORREKTUR: Zieldatenbank auf fact_market_forecasts geändert
        idempotent_upsert(df=df_final, table_name='fact_market_forecasts', engine=engine)

        logger.info("Prognose ETL-Pipeline erfolgreich abgeschlossen!")

    except Exception as e:
        logger.critical(f"ETL-Abbruch: {e}")


if __name__ == "__main__":
    # Test-Lauf für einen sicheren, kleinen Zeitraum
    run_forecasts_etl("2024-01-01", "2024-01-07")