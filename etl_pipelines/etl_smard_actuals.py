import pandas as pd
import logging
import os

# Import deiner bestehenden Konfiguration und Skripte
from api_response_scripts.fetch_smard_data import fetch_smard_data

# Import der neuen Datenbank-Infrastruktur
from src.warehouse.db.connector import get_db_engine
from src.warehouse.db.operations import idempotent_upsert

SMARD_CONFIG = [
    ("4169", "price_day_ahead"),
    ("410",  "actual_total_load"),
    ("4359", "actual_residual_load"),
    ("4067", "actual_wind_onshore"),
    ("4068", "actual_pv"),
    ("1225", "actual_wind_offshore"),
    ("4071", "actual_gas"),
    ("1223", "actual_brown_coal"),
    ("1224", "actual_nuclear"),
    ("1226", "actual_hydro"),
    ("1227", "actual_other_conventional"),
    ("1228", "actual_other_renewables"),
    ("4066", "actual_biomass"),
    ("4069", "actual_hard_coal"),
    ("4070", "actual_pumped_storage")
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

def extract_and_transform_smard(start_date: str, end_date: str) -> pd.DataFrame:
    """
    EXTRACT & TRANSFORM: Alles In-Memory!
    """
    logger.info(f"Starte Extraktion der SMARD-Ist-Werte von {start_date} bis {end_date}...")
    
    # --- 1. EXTRACT (Rohdaten direkt in den RAM laden) ---
    dataframes = []
    for filter_id, metric_name in SMARD_CONFIG:
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
        raise ValueError("Keine SMARD-Daten gefunden. API offline oder fehlerhafter Zeitraum?")

    # --- 2. TRANSFORM (Auf das Star-Schema anpassen) ---
    logger.info("Transformiere Daten für das Data src.warehouse..")
    df_transformed = df_raw.copy()

    # A. Die time_id aus dem UTC-Datum generieren
    df_transformed['time_id'] = df_transformed['date'].dt.strftime('%Y%m%d%H').astype('int64')

    # B. Aufräumen
    df_transformed = df_transformed.drop(columns=['date'])

    # C. Duplikats-Prüfung
    anzahl_vorher = len(df_transformed)
    df_transformed = df_transformed.drop_duplicates(subset=['time_id'], keep='last')
    if anzahl_vorher != len(df_transformed):
        logger.warning(f"{anzahl_vorher - len(df_transformed)} Duplikate entfernt!")
    
    return df_transformed


def run_smard_etl(start_date: str, end_date: str):
    """
    ORCHESTRATOR: Steuert den Datenfluss von der API bis in die Datenbank.
    """
    try:
        # 1. Daten laden und transformieren
        df_final = extract_and_transform_smard(start_date, end_date)

        # 2. Datenbank-Engine initialisieren
        engine = get_db_engine()

        # 3. LOAD (Idempotent in die Datenbank schreiben)
        # fact_market_actuals ist die Tabelle, die wir im schema.sql definiert haben
        idempotent_upsert(df=df_final, table_name='fact_market_actuals', engine=engine)

        logger.info("SMARD ETL-Pipeline erfolgreich abgeschlossen!")

    except Exception as e:
        logger.critical(f"ETL-Abbruch: {e}")


if __name__ == "__main__":
    # Test-Lauf für die erste Januarwoche 2024
    run_smard_etl("2020-01-01", "2025-12-31")