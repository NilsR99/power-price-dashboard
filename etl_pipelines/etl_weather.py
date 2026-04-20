import pandas as pd
import logging

from api_response_scripts.fetch_weather_data import fetch_weather_data 

from warehouse.db.connector import get_db_engine
from warehouse.db.operations import idempotent_upsert

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_and_transform_weather(start_date: str, end_date: str) -> pd.DataFrame:
    """
    EXTRACT & TRANSFORM: Lädt historische Wetterdaten (Deutschland-Proxy) 
    und bereitet sie für das Data Warehouse vor.
    """
    logger.info(f"Starte Extraktion der Wetterdaten von {start_date} bis {end_date}...")
    
    # --- 1. EXTRACT ---
    # Die Funktion holt die Daten von Open-Meteo und aggregiert sie bereits
    df_raw = fetch_weather_data(start_date, end_date)

    if df_raw is None or df_raw.empty:
        raise ValueError("Keine Wetterdaten gefunden. API offline oder fehlerhafter Zeitraum?")

    # --- 2. TRANSFORM ---
    logger.info("Transformiere Wetterdaten für das Data Warehouse...")
    df_transformed = df_raw.copy()

    # A. Zeitstempel standardisieren (UTC erzwingen, falls nicht schon passiert)
    df_transformed['date'] = pd.to_datetime(df_transformed['date'], utc=True)
    
    # B. Den Fremdschlüssel (time_id) generieren
    df_transformed['time_id'] = df_transformed['date'].dt.strftime('%Y%m%d%H').astype('int64')

    # C. Härteprüfung: Duplikate entfernen (Keep Last)
    anzahl_vorher = len(df_transformed)
    df_transformed = df_transformed.drop_duplicates(subset=['time_id'], keep='last')
    if anzahl_vorher != len(df_transformed):
        logger.warning(f"{anzahl_vorher - len(df_transformed)} Wetter-Duplikate entfernt!")

    # D. Schema-Schutz: Nur die Spalten behalten, die fact_weather erwartet
    expected_columns = ['time_id', 'temperature_2m', 'wind_speed_100m', 'cloud_cover']
    df_transformed = df_transformed[[col for col in expected_columns if col in df_transformed.columns]]

    return df_transformed


def run_weather_etl(start_date: str, end_date: str):
    """
    ORCHESTRATOR: Steuert den Datenfluss von der Wetter-API bis in die Faktentabelle.
    """
    try:
        # 1. Daten laden und transformieren
        df_final = extract_and_transform_weather(start_date, end_date)
        
        # 2. Datenbank-Engine initialisieren
        engine = get_db_engine()
        
        # 3. LOAD (Idempotent in die Datenbank schreiben)
        idempotent_upsert(df=df_final, table_name='fact_weather', engine=engine)
        
        logger.info("Wetter ETL-Pipeline erfolgreich abgeschlossen!")
        
    except Exception as e:
        logger.critical(f"Wetter ETL-Abbruch: {e}")


if __name__ == "__main__":
    # Test-Lauf: Wir laden die ersten zwei Wochen von 2024
    run_weather_etl("2024-01-01", "2024-01-14")