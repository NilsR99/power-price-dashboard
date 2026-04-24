import pandas as pd
import logging

from api_response_scripts.fetch_entsoe_data import fetch_entsoe_imbalance

from src.warehousedb.connector import get_db_engine
from src.warehousedb.operations import idempotent_upsert

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_and_transform_entsoe(start_date: str, end_date: str) -> pd.DataFrame:
    """
    EXTRACT & TRANSFORM: Lädt ENTSO-E Ausgleichsenergie-Daten (Pönalen)
    und bereitet sie für die Faktentabelle vor.
    """
    logger.info(f"Starte Extraktion der ENTSO-E Daten von {start_date} bis {end_date}...")
    
    # --- 1. EXTRACT ---
    df_raw = fetch_entsoe_imbalance(start_date, end_date)

    if df_raw is None or df_raw.empty:
        raise ValueError("Keine ENTSO-E Daten gefunden. API offline oder fehlerhafter Zeitraum?")

    # --- 2. TRANSFORM ---
    logger.info("Transformiere ENTSO-E Daten für das Data src.warehouse..")
    df_transformed = df_raw.copy()

    # Zeitstempel und Primärschlüssel (time_id)
    df_transformed['date'] = pd.to_datetime(df_transformed['date'], utc=True)
    df_transformed['time_id'] = df_transformed['date'].dt.strftime('%Y%m%d%H').astype('int64')

    # Duplikate entfernen (Keep Last)
    anzahl_vorher = len(df_transformed)
    df_transformed = df_transformed.drop_duplicates(subset=['time_id'], keep='last')
    if anzahl_vorher != len(df_transformed):
        logger.warning(f"{anzahl_vorher - len(df_transformed)} ENTSO-E-Duplikate entfernt!")

    # Schema-Schutz: Nur die in fact_entsoe_imbalance definierten Spalten behalten
    expected_columns = ['time_id', 'price_imbalance_short', 'price_imbalance_long']
    df_transformed = df_transformed[[col for col in expected_columns if col in df_transformed.columns]]

    return df_transformed


def run_entsoe_etl(start_date: str, end_date: str):
    """
    ORCHESTRATOR: Steuert den Datenfluss für ENTSO-E.
    """
    try:
        df_final = extract_and_transform_entsoe(start_date, end_date)
        engine = get_db_engine()
        idempotent_upsert(df=df_final, table_name='fact_entsoe_imbalance', engine=engine)
        logger.info("ENTSO-E ETL-Pipeline erfolgreich abgeschlossen!")
        
    except Exception as e:
        logger.critical(f"ENTSO-E ETL-Abbruch: {e}")


if __name__ == "__main__":
    run_entsoe_etl("2015-01-01", "2015-12-31")