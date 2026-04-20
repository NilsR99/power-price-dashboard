import pandas as pd
import logging
from sqlalchemy.exc import IntegrityError
from sqlalchemy.engine import Engine
from warehouse.db_connector import get_db_engine

# Konfiguration des Loggers (Idealerweise zentral in einer config.py)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def generate_dim_time(start_year: int = 1950, end_year: int = 2026, local_tz: str = 'Europe/Berlin') -> pd.DataFrame:
    """
    Reine Transformations-Logik. Generiert die Zeitachse als DataFrame.
    Vollständig isoliert von der Infrastruktur (Datenbank), daher trivial testbar.
    """
    if start_year > end_year:
        raise ValueError(f"Logikfehler: start_year ({start_year}) darf nicht nach end_year ({end_year}) liegen.")

    logger.info(f"Generiere Zeitachse von {start_year} bis {end_year} (Lokalzeit: {local_tz})...")
    
    # 1. Stundengenaue UTC-Zeitreihe generieren
    date_range_utc = pd.date_range(
        start=f'{start_year}-01-01 00:00:00', 
        end=f'{end_year}-12-31 23:00:00', 
        freq='h', 
        tz='UTC'
    )
    
    df = pd.DataFrame({'datetime_utc': date_range_utc})
    
    # 2. Lokale Zeit ableiten
    df['datetime_local'] = df['datetime_utc'].dt.tz_convert(local_tz)
    
    # 3. Den Primary Key generieren: YYYYMMDDHH
    df['time_id'] = df['datetime_utc'].dt.strftime('%Y%m%d%H').astype('int64')
    
    # 4. Dimensionen extrahieren
    df['year'] = df['datetime_local'].dt.year
    df['month'] = df['datetime_local'].dt.month
    df['day'] = df['datetime_local'].dt.day
    df['hour'] = df['datetime_local'].dt.hour
    df['weekday'] = df['datetime_local'].dt.weekday  # 0=Montag, 6=Sonntag
    df['is_weekend'] = df['weekday'] >= 5
    
    # 5. Spalten in die Reihenfolge des SQL-Schemas bringen
    return df[['time_id', 'datetime_utc', 'datetime_local', 'year', 'month', 'day', 'hour', 'weekday', 'is_weekend']]


def load_to_database(df: pd.DataFrame, table_name: str, engine: Engine, chunksize: int = 10000) -> None:
    """
    Reine Lade-Logik (IO-Operation).
    Nimmt ein fertiges DataFrame entgegen und persistiert es.
    """
    logger.info(f"Schreibe {len(df)} Zeilen in Tabelle '{table_name}'...")
    try:
        df.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=chunksize)
        logger.info(f"✅ Tabelle '{table_name}' erfolgreich befüllt!")
        
    except IntegrityError as ie:
        logger.error(f"❌ Integritätsfehler (z.B. Primary Key Konflikt) beim Schreiben in '{table_name}': {ie.orig}")
        raise  # Den Fehler werfen, damit übergeordnete Orchestratoren (wie Airflow) den Job abbrechen.
        
    except Exception as e:
        logger.error(f"❌ Unerwarteter IO-Fehler beim Schreiben in '{table_name}': {e}")
        raise


def main():
    """
    Orchestrierungs-Schicht: Verbindet Extraktion/Transformation mit dem Laden.
    """
    try:
        # 1. Daten generieren (Testbar)
        df_time = generate_dim_time(start_year=2024, end_year=2026)
        
        # 2. Infrastruktur initialisieren
        engine = get_db_engine()
        
        # 3. Daten laden
        load_to_database(df=df_time, table_name='dim_time', engine=engine)
        
    except Exception as e:
        logger.critical(f"Pipeline-Abbruch: {e}")


if __name__ == "__main__":
    main()