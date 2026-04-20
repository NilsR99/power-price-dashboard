import os
import logging
import pandas as pd
from entsoe import EntsoePandasClient
from dotenv import load_dotenv
import traceback

load_dotenv()
logger = logging.getLogger(__name__)

def fetch_entsoe_imbalance(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Holt Ausgleichsenergiepreise von ENTSO-E.
    Gibt ein standardisiertes, datenbankfertiges Pandas DataFrame zurück.
    """
    api_key = os.getenv("ENTSOE_API_KEY")
    if not api_key:
        raise ValueError("Kritischer Fehler: 'ENTSOE_API_KEY' fehlt in der .env Datei!")

    logger.info(f"Starte ENTSO-E Imbalance-Abruf von {start_date} bis {end_date}...")
    client = EntsoePandasClient(api_key=api_key)
    
    start = pd.Timestamp(start_date, tz='Europe/Berlin')
    end = pd.Timestamp(end_date, tz='Europe/Berlin')
    country_code = 'DE' 
    
    try:
        imb_prices = client.query_imbalance_prices(country_code, start=start, end=end)
        
        # Schema stabilisieren: Sicherstellen, dass es immer ein DataFrame ist
        if isinstance(imb_prices, pd.Series):
            df_entsoe = pd.DataFrame(imb_prices, columns=['price_imbalance_short'])
            df_entsoe['price_imbalance_long'] = df_entsoe['price_imbalance_short']
        else:
            df_entsoe = imb_prices.rename(columns={
                'Short': 'price_imbalance_short', 
                'Long': 'price_imbalance_long'
            })

        # Resampling auf Stundenbasis (Harmonisierung mit SMARD)
        df_entsoe = df_entsoe.resample('1h').mean()
        
        # Zeitzone sicher auf UTC normieren, um Fehler bei der time_id Generierung zu vermeiden
        df_entsoe = df_entsoe.tz_convert('UTC').reset_index()
        df_entsoe = df_entsoe.rename(columns={'index': 'date'})

        logger.info(f"ENTSO-E Abruf erfolgreich ({len(df_entsoe)} Stunden geladen).")
        return df_entsoe

    except Exception as e:
        logger.error(f"Fehler bei der ENTSO-E API: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame() # Leeres DataFrame bei Fehler zurückgeben, damit der Pipeline-Schutz greift