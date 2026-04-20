import logging

# Import der etablierten Orchestratoren
from etl_pipelines.etl_weather import run_weather_etl
from etl_pipelines.etl_smard_actuals import run_smard_etl
from etl_pipelines.etl_smard_forecast import run_forecasts_etl
from etl_pipelines.etl_entsoe import run_entsoe_etl

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_full_backfill(start_year: int, end_year: int):
    """
    MASTER ORCHESTRATOR: Führt die historische Erstbeladung des Data Warehouses durch.
    Iteriert jahresweise, um API-Timeouts und RAM-Überläufe zu vermeiden.
    """
    logger.info(f"Starte Historical Backfill von {start_year} bis {end_year}...")

    for year in range(start_year, end_year + 1):
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        logger.info(f"{'='*40}")
        logger.info(f"VERARBEITE JAHR: {year}")
        logger.info(f"{'='*40}")

        try:
            # 1. Wetterdaten laden
            run_weather_etl(start_date, end_date)
            
            # 2. SMARD Ist-Werte laden
            run_smard_etl(start_date, end_date)

            # 3. SMARD Prognosen laden
            run_forecasts_etl(start_date, end_date)
            
            # 4. ENTSO-E Pönalen laden
            run_entsoe_etl(start_date, end_date)
            
            logger.info(f"Jahr {year} erfolgreich verarbeitet und in die DB geschrieben.")

        except Exception as e:
            logger.critical(f"Kritischer Fehler beim Backfill für {year}: {e}")
            logger.info("Breche Backfill ab, um Dateninkonsistenzen zu vermeiden.")
            break

    logger.info("Historical Backfill beendet!")

if __name__ == "__main__":
    run_full_backfill(start_year=2000, end_year=2026)