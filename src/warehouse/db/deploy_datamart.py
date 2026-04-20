import logging
import os
from sqlalchemy import text
from warehouse.db.connector import get_db_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def deploy_view():
    """Liest das SQL-Skript und erstellt die Data-Mart-View in der Datenbank."""
    sql_file_path = 'src/warehouse/db/schema_datamart.sql'
    
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as file:
            sql_script = file.read()
            
        engine = get_db_engine()
        
        # Engine.begin() öffnet eine Transaktion für DDL-Befehle
        with engine.begin() as conn:
            conn.execute(text(sql_script))
            
        logger.info("✅ Data Mart (View 'v_power_dashboard') erfolgreich in der Datenbank erstellt!")
        
    except FileNotFoundError as e:
        logger.error(f"CWD: {os.getcwd()}")
        logger.error(f"Pfad existiert nicht: {sql_file_path}")
        logger.error(str(e))
    except Exception as e:
        logger.error(f"Unerwarteter Fehler beim Erstellen der View: {e}")

if __name__ == "__main__":
    deploy_view()