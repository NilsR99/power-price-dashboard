import os
import logging
from sqlalchemy import text
from src.warehousedb.connector import get_db_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def initialize_database():
    """Liest die schema.sql und führt sie auf der PostgreSQL-Datenbank aus."""
    engine = get_db_engine()
    
    # Pfad zur SQL-Datei dynamisch ermitteln
    current_dir = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.join(current_dir, "schema.sql")
    
    if not os.path.exists(schema_path):
        logging.error(f"Fehler: Die Datei {schema_path} wurde nicht gefunden.")
        return

    with open(schema_path, 'r', encoding='utf-8') as file:
        sql_commands = file.read()

    try:
        # Mit SQLAlchemy eine Verbindung öffnen und die Commands ausführen
        with engine.begin() as conn:
            # text() deklariert den String als rohen SQL-Befehl
            conn.execute(text(sql_commands))
        logging.info("✅ Star-Schema erfolgreich in der PostgreSQL-Datenbank angelegt!")
    except Exception as e:
        logging.error(f"❌ Fehler bei der Datenbank-Initialisierung: {e}")

if __name__ == "__main__":
    initialize_database()