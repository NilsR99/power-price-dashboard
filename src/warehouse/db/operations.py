import pandas as pd
import logging
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

def idempotent_upsert(df: pd.DataFrame, table_name: str, engine: Engine) -> None:
    """
    Führt einen idempotenten Ladevorgang (Delete-before-Insert) durch.
    Nutzt eine ACID-Transaktion: Wenn das Einfügen fehlschlägt, wird das Löschen rückgängig gemacht (Rollback).
    """
    if df.empty:
        logger.warning(f"⚠️ DataFrame für Tabelle '{table_name}' ist leer. Überspringe Upsert.")
        return

    if 'time_id' not in df.columns:
        raise ValueError("Kritischer Fehler: Das DataFrame muss eine 'time_id' Spalte für den Upsert enthalten.")

    # 1. Grenzen für das Löschen ermitteln
    min_id = int(df['time_id'].min())
    max_id = int(df['time_id'].max())

    # Parameterisierte Query zum Schutz vor SQL-Injection
    delete_query = text(f"""
        DELETE FROM {table_name}
        WHERE time_id >= :min_id AND time_id <= :max_id
    """)

    logger.info(f"🔄 Starte Idempotenten Upsert für '{table_name}' (Zeitraum: {min_id} bis {max_id})")

    try:
        # engine.begin() öffnet eine sichere Transaktion (Commit bei Erfolg, Rollback bei Fehler)
        with engine.begin() as conn:
            # 2. Alte Daten löschen
            result = conn.execute(delete_query, {"min_id": min_id, "max_id": max_id})
            logger.info(f"Bereinigung: {result.rowcount} alte Datensätze im Zeitraum gelöscht.")

            # 3. Neue Daten einfügen (Wir übergeben die aktive Connection 'conn', nicht die Engine!)
            df.to_sql(table_name, con=conn, if_exists='append', index=False)
            logger.info(f"Insert: {len(df)} neue Datensätze erfolgreich geschrieben.")
            
    except Exception as e:
        logger.error(f"Transaktion fehlgeschlagen! Rollback wurde automatisch ausgeführt. Fehler: {e}")
        raise