import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

def get_db_engine() -> Engine:
    """
    Erstellt eine SQLAlchemy Engine für die PostgreSQL-Datenbank
    basierend auf den .env Zugangsdaten.
    """
    load_dotenv()
    
    db_user = os.getenv("POSTGRES_USER")
    db_pass = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")
    db_host = os.getenv("POSTGRES_HOST")
    db_port = os.getenv("POSTGRES_PORT")

    if not all([db_user, db_pass, db_name, db_host, db_port]):
        raise ValueError("Kritischer Fehler: Datenbank-Credentials fehlen in der .env Datei!")

    # Konstruktion des Connection-Strings
    connection_string = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    
    # Erstellen der Engine
    engine = create_engine(connection_string)
    return engine