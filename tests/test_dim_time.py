import pandas as pd
import pytest
from unittest.mock import patch, MagicMock, ANY
from sqlalchemy.exc import IntegrityError

# Import der zu testenden Funktionen
from src.warehousedim_time import generate_dim_time, load_to_database, main

# --- TESTS FÜR DIE REINE LOGIK (TRANSFORMATION) ---

def test_generate_dim_time_logic():
    """
    Testet die reine Generierung der Zeitachse (ohne Datenbank).
    """
    # 1. Ausführung (Es wird das Jahr 2023, ein Nicht-Schaltjahr, getestet)
    df = generate_dim_time(start_year=2023, end_year=2023)

    # 2. Kontrolle der Länge: 365 Tage * 24 Stunden = 8760 Stunden. 
    assert len(df) == 8760, "Das DataFrame hat nicht die korrekte Anzahl an Stunden für ein Nicht-Schaltjahr."

    # 3. Kontrolle der Spalten
    expected_columns = ['time_id', 'datetime_utc', 'datetime_local', 'year', 'month', 'day', 'hour', 'weekday', 'is_weekend']
    assert list(df.columns) == expected_columns, "Die Spaltennamen oder -reihenfolge stimmt nicht."

    # 4. Kontrolle des Primary Key Formats (YYYYMMDDHH)
    first_time_id = df.iloc[0]['time_id']
    assert first_time_id == 2023010100, f"Der Primary Key {first_time_id} ist falsch formatiert."
    
    # 5. Kontrolle des Datentyps für die spätere Persistierung
    assert df['time_id'].dtype == 'int64', "time_id muss ein 64-bit Integer sein."


def test_generate_dim_time_validation():
    """
    Testet, ob die Methode bei unlogischen Eingaben (Startjahr > Endjahr) korrekt abstürzt.
    """
    with pytest.raises(ValueError, match="Logikfehler"):
        generate_dim_time(start_year=2025, end_year=2024)


# --- TESTS FÜR DIE INFRASTRUKTUR (I/O-OPERATIONEN) ---

@patch("pandas.DataFrame.to_sql")
def test_load_to_database_success(mock_to_sql):
    """
    Testet, ob die generische Lade-Funktion die richtigen Befehle an die Engine schickt.
    """
    # Setup: Simulierte Engine und Dummy-Datenbank
    mock_engine = MagicMock()
    dummy_df = pd.DataFrame({"time_id": [2024010100], "year": [2024]})
    table_name = "test_table"

    # Ausführung
    load_to_database(df=dummy_df, table_name=table_name, engine=mock_engine, chunksize=500)

    # Validierung: Wurde to_sql mit exakt den richtigen Parametern aufgerufen?
    mock_to_sql.assert_called_once_with(
        table_name, 
        con=mock_engine, 
        if_exists='append', 
        index=False, 
        chunksize=500,
        method=ANY
    )


@patch("pandas.DataFrame.to_sql")
def test_load_to_database_integrity_error(mock_to_sql):
    """
    Testet das korrekte Weiterwerfen (Reraise) eines IntegrityErrors (z.B. bei doppelten Primary Keys).
    """
    mock_engine = MagicMock()
    dummy_df = pd.DataFrame({"time_id": [2024010100]})
    
    # Simuliert, dass die Datenbank-Bibliothek einen IntegrityError wirft
    mock_to_sql.side_effect = IntegrityError("statement", "params", "orig")

    # Prüft, ob dieser Fehler von load_to_database nach außen gereicht wird
    with pytest.raises(IntegrityError):
        load_to_database(dummy_df, "test_table", mock_engine)


# --- TESTS FÜR DIE ORCHESTRIERUNG (MAIN) ---

@patch("src.warehousedim_time.load_to_database")
@patch("src.warehousedim_time.get_db_engine")
@patch("src.warehousedim_time.generate_dim_time")
def test_main_orchestration(mock_generate, mock_get_engine, mock_load):
    """
    Testet, ob die main()-Funktion die Bausteine (Generieren, Engine holen, Laden)
    in der korrekten Reihenfolge und mit den richtigen Standardwerten aufruft.
    """
    # Setup der simulierten Rückgabewerte
    mock_df = MagicMock()
    mock_generate.return_value = mock_df
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine

    # Ausführung
    main()

    # Validierung der Prozesskette
    mock_generate.assert_called_once_with(start_year=1950, end_year=2026)
    mock_get_engine.assert_called_once()
    mock_load.assert_called_once_with(df=mock_df, table_name='dim_time', engine=mock_engine)