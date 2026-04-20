import pytest
from unittest.mock import patch, mock_open, MagicMock
from warehouse.db.init_db import initialize_database

@patch("warehouse.db.init_db.get_db_engine")
@patch("warehouse.db.init_db.os.path.exists")
@patch("builtins.open", new_callable=mock_open, read_data="CREATE TABLE test;")
def test_initialize_database_success(mock_file, mock_exists, mock_get_engine):
    # Setup
    mock_exists.return_value = True
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine
    
    # Mocke den Context-Manager (with engine.begin() as conn:)
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    # Ausführung
    initialize_database()

    # Validierung: Wurde execute aufgerufen?
    # Wir prüfen nicht den exakten text()-Aufruf (da SQLAlchemy spezifisch), 
    # aber wir prüfen, DASS die Methode aufgerufen wurde.
    assert mock_conn.execute.called
    assert mock_file.called

@patch("warehouse.db.init_db.logging.error")
@patch("warehouse.db.init_db.os.path.exists")
@patch("warehouse.db.init_db.get_db_engine")
def test_initialize_database_file_not_found(mock_get_engine, mock_exists, mock_log_error):
    # Setup
    mock_exists.return_value = False
    mock_get_engine.return_value = MagicMock()

    # Ausführung
    initialize_database()

    # Validierung
    mock_log_error.assert_called_once()
    assert "Fehler: Die Datei" in mock_log_error.call_args[0][0]