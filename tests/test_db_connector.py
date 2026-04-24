import pytest
from unittest.mock import patch, MagicMock

# Annahme: Der Pfad src.warehousedb.connector ist korrekt
from src.warehousedb.connector import get_db_engine 

@patch("src.warehousedb.connector.create_engine")
@patch("src.warehousedb.connector.os.getenv")
def test_get_db_engine_success(mock_getenv, mock_create_engine):
    # Setup der simulierten Umgebungsvariablen
    def mock_env_vars(key, default=None): # default Parameter hinzugefügt, falls os.getenv einen Default nutzt
        envs = {
            "POSTGRES_USER": "testuser",
            "POSTGRES_PASSWORD": "testpassword",
            "POSTGRES_DB": "testdb",
            "POSTGRES_HOST": "testhost",
            "POSTGRES_PORT": "1234"
        }
        return envs.get(key, default)
    
    mock_getenv.side_effect = mock_env_vars
    mock_create_engine.return_value = MagicMock() # Simuliere die Engine

    # Ausführung
    engine = get_db_engine()

    # Validierung: Der String muss nun exakt den simulierten Werten entsprechen
    expected_conn_string = "postgresql+psycopg2://testuser:testpassword@testhost:1234/testdb"
    
    mock_create_engine.assert_called_once_with(expected_conn_string)
    assert isinstance(engine, MagicMock)

@patch("src.warehousedb.connector.os.getenv")
def test_get_db_engine_missing_credentials(mock_getenv):
    # Simuliere, dass Variablen fehlen (Rückgabe None)
    mock_getenv.return_value = None

    # Validierung, dass der Fehler korrekt fliegt
    with pytest.raises(ValueError, match="Kritischer Fehler: Datenbank-Credentials fehlen in der .env Datei!"):
        get_db_engine()