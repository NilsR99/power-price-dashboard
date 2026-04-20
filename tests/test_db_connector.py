import pytest
from unittest.mock import patch, MagicMock
from warehouse.db_connector import get_db_engine

@patch("warehouse.db_connector.create_engine")
@patch("warehouse.db_connector.os.getenv")
def test_get_db_engine_success(mock_getenv, mock_create_engine):
    # Setup der simulierten Umgebungsvariablen
    def mock_env_vars(key):
        envs = {
            "POSTGRES_USER": "testuser",
            "POSTGRES_PASSWORD": "testpassword",
            "POSTGRES_DB": "testdb"
        }
        return envs.get(key)
    
    mock_getenv.side_effect = mock_env_vars
    mock_create_engine.return_value = MagicMock() # Simuliere die Engine

    # Ausführung
    engine = get_db_engine()

    # Validierung
    expected_conn_string = "postgresql+psycopg2://testuser:testpassword@localhost:5432/testdb"
    mock_create_engine.assert_called_once_with(expected_conn_string)
    assert isinstance(engine, MagicMock)

@patch("warehouse.db_connector.os.getenv")
def test_get_db_engine_missing_credentials(mock_getenv):
    # Simuliere, dass Variablen fehlen (Rückgabe None)
    mock_getenv.return_value = None

    # Validierung, dass der Fehler korrekt fliegt
    with pytest.raises(ValueError, match="Kritischer Fehler: Datenbank-Credentials fehlen in der .env Datei!"):
        get_db_engine()