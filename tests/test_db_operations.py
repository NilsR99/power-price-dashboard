import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from src.warehousedb.operations import idempotent_upsert

def test_idempotent_upsert_missing_time_id():
    """Testet, ob die Methode ohne time_id sofort mit einem ValueError abbricht."""
    df_invalid = pd.DataFrame({"temperature": [20.5, 21.0]})
    mock_engine = MagicMock()
    
    with pytest.raises(ValueError, match="muss eine 'time_id' Spalte"):
        idempotent_upsert(df_invalid, "fact_weather", mock_engine)

@patch("pandas.DataFrame.to_sql")
def test_idempotent_upsert_success(mock_to_sql):
    """Testet den korrekten Ablauf der Transaktion (Delete und Insert)."""
    # Setup
    df_valid = pd.DataFrame({"time_id": [2024010100, 2024010101], "value": [10, 20]})
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    
    # Simuliere den Context Manager für die Transaktion: with engine.begin() as conn
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_conn.execute.return_value.rowcount = 2 # Tu so, als hätten wir 2 Zeilen gelöscht

    # Ausführung
    idempotent_upsert(df_valid, "test_table", mock_engine)

    # Validierung: Wurde Delete aufgerufen?
    assert mock_conn.execute.called
    args, kwargs = mock_conn.execute.call_args
    # Prüfen, ob die SQL-Parameter (min_id und max_id) richtig übergeben wurden
    assert args[1] == {"min_id": 2024010100, "max_id": 2024010101}

    # Validierung: Wurde Insert aufgerufen (mit der CONNECTION, nicht der Engine)?
    mock_to_sql.assert_called_once_with(
        "test_table", 
        con=mock_conn, 
        if_exists="append", 
        index=False
    )