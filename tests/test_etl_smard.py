import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from etl_pipelines.etl_smard_actuals import run_smard_etl, extract_and_transform_smard

# --- 1. TESTS FÜR DIE TRANSFORMATION ---

# NEU: Der Spion überwacht jetzt die neue In-Memory-Merge Funktion
@patch("etl_pipelines.etl_smard_actuals.in_memory_merge") 
@patch("etl_pipelines.etl_smard_actuals.fetch_smard_data")
def test_extract_and_transform_smard(mock_fetch, mock_merge):
    """
    Prüft die Kern-Logik: Wird die time_id richtig berechnet und werden Duplikate entfernt?
    """
    # Setup: Es werden fehlerhafte, rohe API-Daten simuliert (ein absichtliches Duplikat)
    df_raw_fake = pd.DataFrame({
        'date': pd.to_datetime(['2024-01-01 12:00:00+00:00', '2024-01-01 12:00:00+00:00']), 
        'price_day_ahead': [50.0, 55.0], 
        'actual_total_load': [40000.0, 40000.0]
    })
    
    # Dem Mock für den Merger wird das Fake-DataFrame zugewiesen
    mock_merge.return_value = df_raw_fake
    
    # Der Mock für die API-Abfrage gibt ein leeres DataFrame zurück, 
    # damit die Schleife über SMARD_CONFIG fehlerfrei durchläuft, ohne das Netzwerk zu belasten
    mock_fetch.return_value = pd.DataFrame() 

    # Ausführung
    df_result = extract_and_transform_smard("2024-01-01", "2024-01-01")

    # Validierung
    # 1. Die alte date-Spalte muss durch die Transformation gelöscht worden sein
    assert 'date' not in df_result.columns
    
    # 2. Die time_id muss korrekt im Integer-Format generiert worden sein
    assert 'time_id' in df_result.columns
    assert df_result.iloc[0]['time_id'] == 2024010112
    
    # 3. Duplikatsprüfung: Es darf nur noch exakt eine Zeile übrig sein
    assert len(df_result) == 1
    
    # 4. Konfliktlösung: Der letzte Wert ("55.0") muss die Bereinigung überlebt haben
    assert df_result.iloc[0]['price_day_ahead'] == 55.0


# --- 2. TESTS FÜR DIE ORCHESTRIERUNG ---

@patch("etl_pipelines.etl_smard_actuals.idempotent_upsert")
@patch("etl_pipelines.etl_smard_actuals.get_db_engine")
@patch("etl_pipelines.etl_smard_actuals.extract_and_transform_smard")
def test_run_smard_etl_orchestration(mock_extract, mock_get_engine, mock_upsert):
    """
    Prüft, ob der Controller die Module in der methodisch korrekten Reihenfolge aufruft.
    """
    mock_df = MagicMock()
    mock_extract.return_value = mock_df
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine

    # Ausführung
    run_smard_etl("2024-01-01", "2024-01-07")

    # Validierung
    mock_extract.assert_called_once_with("2024-01-01", "2024-01-07")
    mock_get_engine.assert_called_once()
    mock_upsert.assert_called_once_with(
        df=mock_df, 
        table_name='fact_market_actuals', 
        engine=mock_engine
    )