import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from etl_pipelines.etl_weather import run_weather_etl, extract_and_transform_weather

# --- 1. TESTS FÜR DIE TRANSFORMATION ---

@patch("etl_pipelines.etl_weather.fetch_weather_data")
def test_extract_and_transform_weather(mock_fetch):
    """
    Prüft die Kern-Logik: Schema-Schutz und Duplikatsbereinigung.
    """
    # Setup: Wir simulieren Wetter-Daten inklusive einer "Schrott-Spalte", 
    # die einen Datenbank-Crash verursachen würde.
    df_raw_fake = pd.DataFrame({
        'date': ['2024-01-01 00:00:00+00:00', '2024-01-01 00:00:00+00:00'], # Duplikat
        'temperature_2m': [10.0, 12.0], 
        'wind_speed_100m': [15.0, 15.0],
        'cloud_cover': [80, 80],
        'unerwartete_api_spalte': ['schrott', 'schrott'] # Darf nicht in die DB!
    })
    
    mock_fetch.return_value = df_raw_fake

    # Ausführung
    df_result = extract_and_transform_weather("2024-01-01", "2024-01-01")

    # Validierung
    # 1. Duplikate entfernt?
    assert len(df_result) == 1
    assert df_result.iloc[0]['time_id'] == 2024010100
    
    # 2. Schema-Schutz: Wurde die Schrott-Spalte gelöscht?
    assert 'unerwartete_api_spalte' not in df_result.columns
    
    # 3. Sind die Pflicht-Spalten noch da?
    assert 'temperature_2m' in df_result.columns


# --- 2. TESTS FÜR DIE ORCHESTRIERUNG ---

@patch("etl_pipelines.etl_weather.idempotent_upsert")
@patch("etl_pipelines.etl_weather.get_db_engine")
@patch("etl_pipelines.etl_weather.extract_and_transform_weather")
def test_run_weather_etl_orchestration(mock_extract, mock_get_engine, mock_upsert):
    """
    Prüft den korrekten Ablauf der Wetter-Pipeline.
    """
    mock_df = MagicMock()
    mock_extract.return_value = mock_df
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine

    # Ausführung
    run_weather_etl("2024-02-01", "2024-02-28")

    # Validierung: Schreibt er in die korrekte Tabelle (fact_weather)?
    mock_extract.assert_called_once_with("2024-02-01", "2024-02-28")
    mock_upsert.assert_called_once_with(
        df=mock_df, 
        table_name='fact_weather', 
        engine=mock_engine
    )