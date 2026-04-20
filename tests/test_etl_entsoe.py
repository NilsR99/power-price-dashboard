import pandas as pd
from unittest.mock import patch, MagicMock
from etl_pipelines.etl_entsoe import run_entsoe_etl, extract_and_transform_entsoe

# --- 1. TESTS FÜR DIE TRANSFORMATION ---

# NEU: Wir fangen nun gezielt die echte Extraktionsfunktion ab, um Netzwerk-Calls zu verhindern
@patch("etl_pipelines.etl_entsoe.fetch_entsoe_imbalance") 
def test_extract_and_transform_entsoe(mock_fetch):
    """
    Prüft Duplikatsbereinigung und Schema-Schutz für die ENTSO-E Daten.
    """
    # Schema an die tatsächlichen ENTSO-E Preise angepasst
    df_raw_fake = pd.DataFrame({
        'date': ['2024-01-01 12:00:00+00:00', '2024-01-01 12:00:00+00:00'], # Absichtliches Duplikat
        'price_imbalance_short': [100.0, 150.0], # 150.0 soll als letzter Wert "gewinnen"
        'price_imbalance_long': [-200.0, -200.0],
        'ignore_this_api_column': ['secret', 'secret'] # Darf nicht in die DB gelangen!
    })

    # Das Fake-DataFrame dem Mock zuweisen (kein 'with patch...' Block mehr nötig)
    mock_fetch.return_value = df_raw_fake

    # Ausführung
    df_result = extract_and_transform_entsoe("2024-01-01", "2024-01-01")

    # Validierung
    # 1. Duplikate entfernt und letzter Wert behalten?
    assert len(df_result) == 1
    assert df_result.iloc[0]['time_id'] == 2024010112
    assert df_result.iloc[0]['price_imbalance_short'] == 150.0

    # 2. Schema-Schutz: Unerwartete Spalte erfolgreich entfernt?
    assert 'ignore_this_api_column' not in df_result.columns


# --- 2. TESTS FÜR DIE ORCHESTRIERUNG ---

@patch("etl_pipelines.etl_entsoe.idempotent_upsert")
@patch("etl_pipelines.etl_entsoe.get_db_engine")
@patch("etl_pipelines.etl_entsoe.extract_and_transform_entsoe")
def test_run_entsoe_etl_orchestration(mock_extract, mock_get_engine, mock_upsert):
    """
    Prüft den korrekten Ablauf der ENTSO-E Pipeline.
    """
    mock_df = MagicMock()
    mock_extract.return_value = mock_df
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine

    run_entsoe_etl("2024-03-01", "2024-03-10")

    mock_extract.assert_called_once_with("2024-03-01", "2024-03-10")
    mock_upsert.assert_called_once_with(
        df=mock_df, 
        table_name='fact_entsoe_imbalance', 
        engine=mock_engine
    )