import pandas as pd

def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Berechnet abgeleitete Kennzahlen (Deltas) für die Analyse."""
    # Netzabweichung (Actual - Forecast)
    if 'actual_total_load' in df.columns and 'forecast_total_load' in df.columns:
        df["load_delta"] = df["actual_total_load"] - df["forecast_total_load"]
        
    # Prognosefehler Onshore-Wind
    if 'actual_wind_onshore' in df.columns and 'forecast_wind_onshore' in df.columns:
        df["wind_delta"] = df["actual_wind_onshore"] - df["forecast_wind_onshore"]
        
    return df