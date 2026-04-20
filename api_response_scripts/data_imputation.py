import pandas as pd
import logging

def impute_missing_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Führt eine intelligente Zeitreihen-Interpolation durch, 
    um fehlende Werte (NaN) mathematisch korrekt zu schließen.
    """
    if df is None or df.empty:
        logging.warning("Imputation abgebrochen: Leerer DataFrame.")
        return df
        
    df_clean = df.copy()
    
    # --- 1. Vorbereitung der Zeitreihe ---
    # Pandas braucht das Datum zwingend als Index, um die echten zeitlichen 
    # Abstände (Stunden) für die Berechnung der Steigung zu verstehen.
    if 'date' in df_clean.columns:
        df_clean['date'] = pd.to_datetime(df_clean['date'])
        df_clean = df_clean.set_index('date')
    
    df_clean = df_clean.sort_index()

    # Wir schnappen uns nur die Spalten, mit denen man auch rechnen kann
    numeric_cols = df_clean.select_dtypes(include=['number']).columns
    
    missing_before = df_clean[numeric_cols].isna().sum().sum()
    logging.info(f"Starte Imputation. Gefundene NaN-Werte vorab: {missing_before}")

    if missing_before == 0:
        logging.info("Keine Imputation nötig. Datensatz ist bereits lückenlos.")
        return df_clean.reset_index()

    # --- 2. Die Kern-Mathematik: Zeitbasierte Interpolation ---
    # method='time': Berücksichtigt, falls mal eine Stunde in den Daten komplett fehlt.
    # limit=4: ARCHITEKTUR-REGEL! Wir erfinden maximal 4 Stunden am Stück. 
    # Wenn die SMARD-API für 3 Tage ausfällt, dürfen wir nicht einfach eine Gerade 
    # über 72 Stunden ziehen. Das wäre Datenmanipulation. Diese tiefen Lücken bleiben NaNs.
    df_clean[numeric_cols] = df_clean[numeric_cols].interpolate(method='time', limit=4)
    
    # --- 3. Rand-Behandlung (Edge Cases) ---
    # Interpolation greift nicht, wenn der allererste oder allerletzte Wert im Datensatz fehlt 
    # (weil es keinen Start- oder Endpunkt für die Linie gibt).
    # Hier kopieren wir den nächsten bekannten Wert rückwärts (bfill) oder vorwärts (ffill),
    # aber strikt limitiert auf 1 Stunde.
    df_clean[numeric_cols] = df_clean[numeric_cols].bfill(limit=1).ffill(limit=1)
    
    # --- 4. Abschluss-Analyse ---
    missing_after = df_clean[numeric_cols].isna().sum().sum()
    
    if missing_after > 0:
        logging.warning(
            f"Nach der Imputation verbleiben {missing_after} NaN-Werte. "
            "Das deutet auf einen massiven API-Ausfall von > 4 Stunden hin."
        )
    else:
        logging.info(f"Erfolgreich {missing_before} Lücken durch Interpolation geschlossen.")
        
    # Den Index wieder in eine normale Spalte für das flache JSON-Format verwandeln
    df_clean = df_clean.reset_index()
    
    return df_clean
