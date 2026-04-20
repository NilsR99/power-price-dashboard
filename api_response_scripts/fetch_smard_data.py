import requests
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def fetch_smard_data(filter_id: str, start_date: str, end_date: str, metric_name: str) -> pd.DataFrame:
    """
    Holt Daten von der SMARD API und gibt DIREKT ein Pandas DataFrame zurück.
    Kein Speichern auf der Festplatte!
    """
    # 1. Zeitgrenzen für die Filterung definieren (UTC)
    start_dt = pd.to_datetime(start_date, utc=True)
    end_dt = pd.to_datetime(end_date, utc=True)

    # 2. Den Index der verfügbaren Zeitstempel abrufen
    index_url = f"https://www.smard.de/app/chart_data/{filter_id}/DE/index_hour.json"
    try:
        response = requests.get(index_url)
        response.raise_for_status()
        timestamps = response.json().get('timestamps', [])
    except Exception as e:
        logger.error(f"Fehler beim Abrufen des Index für {metric_name}: {e}")
        return pd.DataFrame()

    all_data = []

    # 3. Die eigentlichen Datenpakete abrufen
    for ts in timestamps:
        # Optimierung: Lade nur die Pakete, die in unserem gewünschten Zeitraum liegen
        # SMARD Zeitstempel sind in Millisekunden
        paket_start = pd.to_datetime(ts, unit='ms', utc=True)
        # Ein Paket enthält meist 1-2 Wochen. Wir prüfen grob, ob es relevant sein könnte
        if paket_start > end_dt:
            continue # Paket liegt in der Zukunft

        data_url = f"https://www.smard.de/app/chart_data/{filter_id}/DE/{filter_id}_DE_hour_{ts}.json"
        try:
            res = requests.get(data_url)
            res.raise_for_status()
            series_data = res.json().get('series', [])
            all_data.extend(series_data)
        except Exception as e:
            logger.error(f"Fehler beim Laden des Pakets {ts} für {metric_name}: {e}")
            continue

    if not all_data:
        logger.warning(f"Keine Daten für {metric_name} im Zeitraum gefunden.")
        return pd.DataFrame()

    # 4. In ein DataFrame umwandeln
    df = pd.DataFrame(all_data, columns=['date', metric_name])
    
    # 5. Saubere Datentypen (Millisekunden -> echtes UTC Datetime)
    df['date'] = pd.to_datetime(df['date'], unit='ms', utc=True)
    
    # 6. Exakt auf den gewünschten Zeitraum filtern
    df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]
    
    return df.reset_index(drop=True)