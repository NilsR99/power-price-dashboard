import logging
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

# Setup logging
logger = logging.getLogger(__name__)

def fetch_weather_data(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Zieht historische Wetterdaten für repräsentative Städte in Deutschland
    und bildet einen stundengenauen, bundesweiten Durchschnitt.
    """
    # 1. Repräsentative Verteilung für den deutschen Energiesektor
    cities = {
        "Hamburg": (53.55, 9.99),     # Nord (Windkraft-Proxy)
        "München": (48.13, 11.58),    # Süd (Photovoltaik-Proxy)
        "Köln": (50.93, 6.95),        # West (Industrie-Last-Proxy)
        "Berlin": (52.52, 13.41),     # Ost
        "Frankfurt": (50.11, 8.68),   # Mitte
        "Leipzig": (51.34, 12.37)
    }
    
    # Listenkomprehension zur Trennung von Längen- und Breitengraden
    lats = [coords[0] for coords in cities.values()]
    lons = [coords[1] for coords in cities.values()]

    # 2. API-Client initialisieren
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lats,
        "longitude": lons,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ["temperature_2m", "wind_speed_100m", "cloud_cover"],
        "timeformat": "unixtime",
    }
    
    logger.info(f"Lade Wetterdaten für {len(cities)} deutsche Metropolregionen...")
    responses = openmeteo.weather_api(url, params=params)

    all_city_data = []

    # 3. Schleife über die Antworten aller angefragten Städte
    for i, response in enumerate(responses):
        city_name = list(cities.keys())[i]
        logger.debug(f"Verarbeite Daten für {city_name}...")
        
        hourly = response.Hourly()
        
        # Datumsbereich generieren
        date_range = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )
        
        # DataFrame für die aktuelle Stadt erstellen
        df_city = pd.DataFrame({
            "date": date_range,
            "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
            "wind_speed_100m": hourly.Variables(1).ValuesAsNumpy(),
            "cloud_cover": hourly.Variables(2).ValuesAsNumpy()
        })
        all_city_data.append(df_city)

    # 4. Konsolidierung und Durchschnittsbildung
    logger.info("Füge Städtedaten zusammen und berechne bundesweiten Durchschnitt...")
    df_combined = pd.concat(all_city_data)

    # Gruppierung nach dem Zeitstempel und Berechnung des Mittelwerts über alle Städte
    df_avg = df_combined.groupby("date").mean().reset_index()

    # Rundung auf sinnvolle Nachkommastellen für die Datenbank
    df_avg["temperature_2m"] = df_avg["temperature_2m"].round(2)
    df_avg["wind_speed_100m"] = df_avg["wind_speed_100m"].round(2)
    df_avg["cloud_cover"] = df_avg["cloud_cover"].round(0) # Wolkenbedeckung in % als Ganzzahl

    logger.info(f"Wetter-Proxy erfolgreich generiert. ({len(df_avg)} Stunden geladen).")
    
    return df_avg