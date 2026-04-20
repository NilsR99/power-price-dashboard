import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def create_retry_session(
    retries=5, 
    backoff_factor=1.0, 
    status_forcelist=(429, 500, 502, 503, 504),
    headers=None
):
    """
    Erstellt eine konfigurierbare requests.Session mit Retry-Logik.
    """
    session = requests.Session()
    
    # Standard-Header hinzufügen (z.B. API-Keys oder User-Agents)
    if headers:
        session.headers.update(headers)

    retry = Retry(
        total=retries, # Obergrenze
        read=retries, # Leseversuch der Daten
        connect=retries, # Verbindungsaufbauversuche
        backoff_factor=backoff_factor, # expotentiell wachsende Wartezeit 1 * 2^0, 1 * 2^1
        status_forcelist=status_forcelist,  # HTTP Statuscodes 429: Too Many Requests (Limit der SMARD API). die anderen sind Serverfehler
        # 404 ist bewusst nicht drin, da eine falsche URL nicht nochmal versucht werden muss.
    )
    
    adapter = HTTPAdapter(max_retries=retry) # addaptiert die retry Regeln, damit die Session bei einem Fehler nicht abstürzt
    session.mount('http://', adapter) # immer wenn die URL mit http startet, nutze den Adapter
    session.mount('https://', adapter) # immer wenn die URL mit https startet, nutze den Adapter
    
    return session