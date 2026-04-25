# Power Price Dashboard

## Zielbild (April 2026)
Dieses Projekt nutzt eine robuste PostgreSQL-Anbindung für:
- **ETL-Pipelines** (Batch-Ingestion)
- **Streamlit Dashboard** auf Streamlit Community Cloud
- **Stabile Free-Tier-Kompatibilität** ohne aggressive Verbindungs-Spikes

## 1) Empfohlener Free PostgreSQL Provider

### Empfehlung: **Neon (Free Plan)**
**Warum Neon aktuell am besten passt:**
1. **Native Streamlit-Integration** ist offiziell dokumentiert (inkl. Community Cloud Deployment).
2. **Kostenloses, dauerhaft nutzbares Free-Tier** (kein Trial-Zwang).
3. **IPv4-Kompatibilität** laut Neon-Doku (AWS-Projekte unterstützen IPv4 & IPv6).
4. **Built-in Connection Pooling (pgBouncer)** sowie klassischer PostgreSQL-Zugriff.

Referenzen (Stand: **2026-04-25**):
- Streamlit x Neon Tutorial: https://docs.streamlit.io/develop/tutorials/databases/neon
- Neon Pricing: https://neon.com/pricing
- Neon Projects / Netzwerk / IPv4-Hinweis: https://neon.com/docs/manage/projects

---

## 2) Produktionsreife DB-Anbindung in `dashboard/data_loader.py`

Umgesetzt wurden:
- Sichere Passwort-Kodierung mit `quote_plus`
- Credentials-Priorisierung: `st.secrets` → Fallback `.env`/`os.getenv`
- Erzwungenes `sslmode=require`
- SQLAlchemy Connection-Pooling mit defensiven Free-Tier-Werten (`pool_size`, `max_overflow`, `pool_timeout`, `pool_recycle`, `pool_pre_ping`)

### Lokale .env Vorlage
```bash
POSTGRES_USER=...
POSTGRES_PASSWORD=...
POSTGRES_HOST=...
POSTGRES_PORT=5432
POSTGRES_DB=...
```

---

## 3) Streamlit Secrets Template

Datei: `.streamlit/secrets.toml` (lokal) bzw. identischer Inhalt in Streamlit Cloud Secrets:

```toml
POSTGRES_USER = "your_postgres_user"
POSTGRES_PASSWORD = "your_super_secret_password"
POSTGRES_HOST = "your-postgres-host.provider.com"
POSTGRES_PORT = "5432"
POSTGRES_DB = "your_database_name"
```

Eine commitbare Beispiel-Datei liegt bereit unter:
- `.streamlit/secrets.toml.example`

---

## 4) CI/CD Absicherung gegen DB-Sperren

### Problem
Wenn CI bei jedem Push echte DB-Connections öffnet, können Free-Tier-Limits und Security-Mechanismen (Rate-Limits/IP-Sperren/Circuit Breaker) triggern.

### Methodik (umgesetzt)
1. **Unit-only in CI:**
   - `pytest -m "not integration"`
2. **Tests strikt mocken statt echte DB:**
   - DB-Engine/Netzwerk in Tests patchen
3. **Dummy-DB-ENV in CI setzen:**
   - verhindert echte Secret-Verwendung
4. **Separater Integrationstest-Job (optional):**
   - nur manuell (`workflow_dispatch`) oder nightly

### Optionales Pattern für echte Integrationstests
- Marker `@pytest.mark.integration`
- Nur laufen lassen bei explizitem Flag, z. B. `RUN_INTEGRATION_TESTS=true`

---

## 5) Schritt-für-Schritt Migration (Supabase ➜ Neon)

### Schritt 1: Neon-Projekt anlegen
```bash
# Web-Konsole:
# https://console.neon.tech
# Projekt erstellen, Region wählen (nahe Streamlit Cloud Region)
```

### Schritt 2: Datenbankschema deployen
```bash
python -m src.warehouse.db.init_db
```

### Schritt 3: Lokale Secrets setzen
```bash
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Danach Werte eintragen
```

### Schritt 4: Streamlit lokal testen
```bash
streamlit run dashboard/app.py
```

### Schritt 5: Streamlit Community Cloud konfigurieren
```bash
# In Streamlit Cloud:
# App Settings -> Secrets
# Werte aus secrets.toml dort eintragen
```

### Schritt 6: CI prüfen
```bash
python -m pytest -m "not integration"
```

---

## 6) Harte Betriebsregeln (Best Practices)

1. **Keine DB-Connection in Import-Time** (immer lazy in Funktionen).
2. **Pooling konservativ halten** (Free-Tier zuerst schützen).
3. **Fehlende Credentials mit klarer Exception abbrechen**.
4. **Secrets niemals committen** (`.streamlit/secrets.toml`, `.env`).
5. **ETL-Batches zeitlich staffeln**, nicht parallel fan-out auf dieselbe Free-DB.

---

## 7) Quick Commands (Terminal)

```bash
# 1) Dev deps
pip install -r requirements-dev.txt

# 2) Unit tests ohne DB
python -m pytest -m "not integration"

# 3) Lint
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics

# 4) App starten
streamlit run dashboard/app.py
```
