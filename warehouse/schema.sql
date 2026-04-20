-- 1. DIE ZENTRALE DIMENSION (Das Rückgrat)
CREATE TABLE IF NOT EXISTS dim_time (
    time_id BIGINT PRIMARY KEY,              -- Format: YYYYMMDDHH (z.B. 2024010112)
    datetime_utc TIMESTAMP WITH TIME ZONE NOT NULL UNIQUE,
    datetime_local TIMESTAMP WITH TIME ZONE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    weekday INTEGER NOT NULL,                -- 0=Montag, 6=Sonntag
    is_weekend BOOLEAN NOT NULL
);

-- 2. DIE FAKTENTABELLEN (Die flüchtigen Messwerte)

-- FAKTEN: Historische Realität (SMARD Actuals & Preise)
CREATE TABLE IF NOT EXISTS fact_market_actuals (
    time_id BIGINT PRIMARY KEY REFERENCES dim_time(time_id),
    price_day_ahead NUMERIC,
    actual_total_load NUMERIC,
    actual_residual_load NUMERIC,
    actual_wind_onshore NUMERIC,
    actual_wind_offshore NUMERIC,
    actual_pv NUMERIC,
    actual_gas NUMERIC,
    actual_hard_coal NUMERIC,
    actual_brown_coal NUMERIC,
    actual_nuclear NUMERIC,
    actual_hydro NUMERIC,
    actual_biomass NUMERIC, 
    actual_pumped_storage NUMERIC,
    actual_other_renewables NUMERIC,
    actual_other_conventional NUMERIC
);

-- FAKTEN: Die Erwartungshaltung (SMARD Forecasts)
CREATE TABLE IF NOT EXISTS fact_market_forecasts (
    time_id BIGINT PRIMARY KEY REFERENCES dim_time(time_id),
    forecast_total_load NUMERIC,
    forecast_wind_onshore NUMERIC,
    forecast_wind_offshore NUMERIC,
    forecast_pv NUMERIC,
    forecast_other NUMERIC
);

-- FAKTEN: Die finanziellen Pönalen (ENTSO-E Ausgleichsenergie)
CREATE TABLE IF NOT EXISTS fact_entsoe_imbalance (
    time_id BIGINT PRIMARY KEY REFERENCES dim_time(time_id),
    price_imbalance_short NUMERIC,
    price_imbalance_long NUMERIC
);

-- FAKTEN: Die physikalischen Treiber (Wetter-Historie)
CREATE TABLE IF NOT EXISTS fact_weather (
    time_id BIGINT PRIMARY KEY REFERENCES dim_time(time_id),
    temperature_2m NUMERIC,
    wind_speed_100m NUMERIC,
    cloud_cover NUMERIC,
    shortwave_radiation NUMERIC
);