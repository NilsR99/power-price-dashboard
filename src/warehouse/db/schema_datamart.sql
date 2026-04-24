-- Löscht die View, falls sie bereits existiert (ermöglicht problemlose Updates der Logik)
DROP VIEW IF EXISTS v_power_dashboard;

-- Erstellt die virtuelle Master-Tabelle
CREATE VIEW v_power_dashboard AS
SELECT 
    -- 1. Die Zeitdimension (Das Fundament)
    dt.time_id,
    dt.datetime_local,
    dt.year,
    dt.month,
    dt.day,
    dt.hour,
    dt.weekday,
    dt.is_weekend,
    
    -- 2. SMARD Marktdaten (Ist-Werte)
    fa.price_day_ahead,
    fa.actual_total_load,
    fa.actual_residual_load,
    fa.actual_wind_onshore,
    fa.actual_wind_offshore,
    fa.actual_pv,
    fa.actual_gas,
    fa.actual_hard_coal,
    fa.actual_brown_coal,
    -- NEU: Die restlichen Energieträger
    fa.actual_nuclear,
    fa.actual_hydro,
    fa.actual_biomass,
    fa.actual_pumped_storage,
    fa.actual_other_conventional,
    fa.actual_other_renewables,
    
    -- 3. SMARD Prognosen
    ff.forecast_total_load,
    ff.forecast_wind_onshore,
    ff.forecast_wind_offshore,
    ff.forecast_pv,
    ff.forecast_other,
    
    -- 4. Wetterdaten (Deutschland-Proxy)
    fw.temperature_2m,
    fw.wind_speed_100m,
    fw.cloud_cover,
    
    -- 5. ENTSO-E Ausgleichsenergie (Pönalen)
    fe.price_imbalance_short,
    fe.price_imbalance_long

FROM 
    dim_time dt
-- LEFT JOIN auf die Faktentabellen
LEFT JOIN fact_market_actuals fa ON dt.time_id = fa.time_id
LEFT JOIN fact_market_forecasts ff ON dt.time_id = ff.time_id
LEFT JOIN fact_weather fw ON dt.time_id = fw.time_id
LEFT JOIN fact_entsoe_imbalance fe ON dt.time_id = fe.time_id

-- Performance-Filter: Begrenzt die View auf den relevanten Analyse-Zeitraum
WHERE 
    dt.year >= 2015 AND dt.year <= 2025;