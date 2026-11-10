CREATE OR REPLACE VIEW staging.stg_zones AS
SELECT 
    zone_id,
    TRIM(zone_name) AS zone_name,
    TRIM(city) AS city
FROM raw.zones;