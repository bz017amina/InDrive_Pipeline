CREATE OR REPLACE VIEW staging.stg_vehicles AS
SELECT 
    vehicle_id,
    driver_id,
    TRIM(vehicle_type) AS vehicle_type,
    TRIM(model) AS model
FROM raw.vehicles;