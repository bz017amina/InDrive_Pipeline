CREATE OR REPLACE VIEW staging.stg_drivers AS
SELECT 
    driver_id,
    TRIM(driver_name) AS driver_name,
    rating
FROM raw.drivers;