CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS mart.rides CASCADE;
CREATE TABLE mart.rides AS
SELECT
    r.ride_id,
    r.driver_id,
    d.driver_name,
    d.rating,
    pz.zone_name AS pickup_zone,
    r.distance_km,
    r.fare_amount
FROM staging.stg_rides r
LEFT JOIN staging.stg_drivers d ON r.driver_id = d.driver_id
LEFT JOIN staging.stg_zones pz ON r.pickup_zone_id = pz.zone_id;