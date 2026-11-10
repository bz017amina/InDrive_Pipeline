CREATE OR REPLACE VIEW staging.stg_rides AS
WITH deduplicated_rides AS (
    SELECT 
        ride_id,
        driver_id,
        pickup_zone_id,
        dropoff_zone_id,
        distance_km,
        fare_amount,
        ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY ride_id) as rn
    FROM raw.rides
)
SELECT 
    r.ride_id,
    r.driver_id,
    r.pickup_zone_id,
    r.dropoff_zone_id,
    r.distance_km,
    r.fare_amount
FROM deduplicated_rides r
JOIN raw.drivers d ON r.driver_id = d.driver_id
WHERE r.rn = 1 
  AND r.fare_amount > 0;