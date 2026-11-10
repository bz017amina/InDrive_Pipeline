CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS mart.driver_leaderboard;
CREATE TABLE mart.driver_leaderboard AS
SELECT
    r.driver_id,
    r.driver_name,
    v.vehicle_type,
    COUNT(r.ride_id)   AS total_rides,
    SUM(r.fare_amount) AS total_revenue
FROM mart.rides r
LEFT JOIN staging.stg_vehicles v ON r.driver_id = v.driver_id
GROUP BY r.driver_id, r.driver_name, v.vehicle_type;