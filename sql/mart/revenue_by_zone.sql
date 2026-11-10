CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS mart.revenue_by_zone;
CREATE TABLE mart.revenue_by_zone AS
SELECT
    pickup_zone,
    COUNT(ride_id)   AS total_rides,
    SUM(fare_amount) AS total_revenue
FROM mart.rides
GROUP BY pickup_zone;