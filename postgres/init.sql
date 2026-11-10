-- 1. Our ELT architecture
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

-- 2.  RAW tables  (brutes, sans contraintes complexes)

-- Table des chauffeurs
CREATE TABLE IF NOT EXISTS raw.drivers (
    driver_id INT,
    driver_name VARCHAR(100),
    phone_number VARCHAR(50),
    rating NUMERIC(3,2)
);

-- vehicles table
CREATE TABLE IF NOT EXISTS raw.vehicles (
    vehicle_id INT,
    driver_id INT,
    vehicle_type VARCHAR(50),
    model VARCHAR(100)
);

-- zones  table
CREATE TABLE IF NOT EXISTS raw.zones (
    zone_id INT,
    zone_name VARCHAR(100),
    city VARCHAR(50)
);

-- rides table (Streaming / Événements)
CREATE TABLE IF NOT EXISTS raw.rides (
    ride_id INT,
    driver_id INT,
    pickup_zone_id INT,
    dropoff_zone_id INT,
    distance_km NUMERIC(5,2),
    fare_amount NUMERIC(6,2)
);