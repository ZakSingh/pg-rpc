-- Test file for PostGIS type support
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA postgis_test;

-- Create a table with geography and geometry columns
CREATE TABLE postgis_test.locations (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    geog GEOGRAPHY(POINT, 4326),
    geom GEOMETRY(POINT, 4326)
);

-- Create a composite type with geography field
CREATE TYPE postgis_test.location_info AS (
    name TEXT,
    position GEOGRAPHY
);

-- Create a view using geography types
CREATE VIEW postgis_test.location_view AS
SELECT 
    id,
    name,
    geog,
    geom,
    ST_X(geog::geometry) as longitude,
    ST_Y(geog::geometry) as latitude
FROM postgis_test.locations;

-- Create a function that works with geography
CREATE FUNCTION postgis_test.find_nearby_locations(
    center GEOGRAPHY,
    radius_meters FLOAT
) RETURNS TABLE(
    id INTEGER,
    name TEXT,
    distance FLOAT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        l.id,
        l.name,
        ST_Distance(l.geog, center) as distance
    FROM postgis_test.locations l
    WHERE ST_DWithin(l.geog, center, radius_meters)
    ORDER BY distance;
END;
$$ LANGUAGE plpgsql;