-- Count null values for each column -> no null values
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN CIRCUITID IS NULL THEN 1 ELSE 0 END) AS circuitid_nulls,
    SUM(CASE WHEN CIRCUITREF IS NULL THEN 1 ELSE 0 END) AS circuitref_nulls,
    SUM(CASE WHEN NAME IS NULL THEN 1 ELSE 0 END) AS name_nulls,
    SUM(CASE WHEN LOCATION IS NULL THEN 1 ELSE 0 END) AS location_nulls,
    SUM(CASE WHEN COUNTRY IS NULL THEN 1 ELSE 0 END) AS country_nulls,
    SUM(CASE WHEN LAT IS NULL THEN 1 ELSE 0 END) AS lat_nulls,
    SUM(CASE WHEN LNG IS NULL THEN 1 ELSE 0 END) AS lng_nulls,
    SUM(CASE WHEN ALT IS NULL THEN 1 ELSE 0 END) AS alt_nulls,
    SUM(CASE WHEN URL IS NULL THEN 1 ELSE 0 END) AS url_nulls
FROM 
    PLAYGROUND.DATATHON.CIRCUITS;


-- Create a new table without url and circuit column
CREATE OR REPLACE TABLE EVENT.DATATHON_2025_TEAM_ETA.CIRCUITS AS
SELECT 
    CIRCUITID,
    NAME,
    LOCATION,
    COUNTRY,
    LAT,
    LNG,
    ALT
FROM 
    PLAYGROUND.DATATHON.CIRCUITS;

-- Create a new column categorising altitude as high or low
ALTER TABLE EVENT.DATATHON_2025_TEAM_ETA.CIRCUITS
ADD COLUMN ALTITUDE_CATEGORY VARCHAR(20);
UPDATE EVENT.DATATHON_2025_TEAM_ETA.CIRCUITS
SET ALTITUDE_CATEGORY = 
    CASE 
        WHEN ALT >= 1000 THEN 'HIGH'
        ELSE 'LOW'
    END;
