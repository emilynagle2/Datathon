CREATE OR REPLACE TABLE EVENT.DATATHON_2025_TEAM_ETA.DRIVERS AS
SELECT 
   	DRIVERID,
	CONCAT(FORENAME, ' ', SURNAME) AS FULL_NAME,
	DOB,
	NATIONALITY
FROM 
 EVENT.DATATHON_2025_TEAM_ETA.DRIVERS;

