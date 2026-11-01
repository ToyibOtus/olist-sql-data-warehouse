/*
==========================================================================================
Script: 03_ref_brazil_state_bounds.sql
Location: scripts/00_init/
Author: Otusanya Toyib
Created At: 2026-10-14
==========================================================================================
Script Purpose:
	This script verifies the existence of the table ref_brazil_state_bounds, deletes it if 
	found before recreating another (with a similar name), otherwise, it simply creates the 
	table ref_brazil_state_bounds.

Warning:
	* Running this script will permanently delete all data in the table
	* If data is of any importance, ensure to have proper backup before running
==========================================================================================
*/
-- Connect to database OlistDatabase
USE OlistDatabase;
GO

SET NOCOUNT ON;

IF EXISTS(SELECT 1 FROM sys.tables WHERE name = 'ref_brazil_state_bounds')
BEGIN
	PRINT ('Dropping ref_brazil_state_bounds');
	DROP TABLE etl.ref_brazil_state_bounds;
END;

PRINT('Creating ref_brazil_state_bounds');
CREATE TABLE etl.ref_brazil_state_bounds
(
	geolocation_state NVARCHAR(10) NOT NULL,
	min_lat FLOAT NOT NULL,
	max_lat FLOAT NOT NULL,
	min_lng FLOAT NOT NULL,
	max_lng FLOAT NOT NULL
	CONSTRAINT pk_geolocation_state PRIMARY KEY (geolocation_state)
);
GO

PRINT('Truncating ref_brazil_state_bounds');
TRUNCATE TABLE etl.ref_brazil_state_bounds;

PRINT('Loading ref_brazil_state_bounds');
INSERT INTO etl.ref_brazil_state_bounds
(
	geolocation_state,
	min_lat,
	max_lat,
	min_lng,
	max_lng
)
VALUES
	('AC', -11.15, -7.11, -73.99, -66.60),
	('AL', -10.50, -8.81, -38.24, -35.16),
	('AM', -9.82, 2.25, -73.80, -56.09),
	('AP', 1.41, 4.44, -54.89, -49.88),
	('BA', -18.35, -8.53, -46.61, -37.34),
	('CE', -7.85, -2.78, -41.42, -37.25),
	('DF', -16.05, -15.45, -48.29, -47.31),
	('ES', -21.30, -17.89, -41.87, -28.85),
	('GO', -19.50, -12.39, -53.25, -45.90),
	('MA', -10.26, -1.05, -48.75, -41.79),
	('MG', -22.92, -14.23, -51.04, -39.87),
	('MS', -24.07, -17.16, -57.88, -50.93),
	('MT', -17.87, -7.34, -61.30, -50.21),
	('PA', -9.85, 2.59, -58.90, -46.06),
	('PB', -8.32, -6.02, -38.75, -34.79),
	('PE', -9.48, -7.28, -41.35, -34.80),
	('PI', -10.93, -2.73, -45.99, -41.17),
	('PR', -26.71, -22.50, -54.62, -48.02),
	('RJ', -23.38, -20.76, -44.89, -40.96),
	('RN', -6.77, -4.82, -38.58, -34.96),
	('RO', -13.67, -7.97, -66.82, -59.78),
	('RR', -1.58, 5.27, -64.83, -58.89),
	('RS', -33.74, -27.08, -57.64, -49.69),
	('SC', -29.35, -25.95, -53.84, -48.35),
	('SE', -11.57, -9.51, -38.25, -36.39),
	('SP', -25.31, -19.78, -53.11, -44.15),
	('TO', -13.44, -5.17, -50.84, -45.69);

DECLARE @rows_loaded INT;
SELECT @rows_loaded = COUNT(*) FROM etl.ref_brazil_state_bounds

PRINT('Rows Loaded: ' +  CAST(@rows_loaded AS NVARCHAR(10)));
