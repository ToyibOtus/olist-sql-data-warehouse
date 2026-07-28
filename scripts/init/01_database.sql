/*
==========================================================================================
Script: 01_database.sql
Location: scripts/init/
Author: Otusanya Toyib
Created At: 2026-07-26
==========================================================================================
Script Purpose:
	This script checks the existence of database [OlistDatabase], deletes it
	if it exists, and recreates it. Additionally, it creates 4 schemas:

	* etl: For non-layer-specific objects
	* bronze: For bronze layer
	* silver: For silver layer
	* gold: for gold layer

Warning:
	* Running this script deletes database [OlistDatabase], including all of its data
	* If data is of any importance, ensure to have proper backup before running
==========================================================================================
*/
-- Use master database
USE master;
GO

-- Drop database OlistDatabase if it exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'OlistDatabase')
	BEGIN
		PRINT('Existing OlistDatabase found — dropping...');
		ALTER DATABASE OlistDatabase SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
		DROP DATABASE OlistDatabase;
	END;
GO

-- Create Database OlistDatabase
CREATE DATABASE OlistDatabase;
GO

PRINT('OlistDatabase created successfully');
GO

-- Connect to OlistDatabase
USE OlistDatabase;
GO

-- Create Schema etl
CREATE SCHEMA etl;
GO

-- Create Schema bronze
CREATE SCHEMA bronze;
GO

-- Create Schema silver
CREATE SCHEMA silver;
GO

-- Create Schema gold
CREATE SCHEMA gold;
GO

PRINT('Schemas created: etl, bronze, silver, gold.');
GO
