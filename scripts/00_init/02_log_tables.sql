/*
==========================================================================================
Script: 02_log_tables.sql
Location: scripts/00_init/
Author: Otusanya Toyib
Created At: 2026-08-04
==========================================================================================
Script Purpose:
	This script checks the existence of previous log tables, drops them in an orderly 
	sequence to avoid error from foreign key constraints if found, and recreates them.
	The following log tables will be created with the execution of this script:

		* batch_log: For tracking batch loadings
		* step_log: For tracking steps embedded in each batch
		* error_log: For tracking errors that may occur
		* dq_log: For tracking data quality rules

Warning:
	* Running this script will permanently delete all data in the log tables
	* If data is of any importance, ensure to have proper backup before running
==========================================================================================
*/
-- Connect to OlistDatabase
USE OlistDatabase;

-- Drop existing log tables if found
IF EXISTS (SELECT 1 FROM sys.tables WHERE name IN ('dq_log', 'error_log', 'step_log', 'batch_log'))
BEGIN
	PRINT('Existing log tables found — dropping...');
	DROP TABLE IF EXISTS etl.dq_log;
	DROP TABLE IF EXISTS etl.error_log;
	DROP TABLE IF EXISTS etl.step_log;
	DROP TABLE IF EXISTS etl.batch_log;
END
GO

-- Create log table batch_log
CREATE TABLE etl.batch_log
(
	batch_id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL,
	batch_name NVARCHAR(50) NOT NULL,
	batch_start_time DATETIME NOT NULL,
	batch_end_time DATETIME NULL,
	batch_load_duration INT NULL,
	batch_load_status NVARCHAR(50) NOT NULL,
	total_tables_loaded INT NULL,
	total_rows_processed INT NULL,
	total_rows_loaded INT NULL,
	CONSTRAINT chk_batch_log_batch_load_status CHECK(batch_load_status IN ('Running', 'Successful', 'Failed'))
);
GO

-- Create log table step_log
CREATE TABLE etl.step_log
(
	step_id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL,
	batch_id INT NOT NULL,
	step_name NVARCHAR(50) NOT NULL,
	load_type NVARCHAR(50) NOT NULL,
	layer NVARCHAR(50) NOT NULL,
	source_object NVARCHAR(250) NOT NULL,
	target_object NVARCHAR(50) NOT NULL,
	step_start_time DATETIME NOT NULL,
	step_end_time DATETIME NULL,
	step_load_duration INT NULL,
	step_load_status NVARCHAR(50) NOT NULL,
	rows_extracted INT NOT NULL,
	rows_inserted INT NOT NULL,
	rows_updated INT NOT NULL,
	rows_rejected INT NOT NULL,
	CONSTRAINT fk_step_log_batch_id FOREIGN KEY (batch_id) REFERENCES etl.batch_log (batch_id),
	CONSTRAINT chk_step_log_layer CHECK(layer IN ('Bronze', 'Silver', 'Gold')),
	CONSTRAINT chk_step_log_step_load_status CHECK(step_load_status IN ('Running', 'Successful', 'Failed'))
);
GO

-- Create log table error_log
CREATE TABLE etl.error_log
(
	error_id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL,
	batch_id INT NOT NULL,
	step_id INT NULL,
	error_time DATETIME NOT NULL,
	rows_extracted INT NOT NULL,
	rows_inserted INT NOT NULL,
	rows_updated INT NOT NULL,
	rows_rejected INT NOT NULL,
	error_description NVARCHAR(MAX) NOT NULL,
	CONSTRAINT fk_error_log_batch_id FOREIGN KEY(batch_id) REFERENCES etl.batch_log (batch_id),
	CONSTRAINT fk_error_log_step_id FOREIGN KEY(step_id) REFERENCES etl.step_log (step_id)
);
GO

-- Create log table dq_log
CREATE TABLE etl.dq_log
(
	dq_id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL,
	batch_id INT NOT NULL,
	step_id INT NOT NULL,
	check_name NVARCHAR(50) NOT NULL,
	check_severity NVARCHAR(50) NOT NULL,
	check_time DATETIME DEFAULT GETDATE() NOT NULL,
	records_checked INT NOT NULL,
	records_failed INT NOT NULL,
	dq_status NVARCHAR(50) NOT NULL,
	dq_description NVARCHAR(MAX) NULL,
	CONSTRAINT fk_dq_log_batch_id FOREIGN KEY (batch_id) REFERENCES etl.batch_log (batch_id),
	CONSTRAINT fk_dq_log_step_id FOREIGN KEY (step_id) REFERENCES etl.step_log (step_id),
	CONSTRAINT chk_dq_log_check_severity CHECK(check_severity IN ('Info', 'Warning', 'Critical')),
	CONSTRAINT chk_dq_log_dq_status CHECK(dq_status IN ('Warning', 'Failed', 'Successful'))
);
GO
PRINT('Log Tables Created: batch_log, step_log, error_log, dq_log');
