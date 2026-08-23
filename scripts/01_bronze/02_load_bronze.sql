/*
==================================================================================================
Script: 02_load_bronze  (Source --> Bronze)
Location: scripts/01_bronze/
Author: Otusanya Toyib
Created At: 2026-08-08
==================================================================================================
Script Purpose:
	This script loads records from each source file (CSV) into their corresponding target bronze
	tables. The entire operation is treated as a batch, with multitude of steps (9 to be precise)
	embedded into it, and is logged accurately to facilitate easy tracking and monitoring of both 
	batch and step operations, and thus, enabling efficient debugging if an error occurs.

Parameter: @batch_id

Usage: EXEC bronze.load_bronze

Note:
	Default value of parameter (@batch_id) is NULL, as the script generates its own batch_id and
	passes the value into @batch_id. And it should be noted that no random value should be passed 
	into @batch_id, as this will not only return an error if the value doesn't exist in batch log 
	table, but will ruin the integrity of the data passed into each log table.
==================================================================================================
*/
-- Connect to OlistDatabase
USE OlistDatabase;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze @batch_id INT = NULL AS
BEGIN
	-- Suppress number of rows affected
	SET NOCOUNT ON;
 
	-- =======================================================================================
	-- SECTION 1: DECLARE ALL VARIABLES
	-- =======================================================================================
	DECLARE

	-- Batch-Level Variables
	@batch_start_time DATETIME2(0) = SYSDATETIME(),
	@batch_name NVARCHAR(50) = 'bronze.load_bronze',
	@batch_end_time DATETIME2(0),
	@batch_load_duration INT,
	@batch_load_status NVARCHAR(50) = 'Running',
	@total_tables_loaded INT = 0,
	@total_rows_processed INT = 0,
	@total_rows_loaded INT = 0,

	-- Step-Level Variables
	@step_id INT,
	@layer NVARCHAR(50) = 'Bronze',
	@step_name NVARCHAR(50),
	@load_type NVARCHAR(70),
	@source_object NVARCHAR(250),
	@target_object NVARCHAR(50),
	@step_start_time DATETIME2(0),
	@step_end_time DATETIME2(0),
	@step_load_duration INT,
	@step_load_status NVARCHAR(50),
	@rows_extracted INT,
	@rows_inserted INT,
	@rows_updated INT,
	@rows_rejected INT,

	-- Holds error time 
	@error_time DATETIME2(0),

	-- Holds SQL queries (BULK INSERT)
	@sql NVARCHAR(MAX);


	-- =======================================================================================
	-- SECTION 2: OPEN BATCH — Log the start of this pipeline run
	-- =======================================================================================

	-- Load log details at batch-level if batch id is NULL
	IF @batch_id IS NULL
	BEGIN
		INSERT INTO etl.batch_log
		(
			batch_name,
			batch_start_time,
			batch_load_status,
			total_tables_loaded,
			total_rows_processed,
			total_rows_loaded
		)
		VALUES
		(
			@batch_name,
			@batch_start_time,
			@batch_load_status,
			@total_tables_loaded,
			@total_rows_processed,
			@total_rows_loaded
		);

		-- Retrieve recently generated batch id
		SET @batch_id = SCOPE_IDENTITY();
	END;


	-- =======================================================================================
	-- SECTION 3: LOAD ALL BRONZE TABLES
	-- =======================================================================================
	BEGIN TRY
		-- ========================================================
		-- STEP 1: Load Olist Customers Dataset 
		-- ========================================================

		-- Map values to variables before transactions
		SET @step_start_time = SYSDATETIME();
		SET @step_name = 'load_olist_customers_dataset';
		SET @load_type = 'Incremental Load: Insert, Update & Soft-Delete Flag';
		SET @source_object = 'C:\Users\PC\Documents\Olist Store Datasets\olist_customers_dataset.csv';
		SET @target_object = 'olist_customers_dataset';
		SET @step_load_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;
		SET @rows_updated = 0;
		SET @rows_rejected = 0;

		-- Load log details at step-level
		INSERT INTO etl.step_log
		(
			batch_id,
			layer,
			step_name,
			load_type,
			source_object,
			target_object,
			step_start_time,
			step_load_status,
			rows_extracted,
			rows_inserted,
			rows_updated,
			rows_rejected
		)
		VALUES
		(
			@batch_id,
			@layer,
			@step_name,
			@load_type,
			@source_object,
			@target_object,
			@step_start_time,
			@step_load_status,
			@rows_extracted,
			@rows_inserted,
			@rows_updated,
			@rows_rejected
		);

		-- Retrieve recently generated step id
		SET @step_id = SCOPE_IDENTITY();

		-- Create a temporary staging table #staging_olist_customers_dataset
		CREATE TABLE #staging_olist_customers_dataset
		(
			customer_id NVARCHAR(50),
			customer_unique_id NVARCHAR(50),
			customer_zip_code_prefix CHAR(5),
			customer_city NVARCHAR(50),
			customer_state NVARCHAR(50)
		);

		-- Map BULK INSERT statement to variable @sql
		SET @sql = 'BULK INSERT #staging_olist_customers_dataset FROM ''' + @source_object + 
		''' WITH(FORMAT = ''CSV'', FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''0x0A'', TABLOCK	);';

		-- Execute BULK INSERT statement
		EXEC (@sql);

		-- Add a computed column dwh row hash into staging table
		ALTER TABLE #staging_olist_customers_dataset
		ADD dwh_row_hash AS CAST(HASHBYTES('SHA2_256', CONCAT_WS('|', 
		COALESCE(customer_id, 'N/A'), COALESCE(customer_unique_id, 'N/A'), COALESCE(customer_zip_code_prefix, 1),
		COALESCE(customer_city, 'N/A'), COALESCE(customer_state, 'N/A'))) AS BINARY(32)) PERSISTED; 

		-- Extract total number of records loaded into staging table
		SELECT @rows_extracted = COUNT(*) FROM #staging_olist_customers_dataset;

		-- Load new records from staging to target table olist_customers_dataset
		INSERT INTO bronze.olist_customers_dataset
		(
			customer_id,
			customer_unique_id,
			customer_zip_code_prefix,
			customer_city,
			customer_state,
			dwh_row_hash,
			dwh_batch_id,
			dwh_source_file
		)
		SELECT
			src.customer_id,
			src.customer_unique_id,
			src.customer_zip_code_prefix,
			src.customer_city,
			src.customer_state,
			src.dwh_row_hash,
			@batch_id,
			@source_object
		FROM #staging_olist_customers_dataset src
		LEFT JOIN bronze.olist_customers_dataset tgt
		ON src.customer_unique_id = tgt.customer_unique_id
		AND src.customer_id = tgt.customer_id
		WHERE tgt.customer_unique_id IS NULL;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT;

		-- Update outdated records in bronze table
		UPDATE tgt
			SET
				tgt.customer_zip_code_prefix = src.customer_zip_code_prefix,
				tgt.customer_city = src.customer_city,
				tgt.customer_state = src.customer_state,
				tgt.dwh_row_hash = src.dwh_row_hash,
				tgt.dwh_batch_id = @batch_id,
				tgt.dwh_source_file = @source_object,
				tgt.dwh_is_deleted = 0
			FROM #staging_olist_customers_dataset src
			INNER JOIN bronze.olist_customers_dataset tgt
			ON src.customer_unique_id = tgt.customer_unique_id
			AND src.customer_id = tgt.customer_id
			WHERE tgt.dwh_row_hash <> src.dwh_row_hash;

		-- Retrieve rows updated
		SET @rows_updated = @@ROWCOUNT;

		-- Flag deleted records in bronze table
		UPDATE tgt
			SET
				tgt.dwh_batch_id = @batch_id,
				tgt.dwh_source_file = @source_object,
				tgt.dwh_is_deleted = 1
			FROM bronze.olist_customers_dataset tgt 
			LEFT JOIN #staging_olist_customers_dataset src
			ON tgt.dwh_row_hash = src.dwh_row_hash
			WHERE src.dwh_row_hash IS NULL;

		-- Map values to variables on success
		SET @step_end_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @step_end_time);
		SET @step_load_status = 'Successful';
		SET @rows_rejected = @rows_extracted - (@rows_inserted + @rows_updated);
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + (@rows_inserted + @rows_updated);

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET
				step_end_time = @step_end_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted,
				rows_updated = @rows_updated,
				rows_rejected = @rows_rejected
			WHERE step_id = @step_id AND batch_id = @batch_id;

		-- Drop staging table
		DROP TABLE IF EXISTS #staging_olist_customers_dataset;


		-- ========================================================
		-- STEP 2: Load Olist Geolocation Dataset 
		-- ========================================================

		-- Map values to variables before transactions
		SET @step_start_time = SYSDATETIME();
		SET @step_name = 'load_olist_geolocation_dataset';
		SET @load_type = 'Incremental Load: Insert & Soft-Delete Flag';
		SET @source_object = 'C:\Users\PC\Documents\Olist Store Datasets\olist_geolocation_dataset.csv';
		SET @target_object = 'olist_geolocation_dataset';
		SET @step_load_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;
		SET @rows_updated = 0;
		SET @rows_rejected = 0;

		-- Load log details at step-level
		INSERT INTO etl.step_log
		(
			batch_id,
			layer,
			step_name,
			load_type,
			source_object,
			target_object,
			step_start_time,
			step_load_status,
			rows_extracted,
			rows_inserted,
			rows_updated,
			rows_rejected
		)
		VALUES
		(
			@batch_id,
			@layer,
			@step_name,
			@load_type,
			@source_object,
			@target_object,
			@step_start_time,
			@step_load_status,
			@rows_extracted,
			@rows_inserted,
			@rows_updated,
			@rows_rejected
		);

		-- Retrieve recently generated step id
		SET @step_id = SCOPE_IDENTITY();

		-- Create a temporary staging table #staging_olist_geolocation_dataset
		CREATE TABLE #staging_olist_geolocation_dataset
		(
			geolocation_zip_code_prefix CHAR(5),
			geolocation_lat FLOAT,
			geolocation_lng FLOAT,
			geolocation_city NVARCHAR(50),
			geolocation_state NVARCHAR(50)
		);

		-- Map BULK INSERT statement to variable @sql
		SET @sql = 'BULK INSERT #staging_olist_geolocation_dataset FROM ''' + @source_object + 
		''' WITH (FORMAT = ''CSV'', FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''0x0A'', TABLOCK);';

		-- Execute BULK INSERT statement
		EXEC (@sql);

		-- Add a computed column dwh row hash into staging table
		ALTER TABLE #staging_olist_geolocation_dataset
		ADD dwh_row_hash AS CAST(HASHBYTES('SHA2_256', CONCAT_WS('|', 
		COALESCE(geolocation_zip_code_prefix, 'N/A'), COALESCE(geolocation_lat, 1.0),
		COALESCE(geolocation_lng, 1.0), COALESCE(geolocation_city, 'N/A'), COALESCE(geolocation_state, 'N/A'))) AS BINARY(32)) PERSISTED;

		-- Extract total number of records loaded into staging table
		SELECT @rows_extracted = COUNT(*) FROM #staging_olist_geolocation_dataset;

		-- Load new records from staging to target table olist_geolocation_dataset
		INSERT INTO bronze.olist_geolocation_dataset
		(
			geolocation_zip_code_prefix,
			geolocation_lat,
			geolocation_lng,
			geolocation_city,
			geolocation_state,
			dwh_row_hash,
			dwh_batch_id,
			dwh_source_file
		)
		SELECT
			src.geolocation_zip_code_prefix,
			src.geolocation_lat,
			src.geolocation_lng,
			src.geolocation_city,
			src.geolocation_state,
			src.dwh_row_hash,
			@batch_id,
			@source_object
		FROM #staging_olist_geolocation_dataset src
		LEFT JOIN bronze.olist_geolocation_dataset tgt
		ON tgt.dwh_row_hash = src.dwh_row_hash
		WHERE tgt.dwh_row_hash IS NULL;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT

		-- Flag deleted records in bronze table
		UPDATE tgt
			SET
				tgt.dwh_batch_id = @batch_id,
				tgt.dwh_source_file = @source_object,
				tgt.dwh_is_deleted = 1
			FROM bronze.olist_geolocation_dataset tgt
			LEFT JOIN #staging_olist_geolocation_dataset src
			ON tgt.dwh_row_hash = src.dwh_row_hash
			WHERE src.dwh_row_hash IS NULL;

		-- Map values to variables on success
		SET @step_end_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @step_end_time);
		SET @step_load_status = 'Successful';
		SET @rows_rejected = @rows_extracted - @rows_inserted;
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + @rows_inserted;

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET
				step_end_time = @step_end_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted,
				rows_updated = @rows_updated,
				rows_rejected = @rows_rejected
			WHERE step_id = @step_id AND batch_id = @batch_id; 

		-- Drop staging table
		DROP TABLE IF EXISTS #staging_olist_geolocation_dataset;


		-- ========================================================
		-- STEP 3: Load Olist Order Items Dataset 
		-- ========================================================

		-- Map values to variables before transactions
		SET @step_start_time = SYSDATETIME();
		SET @step_name = 'load_olist_order_items_dataset';
		SET @load_type = 'Incremental Load: Insert, Update & Soft-Delete Flag';
		SET @source_object = 'C:\Users\PC\Documents\Olist Store Datasets\olist_order_items_dataset.csv';
		SET @target_object = 'olist_order_items_dataset';
		SET @step_load_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;
		SET @rows_updated = 0;
		SET @rows_rejected = 0;

		-- Load log details at step-level
		INSERT INTO etl.step_log
		(
			batch_id,
			layer,
			step_name,
			load_type,
			source_object,
			target_object,
			step_start_time,
			step_load_status,
			rows_extracted,
			rows_inserted,
			rows_updated,
			rows_rejected
		)
		VALUES
		(
			@batch_id,
			@layer,
			@step_name,
			@load_type,
			@source_object,
			@target_object,
			@step_start_time,
			@step_load_status,
			@rows_extracted,
			@rows_inserted,
			@rows_updated,
			@rows_rejected
		);

		-- Retrieve recently generated step id
		SET @step_id = SCOPE_IDENTITY();

		-- Create a temporary staging table #staging_olist_order_items_dataset
		CREATE TABLE #staging_olist_order_items_dataset
		(
			order_id NVARCHAR(50),
			order_item_id INT,
			product_id NVARCHAR(50),
			seller_id NVARCHAR(50),
			shipping_limit_date DATETIME2(0),
			price DECIMAL(10, 2),
			freight_value DECIMAL(10, 2)
		);

		-- Map BULK INSERT statement to variable @sql
		SET @SQL = 'BULK INSERT #staging_olist_order_items_dataset FROM ''' + @source_object + ''' WITH 
		(FORMAT = ''CSV'', FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''0x0A'', TABLOCK);';

		-- Execute BULK INSERT statement
		EXEC (@sql);

		-- Add a computed column dwh row hash into staging table
		ALTER TABLE #staging_olist_order_items_dataset
		ADD dwh_row_hash AS CAST(HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(order_id, 'N/A'), COALESCE(order_item_id, 1), 
		COALESCE(product_id, 'N/A'), COALESCE(seller_id, 'N/A'), COALESCE(CONVERT(NVARCHAR(20), shipping_limit_date, 120), '1900-01-01'), 
		COALESCE(price, 1.00), COALESCE(freight_value, 1.00))) AS BINARY(32)) PERSISTED;

		-- Extract total number of records loaded into staging table
		SELECT @rows_extracted = COUNT(*) FROM #staging_olist_order_items_dataset;

		-- Load new records from staging to target table olist_order_items_dataset
		INSERT INTO bronze.olist_order_items_dataset
		(
			order_id,
			order_item_id,
			product_id,
			seller_id,
			shipping_limit_date,
			price,
			freight_value,
			dwh_row_hash,
			dwh_batch_id,
			dwh_source_file
		)
		SELECT
			src.order_id,
			src.order_item_id,
			src.product_id,
			src.seller_id,
			src.shipping_limit_date,
			src.price,
			src.freight_value,
			src.dwh_row_hash,
			@batch_id,
			@source_object
		FROM #staging_olist_order_items_dataset src
		LEFT JOIN bronze.olist_order_items_dataset tgt
		ON src.order_id = tgt.order_id
		AND src.order_item_id = tgt.order_item_id
		WHERE tgt.order_id IS NULL;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT;

		-- Update outdated records in bronze table
		UPDATE tgt
			SET
				tgt.product_id = src.product_id,
				tgt.seller_id = src.seller_id,
				tgt.shipping_limit_date = src.shipping_limit_date,
				tgt.price = src.price,
				tgt.freight_value = src.freight_value,
				tgt.dwh_row_hash = src.dwh_row_hash,
				tgt.dwh_batch_id = @batch_id,
				tgt.dwh_source_file = @source_object,
				tgt.dwh_is_deleted = 0
			FROM #staging_olist_order_items_dataset src
			INNER JOIN bronze.olist_order_items_dataset tgt
			ON src.order_id = tgt.order_id
			AND src.order_item_id = tgt.order_item_id
			WHERE src.dwh_row_hash <> tgt.dwh_row_hash;

		-- Retrieve rows updated
		SET @rows_updated = @@ROWCOUNT;

		-- Flag deleted records in bronze table
		UPDATE tgt
			SET
				dwh_batch_id = @batch_id,
				dwh_source_file = @source_object,
				dwh_is_deleted = 1
			FROM bronze.olist_order_items_dataset tgt
			LEFT JOIN #staging_olist_order_items_dataset src
			ON tgt.dwh_row_hash = src.dwh_row_hash
			WHERE src.order_id IS NULL;

		-- Map values to variables on success
		SET @step_end_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @step_end_time);
		SET @step_load_status = 'Successful';
		SET @rows_rejected = @rows_extracted - (@rows_inserted + @rows_updated);
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + (@rows_inserted + @rows_updated);

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET
				step_end_time = @step_end_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted,
				rows_updated = @rows_updated,
				rows_rejected = @rows_rejected
			WHERE step_id = @step_id AND batch_id = @batch_id; 

		-- Drop staging table
		DROP TABLE IF EXISTS #staging_olist_order_items_dataset;


		-- ========================================================
		-- STEP 4: Load Olist Order Payments Dataset 
		-- ========================================================

		-- Map values to variables before transactions
		SET @step_start_time = SYSDATETIME();
		SET @step_name = 'load_olist_order_payments_dataset';
		SET @load_type = 'Incremental Load: Insert, Update & Soft-Delete Flag';
		SET @source_object = 'C:\Users\PC\Documents\Olist Store Datasets\olist_order_payments_dataset.csv';
		SET @target_object = 'olist_order_payments_dataset';
		SET @step_load_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;
		SET @rows_updated = 0;
		SET @rows_rejected = 0;

		-- Load log details at step-level
		INSERT INTO etl.step_log
		(
			batch_id,
			layer,
			step_name,
			load_type,
			source_object,
			target_object,
			step_start_time,
			step_load_status,
			rows_extracted,
			rows_inserted,
			rows_updated,
			rows_rejected
		)
		VALUES
		(
			@batch_id,
			@layer,
			@step_name,
			@load_type,
			@source_object,
			@target_object,
			@step_start_time,
			@step_load_status,
			@rows_extracted,
			@rows_inserted,
			@rows_updated,
			@rows_rejected
		);

		-- Retrieve recently generated step id
		SET @step_id = SCOPE_IDENTITY();

		-- Create a temporary staging table #staging_olist_order_payment_dataset
		CREATE TABLE #staging_olist_order_payments_dataset
		(
			order_id NVARCHAR(50),
			payment_sequential INT,
			payment_type NVARCHAR(50),
			payment_installments INT,
			payment_value DECIMAL(10, 2)
		);

		-- Map BULK INSERT statement to variable @sql
		SET @sql = 'BULK INSERT #staging_olist_order_payments_dataset FROM ''' + @source_object + ''' WITH 
		(FORMAT = ''CSV'', FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''0x0A'', TABLOCK);';

		-- Execute BULK INSERT statement
		EXEC (@sql);

		-- Add a computed column dwh row hash into staging table
		ALTER TABLE #staging_olist_order_payments_dataset
		ADD dwh_row_hash AS CAST(HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(order_id, 'N/A'), COALESCE(payment_sequential, 1), 
		COALESCE(payment_type, 'N/A'), COALESCE(payment_installments, 1), COALESCE(payment_value, 1.00))) AS BINARY(32)) PERSISTED;

		-- Extract total number of records loaded into staging table
		SELECT @rows_extracted = COUNT(*) FROM #staging_olist_order_payments_dataset;

		-- Load new records from staging to target table olist_order_payments_dataset
		INSERT INTO bronze.olist_order_payments_dataset
		(
			order_id,
			payment_sequential,
			payment_type,
			payment_installments,
			payment_value,
			dwh_row_hash,
			dwh_batch_id,
			dwh_source_file
		)
		SELECT
			src.order_id,
			src.payment_sequential,
			src.payment_type,
			src.payment_installments,
			src.payment_value,
			src.dwh_row_hash,
			@batch_id,
			@source_object
		FROM #staging_olist_order_payments_dataset src
		LEFT JOIN bronze.olist_order_payments_dataset tgt
		ON src.order_id = tgt.order_id
		AND src.payment_sequential = tgt.payment_sequential
		WHERE tgt.order_id IS NULL;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT;

		-- Update outdated records in bronze table
		UPDATE tgt
			SET
				tgt.payment_type = src.payment_type,
				tgt.payment_installments = src.payment_installments,
				tgt.payment_value = src.payment_value,
				tgt.dwh_row_hash = src.dwh_row_hash,
				tgt.dwh_batch_id = @batch_id,
				tgt.dwh_source_file = @source_object,
				tgt.dwh_is_deleted = 0
			FROM #staging_olist_order_payments_dataset src
			INNER JOIN bronze.olist_order_payments_dataset tgt
			ON src.order_id = tgt.order_id
			AND src.payment_sequential = tgt.payment_sequential
			WHERE tgt.dwh_row_hash <> src.dwh_row_hash;

		-- Retrieve rows updated
		SET @rows_updated = @@ROWCOUNT;

		-- Flag deleted records in bronze table
		UPDATE tgt
			SET
				tgt.dwh_batch_id = @batch_id,
				tgt.dwh_source_file = @source_object,
				tgt.dwh_is_deleted = 1
			FROM bronze.olist_order_payments_dataset tgt
			LEFT JOIN #staging_olist_order_payments_dataset src
			ON tgt.dwh_row_hash = src.dwh_row_hash
			WHERE src.dwh_row_hash IS NULL;

		-- Map values to variables on success
		SET @step_end_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @step_end_time);
		SET @step_load_status = 'Successful';
		SET @rows_rejected = @rows_extracted - (@rows_inserted + @rows_updated);
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + (@rows_inserted + @rows_updated);

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET
				step_end_time = @step_end_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted,
				rows_updated = @rows_updated,
				rows_rejected = @rows_rejected
			WHERE step_id = @step_id AND batch_id = @batch_id; 

		-- Drop staging table
		DROP TABLE IF EXISTS #staging_olist_order_payments_dataset;


		-- ========================================================
		-- STEP 5: Load Olist Order Reviews Dataset 
		-- ========================================================

		-- Map values to variables before transactions
		SET @step_start_time = SYSDATETIME();
		SET @step_name = 'load_olist_order_reviews_dataset';
		SET @load_type = 'Incremental Load: Insert, Update & Soft-Delete Flag';
		SET @source_object = 'C:\Users\PC\Documents\Olist Store Datasets\olist_order_reviews_dataset.csv';
		SET @target_object = 'olist_order_reviews_dataset';
		SET @step_load_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;
		SET @rows_updated = 0;
		SET @rows_rejected = 0;

		-- Load log details at step-level
		INSERT INTO etl.step_log
		(
			batch_id,
			layer,
			step_name,
			load_type,
			source_object,
			target_object,
			step_start_time,
			step_load_status,
			rows_extracted,
			rows_inserted,
			rows_updated,
			rows_rejected
		)
		VALUES
		(
			@batch_id,
			@layer,
			@step_name,
			@load_type,
			@source_object,
			@target_object,
			@step_start_time,
			@step_load_status,
			@rows_extracted,
			@rows_inserted,
			@rows_updated,
			@rows_rejected
		);

		-- Retrieve recently generated step id
		SET @step_id = SCOPE_IDENTITY();

		-- Create a temporary staging table #staging_olist_order_reviews_dataset
		CREATE TABLE #staging_olist_order_reviews_dataset
		(
			review_id NVARCHAR(50),
			order_id NVARCHAR(50),
			review_score INT,
			review_comment_title NVARCHAR(50),
			review_comment_message NVARCHAR(MAX),
			review_creation_date DATETIME2(0),
			review_answer_timestamp DATETIME2(0)
		);

		-- Map BULK INSERT statement to variable @sql
		SET @sql = 'BULK INSERT #staging_olist_order_reviews_dataset FROM ''' + @source_object + ''' WITH 
		(FORMAT = ''CSV'', FIRSTROW = 2, TABLOCK);';

		-- Execute BULK INSERT statement
		EXEC (@sql);

		-- Add a computed column dwh row hash into staging table
		ALTER TABLE #staging_olist_order_reviews_dataset
		ADD dwh_row_hash AS CAST(HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(review_id, 'N/A'), COALESCE(order_id, 'N/A'), COALESCE(review_score, 1), 
		COALESCE(review_comment_title, 'N/A'), COALESCE(review_comment_message, 'N/A'),
		COALESCE(CONVERT(NVARCHAR(20), review_creation_date, 120), '1900-01-01'), COALESCE(CONVERT(NVARCHAR(20), 
		review_answer_timestamp, 120), '1900-01-01'))) AS BINARY(32)) PERSISTED;

		-- Extract total number of records loaded into staging table
		SELECT @rows_extracted = COUNT(*) FROM #staging_olist_order_reviews_dataset;

		-- Load new records from staging to target table olist_order_reviews_dataset
		INSERT INTO bronze.olist_order_reviews_dataset
		(
			review_id,
			order_id,
			review_score,
			review_comment_title,
			review_comment_message,
			review_creation_date,
			review_answer_timestamp,
			dwh_row_hash,
			dwh_batch_id,
			dwh_source_file
		)
		SELECT
			src.review_id,
			src.order_id,
			src.review_score,
			src.review_comment_title,
			src.review_comment_message,
			src.review_creation_date,
			src.review_answer_timestamp,
			src.dwh_row_hash,
			@batch_id,
			@source_object
		FROM #staging_olist_order_reviews_dataset src
		LEFT JOIN bronze.olist_order_reviews_dataset tgt
		ON src.review_id = tgt.review_id
		AND src.order_id = tgt.order_id
		WHERE tgt.review_id IS NULL;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT;

		-- Update outdated records in bronze table
		UPDATE tgt
			SET
				tgt.review_score = src.review_score,
				tgt.review_comment_title = src.review_comment_title,
				tgt.review_comment_message = src.review_comment_message,
				tgt.review_creation_date = src.review_creation_date,
				tgt.review_answer_timestamp = src.review_answer_timestamp,
				tgt.dwh_row_hash = src.dwh_row_hash,
				tgt.dwh_batch_id = @batch_id,
				tgt.dwh_source_file = @source_object,
				tgt.dwh_is_deleted = 0
			FROM #staging_olist_order_reviews_dataset src
			INNER JOIN bronze.olist_order_reviews_dataset tgt
			ON src.review_id = tgt.review_id
			AND src.order_id = tgt.order_id
			WHERE tgt.dwh_row_hash <> src.dwh_row_hash;

		-- Retrieve rows updated
		SET @rows_updated = @@ROWCOUNT;

		-- Flag deleted records in bronze table
		UPDATE tgt
			SET
				tgt.dwh_batch_id = @batch_id,
				tgt.dwh_source_file = @source_object,
				tgt.dwh_is_deleted = 1
			FROM bronze.olist_order_reviews_dataset tgt
			LEFT JOIN #staging_olist_order_reviews_dataset src
			ON tgt.dwh_row_hash = src.dwh_row_hash
			WHERE src.dwh_row_hash IS NULL;

		-- Map values to variables on success
		SET @step_end_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @step_end_time);
		SET @step_load_status = 'Successful';
		SET @rows_rejected = @rows_extracted - (@rows_inserted + @rows_updated);
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + (@rows_inserted + @rows_updated);

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET
				step_end_time = @step_end_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted,
				rows_updated = @rows_updated,
				rows_rejected = @rows_rejected
			WHERE step_id = @step_id AND batch_id = @batch_id; 

		-- Drop staging table
		DROP TABLE IF EXISTS #staging_olist_order_reviews_dataset;


		-- ========================================================
		-- STEP 6: Load Olist Orders Dataset 
		-- ========================================================

		-- Map values to variables before transactions
		SET @step_start_time = SYSDATETIME();
		SET @step_name = 'load_olist_orders_dataset';
		SET @load_type = 'Incremental Load: Insert, Update & Soft-Delete Flag';
		SET @source_object = 'C:\Users\PC\Documents\Olist Store Datasets\olist_orders_dataset.csv';
		SET @target_object = 'olist_orders_dataset';
		SET @step_load_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;
		SET @rows_updated = 0;
		SET @rows_rejected = 0;

		-- Load log details at step-level
		INSERT INTO etl.step_log
		(
			batch_id,
			layer,
			step_name,
			load_type,
			source_object,
			target_object,
			step_start_time,
			step_load_status,
			rows_extracted,
			rows_inserted,
			rows_updated,
			rows_rejected
		)
		VALUES
		(
			@batch_id,
			@layer,
			@step_name,
			@load_type,
			@source_object,
			@target_object,
			@step_start_time,
			@step_load_status,
			@rows_extracted,
			@rows_inserted,
			@rows_updated,
			@rows_rejected
		);

		-- Retrieve recently generated step id
		SET @step_id = SCOPE_IDENTITY();

		-- Create a temporary staging table #staging_olist_orders_dataset
		CREATE TABLE #staging_olist_orders_dataset
		(
			order_id NVARCHAR(50),
			customer_id NVARCHAR(50),
			order_status NVARCHAR(50),
			order_purchase_timestamp DATETIME2(0),
			order_approved_at DATETIME2(0),
			order_delivered_carrier_date DATETIME2(0),
			order_delivered_customer_date DATETIME2(0),
			order_estimated_delivery_date DATETIME2(0)
		);

		-- Map BULK INSERT statement to variable @sql
		SET @sql = 'BULK INSERT #staging_olist_orders_dataset FROM ''' + @source_object + ''' WITH 
		(FORMAT = ''CSV'', FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''0x0A'', TABLOCK);';

		-- Execute BULK INSERT statement
		EXEC (@sql);

		-- Add a computed column dwh row hash into staging table
		ALTER TABLE #staging_olist_orders_dataset
		ADD dwh_row_hash AS CAST(HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(order_id, 'N/A'), COALESCE(customer_id, 'N/A'), COALESCE(order_status, 'N/A'), 
		COALESCE(CONVERT(NVARCHAR(20), order_purchase_timestamp, 120), '1900-01-01'), 
		COALESCE(CONVERT(NVARCHAR(20), order_approved_at, 120), '1900-01-01'), 
		COALESCE(CONVERT(NVARCHAR(20), order_delivered_carrier_date, 120), '1900-01-01'), 
		COALESCE(CONVERT(NVARCHAR(20), order_delivered_customer_date, 120), '1900-01-01'), 
		COALESCE(CONVERT(NVARCHAR(20), order_estimated_delivery_date, 120), '1900-01-01'))) AS BINARY(32)) PERSISTED;

		-- Extract total number of records loaded into staging table
		SELECT @rows_extracted = COUNT(*) FROM #staging_olist_orders_dataset;

		-- Load new records from staging to target table olist_orders_dataset
		INSERT INTO bronze.olist_orders_dataset
		(
			order_id,
			customer_id,
			order_status,
			order_purchase_timestamp,
			order_approved_at,
			order_delivered_carrier_date,
			order_delivered_customer_date,
			order_estimated_delivery_date,
			dwh_row_hash,
			dwh_batch_id,
			dwh_source_file
		)
		SELECT
			src.order_id,
			src.customer_id,
			src.order_status,
			src.order_purchase_timestamp,
			src.order_approved_at,
			src.order_delivered_carrier_date,
			src.order_delivered_customer_date,
			src.order_estimated_delivery_date,
			src.dwh_row_hash,
			@batch_id,
			@source_object
		FROM #staging_olist_orders_dataset src
		LEFT JOIN bronze.olist_orders_dataset tgt
		ON src.order_id = tgt.order_id
		WHERE tgt.order_id IS NULL;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT;

		-- Update outdated records in bronze table
		UPDATE tgt
			SET
				tgt.customer_id = src.customer_id,
				tgt.order_status = src.order_status,
				tgt.order_purchase_timestamp = src.order_purchase_timestamp,
				tgt.order_approved_at = src.order_approved_at,
				tgt.order_delivered_carrier_date = src.order_delivered_carrier_date,
				tgt.order_delivered_customer_date = src.order_delivered_customer_date,
				tgt.order_estimated_delivery_date = src.order_estimated_delivery_date,
				tgt.dwh_row_hash = src.dwh_row_hash,
				tgt.dwh_batch_id = @batch_id,
				tgt.dwh_source_file = @source_object,
				tgt.dwh_is_deleted = 0
			FROM #staging_olist_orders_dataset src
			INNER JOIN bronze.olist_orders_dataset tgt
			ON src.order_id = tgt.order_id
			WHERE tgt.dwh_row_hash <> src.dwh_row_hash;

		-- Retrieve rows updated
		SET @rows_updated = @@ROWCOUNT;

		-- Flag deleted records in bronze table
		UPDATE tgt
			SET
				tgt.dwh_batch_id = @batch_id,
				tgt.dwh_source_file = @source_object,
				tgt.dwh_is_deleted = 1
			FROM bronze.olist_orders_dataset tgt
			LEFT JOIN #staging_olist_orders_dataset src
			ON tgt.dwh_row_hash = src.dwh_row_hash
			WHERE src.dwh_row_hash IS NULL;
	
		-- Map values to variables on success
		SET @step_end_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @step_end_time);
		SET @step_load_status = 'Successful';
		SET @rows_rejected = @rows_extracted - (@rows_inserted + @rows_updated);
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + (@rows_inserted + @rows_updated);

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET
				step_end_time = @step_end_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted,
				rows_updated = @rows_updated,
				rows_rejected = @rows_rejected
			WHERE step_id = @step_id AND batch_id = @batch_id; 

		-- Drop staging table
		DROP TABLE IF EXISTS #staging_olist_orders_dataset;


		-- ========================================================
		-- STEP 7: Load Olist Product Category Name Translation 
		-- ========================================================

		-- Map values to variables before transactions
		SET @step_start_time = SYSDATETIME();
		SET @step_name = 'load_olist_product_category_name_translation';
		SET @load_type = 'Incremental Load: Insert, Update & Soft-Delete Flag';
		SET @source_object = 'C:\Users\PC\Documents\Olist Store Datasets\product_category_name_translation.csv';
		SET @target_object = 'olist_product_category_name_translation';
		SET @step_load_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;
		SET @rows_updated = 0;
		SET @rows_rejected = 0;

		-- Load log details at step-level
		INSERT INTO etl.step_log
		(
			batch_id,
			layer,
			step_name,
			load_type,
			source_object,
			target_object,
			step_start_time,
			step_load_status,
			rows_extracted,
			rows_inserted,
			rows_updated,
			rows_rejected
		)
		VALUES
		(
			@batch_id,
			@layer,
			@step_name,
			@load_type,
			@source_object,
			@target_object,
			@step_start_time,
			@step_load_status,
			@rows_extracted,
			@rows_inserted,
			@rows_updated,
			@rows_rejected
		);

		-- Retrieve recently generated step id
		SET @step_id = SCOPE_IDENTITY();

		-- Create a temporary staging table #staging_olist_product_name_translation
		CREATE TABLE #staging_olist_product_name_translation
		(
			product_category_name NVARCHAR(50),
			product_category_name_english NVARCHAR(50)
		);

		-- Map BULK INSERT statement to variable @sql
		SET @sql = 'BULK INSERT #staging_olist_product_name_translation FROM ''' + @source_object + ''' WITH 
		(FORMAT = ''CSV'', FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''0x0A'', TABLOCK);';

		-- Execute BULK INSERT statement
		EXEC (@sql);

		-- Add a computed column dwh row hash into staging table
		ALTER TABLE #staging_olist_product_name_translation
		ADD dwh_row_hash AS CAST(HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(product_category_name, 'N/A'), 
		COALESCE(product_category_name_english, 'N/A'))) AS BINARY(32)) PERSISTED;

		-- Extract total number of records loaded into staging table
		SELECT @rows_extracted = COUNT(*) FROM #staging_olist_product_name_translation;

		-- Load new records from staging to target table olist_product_category_name_translation
		INSERT INTO bronze.olist_product_category_name_translation
		(
			product_category_name,
			product_category_name_english,
			dwh_row_hash,
			dwh_batch_id,
			dwh_source_file
		)
		SELECT
			src.product_category_name,
			src.product_category_name_english,
			src.dwh_row_hash,
			@batch_id,
			@source_object
		FROM #staging_olist_product_name_translation src
		LEFT JOIN bronze.olist_product_category_name_translation tgt
		ON src.product_category_name = tgt.product_category_name
		WHERE tgt.product_category_name IS NULL;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT;

		-- Update outdated records in bronze table
		UPDATE tgt
			SET
				tgt.product_category_name_english = src.product_category_name_english,
				tgt.dwh_row_hash = src.dwh_row_hash,
				tgt.dwh_batch_id = @batch_id,
				tgt.dwh_source_file = @source_object,
				tgt.dwh_is_deleted = 0
			FROM #staging_olist_product_name_translation src
			INNER JOIN bronze.olist_product_category_name_translation tgt
			ON src.product_category_name = tgt.product_category_name
			WHERE tgt.dwh_row_hash <> src.dwh_row_hash;

		-- Retrieve rows updated
		SET @rows_updated = @@ROWCOUNT;

		-- Flag deleted records in bronze table
		UPDATE tgt
			SET
				tgt.dwh_batch_id = @batch_id,
				tgt.dwh_source_file = @source_object,
				tgt.dwh_is_deleted = 1
			FROM bronze.olist_product_category_name_translation tgt
			LEFT JOIN #staging_olist_product_name_translation src
			ON tgt.dwh_row_hash = src.dwh_row_hash
			WHERE src.dwh_row_hash IS NULL;
	
		-- Map values to variables on success
		SET @step_end_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @step_end_time);
		SET @step_load_status = 'Successful';
		SET @rows_rejected = @rows_extracted - (@rows_inserted + @rows_updated);
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + (@rows_inserted + @rows_updated);

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET
				step_end_time = @step_end_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted,
				rows_updated = @rows_updated,
				rows_rejected = @rows_rejected
			WHERE step_id = @step_id AND batch_id = @batch_id; 

		-- Drop staging table
		DROP TABLE IF EXISTS #staging_olist_product_name_translation;


		-- ========================================================
		-- STEP 8: Load Olist Products Dataset 
		-- ========================================================

		-- Map values to variables before transactions
		SET @step_start_time = SYSDATETIME();
		SET @step_name = 'load_olist_products_dataset';
		SET @load_type = 'Incremental Load: Insert, Update & Soft-Delete Flag';
		SET @source_object = 'C:\Users\PC\Documents\Olist Store Datasets\olist_products_dataset.csv';
		SET @target_object = 'olist_products_dataset';
		SET @step_load_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;
		SET @rows_updated = 0;
		SET @rows_rejected = 0;

		-- Load log details at step-level
		INSERT INTO etl.step_log
		(
			batch_id,
			layer,
			step_name,
			load_type,
			source_object,
			target_object,
			step_start_time,
			step_load_status,
			rows_extracted,
			rows_inserted,
			rows_updated,
			rows_rejected
		)
		VALUES
		(
			@batch_id,
			@layer,
			@step_name,
			@load_type,
			@source_object,
			@target_object,
			@step_start_time,
			@step_load_status,
			@rows_extracted,
			@rows_inserted,
			@rows_updated,
			@rows_rejected
		);

		-- Retrieve recently generated step id
		SET @step_id = SCOPE_IDENTITY();

		-- Create a temporary staging table #staging_olist_products_dataset
		CREATE TABLE #staging_olist_products_dataset
		(
			product_id NVARCHAR(50),
			product_category_name NVARCHAR(50),
			product_name_length INT,
			product_description_length INT,
			product_photos_qty INT,
			product_weight_g INT,
			product_length_cm INT,
			product_height_cm INT,
			product_width_cm INT
		);

		-- Map BULK INSERT statement to variable @sql
		SET @sql = 'BULK INSERT #staging_olist_products_dataset FROM ''' + @source_object + ''' WITH 
		(FORMAT = ''CSV'', FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''0x0A'', TABLOCK);';

		-- Execute BULK INSERT statement
		EXEC (@sql);

		-- Add a computed column dwh row hash into staging table
		ALTER TABLE #staging_olist_products_dataset
		ADD dwh_row_hash AS CAST(HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(product_id, 'N/A'), COALESCE(product_category_name, 'N/A'),
		COALESCE(product_name_length, 1), COALESCE(product_description_length, 1), COALESCE(product_photos_qty, 1), COALESCE(product_weight_g, 1), 
		COALESCE(product_length_cm, 1), COALESCE(product_height_cm, 1), COALESCE(product_width_cm, 1))) AS BINARY(32)) PERSISTED;

		-- Extract total number of records loaded into staging table
		SELECT @rows_extracted = COUNT(*) FROM #staging_olist_products_dataset;

		-- Load new records from staging to target table olist_products_dataset
		INSERT INTO bronze.olist_products_dataset
		(
			product_id,
			product_category_name,
			product_name_length,
			product_description_length,
			product_photos_qty,
			product_weight_g,
			product_length_cm,
			product_height_cm,
			product_width_cm,
			dwh_row_hash,
			dwh_batch_id,
			dwh_source_file
		)
		SELECT
			src.product_id,
			src.product_category_name,
			src.product_name_length,
			src.product_description_length,
			src.product_photos_qty,
			src.product_weight_g,
			src.product_length_cm,
			src.product_height_cm,
			src.product_width_cm,
			src.dwh_row_hash,
			@batch_id,
			@source_object
		FROM #staging_olist_products_dataset src
		LEFT JOIN bronze.olist_products_dataset tgt
		ON src.product_id = tgt.product_id
		WHERE tgt.product_id IS NULL;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT;

		-- Update outdated records in bronze table
		UPDATE tgt
			SET
				tgt.product_category_name = src.product_category_name,
				tgt.product_name_length = src.product_name_length,
				tgt.product_description_length = src.product_description_length,
				tgt.product_photos_qty = src.product_photos_qty,
				tgt.product_weight_g = src.product_weight_g,
				tgt.product_length_cm = src.product_length_cm,
				tgt.product_height_cm = src.product_height_cm,
				tgt.product_width_cm = src.product_width_cm,
				tgt.dwh_row_hash = src.dwh_row_hash,
				tgt.dwh_batch_id = @batch_id,
				tgt.dwh_source_file = @source_object,
				tgt.dwh_is_deleted = 0
			FROM #staging_olist_products_dataset src
			INNER JOIN bronze.olist_products_dataset tgt
			ON src.product_id = tgt.product_id
			WHERE tgt.dwh_row_hash <> src.dwh_row_hash;

		-- Retrieve rows updated
		SET @rows_updated = @@ROWCOUNT;

		-- Flag deleted records in bronze table
		UPDATE tgt
			SET
				tgt.dwh_batch_id = @batch_id,
				tgt.dwh_source_file = @source_object,
				tgt.dwh_is_deleted = 1
			FROM bronze.olist_products_dataset tgt
			LEFT JOIN #staging_olist_products_dataset src
			ON tgt.dwh_row_hash = src.dwh_row_hash
			WHERE src.dwh_row_hash IS NULL;

		-- Map values to variables on success
		SET @step_end_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @step_end_time);
		SET @step_load_status = 'Successful';
		SET @rows_rejected = @rows_extracted - (@rows_inserted + @rows_updated);
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + (@rows_inserted + @rows_updated);

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET
				step_end_time = @step_end_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted,
				rows_updated = @rows_updated,
				rows_rejected = @rows_rejected
			WHERE step_id = @step_id AND batch_id = @batch_id; 

		-- Drop staging table
		DROP TABLE IF EXISTS #staging_olist_products_dataset;


		-- ========================================================
		-- STEP 9: Load Olist Sellers Dataset 
		-- ========================================================

		-- Map values to variables before transactions
		SET @step_start_time = SYSDATETIME();
		SET @step_name = 'load_olist_sellers_dataset';
		SET @load_type = 'Incremental Load: Insert, Update & Soft-Delete Flag';
		SET @source_object = 'C:\Users\PC\Documents\Olist Store Datasets\olist_sellers_dataset.csv';
		SET @target_object = 'olist_sellers_dataset';
		SET @step_load_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;
		SET @rows_updated = 0;
		SET @rows_rejected = 0;

		-- Load log details at step-level
		INSERT INTO etl.step_log
		(
			batch_id,
			layer,
			step_name,
			load_type,
			source_object,
			target_object,
			step_start_time,
			step_load_status,
			rows_extracted,
			rows_inserted,
			rows_updated,
			rows_rejected
		)
		VALUES
		(
			@batch_id,
			@layer,
			@step_name,
			@load_type,
			@source_object,
			@target_object,
			@step_start_time,
			@step_load_status,
			@rows_extracted,
			@rows_inserted,
			@rows_updated,
			@rows_rejected
		);

		-- Retrieve recently generated step id
		SET @step_id = SCOPE_IDENTITY();

		-- Create a temporary staging table #staging_olist_sellers_dataset
		CREATE TABLE #staging_olist_sellers_dataset
		(
			seller_id NVARCHAR(50),
			seller_zip_code_prefix CHAR(5),
			seller_city NVARCHAR(50),
			seller_state NVARCHAR(50)
		);

		-- Map BULK INSERT statement to variable @sql
		SET @sql = 'BULK INSERT #staging_olist_sellers_dataset FROM ''' + @source_object + ''' WITH 
		(FORMAT = ''CSV'', FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''0x0A'', TABLOCK);';

		-- Execute BULK INSERT statement
		EXEC (@sql);

		-- Add a computed column dwh row hash into staging table
		ALTER TABLE #staging_olist_sellers_dataset
		ADD dwh_row_hash AS CAST(HASHBYTES('SHA2_256', CONCAT_WS('|', COALESCE(seller_id, 'N/A'), COALESCE(seller_zip_code_prefix, 'N/A'), 
		COALESCE(seller_city, 'N/A'), COALESCE(seller_state, 'N/A'))) AS BINARY(32)) PERSISTED;

		-- Extract total number of records loaded into staging table
		SELECT @rows_extracted = COUNT(*) FROM #staging_olist_sellers_dataset;

		-- Load new records from staging to target table olist_sellers_dataset
		INSERT INTO bronze.olist_sellers_dataset
		(
			seller_id,
			seller_zip_code_prefix,
			seller_city,
			seller_state,
			dwh_row_hash,
			dwh_batch_id,
			dwh_source_file
		)
		SELECT
			src.seller_id,
			src.seller_zip_code_prefix,
			src.seller_city,
			src.seller_state,
			src.dwh_row_hash,
			@batch_id,
			@source_object
		FROM #staging_olist_sellers_dataset src
		LEFT JOIN bronze.olist_sellers_dataset tgt
		ON src.seller_id = tgt.seller_id
		WHERE tgt.seller_id IS NULL;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT;

		-- Update outdated records in bronze table
		UPDATE tgt
			SET
				tgt.seller_zip_code_prefix = src.seller_zip_code_prefix,
				tgt.seller_city = src.seller_city,
				tgt.seller_state = src.seller_state,
				tgt.dwh_row_hash = src.dwh_row_hash,
				tgt.dwh_batch_id = @batch_id,
				tgt.dwh_source_file = @source_object,
				tgt.dwh_is_deleted = 0
			FROM #staging_olist_sellers_dataset src
			INNER JOIN bronze.olist_sellers_dataset tgt
			ON src.seller_id = tgt.seller_id
			WHERE tgt.dwh_row_hash <> src.dwh_row_hash;

		-- Retrieve rows updated
		SET @rows_updated = @@ROWCOUNT;

		-- Flag deleted records in bronze table
		UPDATE tgt
			SET
				tgt.dwh_batch_id = @batch_id,
				tgt.dwh_source_file = @source_object,
				tgt.dwh_is_deleted = 1
			FROM bronze.olist_sellers_dataset tgt
			LEFT JOIN #staging_olist_sellers_dataset src
			ON tgt.dwh_row_hash = src.dwh_row_hash
			WHERE src.dwh_row_hash IS NULL;

		-- Map values to variables on success
		SET @step_end_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @step_end_time);
		SET @step_load_status = 'Successful';
		SET @rows_rejected = @rows_extracted - (@rows_inserted + @rows_updated);
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + (@rows_inserted + @rows_updated);

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET
				step_end_time = @step_end_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted,
				rows_updated = @rows_updated,
				rows_rejected = @rows_rejected
			WHERE step_id = @step_id AND batch_id = @batch_id; 

		-- Drop staging table
		DROP TABLE IF EXISTS #staging_olist_sellers_dataset;


		-- =======================================================================================
		-- SECTION 4: CLOSE BATCH ON SUCCESS
		-- =======================================================================================
		-- Map values to variables
		SET @batch_end_time = SYSDATETIME();
		SET @batch_load_duration = DATEDIFF(second, @batch_start_time, @batch_end_time);
		SET @batch_load_status = 'Successful'; 
		SELECT @total_tables_loaded = COUNT(*) FROM etl.step_log WHERE (batch_id = @batch_id AND step_load_status = 'Successful');

		-- Update log details at batch-level on success
		UPDATE etl.batch_log
			SET
				batch_end_time = @batch_end_time,
				batch_load_duration_second = @batch_load_duration,
				batch_load_status = @batch_load_status,
				total_tables_loaded = @total_tables_loaded,
				total_rows_processed = @total_rows_processed,
				total_rows_loaded = @total_rows_loaded
			WHERE batch_id = @batch_id;
	END TRY

	BEGIN CATCH
		-- Map values to step-level variables on failure
		SET @error_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @error_time);
		SET @step_load_status = 'Failed';

		IF @rows_extracted IS NULL SET @rows_extracted = 0;
		IF @rows_inserted IS NULL SET @rows_inserted = 0;
		IF @rows_updated IS NULL SET @rows_updated = 0;
		SET @rows_rejected = @rows_extracted - (@rows_inserted + @rows_updated);

		-- Update log details at step-level on failure
		UPDATE etl.step_log
			SET
				step_end_time = @error_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted,
				rows_updated = @rows_updated,
				rows_rejected = @rows_rejected
			WHERE step_id = @step_id AND batch_id = @batch_id;

		-- Map values to batch-level variables on failure
		SET @batch_load_duration = DATEDIFF(second, @batch_start_time, @error_time);
		SET @batch_load_status = 'Failed';
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + (@rows_inserted + @rows_updated);
		SELECT @total_tables_loaded = COUNT(*) FROM etl.step_log WHERE batch_id = @batch_id AND step_load_status = 'Successful';

		-- Update log details at batch-level on failure
		UPDATE etl.batch_log
			SET
				batch_end_time = @error_time,
				batch_load_duration_second = @batch_load_duration,
				batch_load_status = @batch_load_status,
				total_tables_loaded = @total_tables_loaded,
				total_rows_processed = @total_rows_processed,
				total_rows_loaded = @total_rows_loaded
			WHERE batch_id = @batch_id;

		-- Insert into error log
		INSERT INTO etl.error_log
		(
			batch_id,
			step_id,
			error_time,
			rows_extracted,
			rows_inserted,
			rows_updated,
			rows_rejected,
			error_description
		)
		VALUES
		(
			@batch_id,
			@step_id,
			@error_time,
			@rows_extracted,
			@rows_inserted,
			@rows_updated,
			@rows_rejected,
			ERROR_MESSAGE()
		);
	END CATCH;
END;
