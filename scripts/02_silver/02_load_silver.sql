/*
=================================================================================================================
Script: 02_load_silver  (Bronze --> Silver)
Location: scripts/02_silver/
Author: Otusanya Toyib
Created At: 2026-11-01
=================================================================================================================
Script Purpose:
	This script extracts data from each bronze table, performs a series of data transformations where necessary, 
	loads the transformed data into a temporary table, on which a series of data quality checks is performed
	and subsequently logged before ingesting into the corresponding silver table.

	The entire operation is treated as a batch, with multitude of steps (9 to be precise) embedded into it, 
	and is logged accurately to facilitate easy tracking and monitoring of batch, step, and data quality operations, 
	and thus, enabling efficient debugging if an error occurs.

Parameter: @batch_id

Usage: EXEC silver.load_silver

Note:
	Default value of parameter (@batch_id) is NULL, as the script generates its own batch_id and
	passes the value into @batch_id. And it should be noted that the parameter is designed to only
	accept a value from the orchestrator proc itself. Manually passing a value, be it a non-existing
	batch, an existing but closed batch, and a once self-generated batch with an incomplete lifecycle,
	returns an error.

	Expect an error if a critical data quality rule (e.g., duplicates in primary keys) is violated.
=================================================================================================================
*/
-- Connect to OlistDatabase
USE OlistDatabase;
GO

CREATE OR ALTER PROCEDURE silver.load_silver @batch_id INT = NULL AS
BEGIN
	-- Suppress number of rows affected
	SET NOCOUNT ON;

	-- Abort transaction when an error occurs
	SET XACT_ABORT ON;

	-- =======================================================================================
	-- SECTION 1: DECLARE ALL VARIABLES
	-- =======================================================================================
	DECLARE
	-- Batch-Level Variables
	@batch_start_time DATETIME2(0) = SYSDATETIME(),
	@batch_name NVARCHAR(50) = 'silver.load_silver',
	@batch_end_time DATETIME2(0),
	@batch_load_duration INT,
	@batch_load_status NVARCHAR(50) = 'Running',
	@total_tables_loaded INT = 0,
	@total_rows_processed INT = 0,
	@total_rows_loaded INT = 0,

	-- Step-Level Variables
	@step_id INT,
	@layer NVARCHAR(50) = 'Silver',
	@step_name NVARCHAR(50),
	@load_type NVARCHAR(70),
	@source_object NVARCHAR(50),
	@target_object NVARCHAR(50),
	@step_start_time DATETIME2(0),
	@step_end_time DATETIME2(0),
	@step_load_duration INT,
	@step_load_status NVARCHAR(50),
	@rows_extracted INT = 0,
	@rows_inserted INT = 0,

	-- Holds error time
	@error_time DATETIME2(0),

	-- Indicates if batch_id is self-generated (0 if externally-generated, and 1 if otherwise)
	@owns_batch BIT = 0,

	-- Indicates validity of batch_id (0 if valid, and 1 if invalid)
	@batch_rejected BIT = 0;


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

		-- Flag batch as self-generated
		SET @owns_batch = 1;
	END;


	-- =======================================================================================
	-- SECTION 3: LOAD ALL SILVER TABLES
	-- =======================================================================================
	BEGIN TRY
		-- Verify existence of @batch_id in batch log table and return an error if not found
		IF NOT EXISTS(SELECT 1 FROM etl.batch_log WHERE batch_id = @batch_id) 
		BEGIN
			SET @batch_rejected = 1;
			THROW 50001, 'Invalid batch_id: batch does not exist in etl.batch_log.', 1;
		END;

		-- If @batch_id exists, verify its load status and abort if batch has been used
		IF EXISTS(SELECT 1 FROM etl.batch_log WHERE batch_id = @batch_id AND batch_load_status <> 'Running')
		BEGIN
			SET @batch_rejected = 1;
			THROW 50002, 'Closed batch_id: This batch has run its full cycle and cannot be reused.', 2;
		END;

		-- If @batch_id is passed into manually, return an error
		IF @owns_batch = 0 AND EXISTS(SELECT 1 FROM etl.batch_log WHERE batch_id = @batch_id AND batch_name <> 'etl.run_pipeline')
		BEGIN
			SET @batch_rejected = 1;
			THROW 50003, 'Unrecognized batch_id: batch_id can only either be self-generated or passed into by the orchestrator proc etl.run_pipeline.', 3;
		END;


		-- =============================================
		-- STEP 1: Load silver.olist_customers_dataset
		-- =============================================

		-- Map values to variables before transactions
		SET @step_start_time = SYSDATETIME();
		SET @step_name = 'load_olist_customers_dataset';
		SET @load_type = 'Full Load: Truncate & Insert';
		SET @source_object = 'bronze.olist_customers_dataset';
		SET @target_object = 'olist_customers_dataset';
		SET @step_load_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;

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
			rows_inserted
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
			@rows_inserted
		);

		-- Retrieve recently generated step id
		SET @step_id = SCOPE_IDENTITY();


		-- Load transformed data into a staging table
		SELECT
			customer_id,
			customer_unique_id,
			customer_zip_code_prefix,
			customer_city,
			customer_state
			INTO #staging_olist_customers_dataset
		FROM bronze.olist_customers_dataset
		WHERE dwh_is_deleted = 0;

		-- Extract total number of records loaded into staging table
		SELECT @rows_extracted = @@ROWCOUNT;

		-- Perform data quality checks on staging table, and load results into a temporary data quality check table 
		SELECT
			COUNT(*) AS records_checked,
			COALESCE(SUM(CASE WHEN customer_id IS NULL OR customer_id = '' OR DATALENGTH(customer_id) <> DATALENGTH(TRIM(customer_id)) 
			THEN 1 ELSE 0 END), 0) AS count_invalid_customer_id,
			COALESCE(SUM(CASE WHEN customer_unique_id IS NULL OR customer_unique_id = '' OR DATALENGTH(customer_unique_id) <> DATALENGTH(TRIM(customer_unique_id))
			THEN 1 ELSE 0 END), 0) AS count_invalid_customer_unique_id_null,
			(SELECT COUNT(*) FROM (SELECT 1 AS num FROM #staging_olist_customers_dataset GROUP BY customer_id HAVING COUNT(*) > 1) SUB) AS count_primary_key_duplicates,
			COALESCE(SUM(CASE WHEN customer_zip_code_prefix = '' OR customer_zip_code_prefix IS NULL OR customer_zip_code_prefix LIKE ('-%') 
			THEN 1 ELSE 0 END), 0) AS count_customer_zip_code_prefix_uncleaned,
			COALESCE(SUM(CASE WHEN customer_city = '' OR customer_city IS NULL OR 
			DATALENGTH(customer_city) <> DATALENGTH(TRIM(customer_city)) THEN 1 ELSE 0 END), 0) AS count_customer_city_uncleaned,
			COALESCE(SUM(CASE WHEN customer_state = '' OR customer_state IS NULL OR 
			DATALENGTH(customer_state) <> DATALENGTH(TRIM(customer_state)) THEN 1 ELSE 0 END), 0) AS count_customer_state_uncleaned
			INTO #dq_checks_customers
		FROM #staging_olist_customers_dataset;

		-- Load data quality results into dq log table
		INSERT INTO etl.dq_log
		(
			batch_id,
			step_id,
			check_name,
			check_severity,
			check_time,
			records_checked,
			records_failed,
			dq_status,
			dq_description
		)
		SELECT @batch_id, @step_id, v.check_name, v.check_severity, SYSDATETIME(), c.records_checked, v.records_failed, CASE WHEN v.records_failed = 0 
		THEN 'Successful' WHEN v.records_failed > 0 AND v.check_severity = 'Critical' THEN 'Failed' ELSE 'Warning' END, CASE WHEN v.records_failed = 0 
		THEN 'Data quality check passed.' ELSE v.fail_message END FROM #dq_checks_customers c CROSS APPLY(
		VALUES
		('Invalid customer_id', 'Critical', count_invalid_customer_id, 'NULL, or empty space or string, or unwanted spaces detected in field customer_id.'),
		('Invalid customer_unique_id', 'Critical', count_invalid_customer_unique_id_null, 'NULL, or empty space or string, or unwanted spaces detected in field customer_unique_id.'),
		('Primary Key Duplicates', 'Critical', count_primary_key_duplicates, 'Duplicates detected in Primary Key pk_customers.'),
		('Uncleaned customer_zip_code_prefix', 'Minor', count_customer_zip_code_prefix_uncleaned, 'NULL, or empty space or string, or negative value detected in field customer_zip_code_prefix.'),
		('Uncleaned customer_city', 'Minor', count_customer_city_uncleaned, 'NULL, or empty space or string, or unwanted spaces detected in field customer_city.'),
		('Uncleaned customer_state', 'Minor', count_customer_state_uncleaned, 'NULL, or empty space or string, or unwanted spaces detected in field customer_state.')
		)v (check_name, check_severity, records_failed, fail_message);

		-- Abort transactions If at least a critical data quality rule is violated
		IF EXISTS (SELECT 1 FROM etl.dq_log WHERE batch_id = @batch_id AND step_id = @step_id AND check_severity = 'Critical' AND dq_status = 'Failed')
		THROW 50004, 'Critical data quality rule violated. Check etl.dq_log for specificity.', 4;

		-- Wrap target-table related transactions in a TRAN block to enable ROLLBACK on error
		BEGIN TRAN;
		-- Delete existing data in target table
		TRUNCATE TABLE silver.olist_customers_dataset;

		-- Load newly retrieved data into target table
		INSERT INTO silver.olist_customers_dataset
		(
			customer_id,
			customer_unique_id,
			customer_zip_code_prefix,
			customer_city,
			customer_state,
			dwh_batch_id
		)
		SELECT
			customer_id,
			customer_unique_id,
			customer_zip_code_prefix,
			customer_city,
			customer_state,
			@batch_id
		FROM #staging_olist_customers_dataset;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT;

		-- Perform row-count reconciliation check
		IF @rows_extracted <> @rows_inserted THROW 50005, 'Row-count reconciliation check failed', 5;
		COMMIT TRAN;

		-- Mark step as successful & map values to variables if row-count reconcilation check passes
		SET @step_load_status = 'Successful';
		SET @step_end_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @step_end_time);
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + @rows_inserted;

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET
				step_end_time = @step_end_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted
			WHERE batch_id = @batch_id AND step_id = @step_id;

		-- Drop staging table
		DROP TABLE IF EXISTS #staging_olist_customers_dataset;

		-- Drop data quality check table
		DROP TABLE IF EXISTS #dq_checks_customers;

		-- ===============================================
		-- STEP 2: Load silver.olist_geolocation_dataset
		-- ===============================================

		-- Map values to variables before transactions
		SET @step_start_time = SYSDATETIME();
		SET @step_name = 'load_olist_geolocation_dataset';
		SET @load_type = 'Full Load: Truncate & Insert';
		SET @source_object = 'bronze.olist_geolocation_dataset';
		SET @target_object = 'olist_geolocation_dataset';
		SET @step_load_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;

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
			rows_inserted
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
			@rows_inserted
		);

		-- Retrieve recently generated step id
		SET @step_id = SCOPE_IDENTITY();

		-- Load transformed data into a staging table
		WITH base_query AS
		(
		SELECT
			geolocation_zip_code_prefix,
			geolocation_lat,
			geolocation_lng,
			geolocation_city,
			geolocation_state,
			COUNT(geolocation_city) OVER(PARTITION BY geolocation_city) AS count_city
		FROM bronze.olist_geolocation_dataset
		WHERE (geolocation_lat >= -33.75 AND geolocation_lat <= 5.27) 
		AND (geolocation_lng >= -73.99 AND geolocation_lng <= -34.79)
		)
		, state_bound_box AS
		(
		SELECT
			bq.geolocation_zip_code_prefix,
			bq.geolocation_lat,
			bq.geolocation_lng,
			bq.geolocation_city,
			bq.geolocation_state,
			bq.count_city
		FROM base_query bq
		INNER JOIN etl.ref_brazil_state_bounds sb
		ON bq.geolocation_state = sb.geolocation_state
		WHERE (ROUND(bq.geolocation_lat, 0) >= ROUND(min_lat, 0) AND ROUND(bq.geolocation_lat, 0) <= ROUND(max_lat, 0))
		AND (ROUND(bq.geolocation_lng, 0) >= ROUND(min_lng, 0) AND ROUND(bq.geolocation_lng, 0) <= ROUND(max_lng, 0))
		)
		, geolocation_flag AS
		(
		SELECT
			geolocation_zip_code_prefix,
			geolocation_lat,
			geolocation_lng,
			geolocation_city,
			geolocation_state,
			count_city,
			ROW_NUMBER() OVER(PARTITION BY geolocation_zip_code_prefix, geolocation_city COLLATE Latin1_General_CI_AI ORDER BY count_city DESC, geolocation_city) AS flag_city
		FROM state_bound_box
		)
		, city_standardization AS
		(
		SELECT
			geolocation_zip_code_prefix,
			geolocation_lat,
			geolocation_lng,
			FIRST_VALUE(geolocation_city) OVER(PARTITION BY geolocation_zip_code_prefix, geolocation_city COLLATE Latin1_General_CI_AI ORDER BY flag_city) AS geolocation_city,
			geolocation_state
		FROM geolocation_flag
		)
		SELECT
			geolocation_zip_code_prefix,
			AVG(geolocation_lat) AS geolocation_lat,
			AVG(geolocation_lng) AS geolocation_lng,
			TRIM(geolocation_city) AS geolocation_city,
			geolocation_state
			INTO #staging_olist_geolocation_dataset
		FROM city_standardization
		GROUP BY
			geolocation_zip_code_prefix,
			geolocation_city,
			geolocation_state;

		-- Extract total number of records loaded into staging table
		SELECT @rows_extracted = @@ROWCOUNT;

		-- Perform data quality checks on staging table, and load results into a temporary data quality check table
		SELECT
			COUNT(*) AS records_checked,
			COALESCE(SUM(CASE WHEN geolocation_zip_code_prefix IS NULL OR geolocation_zip_code_prefix = '' OR geolocation_zip_code_prefix LIKE ('-%') 
			THEN 1 ELSE 0 END), 0) AS count_invalid_geolocation_zip_code_prefix,
			COALESCE(SUM(CASE WHEN geolocation_lat IS NULL THEN 1 ELSE 0 END), 0) AS count_invalid_geolocation_lat,
			COALESCE(SUM(CASE WHEN geolocation_lng IS NULL THEN 1 ELSE 0 END), 0) AS count_invalid_geolocation_lng,
			COALESCE(SUM(CASE WHEN geolocation_city IS NULL OR geolocation_city = '' THEN 1 ELSE 0 END), 0) AS count_invalid_geolocation_city,
			COALESCE(SUM(CASE WHEN geolocation_state IS NULL OR geolocation_state = '' OR DATALENGTH(geolocation_state) <> DATALENGTH(TRIM(geolocation_state))
			THEN 1 ELSE 0 END), 0) AS count_invalid_geolocation_state
			INTO #dq_checks_geolocation
		FROM #staging_olist_geolocation_dataset;

		-- Load data quality results into dq log table
		INSERT INTO etl.dq_log
		(
			batch_id,
			step_id,
			check_name,
			check_severity,
			check_time,
			records_checked,
			records_failed,
			dq_status,
			dq_description
		)
		SELECT @batch_id, @step_id, v.check_name, v.check_severity, SYSDATETIME(), g.records_checked, v.records_failed, 
		CASE WHEN v.records_failed = 0 THEN 'Successful' WHEN v.records_failed > 0 AND v.check_severity = 'Critical' THEN 'Failed' ELSE 'Warning' END,
		CASE WHEN v.records_failed = 0 THEN 'Data quality check passed.' ELSE v.fail_message END FROM #dq_checks_geolocation g CROSS APPLY(
		VALUES
		('Invalid geolocation_zip_code_prefix', 'Critical', count_invalid_geolocation_zip_code_prefix, 'NULL, or empty space or string, or negative value detected in field geolocation_zip_code_prefix.'),
		('Invalid geolocation_lat', 'Minor', count_invalid_geolocation_lat, 'NULL detected in field geolocation_lat.'),
		('Invalid geolocation_lng', 'Minor', count_invalid_geolocation_lng, 'NULL detected in field geolocation_lng.'),
		('Invalid geolocation_city', 'Critical', count_invalid_geolocation_city, 'NULL, or empty space or string detected in field geolocation_city.'),
		('Invalid geolocation_state', 'Critical', count_invalid_geolocation_state, 'NULL, or empty space or string, or unwanted spaces detected in field geolocation_state.')
		) v(check_name, check_severity, records_failed, fail_message)

		-- Abort transactions If at least a critical data quality rule is violated
		IF EXISTS (SELECT 1 FROM etl.dq_log WHERE batch_id = @batch_id AND step_id = @step_id AND check_severity = 'Critical' AND dq_status = 'Failed')
		THROW 50006, 'Critical data quality rule violated. Check etl.dq_log for specificity.', 6; 

		-- Wrap target-table related transactions in a TRAN block to enable ROLLBACK on error
		BEGIN TRAN
		-- Delete existing data in target table
		TRUNCATE TABLE silver.olist_geolocation_dataset;

		-- Load newly retrieved data into target table
		INSERT INTO silver.olist_geolocation_dataset
		(
			geolocation_zip_code_prefix,
			geolocation_lat,
			geolocation_lng,
			geolocation_city,
			geolocation_state,
			dwh_batch_id
		)
		SELECT
			geolocation_zip_code_prefix,
			geolocation_lat,
			geolocation_lng,
			geolocation_city,
			geolocation_state,
			@batch_id
		FROM #staging_olist_geolocation_dataset;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT;

		-- Perform row-count reconciliation check
		IF @rows_extracted <> @rows_inserted THROW 50007, 'Row-count reconciliation check failed', 7;
		COMMIT TRAN;

		-- Mark step as successful & map values to variables if row-count reconcilation check passes
		SET @step_load_status = 'Successful';
		SET @step_end_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @step_end_time);
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + @rows_inserted;

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET
				step_end_time = @step_end_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted
			WHERE batch_id = @batch_id AND step_id = @step_id;

		-- Drop staging table
		DROP TABLE IF EXISTS #staging_olist_geolocation_dataset;

		-- Drop data quality check table
		DROP TABLE IF EXISTS #dq_checks_geolocation;

		-- ===============================================
		-- STEP 3: Load silver.olist_order_items_dataset
		-- ===============================================

		-- Map values to variables before transactions
		SET @step_start_time = SYSDATETIME();
		SET @step_name = 'load_olist_order_items_dataset';
		SET @load_type = 'Full Load: Truncate & Insert';
		SET @source_object = 'bronze.olist_order_items_dataset';
		SET @target_object = 'olist_order_items_dataset';
		SET @step_load_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;

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
			rows_inserted
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
			@rows_inserted
		);

		-- Retrieve recently generated step id
		SET @step_id = SCOPE_IDENTITY();

		-- Load transformed data into a staging table
		SELECT
			order_id,
			order_item_id,
			product_id,
			seller_id,
			shipping_limit_date,
			price,
			freight_value
			INTO #staging_olist_order_items_dataset
		FROM bronze.olist_order_items_dataset
		WHERE dwh_is_deleted = 0;

		-- Extract total number of records loaded into staging table
		SELECT @rows_extracted = @@ROWCOUNT;

		-- Perform data quality checks on staging table, and load results into a temporary data quality check table
		SELECT
			COUNT(*) AS records_checked,
			COALESCE(SUM(CASE WHEN order_id IS NULL OR order_id = '' 
			OR DATALENGTH(order_id) <> DATALENGTH(TRIM(order_id)) THEN 1 ELSE 0 END), 0) AS count_invalid_order_id,
			COALESCE(SUM(CASE WHEN order_item_id IS NULL OR order_item_id <= 0 THEN 1 ELSE 0 END), 0) AS count_invalid_order_item_id,
			(SELECT COUNT(*) FROM (SELECT 1 AS num FROM #staging_olist_order_items_dataset GROUP BY order_id
			HAVING MIN(order_item_id) > 1) SUB) AS count_invalid_minimum_order_item_id,
			(SELECT COUNT(*) FROM (SELECT order_id, order_item_id, LEAD(order_item_id) OVER(PARTITION BY order_id ORDER BY order_item_id) AS order_item_nxt
			FROM #staging_olist_order_items_dataset) SUB WHERE order_item_nxt IS NOT NULL AND order_item_id <> order_item_nxt - 1) AS count_disrupted_squ_order_item_id,
			(SELECT COUNT(*) FROM (SELECT 1 AS num FROM #staging_olist_order_items_dataset 
			GROUP BY order_id, order_item_id HAVING COUNT(*) > 1) SUB) AS count_primary_key_duplicates,
			COALESCE(SUM(CASE WHEN product_id IS NULL OR product_id = '' 
			OR DATALENGTH(product_id) <> DATALENGTH(TRIM(product_id)) THEN 1 ELSE 0 END), 0) AS count_invalid_product_id,
			COALESCE(SUM(CASE WHEN seller_id IS NULL OR seller_id = '' 
			OR DATALENGTH(seller_id) <> DATALENGTH(TRIM(seller_id)) THEN 1 ELSE 0 END), 0) AS count_invalid_seller_id,
			COALESCE(SUM(CASE WHEN shipping_limit_date IS NULL OR shipping_limit_date > SYSDATETIME() THEN 1 ELSE 0 END), 0) AS count_invalid_shipping_limit_date,
			COALESCE(SUM(CASE WHEN price IS NULL OR price <= 0 THEN 1 ELSE 0 END), 0) AS count_invalid_price,
			COALESCE(SUM(CASE WHEN freight_value IS NULL OR freight_value < 0 THEN 1 ELSE 0 END), 0) AS count_invalid_freight_value
			INTO #dq_checks_order_items
		FROM #staging_olist_order_items_dataset;

		-- Load data quality results into dq log table
		INSERT INTO etl.dq_log
		(
			batch_id,
			step_id,
			check_name,
			check_severity,
			check_time,
			records_checked,
			records_failed,
			dq_status,
			dq_description
		)
		SELECT @batch_id, @step_id, v.check_name, v.check_severity, SYSDATETIME(), oi.records_checked, v.records_failed,
		CASE WHEN v.records_failed = 0 THEN 'Successful' WHEN v.records_failed > 0 AND v.check_severity = 'Critical' THEN 'Failed' ELSE 'Warning' END,
		CASE WHEN v.records_failed = 0 THEN 'Data quality check passed.' ELSE v.fail_message END FROM #dq_checks_order_items oi CROSS APPLY(
		VALUES
		('Invalid order_id', 'Critical', count_invalid_order_id, 'NULL, or empty space or string, or unwanted spaces detected in field order_id.'),
		('Invalid order_item_id', 'Critical', count_invalid_order_item_id, 'NULL, or value less than equal to zero detected in field order_item_id.'),
		('Invalid Minimum order_item_id', 'Minor', count_invalid_minimum_order_item_id, 'Minimum value of order_item_id within a number of orders deviates from the expected value of 1.'),
		('Disrupted Sequence in order_item_id', 'Minor', count_disrupted_squ_order_item_id, 'Disruption in sequence of order_item_id within a number of orders detected.'),
		('Primary Key Duplicates', 'Critical', count_primary_key_duplicates, 'Duplicates detected in Primary Key pk_order_items.'),
		('Invalid product_id', 'Minor', count_invalid_product_id, 'NULL, or empty space or string, or unwanted spaces detected in field product_id.'),
		('Invalid seller_id', 'Minor', count_invalid_seller_id, 'NULL, or empty space or string, or unwanted spaces detected in field seller_id.'),
		('Invalid shipping_limit_date', 'Minor', count_invalid_shipping_limit_date, 'NULL, or future date detected in field shipping_limit_date.'),
		('Invalid price', 'Minor', count_invalid_price, 'NULL, or value less than equal to zero detected in field price.'),
		('Invalid freight_value', 'Minor', count_invalid_freight_value, 'NULL, or value less than zero detected in field freight_value.')
		)
		v(check_name, check_severity, records_failed, fail_message);

		-- Abort transactions If at least a critical data quality rule is violated
		IF EXISTS (SELECT 1 FROM etl.dq_log WHERE batch_id = @batch_id AND step_id = @step_id AND check_severity = 'Critical' AND dq_status = 'Failed')
		THROW 50008, 'Critical data quality rule violated. Check etl.dq_log for specificity.', 8;

		-- Wrap target-table related transactions in a TRAN block to enable ROLLBACK on error
		BEGIN TRAN
		-- Delete existing data in target table
		TRUNCATE TABLE silver.olist_order_items_dataset;

		-- Load newly retrieved data into target table
		INSERT INTO silver.olist_order_items_dataset
		(
			order_id,
			order_item_id,
			product_id,
			seller_id,
			shipping_limit_date,
			price,
			freight_value,
			dwh_batch_id
		)
		SELECT
			order_id,
			order_item_id,
			product_id,
			seller_id,
			shipping_limit_date,
			price,
			freight_value,
			@batch_id
		FROM #staging_olist_order_items_dataset;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT

		-- Perform row-count reconciliation check
		IF @rows_extracted <> @rows_inserted THROW 50009, 'Row-count reconciliation check failed', 9;
		COMMIT TRAN;

		-- Mark step as successful & map values to variables if row-count reconcilation check passes
		SET @step_load_status = 'Successful';
		SET @step_end_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @step_end_time);
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + @rows_inserted;

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET
				step_end_time = @step_end_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted
			WHERE batch_id = @batch_id AND step_id = @step_id;

		-- Drop staging table
		DROP TABLE IF EXISTS #staging_olist_order_items_dataset;

		-- Drop data quality check table
		DROP TABLE IF EXISTS #dq_checks_order_items;

		-- ==================================================
		-- STEP 4: Load silver.olist_order_payments_dataset
		-- ==================================================

		-- Map values to variables before transactions
		SET @step_start_time = SYSDATETIME();
		SET @step_name = 'load_olist_order_payments_dataset';
		SET @load_type = 'Full Load: Truncate & Insert';
		SET @source_object = 'bronze.olist_order_payments_dataset';
		SET @target_object = 'olist_order_payments_dataset';
		SET @step_load_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;

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
			rows_inserted
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
			@rows_inserted
		);

		-- Retrieve recently generated step id
		SET @step_id = SCOPE_IDENTITY();

		-- Load transformed data into a staging table
		SELECT
			order_id,
			payment_sequential,
			payment_type,
			CASE 
				WHEN payment_installments = 0 AND payment_type = 'credit_card' AND payment_value > 0 THEN 1
				ELSE payment_installments
			END AS payment_installments,
			payment_value
			INTO #staging_olist_order_payments_dataset
		FROM bronze.olist_order_payments_dataset
		WHERE dwh_is_deleted = 0;

		-- Extract total number of records loaded into staging table
		SELECT @rows_extracted = @@ROWCOUNT;

		-- Perform data quality checks on staging table, and load results into a temporary data quality check table
		SELECT
			COUNT(*) AS records_checked,
			COALESCE(SUM(CASE WHEN order_id IS NULL OR order_id = '' 
			OR DATALENGTH(order_id) <> DATALENGTH(TRIM(order_id)) THEN 1 ELSE 0 END), 0) AS count_invalid_order_id,
			COALESCE(SUM(CASE WHEN payment_sequential IS NULL OR payment_sequential <= 0 THEN 1 ELSE 0 END), 0) AS count_invalid_payment_sequential,
			(SELECT COUNT(*) FROM (SELECT 1 AS num FROM #staging_olist_order_payments_dataset GROUP BY order_id
			HAVING MIN(payment_sequential) > 1) SUB) AS count_invalid_minimum_payment_sequential,
			(SELECT COUNT(*) FROM (SELECT order_id, payment_sequential, LEAD(payment_sequential) OVER(PARTITION BY order_id ORDER BY payment_sequential) 
			AS payment_sequential_nxt FROM #staging_olist_order_payments_dataset) SUB WHERE payment_sequential_nxt IS NOT NULL 
			AND payment_sequential <> payment_sequential_nxt - 1) AS count_disrupted_payment_sequential,
			(SELECT COUNT(*) FROM (SELECT 1 AS num FROM #staging_olist_order_payments_dataset GROUP BY order_id, payment_sequential 
			HAVING COUNT(*) > 1) SUB) AS count_primary_key_duplicates,
			COALESCE(SUM(CASE WHEN payment_type IS NULL OR payment_type = '' OR DATALENGTH(payment_type) <> DATALENGTH(TRIM(payment_type))
			THEN 1 ELSE 0 END), 0) AS count_invalid_payment_type,
			COALESCE(SUM(CASE WHEN (payment_type IS NOT NULL AND payment_type <> '') 
			AND (payment_type NOT IN ('credit_card', 'not_defined', 'debit_card', 'boleto', 'voucher')) THEN 1 ELSE 0 END), 0) AS newly_added_payment_type,
			COALESCE(SUM(CASE WHEN payment_installments IS NULL OR payment_installments <= 0 THEN 1 ELSE 0 END), 0) AS count_invalid_payment_installments,
			COALESCE(SUM(CASE WHEN payment_type <> 'credit_card' AND payment_installments > 1 THEN 1 ELSE 0 END), 0) AS count_incompatible_installments_and_type,
			COALESCE(SUM(CASE WHEN payment_value IS NULL OR payment_value <= 0 THEN 1 ELSE 0 END), 0) AS count_invalid_payment_value
			INTO #dq_checks_order_payments
		FROM #staging_olist_order_payments_dataset;

		-- Load data quality results into dq log table
		INSERT INTO etl.dq_log
		(
			batch_id,
			step_id,
			check_name,
			check_severity,
			check_time,
			records_checked,
			records_failed,
			dq_status,
			dq_description
		)
		SELECT @batch_id, @step_id, v.check_name, v.check_severity, SYSDATETIME(), op.records_checked, v.records_failed,
		CASE WHEN v.records_failed = 0 THEN 'Successful' WHEN v.records_failed > 0 AND v.check_severity = 'Critical' THEN 'Failed' ELSE 'Warning' END,
		CASE WHEN v.records_failed = 0 THEN 'Data quality check passed.' ELSE v.fail_message END FROM #dq_checks_order_payments op CROSS APPLY(
		VALUES
		('Invalid order_id', 'Critical', count_invalid_order_id, 'NULL, or empty space or string, or unwanted spaces detected in field order_id.'),
		('Invalid payment_sequential', 'Critical', count_invalid_payment_sequential, 'NULL, or value less than equal to zero detected in field payment_sequential.'),
		('Invalid Minimum payment_sequential', 'Minor', count_invalid_minimum_payment_sequential, 'Minimum value of payment_sequential within a number of orders deviates from the expected value of 1.'),
		('Disrupted Sequence in payment_sequential', 'Minor', count_disrupted_payment_sequential, 'Disruption in sequence of payment_sequential within a number of orders detected.'),
		('Primary Key Duplicates', 'Critical', count_primary_key_duplicates, 'Duplicates detected in Primary Key pk_order_payments.'),
		('Invalid payment_type', 'Minor', count_invalid_payment_type, 'NULL, or empty space or string, or unwanted spaces detected in field payment_type.'),
		('Newly Added payment_type', 'Info', newly_added_payment_type, 'New value detected in field payment_type.'),
		('Invalid payment_installments', 'Minor', count_invalid_payment_installments, 'NULL, or value less than equal to zero detected in field payment_installments.'),
		('Incompatible payment_type & payment_installments', 'Minor', count_incompatible_installments_and_type, 'Incompatible payment_installments detected in non credit card payment types.'),
		('Invalid payment_value', 'Minor', count_invalid_payment_value, 'NULL, or value less than equal to zero detected in field payment_value.')
		) v(check_name, check_severity, records_failed, fail_message);

		-- Abort transactions If at least a critical data quality rule is violated
		IF EXISTS(SELECT 1 FROM etl.dq_log WHERE batch_id = @batch_id AND step_id = @step_id AND check_severity = 'Critical' AND dq_status = 'Failed')
		THROW 50010, 'Critical data quality rule violated. Check etl.dq_log for specificity.', 10;

		-- Wrap target-table related transactions in a TRAN block to enable ROLLBACK on error
		BEGIN TRAN
		-- Delete existing data in target table
		TRUNCATE TABLE silver.olist_order_payments_dataset

		-- Load newly retrieved data into target table
		INSERT INTO silver.olist_order_payments_dataset
		(
			order_id,
			payment_sequential,
			payment_type,
			payment_installments,
			payment_value,
			dwh_batch_id
		)
		SELECT
			order_id,
			payment_sequential,
			payment_type,
			payment_installments,
			payment_value,
			@batch_id
		FROM #staging_olist_order_payments_dataset;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT;

		-- Perform row-count reconciliation check
		IF @rows_extracted <> @rows_inserted THROW 50011, 'Row-count reconciliation check failed', 11;
		COMMIT TRAN;

		-- Mark step as successful & map values to variables if row-count reconcilation check passes
		SET @step_load_status = 'Successful';
		SET @step_end_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @step_end_time);
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + @rows_inserted;

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET
				step_end_time = @step_end_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted
			WHERE batch_id = @batch_id AND step_id = @step_id;

		-- Drop staging table
		DROP TABLE IF EXISTS #staging_olist_order_payments_dataset;

		-- Drop data quality check table
		DROP TABLE IF EXISTS #dq_checks_order_payments;

		-- ==================================================
		-- STEP 5: Load silver.olist_order_reviews_dataset
		-- ==================================================

		-- Map values to variables before transactions
		SET @step_start_time = SYSDATETIME();
		SET @step_name = 'load_olist_order_reviews_dataset';
		SET @load_type = 'Full Load: Truncate & Insert';
		SET @source_object = 'bronze.olist_order_reviews_dataset';
		SET @target_object = 'olist_order_reviews_dataset';
		SET @step_load_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;

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
			rows_inserted
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
			@rows_inserted
		);

		-- Retrieve recently generated step id
		SET @step_id = SCOPE_IDENTITY();

		-- Load transformed data into a staging table
		SELECT
			review_id,
			order_id,
			review_score,
			CASE	
				WHEN review_comment_title IS NULL OR review_comment_title = '' THEN 'N/A'
				ELSE TRIM(review_comment_title)
			END AS review_comment_title,
			CASE	
				WHEN review_comment_message IS NULL OR review_comment_message = '' THEN 'N/A'
				ELSE TRIM(review_comment_message)
			END AS review_comment_message,
			review_creation_date,
			review_answer_timestamp
			INTO #staging_olist_order_reviews_dataset
		FROM bronze.olist_order_reviews_dataset
		WHERE dwh_is_deleted = 0;

		-- Extract total number of records loaded into staging table
		SELECT @rows_extracted = @@ROWCOUNT;

		-- Perform data quality checks on staging table, and load results into a temporary data quality check table
		SELECT
			COUNT(*) AS records_checked,
			COALESCE(SUM(CASE WHEN review_id IS NULL OR review_id = '' OR DATALENGTH(review_id) <> DATALENGTH(TRIM(review_id)) 
			THEN 1 ELSE 0 END), 0) AS count_invalid_review_id,
			COALESCE(SUM(CASE WHEN order_id IS NULL OR order_id = '' 
			OR DATALENGTH(order_id) <> DATALENGTH(TRIM(order_id)) THEN 1 ELSE 0 END), 0) AS count_invalid_order_id,
			(SELECT COUNT(*) FROM (SELECT 1 AS num FROM #staging_olist_order_reviews_dataset GROUP BY review_id, order_id 
			HAVING COUNT(*) > 1) SUB) AS count_primary_duplicates,
			COALESCE(SUM(CASE WHEN review_score IS NULL OR NOT (review_score >= 1 AND review_score <= 5) THEN 1 ELSE 0 END), 0) AS invalid_review_score,
			COALESCE(SUM(CASE WHEN review_creation_date > SYSDATETIME() OR review_creation_date > review_answer_timestamp 
			THEN 1 ELSE 0 END), 0) AS invalid_review_creation_date,
			COALESCE(SUM(CASE WHEN review_answer_timestamp > SYSDATETIME() THEN 1 ELSE 0 END), 0) AS invalid_review_answer_timestamp
			INTO #dq_checks_order_reviews
		FROM #staging_olist_order_reviews_dataset

		-- Load data quality results into dq log table
		INSERT INTO etl.dq_log
		(
			batch_id,
			step_id,
			check_name,
			check_severity,
			check_time,
			records_checked,
			records_failed,
			dq_status,
			dq_description
		)
		SELECT @batch_id, @step_id, v.check_name, v.check_severity, SYSDATETIME(), ors.records_checked, v.records_failed,
		CASE WHEN v.records_failed = 0 THEN 'Successful' WHEN v.records_failed > 0 AND v.check_severity = 'Critical' THEN 'Failed' ELSE 'Warning' END,
		CASE WHEN v.records_failed = 0 THEN 'Data quality check passed.' ELSE v.fail_message END FROM #dq_checks_order_reviews ors CROSS APPLY(
		VALUES
		('Invalid review_id', 'Critical', count_invalid_review_id, 'NULL, or empty space or string, or unwanted spaces detected in field review_id.'),
		('Invalid order_id', 'Critical', count_invalid_order_id, 'NULL, or empty space or string detected in field order_id.'),
		('Primary Key Duplicates', 'Critical', count_primary_duplicates, 'Duplicates detected in Primary Key pk_order_reviews.'),
		('Invalid review_score', 'Minor', invalid_review_score, 'NULL, or value outside the range of 1-5 in field review_score.'),
		('Invalid review_creation_date', 'Minor', invalid_review_creation_date, 'Future date, or review_creation_date greater than review_answer_timestamp detected.'),
		('Invalid review_answer_timestamp', 'Minor', invalid_review_answer_timestamp, 'Future date detected in field review_answer_timestamp.')
		) v(check_name, check_severity, records_failed, fail_message);

		-- Abort transactions If at least a critical data quality rule is violated
		IF EXISTS(SELECT 1 FROM etl.dq_log WHERE batch_id = @batch_id AND step_id = @step_id AND check_severity = 'Critical' AND dq_status = 'Failed')
		THROW 50012, 'Critical data quality rule violated. Check etl.dq_log for specificity.', 12;

		-- Wrap target-table related transactions in a TRAN block to enable ROLLBACK on error
		BEGIN TRAN
		-- Delete existing data in target table
		TRUNCATE TABLE silver.olist_order_reviews_dataset

		-- Load newly retrieved data into target table
		INSERT INTO silver.olist_order_reviews_dataset
		(
			review_id,
			order_id,
			review_score,
			review_comment_title,
			review_comment_message,
			review_creation_date,
			review_answer_timestamp,
			dwh_batch_id
		)
		SELECT
			review_id,
			order_id,
			review_score,
			review_comment_title,
			review_comment_message,
			review_creation_date,
			review_answer_timestamp,
			@batch_id
		FROM #staging_olist_order_reviews_dataset;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT;

		-- Perform row-count reconciliation check
		IF @rows_extracted <> @rows_inserted THROW 50013, 'Row-count reconciliation check failed', 13;
		COMMIT TRAN;

		-- Mark step as successful & map values to variables if row-count reconcilation check passes
		SET @step_load_status = 'Successful';
		SET @step_end_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @step_end_time);
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + @rows_inserted;

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET
				step_end_time = @step_end_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted
			WHERE batch_id = @batch_id AND step_id = @step_id;

		-- Drop staging table
		DROP TABLE IF EXISTS #staging_olist_order_reviews_dataset;

		-- Drop data quality check table
		DROP TABLE IF EXISTS #dq_checks_order_reviews;

		-- ==================================================
		-- STEP 6: Load silver.olist_orders_dataset
		-- ==================================================

		-- Map values to variables before transactions
		SET @step_start_time = SYSDATETIME();
		SET @step_name = 'load_olist_orders_dataset';
		SET @load_type = 'Full Load: Truncate & Insert';
		SET @source_object = 'bronze.olist_orders_dataset';
		SET @target_object = 'olist_orders_dataset';
		SET @step_load_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;

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
			rows_inserted
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
			@rows_inserted
		);

		-- Retrieve recently generated step id
		SET @step_id = SCOPE_IDENTITY();

		-- Load transformed data into a staging table
		SELECT
			order_id,
			customer_id,
			order_status,
			order_purchase_timestamp,
			order_approved_at,
			order_delivered_carrier_date,
			order_delivered_customer_date,
			order_estimated_delivery_date
			INTO #staging_olist_orders_dataset
		FROM bronze.olist_orders_dataset
		WHERE dwh_is_deleted = 0;

		-- Extract total number of records loaded into staging table
		SET @rows_extracted = @@ROWCOUNT;

		-- Perform data quality checks on staging table, and load results into a temporary data quality check table
		SELECT
			COUNT(*) AS records_checked,
			COALESCE(SUM(CASE WHEN order_id IS NULL OR order_id = '' 
			OR DATALENGTH(order_id) <> DATALENGTH(TRIM(order_id)) THEN 1 ELSE 0 END), 0) AS count_invalid_order_id,
			(SELECT COUNT(*) FROM (SELECT 1 AS num FROM #staging_olist_orders_dataset GROUP BY order_id HAVING COUNT(*) > 1) SUB) AS count_primary_key_duplicates,
			COALESCE(SUM(CASE WHEN customer_id IS NULL OR customer_id = '' 
			OR DATALENGTH(customer_id) <> DATALENGTH(TRIM(customer_id)) THEN 1 ELSE 0 END), 0) AS count_invalid_customer_id,
			COALESCE(SUM(CASE WHEN order_status IS NULL OR order_status = '' 
			OR DATALENGTH(order_status) <> DATALENGTH(TRIM(order_status)) THEN 1 ELSE 0 END), 0) AS count_invalid_order_status,
			COALESCE(SUM(CASE WHEN (order_status IS NOT NULL AND order_status <> '') AND order_status NOT IN ('approved', 'delivered', 'created', 'invoiced',
			'processing', 'unavailable', 'canceled', 'shipped') THEN 1 ELSE 0 END), 0) AS count_newly_added_order_status,
			COALESCE(SUM(CASE WHEN order_purchase_timestamp > SYSDATETIME() THEN 1 ELSE 0 END), 0) AS count_invalid_order_purchase_timestamp,
			COALESCE(SUM(CASE WHEN order_approved_at > SYSDATETIME() THEN 1 ELSE 0 END), 0) AS count_invalid_order_approved_at,
			COALESCE(SUM(CASE WHEN order_delivered_carrier_date > SYSDATETIME() THEN 1 ELSE 0 END), 0) AS count_invalid_order_delivered_carrier_date,
			COALESCE(SUM(CASE WHEN order_delivered_customer_date > SYSDATETIME() THEN 1 ELSE 0 END), 0) AS count_invalid_order_delivered_customer_date,
			COALESCE(SUM(CASE WHEN order_estimated_delivery_date > SYSDATETIME() THEN 1 ELSE 0 END), 0) AS count_invalid_order_estimated_delivery_date
			INTO #dq_checks_orders
		FROM #staging_olist_orders_dataset

		-- Load data quality results into dq log table
		INSERT INTO etl.dq_log
		(
			batch_id,
			step_id,
			check_name,
			check_severity,
			check_time,
			records_checked,
			records_failed,
			dq_status,
			dq_description
		)
		SELECT @batch_id, @step_id, v.check_name, v.check_severity, SYSDATETIME(), o.records_checked, v.records_failed,
		CASE WHEN v.records_failed = 0 THEN 'Successful' WHEN v.records_failed > 0 AND v.check_severity = 'Critical' THEN 'Failed' ELSE 'Warning' END,
		CASE WHEN v.records_failed = 0 THEN 'Data quality check passed.' ELSE v.fail_message END FROM #dq_checks_orders o CROSS APPLY(
		VALUES
		('Invalid order_id', 'Critical', count_invalid_order_id, 'NULL, or empty space or string, or unwanted spaces detected in field order_id.'),
		('Primary Key Duplicates', 'Critical', count_primary_key_duplicates, 'Duplicates detected in Primary Key pk_orders.'),
		('Invalid customer_id', 'Minor', count_invalid_customer_id, 'NULL, or empty space or string, or unwanted spaces detected in field customer_id.'),
		('Invalid order_status', 'Minor', count_invalid_order_status, 'NULL, or empty space or string, or unwanted spaces detected in field order_status.'),
		('Newly Added order_status', 'Info', count_newly_added_order_status, 'New value detected in field order_status.'),
		('Invalid order_purchase_timestamp', 'Minor', count_invalid_order_purchase_timestamp, 'Future date detected in field order_purchase_timestamp.'),
		('Invalid order_approved_at', 'Minor', count_invalid_order_approved_at, 'Future date detected in field order_approved_at.'),
		('Invalid order_delivered_carrier_date', 'Minor', count_invalid_order_delivered_carrier_date, 'Future date detected in field order_delivered_carrier_date.'),
		('Invalid order_delivered_customer_date', 'Minor', count_invalid_order_delivered_customer_date, 'Future date detected in field order_delivered_customer_date.'),
		('Invalid order_estimated_delivery_date', 'Minor', count_invalid_order_estimated_delivery_date, 'Future date detected in field order_estimated_delivery_date.')
		) v(check_name, check_severity, records_failed, fail_message);

		-- Abort transactions If at least a critical data quality rule is violated
		IF EXISTS(SELECT 1 FROM etl.dq_log WHERE batch_id = @batch_id AND step_id = @step_id AND check_severity = 'Critical' AND dq_status = 'Failed')
		THROW 50014, 'Critical data quality rule violated. Check etl.dq_log for specificity.', 14;

		-- Wrap target-table related transactions in a TRAN block to enable ROLLBACK on error
		BEGIN TRAN
		-- Delete existing data in target table
		TRUNCATE TABLE silver.olist_orders_dataset;

		-- Load newly retrieved data into target table
		INSERT INTO silver.olist_orders_dataset
		(
			order_id,
			customer_id,
			order_status,
			order_purchase_timestamp,
			order_approved_at,
			order_delivered_carrier_date,
			order_delivered_customer_date,
			order_estimated_delivery_date,
			dwh_batch_id
		)
		SELECT
			order_id,
			customer_id,
			order_status,
			order_purchase_timestamp,
			order_approved_at,
			order_delivered_carrier_date,
			order_delivered_customer_date,
			order_estimated_delivery_date,
			@batch_id
		FROM #staging_olist_orders_dataset;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT;

		-- Perform row-count reconciliation check
		IF @rows_extracted <> @rows_inserted THROW 50015, 'Row-count reconciliation check failed', 15;
		COMMIT TRAN;

		-- Mark step as successful & map values to variables if row-count reconcilation check passes
		SET @step_load_status = 'Successful';
		SET @step_end_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @step_end_time);
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + @rows_inserted;

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET
				step_end_time = @step_end_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted
			WHERE batch_id = @batch_id AND step_id = @step_id;

		-- Drop staging table
		DROP TABLE IF EXISTS #staging_olist_orders_dataset;

		-- Drop data quality check table
		DROP TABLE IF EXISTS #dq_checks_orders;

		-- ==============================================================
		-- STEP 7: Load silver.olist_product_category_name_translation
		-- ==============================================================

		-- Map values to variables before transactions
		SET @step_start_time = SYSDATETIME();
		SET @step_name = 'load_olist_product_category_name_translation';
		SET @load_type = 'Full Load: Truncate & Insert';
		SET @source_object = 'bronze.olist_product_category_name_translation';
		SET @target_object = 'olist_product_category_name_translation';
		SET @step_load_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;

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
			rows_inserted
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
			@rows_inserted
		);

		-- Retrieve recently generated step id
		SET @step_id = SCOPE_IDENTITY();

		-- Load transformed data into a staging table
		SELECT
			product_category_name,
			product_category_name_english
			INTO #staging_olist_product_category_name_translation
		FROM bronze.olist_product_category_name_translation
		WHERE dwh_is_deleted = 0;

		-- Extract total number of records loaded into staging table
		SET @rows_extracted = @@ROWCOUNT;

		-- Perform data quality checks on staging table, and load results into a temporary data quality check table
		SELECT
			COUNT(*) AS records_checked,
			COALESCE(SUM(CASE WHEN product_category_name IS NULL OR product_category_name = '' 
			OR DATALENGTH(product_category_name) <> DATALENGTH(TRIM(product_category_name)) THEN 1 ELSE 0 END), 0) AS count_invalid_product_category_name,
			COALESCE(SUM(CASE WHEN product_category_name_english IS NULL OR product_category_name_english = '' 
			OR DATALENGTH(product_category_name_english) <> DATALENGTH(TRIM(product_category_name_english)) 
			THEN 1 ELSE 0 END), 0) AS count_invalid_product_category_name_english,
			(SELECT COUNT(*) FROM (SELECT 1 num FROM #staging_olist_product_category_name_translation 
			GROUP BY product_category_name HAVING COUNT(*) > 1) SUB) AS count_primary_key_duplicates,
			(SELECT COUNT(*) FROM (SELECT 1 num FROM #staging_olist_product_category_name_translation 
			GROUP BY product_category_name_english HAVING COUNT(*) > 1) SUB) AS count_duplicate_product_category_name_english
			INTO #dq_checks_product_category
		FROM #staging_olist_product_category_name_translation;

		-- Load data quality results into dq log table
		INSERT INTO etl.dq_log
		(
			batch_id,
			step_id,
			check_name,
			check_severity,
			check_time,
			records_checked,
			records_failed,
			dq_status,
			dq_description
		)
		SELECT @batch_id, @step_id, v.check_name, v.check_severity, SYSDATETIME(), pc.records_checked, v.records_failed,
		CASE WHEN v.records_failed = 0 THEN 'Successful' WHEN v.records_failed > 0 AND v.check_severity = 'Critical' THEN 'Failed' ELSE 'Warning' END,
		CASE WHEN v.records_failed = 0 THEN 'Data quality check passed.' ELSE v.fail_message END FROM #dq_checks_product_category pc CROSS APPLY(
		VALUES
		('Invalid product_category_name', 'Critical', count_invalid_product_category_name, 'NULL, or empty space or string, or unwanted spaces detected in field product_category_name.'),
		('Invalid product_category_name_english', 'Minor', count_invalid_product_category_name_english, 'NULL, or empty space or string, or unwanted spaces detected in field product_category_name_english.'),
		('Primary Key Duplicates', 'Critical', count_primary_key_duplicates, 'Duplicates detected in Primary Key pk_product_category_name_translation.'),
		('Duplicates in product_category_name_english', 'Minor', count_duplicate_product_category_name_english, 'Duplicates detected in field product_category_name_english.')
		) v(check_name, check_severity, records_failed, fail_message);

		-- Abort transactions If at least a critical data quality rule is violated
		IF EXISTS(SELECT 1 FROM etl.dq_log WHERE batch_id = @batch_id AND step_id = @step_id AND check_severity = 'Critical' AND dq_status = 'Failed')
		THROW 50016, 'Critical data quality rule violated. Check etl.dq_log for specificity.', 16;

		-- Wrap target-table related transactions in a TRAN block to enable ROLLBACK on error
		BEGIN TRAN
		-- Delete existing data in target table
		TRUNCATE TABLE silver.olist_product_category_name_translation;

		-- Load newly retrieved data into target table
		INSERT INTO silver.olist_product_category_name_translation
		(
			product_category_name,
			product_category_name_english,
			dwh_batch_id
		)
		SELECT
			product_category_name,
			product_category_name_english,
			@batch_id
		FROM #staging_olist_product_category_name_translation;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT;

		-- Perform row-count reconciliation check
		IF @rows_extracted <> @rows_inserted THROW 50017, 'Row-count reconciliation check failed', 17;
		COMMIT TRAN;

		-- Mark step as successful & map values to variables if row-count reconcilation check passes
		SET @step_load_status = 'Successful';
		SET @step_end_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @step_end_time);
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + @rows_inserted;

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET
				step_end_time = @step_end_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted
			WHERE batch_id = @batch_id AND step_id = @step_id;

		-- Drop staging table
		DROP TABLE IF EXISTS #staging_olist_product_category_name_translation;

		-- Drop data quality check table
		DROP TABLE IF EXISTS #dq_checks_product_category;

		-- ==============================================================
		-- STEP 8: Load silver.olist_products_dataset
		-- ==============================================================

		-- Map values to variables before transactions
		SET @step_start_time = SYSDATETIME();
		SET @step_name = 'load_olist_products_dataset';
		SET @load_type = 'Full Load: Truncate & Insert';
		SET @source_object = 'bronze.olist_products_dataset';
		SET @target_object = 'olist_products_dataset';
		SET @step_load_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;

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
			rows_inserted
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
			@rows_inserted
		);

		-- Retrieve recently generated step id
		SET @step_id = SCOPE_IDENTITY();

		-- Load transformed data into a staging table
		SELECT
			pd.product_id,
			CASE
				WHEN pd.product_category_name IS NULL THEN 'N/A'
				ELSE pd.product_category_name
			END AS product_category_name,
			CASE
				WHEN pc.product_category_name_english IS NULL THEN 'N/A'
				ELSE pc.product_category_name_english
			END AS product_category_name_english,
			pd.product_name_length,
			pd.product_description_length,
			pd.product_photos_qty,
			pd.product_weight_g,
			pd.product_length_cm,
			pd.product_height_cm,
			pd.product_width_cm
			INTO #staging_olist_products_dataset
		FROM bronze.olist_products_dataset pd
		LEFT JOIN bronze.olist_product_category_name_translation pc
		ON pd.product_category_name = pc.product_category_name AND pc.dwh_is_deleted = 0
		WHERE pd.dwh_is_deleted = 0;

		-- Extract total number of records loaded into staging table
		SET @rows_extracted = @@ROWCOUNT;

		-- Perform data quality checks on staging table, and load results into a temporary data quality check table
		SELECT
			COUNT(*) AS records_checked,
			COALESCE(SUM(CASE WHEN product_id IS NULL OR product_id = '' OR DATALENGTH(product_id) <> DATALENGTH(TRIM(product_id)) 
			THEN 1 ELSE 0 END), 0) AS count_invalid_product_id,
			(SELECT COUNT(*) FROM (SELECT 1 AS num FROM #staging_olist_products_dataset GROUP BY product_id HAVING COUNT(*) > 1) SUB) AS count_primary_key_duplicates,
			COALESCE(SUM(CASE WHEN product_category_name = '' OR DATALENGTH(product_category_name) <> DATALENGTH(TRIM(product_category_name)) 
			THEN 1 ELSE 0 END), 0) AS count_invalid_product_category_name,
			COALESCE(SUM(CASE WHEN product_category_name_english = '' OR DATALENGTH(product_category_name_english) <> DATALENGTH(TRIM(product_category_name_english)) 
			THEN 1 ELSE 0 END), 0) AS count_invalid_product_category_name_english,
			COALESCE(SUM(CASE WHEN product_name_length <= 0 THEN 1 ELSE 0 END), 0) AS count_invalid_product_name_length,
			COALESCE(SUM(CASE WHEN product_description_length <= 0 THEN 1 ELSE 0 END), 0) AS count_invalid_product_description_length,
			COALESCE(SUM(CASE WHEN product_photos_qty < 0 THEN 1 ELSE 0 END), 0) AS count_invalid_product_photos_qty,
			COALESCE(SUM(CASE WHEN product_weight_g < 0 THEN 1 ELSE 0 END), 0) AS count_invalid_product_weight_g,
			COALESCE(SUM(CASE WHEN product_length_cm < 0 THEN 1 ELSE 0 END), 0) AS count_invalid_product_length_cm,
			COALESCE(SUM(CASE WHEN product_height_cm < 0 THEN 1 ELSE 0 END), 0) AS count_invalid_product_height_cm,
			COALESCE(SUM(CASE WHEN product_width_cm < 0 THEN 1 ELSE 0 END), 0) AS count_invalid_product_width_cm
			INTO #dq_checks_products
		FROM #staging_olist_products_dataset;

		-- Load data quality results into dq log table
		INSERT INTO etl.dq_log
		(
			batch_id,
			step_id,
			check_name,
			check_severity,
			check_time,
			records_checked,
			records_failed,
			dq_status,
			dq_description
		)
		SELECT @batch_id, @step_id, v.check_name, v.check_severity, SYSDATETIME(), p.records_checked, v.records_failed,
		CASE WHEN v.records_failed = 0 THEN 'Successful' WHEN v.records_failed > 0 AND v.check_severity = 'Critical' THEN 'Failed' ELSE 'Warning' END,
		CASE WHEN v.records_failed = 0 THEN 'Data quality check passed.' ELSE v.fail_message END FROM #dq_checks_products p CROSS APPLY(
		VALUES
		('Invalid product_id', 'Critical', count_invalid_product_id, 'NULL, or empty space or string, or unwanted spaces detected in field product_id.'),
		('Primary Key Duplicates', 'Critical', count_primary_key_duplicates, 'Duplicates detected in primary key pk_products.'),
		('Invalid product_category_name', 'Minor', count_invalid_product_category_name, 'Empty space or string, or unwanted spaces detected in field product_category_name.'),
		('Invalid product_category_name_english', 'Minor', count_invalid_product_category_name_english, 'Empty space or string, or unwanted spaces detected in field product_category_name_english.'),
		('Invalid product_name_length', 'Minor', count_invalid_product_name_length, 'Value less than equal to zero detected in field product_name_length.'),
		('Invalid product_description_length', 'Minor', count_invalid_product_description_length, 'Value less than equal to zero detected in field product_description_length.'),
		('Invalid product_photos_qty', 'Minor', count_invalid_product_photos_qty, 'Value less than zero detected in field product_photos_qty.'),
		('Invalid product_weight_g', 'Minor', count_invalid_product_weight_g, 'Value less than zero detected in field product_weight_g.'),
		('Invalid product_length_cm', 'Minor', count_invalid_product_length_cm, 'Value less than zero detected in field product_length_cm.'),
		('Invalid product_height_cm', 'Minor', count_invalid_product_height_cm, 'Value less than zero detected in field product_height_cm.'),
		('Invalid product_width_cm', 'Minor', count_invalid_product_width_cm, 'Value less than zero detected in field product_width_cm.')
		) v(check_name, check_severity, records_failed, fail_message);

		-- Abort transactions If at least a critical data quality rule is violated
		IF EXISTS(SELECT 1 FROM etl.dq_log WHERE batch_id = @batch_id AND step_id = @step_id AND check_severity = 'Critical' AND dq_status = 'Failed')
		THROW 50018, 'Critical data quality rule violated. Check etl.dq_log for specificity.', 18;

		-- Wrap target-table related transactions in a TRAN block to enable ROLLBACK on error
		BEGIN TRAN
		-- Delete existing data in target table
		TRUNCATE TABLE silver.olist_products_dataset;

		-- Load newly retrieved data into target table
		INSERT INTO silver.olist_products_dataset
		(
			product_id,
			product_category_name,
			product_category_name_english,
			product_name_length,
			product_description_length,
			product_photos_qty,
			product_weight_g,
			product_length_cm,
			product_height_cm,
			product_width_cm,
			dwh_batch_id
		)
		SELECT
			product_id,
			product_category_name,
			product_category_name_english,
			product_name_length,
			product_description_length,
			product_photos_qty,
			product_weight_g,
			product_length_cm,
			product_height_cm,
			product_width_cm,
			@batch_id
		FROM #staging_olist_products_dataset;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT;

		-- Perform row-count reconciliation check
		IF @rows_extracted <> @rows_inserted THROW 50019, 'Row-count reconciliation check failed', 19;
		COMMIT TRAN;

		-- Mark step as successful & map values to variables if row-count reconcilation check passes
		SET @step_load_status = 'Successful';
		SET @step_end_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @step_end_time);
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + @rows_inserted;

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET
				step_end_time = @step_end_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted
			WHERE batch_id = @batch_id AND step_id = @step_id;

		-- Drop staging table
		DROP TABLE IF EXISTS #staging_olist_products_dataset;

		-- Drop data quality check table
		DROP TABLE IF EXISTS #dq_checks_products;

		-- ==============================================================
		-- STEP 9: Load silver.olist_sellers_dataset
		-- ==============================================================

		-- Map values to variables before transactions
		SET @step_start_time = SYSDATETIME();
		SET @step_name = 'load_olist_sellers_dataset';
		SET @load_type = 'Full Load: Truncate & Insert';
		SET @source_object = 'bronze.olist_sellers_dataset';
		SET @target_object = 'olist_sellers_dataset';
		SET @step_load_status = 'Running';
		SET @rows_extracted = 0;
		SET @rows_inserted = 0;

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
			rows_inserted
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
			@rows_inserted
		);

		-- Retrieve recently generated step id
		SET @step_id = SCOPE_IDENTITY();

		-- Load transformed data into a staging table
		SELECT
			seller_id,
			seller_zip_code_prefix,
			CASE	
				WHEN seller_city = '04482255' THEN 'rio de janeiro'
				ELSE seller_city
			END AS seller_city,
			seller_state
			INTO #staging_olist_sellers_dataset
		FROM bronze.olist_sellers_dataset
		WHERE dwh_is_deleted = 0;

		SET @rows_extracted = @@ROWCOUNT;

		-- Perform data quality checks on staging table, and load results into a temporary data quality check table
		SELECT
			COUNT(*) AS records_checked,
			COALESCE(SUM(CASE WHEN seller_id IS NULL OR seller_id = '' OR DATALENGTH(seller_id) <> DATALENGTH(TRIM(seller_id)) 
			THEN 1 ELSE 0 END), 0) AS count_invalid_seller_id,
			(SELECT COUNT(*) FROM (SELECT 1 AS num FROM #staging_olist_sellers_dataset GROUP BY seller_id HAVING COUNT(*) > 1) SUB) AS count_primary_key_duplicates,
			COALESCE(SUM(CASE WHEN seller_zip_code_prefix IS NULL OR seller_zip_code_prefix = '' OR seller_zip_code_prefix LIKE ('-%') 
			THEN 1 ELSE 0 END), 0) AS count_invalid_seller_zip_code_prefix,
			COALESCE(SUM(CASE WHEN seller_city IS NULL OR seller_city = '' OR DATALENGTH(seller_city) <> DATALENGTH(TRIM(seller_city)) 
			THEN 1 ELSE 0 END), 0) AS count_invalid_seller_city,
			COALESCE(SUM(CASE WHEN seller_state IS NULL OR seller_state = '' OR DATALENGTH(seller_state) <> DATALENGTH(TRIM(seller_state)) 
			THEN 1 ELSE 0 END), 0) AS count_invalid_seller_state
			INTO #dq_checks_sellers
		FROM #staging_olist_sellers_dataset;

		-- Load data quality results into dq log table
		INSERT INTO etl.dq_log
		(
			batch_id,
			step_id,
			check_name,
			check_severity,
			check_time,
			records_checked,
			records_failed,
			dq_status,
			dq_description
		)
		SELECT @batch_id, @step_id, v.check_name, v.check_severity, SYSDATETIME(), s.records_checked, v.records_failed,
		CASE WHEN v.records_failed = 0 THEN 'Successful' WHEN v.records_failed > 0 AND v.check_severity = 'Critical' THEN 'Failed' ELSE 'Warning' END,
		CASE WHEN v.records_failed = 0 THEN 'Data quality check passed.' ELSE v.fail_message END FROM #dq_checks_sellers s CROSS APPLY(
		VALUES
		('Invalid seller_id', 'Critical', count_invalid_seller_id, 'NULL, or empty space or string, or unwanted spaces detected in field seller_id.'),
		('Primary Key Duplicates', 'Critical', count_primary_key_duplicates, 'Duplicates deteceted in primary key pk_sellers.'),
		('Invalid seller_zip_code_prefix', 'Minor', count_invalid_seller_zip_code_prefix, 'NULL, or empty space or string, or negative values detected in field seller_zip_code_prefix.'),
		('Invalid seller_city', 'Minor', count_invalid_seller_city, 'NULL, or empty space or string, or unwanted spaces detected in field seller_city.'),
		('Invalid seller_state', 'Minor', count_invalid_seller_state, 'NULL, or empty space or string, or unwanted spaces detected in field seller_state.')
		) v(check_name, check_severity, records_failed, fail_message);

		-- Abort transactions If at least a critical data quality rule is violated
		IF EXISTS(SELECT 1 FROM etl.dq_log WHERE batch_id = @batch_id AND step_id = @step_id AND check_severity = 'Critical' AND dq_status = 'Failed')
		THROW 50020, 'Critical data quality rule violated. Check etl.dq_log for specificity.', 20;

		-- Wrap target-table related transactions in a TRAN block to enable ROLLBACK on error
		BEGIN TRAN
		-- Delete existing data in target table
		TRUNCATE TABLE silver.olist_sellers_dataset;

		-- Load newly retrieved data into target table
		INSERT INTO silver.olist_sellers_dataset
		(
			seller_id,
			seller_zip_code_prefix,
			seller_city,
			seller_state,
			dwh_batch_id
		)
		SELECT
			seller_id,
			seller_zip_code_prefix,
			seller_city,
			seller_state,
			@batch_id
		FROM #staging_olist_sellers_dataset;

		-- Retrieve rows inserted
		SET @rows_inserted = @@ROWCOUNT;

		-- Perform row-count reconciliation check
		IF @rows_extracted <> @rows_inserted THROW 50021, 'Row-count reconciliation check failed', 21;
		COMMIT TRAN;

		-- Mark step as successful & map values to variables if row-count reconcilation check passes
		SET @step_load_status = 'Successful';
		SET @step_end_time = SYSDATETIME();
		SET @step_load_duration = DATEDIFF(second, @step_start_time, @step_end_time);
		SET @total_rows_processed = @total_rows_processed + @rows_extracted;
		SET @total_rows_loaded = @total_rows_loaded + @rows_inserted;

		-- Update log details at step-level on success
		UPDATE etl.step_log
			SET
				step_end_time = @step_end_time,
				step_load_duration_second = @step_load_duration,
				step_load_status = @step_load_status,
				rows_extracted = @rows_extracted,
				rows_inserted = @rows_inserted
			WHERE batch_id = @batch_id AND step_id = @step_id;

		-- Drop staging table
		DROP TABLE IF EXISTS #staging_olist_sellers_dataset;

		-- Drop data quality check table
		DROP TABLE IF EXISTS #dq_checks_sellers;

		-- =======================================================================================
		-- SECTION 4:  CLOSE BATCH ON SUCCESS
		-- =======================================================================================

		-- Load into batch log table if batch_id is self-generated
		IF @owns_batch = 1
		BEGIN
			-- Map values to variables
			SET @batch_load_status = 'Successful';
			SET @batch_end_time = SYSDATETIME();
			SET @batch_load_duration = DATEDIFF(second, @batch_start_time, @batch_end_time);
			SELECT @total_tables_loaded = COUNT(*) FROM etl.step_log WHERE batch_id = @batch_id AND step_load_status = 'Successful';

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
		END;
	END TRY


	BEGIN CATCH
		-- Capture error time
		SET @error_time = SYSDATETIME();

		-- Rollback transaction if still active
		IF XACT_STATE() <> 0 ROLLBACK TRAN;

		-- Load into log tables if batch_id is invalid
		IF @batch_rejected = 1
			BEGIN
				-- Map values to variables
				SET @batch_load_status = 'Failed';
				SET @batch_load_duration = DATEDIFF(second, @batch_start_time, @error_time);

				-- Load into batch log table
				INSERT INTO etl.batch_log
				(
					batch_name,
					batch_start_time,
					batch_end_time,
					batch_load_duration_second,
					batch_load_status,
					total_tables_loaded,
					total_rows_processed,
					total_rows_loaded
				)
				VALUES
				(
					@batch_name,
					@batch_start_time,
					@error_time,
					@batch_load_duration,
					@batch_load_status,
					@total_tables_loaded,
					@total_rows_processed,
					@total_rows_loaded
				);

				-- Capture recently generated batch_id
				SET @batch_id = SCOPE_IDENTITY();

				-- Load into step log table
				INSERT INTO etl.step_log
				(
					batch_id,
					layer,
					step_name,
					load_type,
					source_object,
					target_object,
					step_start_time,
					step_end_time,
					step_load_duration_second,
					step_load_status,
					rows_extracted,
					rows_inserted
				)
				VALUES
				(
					@batch_id,
					@layer,
					'N/A',
					'N/A',
					'N/A',
					'N/A',
					@batch_start_time,
					@error_time,
					@batch_load_duration,
					@batch_load_status,
					@rows_extracted,
					@rows_inserted
				);

				-- Capture recently generated step_id
				SET @step_id = SCOPE_IDENTITY();

				-- Load into error log table
				INSERT INTO etl.error_log
				(
					batch_id,
					step_id,
					error_time,
					error_description
				)
				VALUES
				(
					@batch_id,
					@step_id,
					@error_time,
					ERROR_MESSAGE()
				);
			END;

		-- Otherwise load into log tables if batch_id is valid
		ELSE
			BEGIN
				-- Map values to step-level variables on failure
				SET @step_load_status = 'Failed';
				SET @step_load_duration = DATEDIFF(second, @step_start_time, @error_time);
				SET @rows_inserted = 0;

				-- Update log details at step-level on failure
				UPDATE etl.step_log
					SET
						step_end_time = @error_time,
						step_load_duration_second = @step_load_duration,
						step_load_status = @step_load_status,
						rows_extracted = @rows_extracted,
						rows_inserted = @rows_inserted
					WHERE batch_id = @batch_id AND step_id = @step_id;

				-- Load into batch log table if batch_id is self-generated
				IF @owns_batch = 1
				BEGIN
					-- Map values to batch-level variables on failure
					SET @batch_load_status = 'Failed';
					SET @batch_load_duration = DATEDIFF(second, @batch_start_time, @error_time);
					SELECT @total_tables_loaded = COUNT(*) FROM etl.step_log WHERE batch_id = @batch_id AND step_load_status = 'Successful';
					SET @total_rows_processed = @total_rows_processed + @rows_extracted;

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
				END;

				-- Insert into error log
				INSERT INTO etl.error_log
				(
					batch_id,
					step_id,
					error_time,
					error_description
				)
				VALUES
				(
					@batch_id,
					@step_id,
					@error_time,
					ERROR_MESSAGE()
				);
			END;


		THROW;
	END CATCH;
END;
