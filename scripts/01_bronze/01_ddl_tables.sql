/*
==========================================================================================
Script: 01_ddl_tables
Location: scripts/01_bronze/
Author: Otusanya Toyib
Created At: 2026-08-08
==========================================================================================
Script Purpose:
	This script checks the existence of previous bronze tables, drops them if found, and
	recreates them. The following bronze tables are created from the execution of this
	script:

		* olist_customers_dataset
		* olist_geolocation_dataset
		* olist_order_items_dataset
		* olist_order_payments_dataset
		* olist_order_reviews_dataset
		* olist_orders_dataset
		* olist_products_dataset
		* olist_sellers_dataset
		* olist_product_category_name_translation
==========================================================================================
*/
-- Connect to OlistDatabase
USE OlistDatabase;

-- Drop all bronze tables if found
IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'bronze')
	BEGIN
		PRINT('Existing bronze tables found — dropping...');
		DROP TABLE IF EXISTS bronze.olist_customers_dataset;
		DROP TABLE IF EXISTS bronze.olist_geolocation_dataset;
		DROP TABLE IF EXISTS bronze.olist_order_items_dataset;
		DROP TABLE IF EXISTS bronze.olist_order_payments_dataset;
		DROP TABLE IF EXISTS bronze.olist_order_reviews_dataset;
		DROP TABLE IF EXISTS bronze.olist_orders_dataset;
		DROP TABLE IF EXISTS bronze.olist_products_dataset;
		DROP TABLE IF EXISTS bronze.olist_sellers_dataset;
		DROP TABLE IF EXISTS bronze.olist_product_category_name_translation;
	END;

-- Create bronze table olist_customers_dataset
CREATE TABLE bronze.olist_customers_dataset
(
	customer_id NVARCHAR(50),
	customer_unique_id NVARCHAR(50),
	customer_zip_code_prefix CHAR(5),
	customer_city NVARCHAR(50),
	customer_state NVARCHAR(50),
	dwh_batch_id INT,
	dwh_load_timestamp DATETIME2(0) DEFAULT SYSDATETIME(),
	dwh_source_file NVARCHAR(250)
);
GO

-- Create bronze table olist_geolocation_dataset
CREATE TABLE bronze.olist_geolocation_dataset
(
	geolocation_zip_code_prefix CHAR(5),
	geolocation_lat FLOAT,
	geolocation_lng FLOAT,
	geolocation_city NVARCHAR(50),
	geolocation_state NVARCHAR(50),
	dwh_batch_id INT,
	dwh_load_timestamp DATETIME2(0) DEFAULT SYSDATETIME(),
	dwh_source_file NVARCHAR(250)
);
GO

-- Create bronze table olist_order_items_dataset
CREATE TABLE bronze.olist_order_items_dataset
(
	order_id NVARCHAR(50),
	order_item_id INT,
	product_id NVARCHAR(50),
	seller_id NVARCHAR(50),
	shipping_limit_date DATETIME2(0),
	price DECIMAL(10, 2),
	freight_value DECIMAL(10, 2),
	dwh_batch_id INT,
	dwh_load_timestamp DATETIME2(0) DEFAULT SYSDATETIME(),
	dwh_source_file NVARCHAR(250)
);
GO

-- Create bronze table olist_order_payments_dataset
CREATE TABLE bronze.olist_order_payments_dataset
(
	order_id NVARCHAR(50),
	payment_sequential INT,
	payment_type NVARCHAR(50),
	payment_installments INT,
	payment_value DECIMAL(10, 2),
	dwh_batch_id INT,
	dwh_load_timestamp DATETIME2(0) DEFAULT SYSDATETIME(),
	dwh_source_file NVARCHAR(250)
);
GO

-- Create bronze table olist_order_reviews_dataset
CREATE TABLE bronze.olist_order_reviews_dataset
(
	review_id NVARCHAR(50),
	order_id NVARCHAR(50),
	review_score INT,
	review_comment_title NVARCHAR(50),
	review_comment_message NVARCHAR(MAX),
	review_creation_date DATETIME2(0),
	review_answer_timestamp DATETIME2(0),
	dwh_batch_id INT,
	dwh_load_timestamp DATETIME2(0) DEFAULT SYSDATETIME(),
	dwh_source_file NVARCHAR(250)
);
GO

-- Create bronze table olist_orders_dataset
CREATE TABLE bronze.olist_orders_dataset
(
	order_id NVARCHAR(50),
	customer_id NVARCHAR(50),
	order_status NVARCHAR(50),
	order_purchase_timestamp DATETIME2(0),
	order_approved_at DATETIME2(0),
	order_delivered_carrier_date DATETIME2(0),
	order_delivered_customer_date DATETIME2(0),
	order_estimated_delivery_date DATETIME2(0),
	dwh_batch_id INT,
	dwh_load_timestamp DATETIME2(0) DEFAULT SYSDATETIME(),
	dwh_source_file NVARCHAR(250)
);
GO

-- Create bronze table olist_products_dataset
CREATE TABLE bronze.olist_products_dataset
(
	product_id NVARCHAR(50),
	product_category_name NVARCHAR(50),
	product_name_length INT,
	product_description_length INT,
	product_photos_qty INT,
	product_weight_g INT,
	product_length_cm INT,
	product_height_cm INT,
	product_width_cm INT,
	dwh_batch_id INT,
	dwh_load_timestamp DATETIME2(0) DEFAULT SYSDATETIME(),
	dwh_source_file NVARCHAR(250)
);
GO

-- Create bronze table olist_sellers_dataset
CREATE TABLE bronze.olist_sellers_dataset
(
	seller_id NVARCHAR(50),
	seller_zip_code_prefix CHAR(5),
	seller_city NVARCHAR(50),
	seller_state NVARCHAR(50),
	dwh_batch_id INT,
	dwh_load_timestamp DATETIME2(0) DEFAULT SYSDATETIME(),
	dwh_source_file NVARCHAR(250)
);
GO

-- Create bronze table olist_product_category_name_translation
CREATE TABLE bronze.olist_product_category_name_translation
(
	product_category_name NVARCHAR(50),
	product_category_name_english NVARCHAR(50),
	dwh_batch_id INT,
	dwh_load_timestamp DATETIME2(0) DEFAULT SYSDATETIME(),
	dwh_source_file NVARCHAR(250)
);
GO

-- Declare variable @total_tables
DECLARE @total_tables INT;

-- Map value to @total_tables
SELECT @total_tables = COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'bronze';

-- Reveal value mapped to variable @total_tables
PRINT('Total Number of Bronze Tables Created: ' + CAST(@total_tables AS NVARCHAR));
