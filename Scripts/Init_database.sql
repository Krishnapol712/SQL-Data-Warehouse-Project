/* Creating the Database and Schemas*/

Use Master

Create Database DataWarehouse

Use DataWarehouse

Create SCHEMA Bronze
go
  
Create SCHEMA Silver
go
  
Create SCHEMA Gold

/* creating the folders for erp and crm*/
use DataWarehouse
IF OBJECT ID ('Bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE Bronze.crm_cust_info;

CREATE TABLE Bronze.crm_cust_info (
cst_id INT,
cst_key	NVARCHAR(50),
st_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
);

CREATE TABLE Bronze.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
	);

CREATE TABLE Bronze.crm_Sales_Details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
	);

CREATE TABLE Bronze.erp_CUST_AZ12 (
	CID NVARCHAR(50),
	BDATE DATE,
	GEN NVARCHAR(50)
	);

CREATE TABLE Bronze.erp_LOC_A101 (
	CID NVARCHAR(50),
	CNTRY NVARCHAR(50)
	);

CREATE TABLE Bronze.erp_PX_CAT_G1V2 (
	ID NVARCHAR(50),
	CAT	NVARCHAR(50),
	SUBCAT NVARCHAR(50),
	MAINTENANCE NVARCHAR(50)
	);

/* Truncate and Bulk loading data into Bronze layer AND create stored procedure since this script is going to be used often*/

CREATE OR ALTER PROCEDURE bronze.load_Bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @BatchStartTime DATETIME, @BatchEndTime DATETIME;
SET @BatchStartTime = GETDATE();
	BEGIN TRY
		PRINT '===========================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '===========================================';

		PRINT '===========================================';
		PRINT 'LOADING CRM Tables';
		PRINT '===========================================';

		SET @start_time = GETDATE();
		PRINT ' Truncating bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT ' Inserting data into bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\College\projects\DATASCI\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(Second, @start_time, @end_time) as NVARCHAR)+ 'secs'

		SET @start_time = GETDATE();
		PRINT 'Truncating bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT ' Inserting data into bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\College\projects\DATASCI\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(Second, @start_time, @end_time) as NVARCHAR) + 'Secs'

		SET @Start_time = GETDATE();
		PRINT 'Truncating bronze.crm_Sales_Details'
		TRUNCATE TABLE bronze.crm_Sales_Details;

		
		PRINT ' Inserting data into bronze.crm_Sales_Details'
		BULK INSERT bronze.crm_Sales_Details
		FROM 'C:\College\projects\DATASCI\sql-data-warehouse-project\datasets\source_crm\Sales_Details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATOIN: ' + CAST( DATEDIFF(Second, @start_time, @end_time) as NVARCHAR) + 'SECS'

		PRINT '===========================================';
		PRINT 'LOADING ERP Tables';
		PRINT '===========================================';

		SET @Start_time = GETDATE();
		PRINT 'Truncating bronze.erp_CUST_AZ12'
		TRUNCATE TABLE bronze.erp_CUST_AZ12;

		PRINT ' Inserting data into bronze.erp_CUST_AZ12'
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\College\projects\DATASCI\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @End_Time = GETDATE();
		PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(Second, @Start_time, @End_Time) as NVARCHAR) + 'secs'

		SET @Start_time = GETDATE();
		PRINT 'Truncating bronze.erp_LOC_A101'
		TRUNCATE TABLE bronze.erp_LOC_A101;

		PRINT ' Inserting data into bronze.erp_LOC_A101'
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\College\projects\DATASCI\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @End_Time = GETDATE();
		PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(Second, @Start_time, @End_Time) as NVARCHAR) + 'secs'

		SET @Start_time = GETDATE();
		PRINT 'Truncating bronze.erp_PX_CAT_G1V2'
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

		PRINT ' Inserting data into bronze.erp_PX_CAT_G1V2'
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\College\projects\DATASCI\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @End_Time = GETDATE();
		PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(Second, @Start_time, @End_Time) as NVARCHAR) + 'secs'

		END TRY
		BEGIN CATCH
			PRINT '---------------------------'
			PRINT 'ERROR WHEN LOADING BRONZE LAYER'
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
			PRINT '---------------------------'
		END CATCH

SET @BatchEndTime = GETDATE();
PRINT 'TOTAL LOAD DURATION: ' + CAST(DATEDIFF(Second, @batchstarttime, @batchendtime) as NVARCHAR) + 'secs'

END

