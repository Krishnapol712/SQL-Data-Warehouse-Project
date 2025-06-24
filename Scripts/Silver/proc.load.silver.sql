CREATE OR ALTER PROCEDURE Silver.load_Silver AS
BEGIN
	--1
	PRINT 'Truncating table silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT 'Inserting Data into silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info ( 
			cst_id,
			cst_key,
			st_firstname,
			cst_LastName,
			cst_marital_status,
			cst_gndr,
			cst_create_date)
	SELECT
	cst_id,
	cst_key,
	trim(st_firstname) as cst_FirstName,
	trim(cst_lastname) as cst_LastName,
	CASE
		when UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		when UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		else 'n/a'
	end cst_marial_status,

	CASE
		when UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		when UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		else 'n/a'
	END cst_gndr,
	cst_create_date
	from (
		SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id order by cst_create_date DESC) AS Flag_Last
		from bronze.crm_cust_info
		where cst_id is not null
		)t where flag_last = 1



	--2

	PRINT 'Truncating table SILVER.CRM_PRD_INFO';
	TRUNCATE TABLE SILVER.CRM_PRD_INFO ;
	PRINT 'Inserting data in table SILVER.CRM_PRD_INFO';
	INSERT INTO SILVER.CRM_PRD_INFO (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as Cat_id,
	SUBSTRING(prd_key, 7, len(prd_key)) as Prd_key,
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	case
		when UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line))= 'S' THEN 'Other Sales'
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
	prd_start_dt,
	DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
	from bronze.crm_prd_info

	--3

	PRINT 'Truncating table silver.crm_Sales_Details';
	TRUNCATE TABLE silver.crm_Sales_Details;
	PRINT 'Inserting data in silver.crm_Sales_Details';
	INSERT INTO silver.crm_Sales_Details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_Price
	)
	SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE
			WHEN LEN(sls_order_dt) != 8 or sls_order_dt = 0 then NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,

		CASE
			WHEN LEN(sls_ship_dt) != 8 or sls_ship_dt = 0 then NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,

		CASE
			WHEN LEN(sls_due_dt) != 8 or sls_ship_dt = 0 then NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,

		CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price is NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price
		END AS sls_price
	FROM bronze.crm_sales_details

	--4


	PRINT '>> Truncating table Silver.erp_CUST_AZ12';
	TRUNCATE TABLE Silver.erp_CUST_AZ12;
	PRINT '>> Inserting data into Silver.erp_CUST_AZ12';
	INSERT INTO Silver.erp_CUST_AZ12 (CID,BDATE,GEN)

	SELECT

	CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
		ELSE CID
	END CID,

	CASE WHEN BDATE < '1924-01-01' OR BDATE > GETDATE()
		THEN NULL
	ELSE BDATE
	END AS BDATE,

	CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
	ELSE GEN
	END AS GEN

	FROM bronze.erp_CUST_AZ12

	-- 5
	PRINT 'Truncatinf table silver.erp_LOC_A101';
	Truncate table silver.erp_LOC_A101;
	PRINT 'Inserting into table silver.erp_LOC_A101'; 
	INSERT INTO silver.erp_LOC_A101 (CID, CNTRY)
	select 

	replace(cid, '-', '') as CID,

	CASE 
	WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
	WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
	when trim(cntry) = '' or cntry is null then 'n/a'
	ELSE TRIM(CNTRY)
	end as CNTRY

	from bronze.erp_loc_a101

	--6
	PRINT 'Truncating table silver.erp_px_cat_g1v2';
	TRUNCATE table silver.erp_px_cat_g1v2;
	PRINT ' Inserting into table silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)

	select
	id,
	cat,
	subcat,
	maintenance
	from bronze.erp_px_cat_g1v2

END
