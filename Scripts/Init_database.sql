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

