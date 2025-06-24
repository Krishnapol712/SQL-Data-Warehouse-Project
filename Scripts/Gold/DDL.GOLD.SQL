/* Creates Views for the gold layer in the data warehouse. The gold layer is teh final virtual layer of the medallion architexture data
warehouse. Each view does transformations and combines the data that was refined in the silver layer.
The gold layer Represents the Final dimension and fact tables (star schema)*/

CREATE VIEW Gold.fact_Sales AS
SELECT
sd.sls_ord_num as Order_Number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as Order_Date,
sd.sls_ship_dt AS Shipping_Date,
sd.sls_due_dt AS Due_Date,
sd.sls_sales AS Sales_Amount,
sd.sls_quantity AS Quantity,
sd.sls_price as Price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id

CREATE VIEW Gold.Dim_Products AS
SELECT 
ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS Product_key,
pn.prd_id AS Poduct_id,
pn.prd_key AS Product_Number,
pn.prd_nm AS Product_Name,
pn.cat_id AS Category_ID,
pc.cat AS Category,
pc.subcat AS Sub_Category,
pc.maintenance AS Maintenance,
pn.prd_cost AS Product_Cost,
pn.prd_line AS Product_Line,
pn.prd_start_dt AS Product_Start_Date
from silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
WHERE prd_end_dt is null


CREATE VIEW GOLD.dim_Customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS Customer_Key,
	ci.cst_id AS Customer_id,
	ci.cst_key AS Customer_Number,
	ci.st_firstname AS First_name,
	la.cntry as Country,
	ci.cst_lastname AS Last_name,
	ci.cst_marital_status AS Marital_Status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		ELSE coalesce(ca.gen, 'N/A')
	END AS Gender,
	ca.bdate as Birthday,
	ci.cst_create_date as Create_Date
	
from silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la 
on ci.cst_key = la.cid 
