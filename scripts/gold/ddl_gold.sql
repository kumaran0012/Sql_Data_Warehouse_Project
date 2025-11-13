/*
DDL SCRIPT: Create Gold Views

views performs Transformations and combine data from from the silver layer 
to produce a clean, enriched and business
 views can be queried directly for analytics and reporting

*/
-- DIMENSION CUSTOMERS
create view dim_customers as
SELECT row_number() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
        CASE
            WHEN ci.cst_gndr::text <> 'N/A'::text THEN ci.cst_gndr
            ELSE COALESCE(ca.gen, 'N/A'::character varying)
        END AS gender,
    ca.bdate AS birth_date,
    ci.cst_create_date AS create_date
   FROM silver.crm_cust_info ci
     LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key::text = ca.cid::text
     LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key::text = la.cid::text;
-- DIMENSION PRODUCTS
create view gold.dim_products as
select 
	row_number() over (order by pn.prd_start_dt,pn.prd_key) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as  product_number,
	pn.prd_nm  product_name,
	pn.cat_id as category_id,
	pc.cat as category_name,
	pc.subcat as subcategory,
	pc.maintenance,
	pn.prd_cost as product_cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc on pn.cat_id = pc.id 
where prd_end_dt is null;
F
/* FACTS SALES*/

create view gold.fact_sales as 
SELECT 
	sd.sls_ord_num as order_number, 
	dp.product_key,
	dc.customer_key,
	sd.sls_order_dt as order_date, 
	sd.sls_ship_dt as ship_date, 
	sd.sls_due_dt as due_date, 
	sd.sls_sales sales_amount, 
	sd.sls_quantity as quantity, 
	sd.sls_price as price
FROM silver.crm_sales_details sd
left join gold.dim_products dp
on 	sd.sls_prd_key = dp.product_number
left join gold.dim_customers dc
on sd.sls_cust_id = dc.customer_id;
