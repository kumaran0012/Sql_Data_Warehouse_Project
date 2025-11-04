CREATE OR REPLACE FUNCTION bronze.load_bronze()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  DECLARE
  	start_BATCH TIMESTAMP;
      end_BATCH TIMESTAMP;
      start_time TIMESTAMP;
      end_time TIMESTAMP;
  BEGIN
  	start_BATCH := NOW();
      RAISE NOTICE 'LOADING BRONZE LAYER';
  
      -- crm_cust_info
      start_time := NOW();
      TRUNCATE TABLE bronze.crm_cust_info;
      RAISE NOTICE 'Truncated table: crm_cust_info';
      EXECUTE $cmd$
          COPY bronze.crm_cust_info
          FROM 'D:/downloads/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
          DELIMITER ',' CSV HEADER;
      $cmd$;
      RAISE NOTICE 'Loaded table: crm_cust_info';
      end_time := NOW();
      RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
  
      -- crm_prd_info
      start_time := NOW();
      TRUNCATE TABLE bronze.crm_prd_info;
      RAISE NOTICE 'Truncated table: crm_prd_info';
      EXECUTE $cmd$
          COPY bronze.crm_prd_info
          FROM 'D:/downloads/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
          DELIMITER ',' CSV HEADER;
      $cmd$;
      RAISE NOTICE 'Loaded table: crm_prd_info';
      end_time := NOW();
      RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
  
      -- crm_sales_details
      start_time := NOW();
      TRUNCATE TABLE bronze.crm_sales_details;
      RAISE NOTICE 'Truncated table: crm_sales_details';
      EXECUTE $cmd$
          COPY bronze.crm_sales_details
          FROM 'D:/downloads/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
          DELIMITER ',' CSV HEADER;
      $cmd$;
      RAISE NOTICE 'Loaded table: crm_sales_details';
      end_time := NOW();
      RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
  
      -- erp_cust_az12
      start_time := NOW();
      TRUNCATE TABLE bronze.erp_cust_az12;
      RAISE NOTICE 'Truncated table: erp_cust_az12';
      EXECUTE $cmd$
          COPY bronze.erp_cust_az12
          FROM 'D:/downloads/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
          DELIMITER ',' CSV HEADER;
      $cmd$;
      RAISE NOTICE 'Loaded table: erp_cust_az12';
      end_time := NOW();
      RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
  
      -- erp_loc_a101
      start_time := NOW();
      TRUNCATE TABLE bronze.erp_loc_a101;
      RAISE NOTICE 'Truncated table: erp_loc_a101';
      EXECUTE $cmd$
          COPY bronze.erp_loc_a101
          FROM 'D:/downloads/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
          DELIMITER ',' CSV HEADER;
      $cmd$;
      RAISE NOTICE 'Loaded table: erp_loc_a101';
      end_time := NOW();
      RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
  
      -- erp_px_cat_g1v2
      start_time := NOW();
      TRUNCATE TABLE bronze.erp_px_cat_g1v2;
      RAISE NOTICE 'Truncated table: erp_px_cat_g1v2';
      EXECUTE $cmd$
          COPY bronze.erp_px_cat_g1v2
          FROM 'D:/downloads/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
          DELIMITER ',' CSV HEADER;
      $cmd$;
      RAISE NOTICE 'Loaded table: erp_px_cat_g1v2';
      end_time := NOW();
      RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
  
      RAISE NOTICE 'All bronze tables loaded successfully.';
  	end_BATCH := NOW();
  	RAISE NOTICE 'TOTAL Duration: % seconds', EXTRACT(EPOCH FROM (end_BATCH - start_BATCH));
  END;
END;
$$;
CALL bronze.load_bronze();
