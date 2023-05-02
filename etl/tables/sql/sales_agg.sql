---------------------------------------------------------------
--  
-- SCHEMA:  TMP
-- NAME:    tmp_f_sls_prod_mon_store_agg
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE tmp.tmp_f_sls_prod_mon_store_agg (
    sls_agg_ky VARCHAR NOT NULL PRIMARY KEY,
    --
    STORE_KY INT FOREIGN KEY REFERENCES DWH.DWH_D_STORE_LU (STORE_KY),
    STORE_ID INT FOREIGN KEY REFERENCES DWH.DWH_D_STORE_LU (STORE_ID),
    PRODUCT_KY INT FOREIGN KEY REFERENCES DWH.DWH_D_PRODUCT_LU (PRODUCT_KY),
    PRODUCT_ID INT FOREIGN KEY REFERENCES DWH.DWH_D_PRODUCT_LU (PRODUCT_ID),
    --
    MONTH_ID VARCHAR,
    YEAR_ID VARCHAR,
    TOTAL_SALES NUMBER (20, 2)
);


---------------------------------------------------------------
--  
-- SCHEMA:  DWH
-- NAME:    dwh_f_sls_prod_mon_store_agg
-- 
---------------------------------------------------------------
CREATE
OR REPLACE TABLE dwh.dwh_f_sls_prod_mon_store_agg (
    sls_agg_ky VARCHAR NOT NULL PRIMARY KEY,
    --
    STORE_KY INT FOREIGN KEY REFERENCES DWH.DWH_D_STORE_LU (STORE_KY),
    STORE_ID INT FOREIGN KEY REFERENCES DWH.DWH_D_STORE_LU (STORE_ID),
    PRODUCT_KY INT FOREIGN KEY REFERENCES DWH.DWH_D_PRODUCT_LU (PRODUCT_KY),
    PRODUCT_ID INT FOREIGN KEY REFERENCES DWH.DWH_D_PRODUCT_LU (PRODUCT_ID),
    MONTH_ID VARCHAR,
    YEAR_ID VARCHAR,
    TOTAL_SALES NUMBER (20, 2),
    --
    INSERTED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);