USE DATABASE BHATBHATENI_ETL;

---============================================================
--  
--                          SCHEMAS  
--
---============================================================


CREATE SCHEMA STG;
CREATE SCHEMA TMP;
CREATE SCHEMA TGT;

---============================================================
--  
--                          TABLES  
--
---============================================================



-- ############################################################
--                      COUNTRY
-- ############################################################
---------------------------------------------------------------
--  
-- SCHEMA:  STG
-- NAME:    STG_D_COUNTRY_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE STG.STG_D_COUNTRY_LU (
    ID INT UNIQUE NOT NULL PRIMARY KEY,
    COUNTRY_DESC VARCHAR
);


---------------------------------------------------------------
--  
-- SCHEMA:  TMP
-- NAME:    TMP_D_COUNTRY_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE TMP.TMP_D_COUNTRY_LU (
    ID INT NOT NULL PRIMARY KEY,
    COUNTRY_DESC VARCHAR(1024)
);


---------------------------------------------------------------
--  
-- SCHEMA:  DWH
-- NAME:    DWH_D_COUNTRY_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE DWH.DWH_D_COUNTRY_LU (
    COUNTRY_KY INT PRIMARY KEY,
    COUNTRY_ID INT UNIQUE NOT NULL,
    COUNTRY_DESC VARCHAR(1024),
    -- 
    ACTIVE_FLG BOOLEAN DEFAULT TRUE,
    CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- ############################################################
--                      REGION
-- ############################################################
---------------------------------------------------------------
--  
-- SCHEMA:  STG
-- NAME:    STG_D_REGION_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE STG.STG_D_REGION_LU (
    ID INT NOT NULL PRIMARY KEY,
    COUNTRY_ID INT FOREIGN KEY REFERENCES STG.STG_D_COUNTRY_LU (ID),
    REGION_DESC VARCHAR
);


---------------------------------------------------------------
--  
-- SCHEMA:  TMP
-- NAME:    TMP_D_REGION_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE TMP.TMP_D_REGION_LU (
    ID INT NOT NULL PRIMARY KEY,
    COUNTRY_ID INT FOREIGN KEY REFERENCES TMP.TMP_D_COUNTRY_LU (ID),
    REGION_DESC VARCHAR
);


---------------------------------------------------------------
--  
-- SCHEMA:  DWH
-- NAME:    DWH_D_REGION_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE DWH.DWH_D_REGION_LU (
    REGION_KY INT PRIMARY KEY,
    REGION_ID INT UNIQUE NOT NULL,
    REGION_DESC VARCHAR(1024),
    -- 
    COUNTRY_KY INT FOREIGN KEY REFERENCES DWH.DWH_D_COUNTRY_LU (COUNTRY_KY),
    -- 
    ACTIVE_FLG BOOLEAN DEFAULT TRUE,
    CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- ############################################################
--                      STORE
-- ############################################################
---------------------------------------------------------------
--  
-- SCHEMA:  STG
-- NAME:    STG_D_STORE_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE STG.STG_D_STORE_LU (
    ID INT NOT NULL PRIMARY KEY,
    REGION_ID INT FOREIGN KEY REFERENCES STG.STG_D_REGION_LU (ID),
    STORE_DESC VARCHAR
);


---------------------------------------------------------------
--  
-- SCHEMA:  TMP
-- NAME:    TMP_D_STORE_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE TMP.TMP_D_STORE_LU (
    ID INT NOT NULL PRIMARY KEY,
    REGION_ID INT FOREIGN KEY REFERENCES TMP.TMP_D_REGION_LU (ID),
    STORE_DESC VARCHAR
);


---------------------------------------------------------------
--  
-- SCHEMA:  DWH
-- NAME:    DWH_D_STORE_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE DWH.DWH_D_STORE_LU (
    STORE_KY INT PRIMARY KEY,
    STORE_ID INT UNIQUE NOT NULL,
    STORE_DESC VARCHAR(1024),
    -- 
    REGION_KY INT FOREIGN KEY REFERENCES DWH.DWH_D_REGION_LU (REGION_KY),
    -- 
    ACTIVE_FLG BOOLEAN DEFAULT TRUE,
    CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- ############################################################
--                      CATEGORY
-- ############################################################
---------------------------------------------------------------
--  
-- SCHEMA:  STG
-- NAME:    STG_D_CATEGORY_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE STG.STG_D_CATEGORY_LU (
    ID INT NOT NULL PRIMARY KEY,
    CATEGORY_DESC VARCHAR
);


---------------------------------------------------------------
--  
-- SCHEMA:  TMP
-- NAME:    TMP_D_CATEGORY_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE TMP.TMP_D_CATEGORY_LU (
    ID INT NOT NULL PRIMARY KEY,
    CATEGORY_DESC VARCHAR
);


---------------------------------------------------------------
--  
-- SCHEMA:  DWH
-- NAME:    DWH_D_CATEGORY_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE DWH.DWH_D_CATEGORY_LU (
    CATEGORY_KY INT PRIMARY KEY,
    CATEGORY_ID INT UNIQUE NOT NULL,
    CATEGORY_DESC VARCHAR(1024),
    -- 
    ACTIVE_FLG BOOLEAN DEFAULT TRUE,
    CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- ############################################################
--                      SUBCATEGORY
-- ############################################################
---------------------------------------------------------------
--  
-- SCHEMA:  STG
-- NAME:    STG_D_SUBCATEGORY_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE STG.STG_D_SUBCATEGORY_LU (
    ID INT NOT NULL PRIMARY KEY,
    CATEGORY_ID INT FOREIGN KEY REFERENCES STG.STG_D_CATEGORY_LU (ID),
    SUBCATEGORY_DESC VARCHAR
);


---------------------------------------------------------------
--  
-- SCHEMA:  TMP
-- NAME:    TMP_D_SUBCATEGORY_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE TMP.TMP_D_SUBCATEGORY_LU (
    ID INT NOT NULL PRIMARY KEY,
    CATEGORY_ID INT FOREIGN KEY REFERENCES TMP.TMP_D_CATEGORY_LU (ID),
    SUBCATEGORY_DESC VARCHAR
);


---------------------------------------------------------------
--  
-- SCHEMA:  DWH
-- NAME:    DWH_D_SUBCATEGORY_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE DWH.DWH_D_SUBCATEGORY_LU (
    SUBCATEGORY_KY INT PRIMARY KEY,
    SUBCATEGORY_ID INT UNIQUE NOT NULL,
    SUBCATEGORY_DESC VARCHAR(1024),
    -- 
    CATEGORY_KY INT FOREIGN KEY REFERENCES DWH.DWH_D_CATEGORY_LU (CATEGORY_KY),
    -- 
    ACTIVE_FLG BOOLEAN DEFAULT TRUE,
    CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- ############################################################
--                      PRODUCT
-- ############################################################
---------------------------------------------------------------
--  
-- SCHEMA:  STG
-- NAME:    STG_D_PRODUCT_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE STG.STG_D_PRODUCT_LU (
    ID INT NOT NULL PRIMARY KEY,
    SUBCATEGORY_ID INT FOREIGN KEY REFERENCES STG.STG_D_SUBCATEGORY_LU (ID),
    PRODUCT_DESC VARCHAR
);


---------------------------------------------------------------
--  
-- SCHEMA:  TMP
-- NAME:    TMP_D_PRODUCT_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE TMP.TMP_D_PRODUCT_LU (
    ID INT NOT NULL PRIMARY KEY,
    SUBCATEGORY_ID INT FOREIGN KEY REFERENCES TMP.TMP_D_SUBCATEGORY_LU (ID),
    PRODUCT_DESC VARCHAR
);


---------------------------------------------------------------
--  
-- SCHEMA:  DWH
-- NAME:    DWH_D_PRODUCT_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE DWH.DWH_D_PRODUCT_LU (
    PRODUCT_KY INT PRIMARY KEY,
    PRODUCT_ID INT UNIQUE NOT NULL,
    PRODUCT_DESC VARCHAR(1024),
    -- 
    SUBCATEGORY_KY INT FOREIGN KEY REFERENCES DWH.DWH_D_SUBCATEGORY_LU (SUBCATEGORY_KY),
    -- 
    ACTIVE_FLG BOOLEAN DEFAULT TRUE,
    CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- ############################################################
--                      CUSTOMER
-- ############################################################
---------------------------------------------------------------
--  
-- SCHEMA:  STG
-- NAME:    STG_D_CUSTOMER_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE STG.STG_D_CUSTOMER_LU (
    ID INT NOT NULL PRIMARY KEY,
    CUSTOMER_FIRST_NAME VARCHAR(256),
    CUSTOMER_MIDDLE_NAME VARCHAR(256),
    CUSTOMER_LAST_NAME VARCHAR(256),
    CUSTOMER_ADDRESS VARCHAR(256)
);


---------------------------------------------------------------
--  
-- SCHEMA:  TMP
-- NAME:    TMP_D_CUSTOMER_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE TMP.TMP_D_CUSTOMER_LU (
    ID INT NOT NULL PRIMARY KEY,
    CUSTOMER_FIRST_NAME VARCHAR(256),
    CUSTOMER_MIDDLE_NAME VARCHAR(256),
    CUSTOMER_LAST_NAME VARCHAR(256),
    CUSTOMER_ADDRESS VARCHAR(256)
);


---------------------------------------------------------------
--  
-- SCHEMA:  DWH
-- NAME:    DWH_D_CUSTOMER_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE DWH.DWH_D_CUSTOMER_LU (
    CUSTOMER_KY INT PRIMARY KEY,
    CUSTOMER_ID INT UNIQUE NOT NULL,
    CUSTOMER_FIRST_NAME VARCHAR(1024),
    CUSTOMER_MIDDLE_NAME VARCHAR(1024),
    CUSTOMER_LAST_NAME VARCHAR(1024),
    CUSTOMER_ADDRESS VARCHAR(1024),
    -- 
    ACTIVE_FLG BOOLEAN DEFAULT TRUE,
    CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- ############################################################
--                      SALES
-- ############################################################
-- ##################################
--              BASE
-- ##################################
---------------------------------------------------------------
--  
-- SCHEMA:  STG
-- NAME:    STG_F_BHATBHATENI_SLS_TRXN_B
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE STG.STG_F_BHATBHATENI_SLS_TRXN_B (
    ID INT NOT NULL PRIMARY KEY,
    STORE_ID INT FOREIGN KEY REFERENCES STG.STG_D_STORE_LU (ID),
    PRODUCT_ID INT FOREIGN KEY REFERENCES STG.STG_D_PRODUCT_LU (ID),
    CUSTOMER_ID INT FOREIGN KEY REFERENCES STG.STG_D_CUSTOMER_LU (ID),
    TRANSACTION_TIME TIMESTAMP_NTZ,
    QUANTITY NUMBER (38, 0),
    AMOUNT NUMBER (20, 2),
    DISCOUNT NUMBER (20, 2)
);


---------------------------------------------------------------
--  
-- SCHEMA:  TMP
-- NAME:    TMP_F_BHATBHATENI_SLS_TRXN_B
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE TMP.TMP_F_BHATBHATENI_SLS_TRXN_B (
    ID INT NOT NULL PRIMARY KEY,
    STORE_ID INT FOREIGN KEY REFERENCES TMP.TMP_D_STORE_LU (ID),
    PRODUCT_ID INT FOREIGN KEY REFERENCES TMP.TMP_D_PRODUCT_LU (ID),
    CUSTOMER_ID INT FOREIGN KEY REFERENCES TMP.TMP_D_CUSTOMER_LU (ID),
    TRANSACTION_TIME TIMESTAMP_NTZ (9),
    QUANTITY NUMBER (38, 0),
    AMOUNT NUMBER (20, 2),
    DISCOUNT NUMBER (20, 2)
);


---------------------------------------------------------------
--  
-- SCHEMA:  DWH
-- NAME:    DWH_F_BHATBHATENI_SLS_TRXN_B
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE DWH.DWH_F_BHATBHATENI_SLS_TRXN_B (
    SALES_TXN_ID INT NOT NULL PRIMARY KEY,
    --
    STORE_KY INT FOREIGN KEY REFERENCES DWH.DWH_D_STORE_LU (STORE_KY),
    STORE_ID INT FOREIGN KEY REFERENCES DWH.DWH_D_STORE_LU (STORE_ID),
    PRODUCT_KY INT FOREIGN KEY REFERENCES DWH.DWH_D_PRODUCT_LU (PRODUCT_KY),
    PRODUCT_ID INT FOREIGN KEY REFERENCES DWH.DWH_D_PRODUCT_LU (PRODUCT_ID),
    CUSTOMER_KY INT FOREIGN KEY REFERENCES DWH.DWH_D_CUSTOMER_LU (CUSTOMER_KY),
    CUSTOMER_ID INT FOREIGN KEY REFERENCES DWH.DWH_D_CUSTOMER_LU (CUSTOMER_ID),
    --
    DAY_KY VARCHAR,
    -- 
    TRANSACTION_TIME TIMESTAMP_NTZ (9),
    QUANTITY NUMBER (38, 0),
    AMOUNT NUMBER (20, 2),
    DISCOUNT NUMBER (20, 2),
    NET_AMOUNT NUMBER (20, 2),
    -- 
    INSERTED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
);


-- ##################################
--              AGG
-- ##################################
---------------------------------------------------------------
--  
-- SCHEMA:  TMP
-- NAME:    TMP_F_SLS_PROD_MON_STORE_AGG
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