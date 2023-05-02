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