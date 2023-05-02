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
OR REPLACE TABLE TMP.TMP_D_COUNTRY_LU (ID INT NOT NULL PRIMARY KEY, COUNTRY_DESC VARCHAR);


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