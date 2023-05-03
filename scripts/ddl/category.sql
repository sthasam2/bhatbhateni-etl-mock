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