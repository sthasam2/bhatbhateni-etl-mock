---------------------------------------------------------------
--  
-- SCHEMA:  STG
-- NAME:    STG_D_STORE_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE STG.STG_D_STORE_LU (
    ID INT NOT NULL PRIMARY KEY,
    REGION_ID INT FOREIGN KEY REFERENCES STG_D_REGION_LU (ID),
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
    REGION_ID INT FOREIGN KEY REFERENCES TMP_D_REGION_LU (ID),
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
    REGION_KY INT FOREIGN KEY REFERENCES DWH_D_REGION_LU (REGION_KY),
    -- 
    ACTIVE_FLG BOOLEAN DEFAULT TRUE,
    CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);