---------------------------------------------------------------
--  
-- SCHEMA:  STG
-- NAME:    STG_D_REGION_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE STG.STG_D_REGION_LU (
    ID INT NOT NULL PRIMARY KEY,
    COUNTRY_ID INT FOREIGN KEY REFERENCES STG_D_COUNTRY_LU (ID),
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
    COUNTRY_ID INT FOREIGN KEY REFERENCES TMP_D_COUNTRY_LU (ID),
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
    COUNTRY_KY INT FOREIGN KEY REFERENCES DWH_D_COUNTRY_LU (COUNTRY_KY),
    -- 
    ACTIVE_FLG BOOLEAN DEFAULT TRUE,
    CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);