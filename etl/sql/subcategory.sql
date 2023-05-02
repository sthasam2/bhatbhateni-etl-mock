---------------------------------------------------------------
--  
-- SCHEMA:  STG
-- NAME:    STG_D_SUBCATEGORY_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE STG.STG_D_SUBCATEGORY_LU (
    ID INT NOT NULL PRIMARY KEY,
    CATEGORY_ID INT FOREIGN KEY REFERENCES STG_D_CATEGORY_LU (ID),
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
    CATEGORY_ID INT FOREIGN KEY REFERENCES TMP_D_CATEGORY_LU (ID),
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
    CATEGORY_KY INT FOREIGN KEY REFERENCES DWH_D_CATEGORY_LU (CATEGORY_KY),
    -- 
    ACTIVE_FLG BOOLEAN DEFAULT TRUE,
    CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);