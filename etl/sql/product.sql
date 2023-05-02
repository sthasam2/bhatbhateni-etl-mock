---------------------------------------------------------------
--  
-- SCHEMA:  STG
-- NAME:    STG_D_PRODUCT_LU
--
---------------------------------------------------------------
CREATE
OR REPLACE TABLE STG.STG_D_PRODUCT_LU (
    ID INT NOT NULL PRIMARY KEY,
    SUBCATEGORY_ID INT FOREIGN KEY REFERENCES STG_D_SUBCATEGORY_LU (ID),
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
    SUBCATEGORY_ID INT FOREIGN KEY REFERENCES TMP_D_SUBCATEGORY_LU (ID),
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
    SUBCATEGORY_KY INT FOREIGN KEY REFERENCES DWH_D_SUBCATEGORY_LU (SUBCATEGORY_KY),
    -- 
    ACTIVE_FLG BOOLEAN DEFAULT TRUE,
    CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);