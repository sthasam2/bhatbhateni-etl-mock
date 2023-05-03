# ETL Assignment

## 1. Export Data from Location Hierarchy table in file format

## 2. Export Data from Sales table in file format

## 3. Create schema for STG, TMP and TGT (The stg schema will contain all the staging tables and so on)

## 4. Load the dimension data to STG table, TMP table then Target table

- Keep in mind naming convention of STG, TMP and TGT tables
 The naming convention for a dimension in target schema would be - DWH_D_PRODUCT_LU.
 Please remember to change the DWH prefix while creating tables in other schemas.  
- It should handle key generation for new Data (Surrogate Keys as explained in DWH session)  
- It should handle data updates for minor change  
- It should handle closing dimension in case of dimension is missing (Concept of active flag)  
- It should handle re-class and addition of new row for such case  

>
> HINTS:
> a. Once you create the table in stg temp and target schema, you can start developing codebase on python.
> b. Use snowflake connector to connect to snowflake and perform ETL.
> c. Use put command to put the csv extracts to Snowflake file stage.
> d. Use copy into table command to load the staging table from file stage.
> e. You can have one python file to perform ETL for one particular table.If you're trying to load country lookup >table, build a file (coumtry.py)
> to load data into stg/tmp/target.
> f. Add a step to truncate your stage and temp tables everytime before you load them.
> g. Follow the upsert technique to load the target tables.
>
## 5. Load the fact base table for Sales:DWH_F_BHATBHATENI_SLS_TRXN_B

- You have to use the incoming sales data from Bhatbhateni OLTP source
    Since this is a base table, please take most granular dimensions available (ILD)

## 6. Create fact aggregation Script for Sales: DWH_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T tables

- You have to use the incoming sales data from Bhatbhateni OLTP source
- Use the TMP table for aggregation

> In above scenarios you might have to use/join other DW table which might have no data. In that case you can > manually insert data for specific purpose
> To conduct above:
>
> - Create copy of existing BHATBATENI Database, with your name as prefix for unique identification
> - Create copy of existing BHATBHATENI_DWH Database, with your name as prefix for unique identification
