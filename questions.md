# ETL Questions

1. Export Data from Location Hierarchy table in file format
2. Export Data from Sales table in file format
3. Create schema for STG, TMP and TGT
4. Load location Hierarchy data to STG table, TMP table then Target table. Keep in mind naming convention of STG, TMP and TGT tables

   - It should handle key generation for new Data
   - It should handle data updates for minor change
   - It should handle closing dimension in case of dimension is missing
   - It should handle re-class and addition of new row for such case

5. Create fact aggregation Script for Sales: F_BHATBHATENI_AGG_SLS_PLC_MONTH_T tables

   - You have to use the incoming sales data from Bhatbhateni OLTP source
   - Use the TMP table for aggregation

Note: In above scenarios you might have to use/join other DW table which might have no data. In that case you can manually insert data for specific purpose
To conduct above:

- Create copy of existing BHATBATENI_KEC Database, with your name as prefix for unique identification
- Create copy of existing BHATBHATENI_KEC_DWH Database, with your name as prefix for unique identification
