"""
Main file point for executing the script
"""

from dotenv import load_dotenv

from etc.variables import *
from lib import tasks
from lib.database import Connection
from lib.utils import *
from src._tables import *

load_dotenv()
ctx = None


################################
#   DRIVER CODE
################################

if __name__ == "__main__":
    try:
        # CONNECT TO DATABASE

        connection = Connection(
            user=get_env_var("USER"),
            password=get_env_var("PASSWORD"),
            account=get_env_var("ACCOUNT"),
            database=get_env_var("DATABASE"),
        )
        ctx = connection.db_ctx

        # create table objects for easier usage
        table_objects = dict(
            country_obj=Country(ctx),
            region_obj=Region(ctx),
            store_obj=Store(ctx),
            customer_obj=Customer(ctx),
            category_obj=Category(ctx),
            subcategory_obj=Subcategory(ctx),
            product_obj=Product(ctx),
        )

        # time_table_objs = dict(year_obj=Year(ctx))

        # ############################
        #           TASKS
        # ############################

        # # DOWNLOAD FILES
        # tasks.export_transaction_tables_to_csv(ctx)

        # CREATE STG TMP TGT Schemas
        tasks.create_stg_tmp_tgt_schemas(ctx)

        # PUT CSV FILES INTO FILE STAGE
        tasks.put_all_csv_to_stg_file_stage(ctx, table_objects)

        # EXTRACT ALL INTERNAL STORAGE CSV TO STG
        tasks.extract_all_file_stage_csv_to_stg_tables(ctx, table_objects)

        # TRANSFORM STG TABLES TO TMP TABLES
        tasks.transform_all_stg_tables_to_tmp_tables(ctx, table_objects)

        # LOAD TMP TABLES TO DWH
        tasks.load_all_tmp_tables_to_tgt_tables(ctx, table_objects)

        # TIME HIERARCHY

        # CREATE FACT BASE AND AGGREGATE TABLES
        tasks.load_fact_tables(ctx)

        print("end")

    except Exception as e:
        print(e)

    finally:
        if ctx:
            ctx.close()
        print("\nConnection Closed")
