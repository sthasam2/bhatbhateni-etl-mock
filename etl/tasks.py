from snowflake.connector.connection import SnowflakeConnection

from constant import *
from tables._tables import *
from utils import *


@try_except_decorator
def export_transaction_tables_to_csv(ctx: SnwfConn):
    """
    Export Data from tables in file format
    """
    # LOCATION HIERARCHY
    export_schema_table_to_csv(ctx, "transactions", "country", CSV_PATH)
    export_schema_table_to_csv(ctx, "transactions", "region", CSV_PATH)
    export_schema_table_to_csv(ctx, "transactions", "store", CSV_PATH)
    # PRODUCT HIERARCHY
    export_schema_table_to_csv(ctx, "transactions", "category", CSV_PATH)
    export_schema_table_to_csv(ctx, "transactions", "subcategory", CSV_PATH)
    export_schema_table_to_csv(ctx, "transactions", "product", CSV_PATH)
    # OTHERS
    export_schema_table_to_csv(ctx, "transactions", "customer", CSV_PATH)
    export_schema_table_to_csv(ctx, "transactions", "sales", CSV_PATH)


@try_except_decorator
def create_stg_tmp_tgt_schemas(ctx: SnwfConn):
    """
    Create schema for STG, TMP and TGT
    """

    ctx.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {STAGE_SCHEMA}")
    ctx.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {TMP_SCHEMA}")
    ctx.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {TGT_SCHEMA}")


@try_except_decorator
def put_all_csv_to_stg_file_stage(ctx: SnwfConn, table_objects):
    """
    Put all csv from device to snowflake internalStorage
    """

    ctx.cursor().execute(f"USE SCHEMA {STAGE_SCHEMA}")
    ctx.cursor().execute(f"CREATE OR REPLACE STAGE stg.file_stage")

    ctx.cursor().execute(
        "CREATE FILE FORMAT IF NOT EXISTS csv_format TYPE = 'csv' FIELD_DELIMITER = ',' SKIP_HEADER = 1;"
    )

    for _, table in table_objects.items():
        table.put_csv_to_file_stage()


@try_except_decorator
def extract_all_file_stage_csv_to_stg_tables(ctx: SnwfConn, table_objects):
    """
    Extract csv files from snowflake internalStorage to stg
    """

    for _, table in table_objects.items():
        table.extract_staged_csv_to_stg()


@try_except_decorator
def transform_all_stg_tables_to_tmp_tables(ctx: SnwfConn, table_objects):
    """"""

    for _, table in table_objects.items():
        table.transform_stg_table_into_temp()


@try_except_decorator
def load_all_tmp_tables_to_tgt_tables(ctx: SnwfConn, table_objects):
    """"""

    for _, table in table_objects.items():
        table.load_tmp_table_to_tgt()


@try_except_decorator
def load_fact_tables(ctx: SnwfConn, agg_table_objects=None):
    """"""

    # # BASE TRANSACTION TABLE
    # sales_b = Sales(ctx)
    # sales_b.put_csv_to_file_stage()
    # sales_b.extract_staged_csv_to_stg()
    # sales_b.transform_stg_table_into_temp()
    # sales_b.load_tmp_table_to_tgt()

    # AGGREGATION TABLE AT PLACE, MONTH
    sales_agg = SalesAgg(ctx)
    sales_agg.transform_temp_tables()
    sales_agg.load_tmp_table_to_tgt()