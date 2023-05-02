"""
Utility functions for ETL
"""


import os
import time

import dotenv
import snowflake.connector

from constant import *

en_cfg = dotenv.dotenv_values("./.env")


def try_except_decorator(function):
    """
    Decorator function for wrapping a given function inside
    try except
    as well as console logging
    """

    def wrapper(*args, **kwargs):
        try:
            print(f"\n[{time.asctime()}]")
            print(f"Executing '{function.__name__}'...")
            return function(*args, **kwargs)

        except Exception as e:
            print("")
            print("!" * 80)
            print(f"\t\t\tError at '{function.__name__}'")
            print(e)
            print("!" * 80, "\n")

    return wrapper


def get_env_var(var_name: str) -> str:
    """
    Get the environment variable from your os or return an exception
    """

    try:
        if en_cfg.get(var_name):
            print("dotenv", var_name)
            return en_cfg.get(var_name)
        elif os.environ[var_name]:
            print("environ", var_name)
            return os.environ[var_name]
        else:
            raise KeyError

    except KeyError:
        error_msg = f"{var_name} not found!Set the '{var_name}' environment variable"
        raise EnvironmentError(error_msg)


#########################
# DB
#########################


@try_except_decorator
def create_connection(
    user: str,
    password: str,
    account: str,
    database: str = None,
    warehouse: str = None,
    schema: str = None,
):
    """
    Create database connection
    """

    connection = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        database=database,
        warehouse=warehouse,
        schema=schema,
    )
    print("Connected Successfully!")
    return connection


def export_schema_table_to_csv(
    ctx: SnwfConn, schema: str, table: str, csv_dir_location: str
):
    """
    Export schema.table to csv in desired location using a database connection
    """

    file_location = os.path.join(csv_dir_location, f"{table}.csv").replace("\\", "/")
    cur = ctx.cursor()
    cur.execute(f"SELECT * FROM {schema}.{table}")
    cur.fetch_pandas_all().to_csv(file_location, index=False)
    print(f"Exported '{schema}.{table}' to '{table}.csv' successfully!")


def export_csv_to_schema_file_stage(
    ctx: SnwfConn, schema: str, stage: str, file_path: str
):
    """
    Load csv in desired location using a database connection to the given stage in
    """

    cur = ctx.cursor()
    cur.execute(f"USE SCHEMA {schema}")
    cur.execute(f"CREATE STAGE IF NOT EXISTS {stage}")
    cur.execute(f"PUT 'file:///{file_path}' @{stage}")


# DDL


def create_schema_table_if_not_exists(
    ctx: SnwfConn, schema: str, table: str, fields_str: str
):
    """
    Create a table if it doesnt exist in the given schema with provided fields
    """

    ctx.cursor().execute(f"USE SCHEMA {schema}")
    ctx.cursor().execute(f"CREATE TABLE IF NOT EXISTS {table} {fields_str}")


# DQL


def check_schema_table_exists(ctx: SnwfConn, schema: str, table: str) -> bool:
    """ """

    return (
        len(
            ctx.cursor()
            .execute(f"SHOW TABLES LIKE '{table}' IN SCHEMA {schema}")
            .fetchone()
        )
        != 0
    )


def drop_and_create_sequence(ctx: SnwfConn, schema: str, seq: str):
    """ """

    ctx.cursor().execute(f"DROP SEQUENCE IF EXISTS {schema}.{seq}")
    ctx.cursor().execute(f"CREATE OR REPLACE sequence {schema}.{seq}")
