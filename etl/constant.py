"""
Consists of all the variables used commonly in the script
"""

import os

from snowflake.connector import SnowflakeConnection as SnwfConn

# SCHEMAS
STAGE_SCHEMA = "stg"
TMP_SCHEMA = "tmp"
TGT_SCHEMA = "dwh"
TRANSACTION_SCHEMA = "transactions"

# PATHS
FILE_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
CSV_PATH = os.path.join(FILE_DIR_PATH, "csv")
# CSV_PATH = os.path.join(FILE_DIR_PATH, "csv/modified")


# TABLES
