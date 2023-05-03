import os

from etc.variables import *
from lib.utils import *

# _customer dimensions
# id, region_id, customer_desc


class Customer:
    """
    Class for representing customer table
    """

    customer_csv_path: str = os.path.join(CSV_PATH, "customer.csv").replace("\\", "/")
    db_ctx: SnwfConn = None
    stg_customer_table = "stg_d_customer_lu"
    tmp_customer_table = "tmp_d_customer_lu"
    tgt_customer_table = "dwh_d_customer_lu"

    def __init__(self, db_ctx):
        self.db_ctx = db_ctx

    @start_end_decorator
    @try_except_decorator
    def put_csv_to_file_stage(self):
        print("@PUT ", self.__class__.__name__)
        return self.__put_customer_csv_to_file_stage()

    @start_end_decorator
    @try_except_decorator
    def extract_staged_csv_to_stg(self):
        print("@EXTRACT ", self.__class__.__name__)
        return self.__extract_customer_into_stg()

    @start_end_decorator
    @try_except_decorator
    def transform_stg_table_into_temp(self):
        print("@TRANSFORM ", self.__class__.__name__)
        return self.__transform_customer_into_temp()

    @start_end_decorator
    @try_except_decorator
    def load_tmp_table_to_tgt(self):
        print("@LOAD ", self.__class__.__name__)
        return self.__load_customer_into_target()

    #############################
    #   PRIVATE FUNCTIONS
    #############################

    def __put_customer_csv_to_file_stage(self):
        """ """

        export_csv_to_schema_file_stage(
            self.db_ctx, STAGE_SCHEMA, "file_stage", self.customer_csv_path
        )

    def __extract_customer_into_stg(self):
        """ """

        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {STAGE_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE IF EXISTS  {self.stg_customer_table}")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     STAGE_SCHEMA,
        #     self.stg_customer_table,
        #     """
        #     (
        #         id int not null primary key,
        #         customer_first_name varchar(256),
        #         customer_middle_name varchar(256),
        #         customer_last_name varchar(256),
        #         customer_address varchar(256)
        #     )
        #     """,
        # )

        cur.execute(
            """
                COPY INTO stg_d_customer_lu 
                FROM @file_stage/customer.csv.gz
                FILE_FORMAT = (FORMAT_NAME = 'csv_format')
            """
        )

        print(f"Extracted from 'file_stage' sucessfully to 'stg.stg_d_customer_lu'")

    def __transform_customer_into_temp(self):
        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {TMP_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE IF EXISTS  {self.tmp_customer_table}")
        cur.execute("BEGIN TRANSACTION")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     TMP_SCHEMA,
        #     self.tmp_customer_table,
        #     """
        #     (
        #         id int not null primary key,
        #         customer_first_name varchar(256),
        #         customer_middle_name varchar(256),
        #         customer_last_name varchar(256),
        #         customer_address varchar(256)
        #     )
        #     """,
        # )

        cur.execute(
            f"""
            INSERT INTO {self.tmp_customer_table}(
                id,
                customer_first_name,
                customer_middle_name,
                customer_last_name,
                customer_address
            )
            SELECT 
                id,
                customer_first_name,
                customer_middle_name,
                customer_last_name,
                customer_address
            FROM 
                {STAGE_SCHEMA}.{self.stg_customer_table}
            """
        )
        cur.execute("COMMIT")

        print(f"Transformed 'stg.stg_d_customer_lu' to 'tmp.tmp_d_customer_lu'")

    def __load_customer_into_target(self):
        """ """

        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {TGT_SCHEMA}")
        cur.execute("BEGIN TRANSACTION")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     TGT_SCHEMA,
        #     self.tgt_customer_table,
        #     """
        #     (
        #         customer_ky int PRIMARY KEY,
        #         customer_id int UNIQUE NOT NULL,
        #         customer_first_name varchar(1024),
        #         customer_middle_name varchar(1024),
        #         customer_last_name varchar(1024),
        #         customer_address varchar(1024),
        #         --
        #         active_flg boolean default true,
        #         created_ts timestamp DEFAULT CURRENT_TIMESTAMP(),
        #         updated_ts timestamp DEFAULT CURRENT_TIMESTAMP()

        #     )
        #     """,
        # )

        cur.execute(
            """
            UPDATE dwh.dwh_d_customer_lu target
            SET
                active_flg = FALSE,
                updated_ts = CURRENT_TIMESTAMP()
            FROM
                dwh.dwh_d_customer_lu tgt
                LEFT JOIN tmp.tmp_d_customer_lu src ON tgt.customer_id = src.id
            WHERE
                target.active_flg = TRUE -- only active records
                AND (
                    -- matched either id or desc
                    (
                        target.customer_id = src.id
                        AND (
                            nvl (target.customer_first_name, '') <> nvl (src.customer_first_name, '')
                            OR nvl (target.customer_middle_name, '') <> nvl (src.customer_middle_name, '')
                            OR nvl (target.customer_last_name, '') <> nvl (src.customer_last_name, '')
                            OR nvl (target.customer_address, '') <> nvl (src.customer_address, '')
                        )
                    )
                    -- not matched ie deleted
                    OR (
                        target.customer_id = tgt.customer_id
                        AND src.id IS NULL -- deleted
                    )
                );
            """
        )

        drop_and_create_sequence(self.db_ctx, TGT_SCHEMA, "seq")

        cur.execute(
            """
            INSERT INTO
                dwh.dwh_d_customer_lu(
                    customer_ky,
                    customer_id,
                    customer_first_name,
                    customer_middle_name,
                    customer_last_name,
                    customer_address
                )
            SELECT
                _max.max_ky + s.nextval,
                src.id,
                src.customer_first_name,
                src.customer_middle_name,
                src.customer_last_name,
                src.customer_address
            FROM
                (
                    dwh.dwh_d_customer_lu tgt
                    RIGHT JOIN tmp.tmp_d_customer_lu src ON (tgt.customer_id = src.id)
                ),
                (SELECT NVL(MAX(customer_ky), 0) max_ky from dwh.dwh_d_customer_lu) _max,
                TABLE (getnextval (seq)) s
            WHERE
                tgt.customer_id <> src.id
                OR nvl(tgt.customer_first_name, '') <> nvl(src.customer_first_name, '')
                OR nvl(tgt.customer_middle_name, '') <> nvl(src.customer_middle_name, '')
                OR nvl(tgt.customer_last_name, '') <> nvl(src.customer_last_name, '')
                OR nvl(tgt.customer_address, '') <> nvl(src.customer_address, '')
                OR tgt.customer_id IS NULL;
            """
        )
        cur.execute("COMMIT")

        print(f"Loaded 'tmp.tmp_d_customer_lu' to 'dwh.dwh_d_customer_lu'")
