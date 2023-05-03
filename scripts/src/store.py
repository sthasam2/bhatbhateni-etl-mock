import os

from etc.variables import *
from lib.utils import *

# _store dimensions
# id, region_id, store_desc


class Store:
    """
    Class for representing Store table
    """

    store_csv_path: str = os.path.join(CSV_PATH, "store.csv").replace("\\", "/")
    db_ctx: SnwfConn = None

    stg_store_table = "stg_d_store_lu"
    tmp_store_table = "tmp_d_store_lu"
    tgt_store_table = "dwh_d_store_lu"

    def __init__(self, db_ctx):
        self.db_ctx = db_ctx

    @start_end_decorator
    @try_except_decorator
    def put_csv_to_file_stage(self):
        print("@PUT ", self.__class__.__name__)
        return self.__put_store_csv_to_file_stage()

    @start_end_decorator
    @try_except_decorator
    def extract_staged_csv_to_stg(self):
        print("@EXTRACT ", self.__class__.__name__)
        return self.__extract_store_into_stg()

    @start_end_decorator
    @try_except_decorator
    def transform_stg_table_into_temp(self):
        print("@TRANSFORM ", self.__class__.__name__)
        return self.__transform_store_into_temp()

    @start_end_decorator
    @try_except_decorator
    def load_tmp_table_to_tgt(self):
        print("@LOAD ", self.__class__.__name__)
        return self.__load_store_into_target()

    #############################
    #   PRIVATE FUNCTIONS
    #############################

    def __put_store_csv_to_file_stage(self):
        """ """

        export_csv_to_schema_file_stage(
            self.db_ctx, STAGE_SCHEMA, "file_stage", self.store_csv_path
        )

    def __extract_store_into_stg(self):
        """ """

        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {STAGE_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE IF EXISTS  {self.stg_store_table}")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     STAGE_SCHEMA,
        #     self.stg_store_table,
        #     """
        #     (
        #         id int NOT NULL PRIMARY KEY,
        #         region_id int FOREIGN KEY REFERENCES stg_d_region_lu(id),
        #         store_desc varchar
        #     )
        #     """,
        # )

        cur.execute(
            """
                COPY INTO stg_d_store_lu 
                FROM @file_stage/store.csv.gz
                FILE_FORMAT = (FORMAT_NAME = 'csv_format')
            """
        )

        print(f"Extracted from 'file_stage' sucessfully to 'stg.stg_d_customer_lu'")

    def __transform_store_into_temp(self):
        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {TMP_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE IF EXISTS  {self.tmp_store_table}")
        cur.execute("BEGIN TRANSACTION")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     TMP_SCHEMA,
        #     self.tmp_store_table,
        #     """
        #     (
        #         id int NOT NULL PRIMARY KEY,
        #         region_id int FOREIGN KEY REFERENCES tmp_d_region_lu(id),
        #         store_desc varchar
        #     )
        #     """,
        # )

        cur.execute(
            f"""
            INSERT INTO {self.tmp_store_table}(
                id,
                region_id,
                store_desc
            )
            SELECT 
                id,
                region_id,
                store_desc
            FROM 
                {STAGE_SCHEMA}.{self.stg_store_table}
            """
        )
        cur.execute("COMMIT")

        print(f"Transformed 'stg.stg_d_store_lu' to 'tmp.tmp_d_store_lu'")

    def __load_store_into_target(self):
        """ """

        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {TGT_SCHEMA}")
        cur.execute("BEGIN TRANSACTION")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     TGT_SCHEMA,
        #     self.tgt_store_table,
        #     """
        #     (
        #         store_ky int PRIMARY KEY,
        #         store_id int UNIQUE NOT NULL,
        #         store_desc varchar(1024),
        #         --
        #         region_ky int FOREIGN KEY REFERENCES dwh_d_region_lu(region_ky),
        #         --
        #         active_flg boolean default true,
        #         created_ts timestamp DEFAULT CURRENT_TIMESTAMP(),
        #         updated_ts timestamp DEFAULT CURRENT_TIMESTAMP()
        #     )
        #     """,
        # )

        # create views for ease
        cur.execute(
            """
            CREATE OR REPLACE TABLE
                tmp.extended_dwh_d_store_lu AS
            SELECT
                reg.region_id,
                src.*
            FROM
                dwh.dwh_d_store_lu src
                INNER JOIN dwh.dwh_d_region_lu reg ON reg.region_ky = src.region_ky 
                and src.active_flg
            """
        )
        cur.execute(
            """
            CREATE OR REPLACE TABLE
                tmp.extended_tmp_d_store_lu AS
            SELECT
                src.*,
                reg.region_ky region_ky
            FROM
                tmp.tmp_d_store_lu src
                INNER JOIN dwh.dwh_d_region_lu reg ON reg.region_id = src.region_id
                and reg.active_flg
            """
        )
        cur.execute(
            """
            UPDATE dwh.dwh_d_store_lu target
            SET
                active_flg = FALSE,
                updated_ts = CURRENT_TIMESTAMP()
            FROM
                tmp.extended_dwh_d_store_lu tgt
                LEFT JOIN tmp.extended_tmp_d_store_lu src ON (
                    tgt.store_id = src.id 
                )
            WHERE
                target.active_flg = TRUE
                AND (
                    target.store_id = src.id
                    AND (
                        tgt.store_desc <> src.store_desc 
                        OR tgt.region_id <> src.region_id 
                        OR tgt.region_ky <> src.region_ky 
                    ) 
                    OR (
                        target.store_id = tgt.store_id
                        AND src.id IS NULL 
                    )
                );
            """
        )

        drop_and_create_sequence(self.db_ctx, TGT_SCHEMA, "seq")

        cur.execute(
            """
            INSERT INTO
                dwh.dwh_d_store_lu (store_ky, store_id, store_desc, region_ky)
            SELECT
                _max.max_ky + s.nextval,
                src.id,
                src.store_desc,
                src.region_ky
            FROM
                tmp.extended_dwh_d_store_lu tgt
                RIGHT JOIN tmp.extended_tmp_d_store_lu src ON (
                    tgt.store_id = src.id 
                ),
                (SELECT NVL(MAX(store_ky), 0) max_ky from dwh.dwh_d_store_lu) _max,
                TABLE (getnextval (seq)) s
            WHERE
                tgt.store_id <> src.id                
                OR tgt.region_id <> src.region_id     
                OR tgt.region_ky <> src.region_ky 
                OR tgt.store_desc <> src.store_desc   
                OR tgt.store_ky IS NULL  

        """
        )
        cur.execute("COMMIT")

        print(f"Loaded 'tmp.tmp_d_store_lu' to 'dwh.dwh_d_store_lu'")
