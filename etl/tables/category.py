import os

from constant import *
from utils import (
    create_schema_table_if_not_exists,
    drop_and_create_sequence,
    export_csv_to_schema_file_stage,
    try_except_decorator,
)

# category dimensions
# id, category_desc


class Category:
    """
    Class for representing category table
    """

    category_csv_path: str = os.path.join(CSV_PATH, "category.csv").replace("\\", "/")
    db_ctx: SnwfConn = None

    stg_category_table = "stg_d_category_lu"
    tmp_category_table = "tmp_d_category_lu"
    tgt_category_table = "dwh_d_category_lu"

    def __init__(self, db_ctx):
        self.db_ctx = db_ctx

    @try_except_decorator
    def put_csv_to_file_stage(self):
        print("@PUT ", self.__class__.__name__)
        return self.__put_category_csv_to_file_stage()

    @try_except_decorator
    def extract_staged_csv_to_stg(self):
        print("@EXTRACT ", self.__class__.__name__)
        return self.__extract_category_into_stg()

    @try_except_decorator
    def transform_stg_table_into_temp(self):
        print("@TRANSFORM ", self.__class__.__name__)
        return self.__transform_category_into_temp()

    @try_except_decorator
    def load_tmp_table_to_tgt(self):
        print("@LOAD ", self.__class__.__name__)
        return self.__load_category_into_target()

    #############################
    #   PRIVATE FUNCTIONS
    #############################

    def __put_category_csv_to_file_stage(self):
        """
        Exports category.csv file from location to file_stage in stg schema
        """
        export_csv_to_schema_file_stage(
            self.db_ctx, STAGE_SCHEMA, "file_stage", self.category_csv_path
        )
        print(f"PUT 'category.csv' to '{STAGE_SCHEMA}.file_stage' successfully!")

    def __extract_category_into_stg(self):
        """
        Copies internalStorage file_stage category.csv to stg_d_category_lu table
        """
        cur = self.db_ctx.cursor()

        cur.execute(f"USE SCHEMA {STAGE_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE IF EXISTS {self.stg_category_table}")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     STAGE_SCHEMA,
        #     self.stg_category_table,
        #     "(id int NOT NULL PRIMARY KEY, category_desc varchar)",
        # )

        cur.execute(
            """ COPY INTO 
                    stg_d_category_lu 
                FROM 
                    @file_stage/category.csv.gz
                    FILE_FORMAT = (FORMAT_NAME = 'csv_format')
            """
        )
        print(
            f"Extracted 'category.csv' from 'file_stage' sucessfully to 'stg.stg_d_category_lu'"
        )

    def __transform_category_into_temp(self):
        """"""

        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {TMP_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE IF EXISTS  {self.tmp_category_table}")
        cur.execute("BEGIN TRANSACTION")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     TMP_SCHEMA,
        #     self.tmp_category_table,
        #     """
        #     (
        #         id int UNIQUE NOT NULL PRIMARY KEY,
        #         category_desc varchar
        #     )
        #     """,
        # )

        cur.execute(
            f"""
            INSERT INTO {self.tmp_category_table}(
                id, category_desc
            )
            SELECT 
                id, category_desc
            FROM 
                {STAGE_SCHEMA}.{self.stg_category_table}
            """
        )
        cur.execute("COMMIT")

        print(f"Transformed 'stg.stg_d_category_lu' to 'tmp.tmp_d_category_lu'")

    def __load_category_into_target(self):
        """"""

        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {TGT_SCHEMA}")
        cur.execute("BEGIN TRANSACTION")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     TGT_SCHEMA,
        #     self.tgt_category_table,
        #     """
        #     (
        #         category_ky int PRIMARY KEY,
        #         category_id int UNIQUE NOT NULL,
        #         category_desc varchar(1024),
        #         --
        #         active_flg boolean default true,
        #         created_ts timestamp DEFAULT CURRENT_TIMESTAMP(),
        #         updated_ts timestamp DEFAULT CURRENT_TIMESTAMP()

        #     )
        #     """,
        # )

        cur.execute(
            """
            UPDATE dwh.dwh_d_category_lu target
            SET
                active_flg = FALSE,
                updated_ts = CURRENT_TIMESTAMP()
            FROM
                dwh.dwh_d_category_lu tgt
                LEFT JOIN tmp.tmp_d_category_lu src ON tgt.category_id = src.id
            WHERE
                target.active_flg = TRUE 
                AND (
                    (
                        target.category_id = src.id
                        AND target.category_desc <> src.category_desc 
                    ) 
                    OR (
                        target.category_id = tgt.category_id
                        AND src.id IS NULL 
                    )
                )
            """
        )

        drop_and_create_sequence(self.db_ctx, TGT_SCHEMA, "seq")

        cur.execute(
            """
            INSERT INTO
                dwh.dwh_d_category_lu (category_ky, category_id, category_desc)
            SELECT
                _max.max_ky + s.nextval,
                src.id,
                src.category_desc
            FROM
                (
                    dwh.dwh_d_category_lu tgt
                    RIGHT JOIN tmp.tmp_d_category_lu src ON tgt.category_id = src.id 
                    and tgt.active_flg
                ),
                (
                    SELECT
                        NVL (MAX(category_ky), 0) max_ky
                    FROM
                        dwh.dwh_d_category_lu
                ) _max,
                TABLE (getnextval (seq)) s
            WHERE
                tgt.category_id <> src.id
                OR tgt.category_desc <> src.category_desc
                OR tgt.category_id IS NULL
            """
        )

        cur.execute("COMMIT")
        print(f"Loaded 'tmp.tmp_d_category_lu' to 'dwh.dwh_d_category_lu'")
