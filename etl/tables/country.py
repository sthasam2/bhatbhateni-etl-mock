import os

from constant import *
from utils import (
    create_schema_table_if_not_exists,
    drop_and_create_sequence,
    export_csv_to_schema_file_stage,
    try_except_decorator,
)

# country dimensions
# id, country_desc


class Country:
    """
    Class for representing country table
    """

    country_csv_path: str = os.path.join(CSV_PATH, "country.csv").replace("\\", "/")
    db_ctx: SnwfConn = None

    stg_country_table = "stg_d_country_lu"
    tmp_country_table = "tmp_d_country_lu"
    tgt_country_table = "dwh_d_country_lu"

    def __init__(self, db_ctx):
        self.db_ctx = db_ctx

    @try_except_decorator
    def put_csv_to_file_stage(self):
        print("@PUT ", self.__class__.__name__)
        return self.__put_country_csv_to_file_stage()

    @try_except_decorator
    def extract_staged_csv_to_stg(self):
        print("@EXTRACT ", self.__class__.__name__)
        return self.__extract_country_into_stg()

    @try_except_decorator
    def transform_stg_table_into_temp(self):
        print("@TRANSFORM ", self.__class__.__name__)
        return self.__transform_country_into_temp()

    @try_except_decorator
    def load_tmp_table_to_tgt(self):
        print("@LOAD ", self.__class__.__name__)
        return self.__load_country_into_target()

    #############################
    #   PRIVATE FUNCTIONS
    #############################

    def __put_country_csv_to_file_stage(self):
        """ """

        export_csv_to_schema_file_stage(
            self.db_ctx, STAGE_SCHEMA, "file_stage", self.country_csv_path
        )
        print("PUT country.csv to file_stage successfully!")

    def __extract_country_into_stg(self):
        """ """

        cur = self.db_ctx.cursor()

        cur.execute(f"USE SCHEMA {STAGE_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE IF EXISTS {self.stg_country_table}")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     STAGE_SCHEMA,
        #     self.stg_country_table,
        #     """
        #     (
        #         id int NOT NULL PRIMARY KEY,
        #         country_desc varchar
        #     )
        #     """,
        # )

        cur.execute(
            """
                COPY INTO stg_d_country_lu
                FROM @file_stage/country.csv.gz
                FILE_FORMAT = (FORMAT_NAME = 'csv_format')
            """
        )
        print(f"Extracted from 'file_stage' sucessfully to 'stg.stg_d_country_lu'")

    def __transform_country_into_temp(self):
        """"""

        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {TMP_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE IF EXISTS  {self.tmp_country_table}")
        cur.execute("BEGIN TRANSACTION")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     TMP_SCHEMA,
        #     self.tmp_country_table,
        #     """
        #     (
        #         id int UNIQUE NOT NULL PRIMARY KEY,
        #         country_desc varchar
        #     )
        #     """,
        # )
        cur.execute(
            f"""
            INSERT INTO {self.tmp_country_table}(
                id, country_desc
            )
            SELECT 
                id, country_desc
            FROM 
                {STAGE_SCHEMA}.{self.stg_country_table}
            """
        )

        cur.execute("COMMIT")
        print(f"Transformed 'stg.stg_d_country_lu' to 'tmp.tmp_d_country_lu'")

    def __load_country_into_target(self):
        """ """

        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {TGT_SCHEMA}")
        cur.execute("BEGIN TRANSACTION")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     TGT_SCHEMA,
        #     self.tgt_country_table,
        #     """
        #     (
        #         country_ky int PRIMARY KEY,
        #         country_id int UNIQUE NOT NULL,
        #         country_desc varchar(1024),
        #         --
        #         active_flg boolean default true,
        #         created_ts timestamp DEFAULT CURRENT_TIMESTAMP(),
        #         updated_ts timestamp DEFAULT CURRENT_TIMESTAMP()

        #     )
        #     """,
        # )
        cur.execute(
            """
            UPDATE dwh.dwh_d_country_lu target
            SET
                active_flg = FALSE,
                updated_ts = CURRENT_TIMESTAMP()
            FROM
                dwh.dwh_d_country_lu tgt
                LEFT JOIN tmp.tmp_d_country_lu src ON tgt.country_id = src.id
                and tgt.active_flg
            WHERE
                target.active_flg = TRUE 
                AND (
                    (
                        target.country_id = src.id
                        AND target.country_desc <> src.country_desc 
                    ) 
                    OR (
                        target.country_id = tgt.country_id
                        AND src.id IS NULL 
                    )
                )
            """
        )

        drop_and_create_sequence(self.db_ctx, TGT_SCHEMA, "seq")

        cur.execute(
            """
            INSERT INTO
                dwh.dwh_d_country_lu (country_ky, country_id, country_desc)
            SELECT
                _max.max_ky + s.nextval country_ky,
                src.id country_id,
                src.country_desc country_desc
            FROM
                (
                    dwh.dwh_d_country_lu tgt
                    RIGHT JOIN tmp.tmp_d_country_lu src ON tgt.country_id = src.id
                    and tgt.active_flg
                ),
                (
                    SELECT
                        NVL (MAX(country_ky), 0) max_ky
                    FROM
                        dwh.dwh_d_country_lu
                ) _max,
                TABLE (getnextval (seq)) s
            WHERE
                tgt.country_id <> src.id
                OR tgt.country_desc <> src.country_desc
                OR tgt.country_desc IS NULL;
            """
        )

        cur.execute("COMMIT")
        print(f"Loaded 'tmp.tmp_d_country_lu' to 'dwh.dwh_d_country_lu'")
