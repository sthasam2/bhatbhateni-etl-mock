import os

from etc.variables import *
from lib.utils import *

# _region dimensions
# id, country_id, region_desc


class Region:
    """
    Class for representing region table
    """

    region_csv_path: str = os.path.join(CSV_PATH, "region.csv").replace("\\", "/")
    db_ctx: SnwfConn = None

    stg_region_table = "stg_d_region_lu"
    tmp_region_table = "tmp_d_region_lu"
    tgt_region_table = "dwh_d_region_lu"

    def __init__(self, db_ctx):
        self.db_ctx = db_ctx

    @start_end_decorator
    @try_except_decorator
    def put_csv_to_file_stage(self):
        print("@PUT ", self.__class__.__name__)
        return self.__put_region_csv_to_file_stage()

    @start_end_decorator
    @try_except_decorator
    def extract_staged_csv_to_stg(self):
        print("@EXTRACT ", self.__class__.__name__)
        return self.__extract_region_into_stg()

    @start_end_decorator
    @try_except_decorator
    def transform_stg_table_into_temp(self):
        print("@TRANSFORM ", self.__class__.__name__)
        return self.__transform_region_into_temp()

    @start_end_decorator
    @try_except_decorator
    def load_tmp_table_to_tgt(self):
        print("@LOAD ", self.__class__.__name__)
        return self.__load_region_into_target()

    #############################
    #   PRIVATE FUNCTIONS
    #############################

    def __put_region_csv_to_file_stage(self):
        """ """

        export_csv_to_schema_file_stage(
            self.db_ctx, STAGE_SCHEMA, "file_stage", self.region_csv_path
        )

    def __extract_region_into_stg(self):
        """ """
        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {STAGE_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE IF EXISTS  {self.stg_region_table}")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     STAGE_SCHEMA,
        #     self.stg_region_table,
        #     """
        #     (
        #         id int NOT NULL PRIMARY KEY,
        #         country_id int FOREIGN KEY REFERENCES stg_d_country_lu(id),
        #         region_desc varchar
        #     )
        #     """,
        # )

        cur.execute(
            """ 
                COPY INTO stg_d_region_lu 
                FROM @file_stage/region.csv.gz
                FILE_FORMAT = (FORMAT_NAME = 'csv_format')
            """
        )

        print(f"Extracted from 'file_stage' sucessfully to 'stg.stg_d_region_lu'")

    def __transform_region_into_temp(self):
        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {TMP_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE IF EXISTS  {self.tmp_region_table}")
        cur.execute("BEGIN TRANSACTION")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     TMP_SCHEMA,
        #     self.tmp_region_table,
        #     """
        #     (
        #         id int NOT NULL PRIMARY KEY,
        #         country_id int FOREIGN KEY REFERENCES tmp_d_country_lu(id),
        #         region_desc varchar
        #     )
        #     """,
        # )

        cur.execute(
            f"""
            INSERT INTO {self.tmp_region_table}(
                id,
                country_id,
                region_desc
            )
            SELECT 
                id,
                country_id,
                region_desc            
            FROM 
                {STAGE_SCHEMA}.{self.stg_region_table}
            """
        )

        print(f"Transformed 'stg.stg_d_region_lu' to 'tmp.tmp_d_region_lu'")

    def __load_region_into_target(self):
        """ """

        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {TGT_SCHEMA}")
        cur.execute("BEGIN TRANSACTION")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     TGT_SCHEMA,
        #     self.tgt_region_table,
        #     """
        #     (
        #         region_ky int PRIMARY KEY,
        #         region_id int UNIQUE NOT NULL,
        #         region_desc varchar(1024),
        #         --
        #         country_ky int FOREIGN KEY REFERENCES dwh_d_country_lu(country_ky),
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
                tmp.extended_dwh_d_region_lu AS
            SELECT
                cu.country_id,
                src.*
            FROM
                dwh.dwh_d_region_lu src
                INNER JOIN dwh.dwh_d_country_lu cu ON cu.country_ky = src.country_ky 
                and src.active_flg
            """
        )
        cur.execute(
            """
            CREATE OR REPLACE TABLE
                tmp.extended_tmp_d_region_lu AS
            SELECT
                src.*,
                cu.country_ky country_ky
            FROM
                tmp.tmp_d_region_lu src
                INNER JOIN dwh.dwh_d_country_lu cu ON cu.country_id = src.country_id 
                and cu.active_flg
            """
        )
        cur.execute(
            """
            UPDATE dwh.dwh_d_region_lu target
            SET
                active_flg = FALSE,
                updated_ts = CURRENT_TIMESTAMP()
            FROM
                tmp.extended_dwh_d_region_lu tgt
                LEFT JOIN tmp.extended_tmp_d_region_lu src ON (
                    tgt.region_id = src.id 
                )
            WHERE
                target.active_flg = TRUE
                AND (
                    target.region_id = src.id
                    AND (
                        tgt.region_desc <> src.region_desc 
                        OR tgt.country_id <> src.country_id
                        OR tgt.country_ky <> src.country_ky
                    )  
                    OR (
                        target.region_id = tgt.region_id
                        AND src.id IS NULL 
                    )
                );
            """
        )

        drop_and_create_sequence(self.db_ctx, TGT_SCHEMA, "seq")

        cur.execute(
            """
            INSERT INTO
                dwh.dwh_d_region_lu (region_ky, region_id, region_desc, country_ky)
            SELECT
                _max.max_ky + s.nextval,
                src.id,
                src.region_desc,
                src.country_ky
            FROM
                tmp.extended_dwh_d_region_lu tgt
                RIGHT JOIN tmp.extended_tmp_d_region_lu src ON (
                    tgt.region_id = src.id
                ),
                (SELECT NVL(MAX(region_ky), 0) max_ky from dwh.dwh_d_region_lu) _max,
                TABLE (getnextval (seq)) s
            WHERE
                tgt.region_id <> src.id                 -- changed region id
                OR tgt.country_id <> src.country_id     -- changed country id
                OR tgt.country_ky <> src.country_ky
                OR tgt.region_desc <> src.region_desc   -- changed region desc
                OR tgt.region_ky IS NULL;               -- new value                                      
        """
        )
        cur.execute("COMMIT")

        print(f"Loaded 'tmp.tmp_d_region_lu' to 'dwh.dwh_d_region_lu'")
