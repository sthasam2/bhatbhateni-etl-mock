import os

from etc.variables import *
from lib.utils import *

# _subcategory dimensions
# id, category_id, subcategory_desc


class Subcategory:
    """
    Class for representing Subcategory table
    """

    subcategory_csv_path: str = os.path.join(CSV_PATH, "subcategory.csv").replace(
        "\\", "/"
    )
    db_ctx: SnwfConn = None

    stg_subcategory_table = "stg_d_subcategory_lu"
    tmp_subcategory_table = "tmp_d_subcategory_lu"
    tgt_subcategory_table = "dwh_d_subcategory_lu"

    def __init__(self, db_ctx):
        self.db_ctx = db_ctx

    @start_end_decorator
    @try_except_decorator
    def put_csv_to_file_stage(self):
        print("@PUT ", self.__class__.__name__)
        return self.__put_subcategory_csv_to_file_stage()

    @start_end_decorator
    @try_except_decorator
    def extract_staged_csv_to_stg(self):
        print("@EXTRACT ", self.__class__.__name__)
        return self.__extract_subcategory_into_stg()

    @start_end_decorator
    @try_except_decorator
    def transform_stg_table_into_temp(self):
        print("@TRANSFORM ", self.__class__.__name__)
        return self.__transform_subcategory_into_temp()

    @start_end_decorator
    @try_except_decorator
    def load_tmp_table_to_tgt(self):
        print("@LOAD ", self.__class__.__name__)
        return self.__load_subcategory_into_target()

    #############################
    #   PRIVATE FUNCTIONS
    #############################

    def __put_subcategory_csv_to_file_stage(self):
        """ """

        export_csv_to_schema_file_stage(
            self.db_ctx,
            STAGE_SCHEMA,
            "file_stage",
            self.subcategory_csv_path,
        )

    def __extract_subcategory_into_stg(self):
        """ """

        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {STAGE_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE IF EXISTS  {self.stg_subcategory_table}")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     STAGE_SCHEMA,
        #     self.stg_subcategory_table,
        #     """
        #     (
        #         id int NOT NULL PRIMARY KEY,
        #         category_id int FOREIGN KEY REFERENCES stg_d_category_lu(id),
        #         subcategory_desc varchar
        #     )
        #     """,
        # )

        cur.execute(
            " COPY INTO stg_d_subcategory_lu "
            + " FROM @file_stage/subcategory.csv.gz"
            + " FILE_FORMAT = (FORMAT_NAME = 'csv_format')"
        )

        print(f"Extracted from 'file_stage' sucessfully to 'stg.stg_d_subcategory_lu'")

    def __transform_subcategory_into_temp(self):
        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {TMP_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE IF EXISTS  {self.tmp_subcategory_table}")
        cur.execute("BEGIN TRANSACTION")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     TMP_SCHEMA,
        #     self.tmp_subcategory_table,
        #     """
        #     (
        #         id int NOT NULL PRIMARY KEY,
        #         category_id int FOREIGN KEY REFERENCES tmp_d_category_lu(id),
        #         subcategory_desc varchar
        #     )
        #     """,
        # )

        cur.execute(
            f"""
            INSERT INTO {self.tmp_subcategory_table}(
                id,
                category_id,
                subcategory_desc
            )
            SELECT 
                id,
                category_id,
                subcategory_desc
            FROM 
                {STAGE_SCHEMA}.{self.stg_subcategory_table}
            """
        )
        cur.execute("COMMIT")

        print(f"Transformed 'stg.stg_d_subcategory_lu' to 'tmp.tmp_d_subcategory_lu'")

    def __load_subcategory_into_target(self):
        """ """

        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {TGT_SCHEMA}")
        cur.execute("BEGIN TRANSACTION")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     TGT_SCHEMA,
        #     self.tgt_subcategory_table,
        #     """
        #     (
        #         subcategory_ky int PRIMARY KEY,
        #         subcategory_id int UNIQUE NOT NULL,
        #         subcategory_desc varchar(1024),
        #         --
        #         category_ky int FOREIGN KEY REFERENCES dwh_d_category_lu(category_ky),
        #         --
        #         active_flg boolean default true,
        #         created_ts timestamp DEFAULT CURRENT_TIMESTAMP(),
        #         updated_ts timestamp DEFAULT CURRENT_TIMESTAMP()
        #     )
        #     """,
        # )

        # create views for ease
        cur.execute(
            """CREATE OR REPLACE TABLE
                    tmp.extended_dwh_d_subcategory_lu AS
                SELECT
                    cu.category_id,
                    src.*
                FROM
                    dwh.dwh_d_subcategory_lu src
                    INNER JOIN dwh.dwh_d_category_lu cu ON cu.category_ky = src.category_ky
                    and src.active_flg
            """
        )
        cur.execute(
            """
            CREATE OR REPLACE TABLE
                tmp.extended_tmp_d_subcategory_lu AS
            SELECT
                src.*,
                cu.category_ky category_ky
            FROM
                tmp.tmp_d_subcategory_lu src
                INNER JOIN dwh.dwh_d_category_lu cu ON cu.category_id = src.category_id
                and cu.active_flg
            """
        )
        cur.execute(
            """
            UPDATE dwh.dwh_d_subcategory_lu target
            SET
                active_flg = FALSE,
                updated_ts = CURRENT_TIMESTAMP()
            FROM
                tmp.extended_dwh_d_subcategory_lu tgt
                LEFT JOIN tmp.extended_tmp_d_subcategory_lu src ON
                    tgt.subcategory_id = src.id
            WHERE
                target.active_flg = TRUE 
                AND (
                    (
                        target.subcategory_id = src.id
                        AND (
                            tgt.subcategory_desc <> src.subcategory_desc 
                            OR tgt.category_id <> src.category_id
                            OR tgt.category_ky <> src.category_ky
                        ) 
                    )
                    OR (
                        target.subcategory_id = tgt.subcategory_id
                        AND src.id IS NULL 
                    )
                );
            """
        )

        drop_and_create_sequence(self.db_ctx, TGT_SCHEMA, "seq")

        cur.execute(
            """
            INSERT INTO
                dwh.dwh_d_subcategory_lu (subcategory_ky, subcategory_id, subcategory_desc, category_ky)
            SELECT
                _max.max_ky + s.nextval,
                src.id,
                src.subcategory_desc,
                src.category_ky
            FROM
                tmp.extended_dwh_d_subcategory_lu tgt
                RIGHT JOIN tmp.extended_tmp_d_subcategory_lu src ON 
                    tgt.subcategory_id = src.id -- tgt and src with subcategory id equal
                ,
                (SELECT NVL(MAX(category_ky), 0) max_ky from dwh.dwh_d_category_lu) _max,
                TABLE (getnextval (seq)) s
            WHERE
                tgt.subcategory_id <> src.id                 -- changed subcategory id
                OR tgt.category_id <> src.category_id     -- changed category id
                OR tgt.subcategory_desc <> src.subcategory_desc   -- changed subcategory desc
                OR tgt.subcategory_ky IS NULL;               -- new value                                      
        """
        )
        cur.execute("COMMIT")

        print(f"Loaded 'tmp.tmp_d_subcategory_lu' to 'dwh.dwh_d_subcategory_lu'")
