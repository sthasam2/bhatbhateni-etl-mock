import os

from constant import *
from utils import (
    create_schema_table_if_not_exists,
    drop_and_create_sequence,
    export_csv_to_schema_file_stage,
    try_except_decorator,
)

# _product dimensions
# id, subcategory_id, product_desc


class Product:
    product_csv_path: str = os.path.join(CSV_PATH, "product.csv").replace("\\", "/")

    db_ctx: SnwfConn = None

    stg_product_table = "stg_d_product_lu"
    tmp_product_table = "tmp_d_product_lu"
    tgt_product_table = "dwh_d_product_lu"

    def __init__(self, db_ctx):
        self.db_ctx = db_ctx

    @try_except_decorator
    def put_csv_to_file_stage(self):
        print("@PUT ", self.__class__.__name__)
        return self.__put_product_csv_to_file_stage()

    @try_except_decorator
    def extract_staged_csv_to_stg(self):
        print("@EXTRACT ", self.__class__.__name__)
        return self.__extract_product_into_stg()

    @try_except_decorator
    def transform_stg_table_into_temp(self):
        print("@TRANSFORM ", self.__class__.__name__)
        return self.__transform_product_into_temp()

    @try_except_decorator
    def load_tmp_table_to_tgt(self):
        print("@LOAD ", self.__class__.__name__)
        return self.__load_product_into_target()

    #############################
    #   PRIVATE FUNCTIONS
    #############################

    def __put_product_csv_to_file_stage(self):
        """ """

        export_csv_to_schema_file_stage(
            self.db_ctx, STAGE_SCHEMA, "file_stage", self.product_csv_path
        )

    def __extract_product_into_stg(self):
        """ """

        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {STAGE_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE IF EXISTS  {self.stg_product_table}")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     STAGE_SCHEMA,
        #     "stg_d_product_lu",
        #     """
        #     (
        #         id int NOT NULL PRIMARY KEY,
        #         subcategory_id int FOREIGN KEY REFERENCES stg_d_subcategory_lu(id),
        #         product_desc varchar
        #     )
        #     """,
        # )

        cur.execute(
            " COPY INTO stg_d_product_lu "
            + " FROM @file_stage/product.csv.gz"
            + " FILE_FORMAT = (FORMAT_NAME = 'csv_format')"
        )

        print(f"Extracted from 'file_stage' sucessfully to 'stg.stg_d_customer_lu'")

    def __transform_product_into_temp(self):
        """ """

        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {TMP_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE IF EXISTS  {self.tmp_product_table}")
        cur.execute("BEGIN TRANSACTION")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     TMP_SCHEMA,
        #     self.tmp_product_table,
        #     """
        #     (
        #         id int NOT NULL PRIMARY KEY,
        #         subcategory_id int FOREIGN KEY REFERENCES tmp_d_subcategory_lu(id),
        #         product_desc varchar
        #     )
        #     """,
        # )

        cur.execute(
            f"""
            INSERT INTO {self.tmp_product_table}(
                id,
                subcategory_id,
                product_desc
            )
            SELECT 
                id,
                subcategory_id,
                product_desc
            FROM 
                {STAGE_SCHEMA}.{self.stg_product_table}
            """
        )
        cur.execute("COMMIT")

        print(f"Transformed 'stg.stg_d_product_lu' to 'tmp.tmp_d_product_lu'")

    def __load_product_into_target(self):
        """"""

        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {TGT_SCHEMA}")
        cur.execute("BEGIN TRANSACTION")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     TGT_SCHEMA,
        #     self.tgt_product_table,
        #     """
        #     (
        #         product_ky int PRIMARY KEY,
        #         product_id int UNIQUE NOT NULL,
        #         product_desc varchar(1024),
        #         --
        #         subcategory_ky int FOREIGN KEY REFERENCES dwh_d_subcategory_lu(subcategory_ky),
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
                tmp.extended_dwh_d_product_lu AS
            SELECT
                sub.subcategory_id,
                src.*
            FROM
                dwh.dwh_d_product_lu src
                INNER JOIN dwh.dwh_d_subcategory_lu sub ON sub.subcategory_ky = src.subcategory_ky
                and src.active_flg
            """
        )
        cur.execute(
            """
            CREATE OR REPLACE TABLE
                tmp.extended_tmp_d_product_lu AS
            SELECT
                src.*,
                sub.subcategory_ky subcategory_ky
            FROM
                tmp.tmp_d_product_lu src
                INNER JOIN dwh.dwh_d_subcategory_lu sub ON sub.subcategory_id = src.subcategory_id
                and sub.active_flg
            """
        )
        cur.execute(
            """
            UPDATE dwh.dwh_d_product_lu target
            SET
                active_flg = FALSE,
                updated_ts = CURRENT_TIMESTAMP()
            FROM
                tmp.extended_dwh_d_product_lu tgt
                LEFT JOIN tmp.extended_tmp_d_product_lu src ON tgt.product_id = src.id 
            WHERE
                target.active_flg = TRUE 
                AND (
                    target.product_id = src.id
                    AND (
                        tgt.product_desc <> src.product_desc 
                        OR tgt.subcategory_id <> src.subcategory_id 
                        OR tgt.subcategory_ky <> src.subcategory_ky 
                    ) 
                    OR (
                        target.product_id = tgt.product_id
                        AND src.id IS NULL
                    )
                );
            """
        )

        drop_and_create_sequence(self.db_ctx, TGT_SCHEMA, "seq")

        cur.execute(
            """
            INSERT INTO
                dwh.dwh_d_product_lu (product_ky, product_id, product_desc, subcategory_ky)
            SELECT
                _max.max_ky + s.nextval,
                src.id,
                src.product_desc,
                src.subcategory_ky
            FROM
                tmp.extended_dwh_d_product_lu tgt
                RIGHT JOIN tmp.extended_tmp_d_product_lu src ON (
                    tgt.product_id = src.id -- tgt and src with product id equal
                ),
                (SELECT NVL(MAX(product_ky), 0) max_ky from dwh.dwh_d_product_lu) _max,
                TABLE (getnextval (seq)) s
            WHERE
                tgt.product_id <> src.id
                OR tgt.subcategory_id <> src.subcategory_id
                OR tgt.subcategory_ky <> src.subcategory_ky
                OR tgt.product_desc <> src.product_desc
                OR tgt.product_ky IS NULL;
        """
        )

        print(f"Loaded 'tmp.tmp_d_product_lu' to 'dwh.dwh_d_product_lu'")
