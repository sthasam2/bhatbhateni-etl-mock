import os

from constant import *
from utils import (
    create_schema_table_if_not_exists,
    drop_and_create_sequence,
    export_csv_to_schema_file_stage,
    try_except_decorator,
)

# _sales dimensions
# id, region_id, sales_desc


class Sales:
    """
    Class for representing Sales Transaction Base table
    """

    sales_csv_path: str = os.path.join(CSV_PATH, "sales.csv").replace("\\", "/")
    db_ctx: SnwfConn = None

    stg_sales_table = "stg_f_bhatbhateni_sls_trxn_b"
    tmp_sales_table = "tmp_f_bhatbhateni_sls_trxn_b"
    tgt_sales_table = "dwh_f_bhatbhateni_sls_trxn_b"

    def __init__(self, db_ctx):
        self.db_ctx = db_ctx

    @try_except_decorator
    def put_csv_to_file_stage(self):
        print("@PUT ", self.__class__.__name__)
        return self.__put_sales_csv_to_file_stage()

    @try_except_decorator
    def extract_staged_csv_to_stg(self):
        print("@EXTRACT ", self.__class__.__name__)
        return self.__extract_sales_into_stg()

    @try_except_decorator
    def transform_stg_table_into_temp(self):
        print("@TRANSFORM ", self.__class__.__name__)
        return self.__transform_sales_into_temp()

    @try_except_decorator
    def load_tmp_table_to_tgt(self):
        print("@LOAD ", self.__class__.__name__)
        return self.__load_sales_into_target()

    #############################
    #   PRIVATE FUNCTIONS
    #############################

    def __put_sales_csv_to_file_stage(self):
        """ """

        export_csv_to_schema_file_stage(
            self.db_ctx,
            STAGE_SCHEMA,
            "file_stage",
            self.sales_csv_path,
        )

    def __extract_sales_into_stg(self):
        """ """
        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {STAGE_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE IF EXISTS  {self.stg_sales_table}")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     STAGE_SCHEMA,
        #     self.stg_sales_table,
        #     """
        #     (
        #         id int not null primary key,
        #         store_id int foreign key references stg_d_store_lu(id),
        #         product_id int foreign key references stg_d_product_lu(id),
        #         customer_id int foreign key references stg_d_customer_lu(id),
        #         transaction_time timestamp_ntz,
        #         quantity number(38,0),
        #         amount number(20,2),
        #         discount number(20,2)
        #     )
        #     """,
        # )

        cur.execute(
            """ 
            copy into stg_f_bhatbhateni_sls_trxn_b 
            from @file_stage/sales.csv.gz
            file_format = (format_name = 'csv_format')
            """
        )

        print(
            f"Extracted from 'file_stage' sucessfully to 'stg.stg_f_bhatbhateni_sls_trxn_b'"
        )

    def __transform_sales_into_temp(self):
        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {TMP_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE IF EXISTS  {self.tmp_sales_table}")
        cur.execute("BEGIN TRANSACTION")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     TMP_SCHEMA,
        #     self.tmp_sales_table,
        #     """
        #     (
        #         id int not null primary key,
        #         store_id int foreign key references tmp_d_store_lu(id),
        #         product_id int foreign key references tmp_d_product_lu(id),
        #         customer_id int foreign key references tmp_d_customer_lu(id),
        #         transaction_time timestamp_ntz(9),
        #         quantity number(38,0),
        #         amount number(20,2),
        #         discount number(20,2)
        #     )
        #     """,
        # )

        cur.execute(
            f"""
            INSERT INTO {self.tmp_sales_table}(
                id,
                store_id,
                product_id,
                customer_id,
                transaction_time,
                quantity,
                amount,
                discount
            )
            SELECT 
                id,
                store_id,
                product_id,
                customer_id,
                transaction_time,
                nvl(quantity, 0),
                nvl(amount, 0),
                nvl(discount, 0)
            FROM 
                {STAGE_SCHEMA}.{self.stg_sales_table}
            """
        )

        cur.execute(
            f"""
            CREATE OR REPLACE TABLE tmp.extended_tmp_f_sales_trxn_b AS
            SELECT 
                src.*, 
                src.amount - src.discount net_amount,
                store_src.store_ky store_ky,
                customer_src.customer_ky customer_ky,
                product_src.product_ky product_ky
            FROM 
                {TMP_SCHEMA}.{self.tmp_sales_table} src
                LEFT JOIN {TGT_SCHEMA}.dwh_d_store_lu store_src ON (src.store_id = store_src.store_id AND store_src.active_flg)
                LEFT JOIN {TGT_SCHEMA}.dwh_d_customer_lu customer_src ON (src.customer_id = customer_src.customer_id AND customer_src.active_flg)
                LEFT JOIN {TGT_SCHEMA}.dwh_d_product_lu product_src ON (src.product_id = product_src.product_id AND product_src.active_flg)
            """
        )

        cur.execute("COMMIT")

        print(
            f"Transformed 'stg.stg_f_bhatbhateni_sls_trxn_b' to 'tmp.tmp_f_bhatbhateni_sls_trxn_b'"
        )

    def __load_sales_into_target(self):
        """ """

        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {TGT_SCHEMA}")
        cur.execute("BEGIN TRANSACTION")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     TGT_SCHEMA,
        #     self.tgt_sales_table,
        #     """
        #     (
        #         sales_txn_id int not null primary key,
        #         --
        #         store_ky int foreign key references dwh_d_store_lu(store_ky),
        #         store_id int foreign key references dwh_d_store_lu(store_id),
        #         product_ky int foreign key references dwh_d_product_lu(product_ky),
        #         product_id int foreign key references dwh_d_product_lu(product_id),
        #         customer_ky int foreign key references dwh_d_customer_lu(customer_ky),
        #         customer_id int foreign key references dwh_d_customer_lu(customer_id),
        #         --
        #         day_ky varchar,
        #         --
        #         transaction_time timestamp_ntz(9),
        #         quantity number(38,0),
        #         amount number(20,2),
        #         discount number(20,2),
        #         net_amount number(20,2),
        #         --
        #         inserted_ts timestamp DEFAULT CURRENT_TIMESTAMP(),
        #     )
        #     """,
        # )

        # cur.execute(
        #     f"""
        #     UPDATE {TGT_SCHEMA}.{self.tgt_sales_table} target
        #     SET
        #         active_flg = FALSE,
        #         updated_ts = CURRENT_TIMESTAMP()
        #     FROM
        #         {TGT_SCHEMA}.{self.tgt_sales_table} tgt
        #         LEFT JOIN tmp.extended_tmp_f_sales_trxn_b src ON (
        #             tgt.sales_txn_id = src.id
        #         )
        #     WHERE
        #             (
        #             target.sales_txn_id = src.id
        #                 AND (
        #                     nvl(tgt.customer_id, -1) <> nvl(src.customer_id, -1)
        #                     OR nvl(tgt.product_id, -1) <> nvl(src.product_id, -1)
        #                     OR nvl(tgt.store_id, -1) <> nvl(src.store_id, -1)
        #                     --
        #                     OR tgt.transaction_time <> src.transaction_time
        #                     OR tgt.discount <> src.discount
        #                     OR tgt.quantity <> src.quantity
        #                     OR tgt.amount <> src.amount
        #                 )
        #             )
        #             OR (
        #                 target.sales_txn_id = tgt.sales_txn_id
        #                 AND src.id IS NULL
        #             );
        #     """
        # )

        drop_and_create_sequence(self.db_ctx, TGT_SCHEMA, "seq")

        cur.execute(
            f"""
                INSERT INTO
                    {TGT_SCHEMA}.{self.tgt_sales_table}(
                        sales_txn_id,
                        --
                        store_ky,
                        store_id,
                        product_ky,
                        product_id,
                        customer_ky,
                        customer_id,
                        --
                        day_ky,
                        -- 
                        transaction_time,
                        quantity,
                        amount,
                        discount,
                        net_amount
                    )
                SELECT
                    src.id,
                    --
                    src.store_ky,
                    src.store_id,
                    src.product_ky,
                    src.product_id,
                    src.customer_ky,
                    src.customer_id,
                    --
                    TO_DATE (src.transaction_time) date_id,
                    -- 
                    src.transaction_time,
                    src.quantity,
                    src.amount,
                    src.discount,
                    src.net_amount
                FROM
                   {TGT_SCHEMA}.{self.tgt_sales_table} tgt
                    RIGHT JOIN tmp.extended_tmp_f_sales_trxn_b src ON tgt.sales_txn_id = src.id
                WHERE
                    tgt.sales_txn_id IS NULL
                    --
                    OR nvl(tgt.store_id, -1) <> nvl(src.store_id, -1)
                    OR nvl(tgt.product_id, -1) <> nvl(src.product_id, -1)
                    OR nvl(tgt.customer_id, -1) <> nvl(src.customer_id, -1)
                    --
                    OR tgt.quantity <> src.quantity
                    OR tgt.amount <> src.amount
                    OR tgt.discount <> src.discount;                      
            """
        )

        cur.execute("COMMIT")
        print(f"Loaded 'tmp.tmp_f_sales_b' to 'dwh.dwh_f_bhatbhateni_sls_trxn_b'")
