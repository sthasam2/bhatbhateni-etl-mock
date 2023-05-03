import os

from etc.variables import *
from lib.utils import *

# _sales dimensions
# id, region_id, sales_desc


class SalesAgg:
    """
    Class for representing Sales Aggregation table
    """

    db_ctx: SnwfConn = None

    tmp_sales_agg_table = "tmp_f_sls_prod_mon_store_agg"
    tgt_sales_agg_table = "dwh_f_sls_prod_mon_store_agg"

    def __init__(self, db_ctx):
        self.db_ctx = db_ctx

    @start_end_decorator
    @try_except_decorator
    def transform_temp_tables(self):
        print("@TRANSFORM ", self.__class__.__name__)
        return self.__transform_sales_into_temp()

    @start_end_decorator
    @try_except_decorator
    def load_tmp_table_to_tgt(self):
        print("@LOAD ", self.__class__.__name__)
        return self.__load_sales_into_target()

    #############################
    #   PRIVATE FUNCTIONS
    #############################

    def __transform_sales_into_temp(self):
        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {TMP_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE IF EXISTS  {self.tmp_sales_agg_table}")
        cur.execute("BEGIN TRANSACTION")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     TMP_SCHEMA,
        #     self.tmp_sales_agg_table,
        #     """
        #     (
        #         sls_agg_ky varchar not null primary key,
        #         --
        #         store_ky int foreign key references dwh.dwh_d_store_lu(store_ky),
        #         store_id int foreign key references dwh.dwh_d_store_lu(store_id),
        #         --
        #         month_id varchar,
        #         year_id varchar,
        #         total_sales number(20,2)
        #     )
        #     """,
        # )

        drop_and_create_sequence(self.db_ctx, TMP_SCHEMA, "seq")

        cur.execute(
            f"""
            INSERT INTO {self.tmp_sales_agg_table}(
                sls_agg_ky,
                store_ky,
                store_id,
                product_ky,
                product_id,
                month_id,
                year_id,
                total_sales
            )
            SELECT
                s.nextval,
                src2.store_ky,
                src2.store_id,
                src3.product_ky,
                src3.product_id,
                month_id,
                year_id, 
                total_sales
            FROM
                (
                    SELECT
                        product_id,
                        store_id,
                        MONTH(transaction_time) month_id,
                        YEAR(transaction_time) year_id,
                        SUM(net_amount) total_sales
                    FROM
                        dwh.dwh_f_bhatbhateni_sls_trxn_b src
                    GROUP BY
                        product_id,
                        store_id,
                        month_id,
                        year_id
                    ORDER BY
                        year_id,
                        month_id,
                        store_id,
                        product_id
                ) src 
                INNER JOIN dwh.dwh_d_store_lu src2 on src.store_id = src2.store_id
                INNER JOIN dwh.dwh_d_product_lu src3 on src.product_id = src3.product_id
                , table(getnextval(seq)) s
            """
        )

        cur.execute("COMMIT")

        print(f"Transformed 'stg.stg_d_sales_lu' to 'tmp.tmp_d_sales_lu'")

    def __load_sales_into_target(self):
        """ """

        cur = self.db_ctx.cursor()
        cur.execute(f"USE SCHEMA {TGT_SCHEMA}")
        cur.execute("BEGIN TRANSACTION")

        # create_schema_table_if_not_exists(
        #     self.db_ctx,
        #     TGT_SCHEMA,
        #     self.tgt_sales_agg_table,
        #     """
        #     (
        #         sls_agg_ky varchar not null primary key,
        #         --
        #         store_ky int foreign key references dwh_d_store_lu(store_ky),
        #         store_id int foreign key references dwh_d_store_lu(store_id),
        #         month_id varchar,
        #         year_id varchar,
        #         total_sales number(20,2),
        #         --
        #         inserted_ts timestamp DEFAULT CURRENT_TIMESTAMP(),
        #     )
        #     """,
        # )

        # cur.execute(
        #     """
        #     UPDATE dwh.dwh_f_sls_prod_mon_store_agg target
        #     SET
        #         active_flg = FALSE,
        #         updated_ts = CURRENT_TIMESTAMP()
        #     FROM
        #         dwh.dwh_f_sls_prod_mon_store_agg tgt
        #         LEFT JOIN tmp.tmp_f_sls_prod_mon_store_agg src ON (
        #             tgt.sls_agg_ky = src.sls_agg_ky
        #         )
        #     WHERE
        #         target.active_flg = TRUE
        #         AND (
        #             (
        #               target.sls_agg_ky = src.sls_agg_ky
        #               AND tgt.total_sales <> src.total_sales
        #             )
        #             OR (
        #                 target.sls_agg_ky = tgt.sls_agg_ky
        #                 AND src.sls_agg_ky IS NULL
        #             )
        #         );
        #     """
        # )

        drop_and_create_sequence(self.db_ctx, TGT_SCHEMA, "seq")

        cur.execute(
            """
            INSERT INTO
                dwh.dwh_f_sls_prod_mon_store_agg (
                    sls_agg_ky,
                    store_ky,
                    store_id,
                    product_ky,
                    product_id,
                    month_id,
                    year_id,
                    total_sales
                )
            SELECT
                src.sls_agg_ky,
                src.store_ky,
                src.store_id,
                src.product_ky,
                src.product_id,
                src.month_id,
                src.year_id,
                src.total_sales
            FROM
                dwh.dwh_f_sls_prod_mon_store_agg tgt
                RIGHT JOIN tmp.tmp_f_sls_prod_mon_store_agg src ON tgt.sls_agg_ky = src.sls_agg_ky
            WHERE
                tgt.sls_agg_ky IS NULL
                OR (
                    tgt.sls_agg_ky = src.sls_agg_ky 
                    AND tgt.total_sales <> src.total_sales
                )
            """
        )

        cur.execute("COMMIT")
        print(
            f"Loaded 'tmp.tmp_f_sls_prod_mon_store_agg' to 'dwh.dwh_f_sls_prod_mon_store_agg'"
        )
