from etc.variables import *
from lib.utils import *


class Year:
    """"""

    db_ctx: SnwfConn = None
    tgt_year_table = "dwh_d_year_lu"

    def __init__(self, ctx: SnwfConn):
        self.db_ctx = ctx
        # self.load_year_table_to_tgt()

    @try_except_decorator
    def load_year_table_to_tgt(self):
        print("@LOAD ", self.__class__.__name__)

        cur = self.db_ctx.cursor()

        create_schema_table_if_not_exists(
            self.db_ctx,
            TGT_SCHEMA,
            self.tgt_year_table,
            """
            (
                year_ky int NOT NULL PRIMARY KEY,
                year_desc varchar,
                --
                inserted_ts timestamp DEFAULT CURRENT_TIMESTAMP()
            )
            """,
        )

        cur.execute("CREATE OR REPLACE SEQUENCE year START = 1")

        cur.execute(
            """
            DECLARE
                counter number(4,0) default 1;
            BEGIN
                WHILE (counter <= 130) DO
                    INSERT INTO dwh.dwh_d_year_lu(year_ky, year_desc)
                    VALUES (counter, 1970 + counter);
                    counter := counter +1;
                END WHILE;
            END
            """
        )


class HalfYear:
    """"""

    pass


class Quarter:
    """"""

    pass


class Month:
    """"""

    pass


class Day:
    """"""

    pass
