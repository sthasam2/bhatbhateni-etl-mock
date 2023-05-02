# cur = self.db_ctx.cursor()
#         cur.execute(f"USE SCHEMA {TGT_SCHEMA}")

#         create_schema_table_if_not_exists(
#             self.db_ctx,
#             TGT_SCHEMA,
#             self.tgt_customer_table,
#             """
#             (
#                 customer_ky int PRIMARY KEY,
#                 customer_id int UNIQUE NOT NULL,
#                 customer_first_name varchar(1024),
#                 customer_middle_name varchar(1024),
#                 customer_last_name varchar(1024),
#                 --
#                 active_flg boolean default true,
#                 created_ts timestamp DEFAULT CURRENT_TIMESTAMP(),
#                 updated_ts timestamp DEFAULT CURRENT_TIMESTAMP()

#             )
#             """,
#         )

#         cur.execute(
#             """
#             CREATE VIEW
#                 tmp.tmp_d_fullname_customer_lu AS
#             SELECT
#                 id,
#                 CONCAT_WS(
#                     ' ',
#                     NVL(customer_first_name, ''),
#                     NVL(customer_middle_name, ''),
#                     NVL(customer_last_name, '')
#                 ) customer_fullname,
#                 address
#             FROM
#                 tmp.tmp_d_customer_lu
#             """
#         )

#         cur.execute(
#             """
#             CREATE VIEW
#                 tmp.dwh_d_fullname_customer_lu AS
#             SELECT
#                 customer_ky,
#                 cutomer_id,
#                 CONCAT_WS(
#                     ' ',
#                     NVL(customer_first_name, ''),
#                     NVL(customer_middle_name, ''),
#                     NVL(customer_last_name, '')
#                 ) customer_fullname,
#                 address,
#                 --
#                 active_flg,
#                 created_ts,
#                 updated_ts
#             FROM
#                 dwh.dwh_d_customer_lu
#             """
#         )

#         cur.execute(
#             """
#             UPDATE dwh.dwh_d_customer_lu target
#             SET
#                 active_flg = FALSE,
#                 updated_ts = CURRENT_TIMESTAMP()
#             FROM
#                 dwh.dwh_d_fullname_customer_lu tgt
#                 LEFT JOIN tmp.tmp_d_fullname_customer_lu src ON (
#                     tgt.customer_id = src.id
#                     OR tgt.customer_fullname = src.customer_fullname
#                 )
#             WHERE
#                 target.active_flg = TRUE -- only active records
#                 AND (
#                     -- matched either id or desc
#                     (
#                         target.customer_id = src.id
#                         AND (
#                             target.customer_fullname <> src.customer_fullname -- change in desc
#                             OR target.address <> src.address
#                         )
#                     )
#                     OR (
#                         target.customer_fullname = src.customer_fullname
#                         AND target.address = src.address
#                         AND target.customer_id <> src.id -- change in id
#                     ) -- not matched ie deletec
#                     OR (
#                         target.customer_id = tgt.customer_id
#                         AND src.id IS NULL -- deleted
#                     )
#                 );
#             """
#         )

#         drop_and_create_sequence(self.db_ctx, TGT_SCHEMA, "seq")

#         # cur.execute(
#         #     """
#         #     INSERT INTO
#         #         dwh.dwh_d_customer_lu(customer_ky, customer_id, customer_desc)
#         #     SELECT
#         #         nvl(max(tgt.customer_ky) over(), 0) + s.nextval customer_ky,
#         #         src.id customer_id,
#         #         src.customer_desc customer_desc
#         #     FROM
#         #         (

#         #             dwh.dwh_d_customer_lu tgt
#         #             RIGHT JOIN tmp.tmp_d_customer_lu src ON (
#         #                 tgt.customer_id = src.id
#         #                 OR tgt.customer_desc = src.customer_desc
#         #             )
#         #         ),
#         #         TABLE(getnextval(seq)) s
#         #     WHERE
#         #         tgt.customer_id <> src.id
#         #         OR tgt.customer_desc <> src.customer_desc
#         #         OR tgt.customer_desc IS NULL;
#         #     """
#         # )

#         print(f"Loaded 'tmp.tmp_d_customer_lu' to 'dwh.dwh_d_customer_lu'")
