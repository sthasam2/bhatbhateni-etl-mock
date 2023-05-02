UPDATE dwh.dwh_d_product_lu target
SET
    active_flg = FALSE,
    updated_ts = CURRENT_TIMESTAMP()
FROM
    tmp.extended_dwh_d_product_lu tgt
    LEFT JOIN tmp.extended_tmp_d_product_lu src ON tgt.product_id = src.id -- product id same
WHERE
    target.active_flg = TRUE -- active record
    AND (
        -- matched id, so description changed or subcategory changed
        target.product_id = src.id
        AND (
            tgt.product_desc <> src.product_desc -- desc change
            OR tgt.subcategory_id <> src.subcategory_id -- subcategory id changed
        ) -- nothing matching, so deleted
        OR (
            target.product_id = tgt.product_id
            AND src.id IS NULL -- deleted records
        )
    )