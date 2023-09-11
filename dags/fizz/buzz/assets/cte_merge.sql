(
WITH
latest AS (
SELECT
    DISTINCT *
FROM
    `{full_target_bq_table_temp}` t QUALIFY 1 = ROW_NUMBER() OVER win
WINDOW
    win AS (
    PARTITION BY
    TO_JSON_STRING( (
        SELECT
        AS STRUCT * EXCEPT (created_at, updated_at, load_timestamp)
        FROM
        UNNEST ([t]) ) )
    ORDER BY
    created_at, updated_at, load_timestamp DESC ) )
SELECT
*
FROM (
SELECT
    DISTINCT *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC ) AS rownum
FROM
    latest )
WHERE
rownum = 1 )