# Query
SOURCE_INFORMATION_SCHEMA_QUERY = """SELECT column_name AS name,
  data_type AS type,
  'NULLABLE' AS mode
FROM information_schema.columns
  WHERE table_name = '{}'
  AND table_schema = '{}'"""

SOURCE_EXTRACT_QUERY = """SELECT {load_timestamp} AS load_timestamp,
  {selected_fields}
FROM {source_schema}.{source_table_name}"""

UPSERT_QUERY = """MERGE
  `{target_bq_table}` AS x
USING `{target_bq_table_temp}` AS y
ON
  {on_keys}
  WHEN MATCHED THEN
    UPDATE SET {update_fields}
  WHEN NOT MATCHED THEN
    INSERT ({insert_fields}) VALUES ({insert_fields})"""

DELSERT_QUERY = """MERGE
  `{target_bq_table}` AS x
USING `{target_bq_table_temp}` AS y
  ON {on_keys}
WHEN MATCHED {audit_condition} THEN
  DELETE
WHEN NOT MATCHED THEN
  INSERT ({insert_fields}) VALUES ({insert_fields})"""

TEMP_TABLE_PARTITION_DATE_QUERY = """DECLARE
  formatted_dates ARRAY<TIMESTAMP>;
EXECUTE IMMEDIATE
  (
  SELECT
    CONCAT('SELECT [', ARRAY_TO_STRING(ARRAY(
        SELECT
          CONCAT('TIMESTAMP "', FORMAT_TIMESTAMP('%Y-%m-%d', flattened_dates_array), '"')
        FROM (
          SELECT
            DISTINCT DATE({partition_key}) AS flattened_dates_array
          FROM
            `{target_bq_table_temp}` ) AS t
        ORDER BY
          flattened_dates_array), ', '),'] AS formatted_dates')) INTO formatted_dates;

"""