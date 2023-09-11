from string import Template

SOURCE_INFORMATION_SCHEMA_QUERY = Template("""SELECT column_name AS name,
  data_type AS type,
  'NULLABLE' AS mode
FROM information_schema.columns
  WHERE table_name = '$source_table'
  AND table_schema = '$source_schema'""")

SOURCE_EXTRACT_QUERY = Template(
    """SELECT $selected_fields, $load_timestamp AS load_timestamp FROM $source_table_name""")

UPSERT_QUERY = Template("""MERGE
  `$merge_target` AS x
USING `$merge_source` AS y
  ON $on_keys
  $partition_filter
WHEN MATCHED THEN
  UPDATE SET $update_fields
WHEN NOT MATCHED THEN
  INSERT ($insert_fields) VALUES ($insert_fields);
""")

DELSERT_QUERY = Template("""MERGE
  `$merge_target` AS x
USING `$merge_source` AS y
  ON $on_keys
  $partition_filter
WHEN MATCHED THEN
  DELETE
WHEN NOT MATCHED THEN
  INSERT ($insert_fields) VALUES ($insert_fields);
""")

TEMP_TABLE_PARTITION_DATE_QUERY = Template("""DECLARE
  formatted_dates ARRAY<DATE>;
EXECUTE IMMEDIATE
  (
  SELECT
    CONCAT('SELECT [', ARRAY_TO_STRING(ARRAY(
        SELECT
          CONCAT("DATE '", FORMAT_DATE('%Y-%m-%d', flattened_dates_array), "'")
        FROM (
          SELECT
            DISTINCT DATE($partition_key) AS flattened_dates_array
          FROM
            `$target_bq_table_temp` ) AS t
        ORDER BY
          flattened_dates_array ), ', '), '] AS formatted_dates') );
""")