SOURCE_INFORMATION_SCHEMA_QUERY = "SELECT column_name AS name, data_type AS type, 'NULLABLE' AS mode FROM information_schema.columns WHERE table_name = '{}' AND table_schema = '{}'"
SOURCE_EXTRACT_QUERY = "SELECT {selected_fields}, {load_timestamp} AS load_timestamp FROM {source_schema}.{source_table_name}"
UPSERT_QUERY = "MERGE `{target_bq_table}` x USING `{target_bq_table_temp}` y ON {on_keys} WHEN MATCHED THEN UPDATE SET {merge_fields} WHEN NOT MATCHED THEN INSERT ({fields}) VALUES ({fields})"
