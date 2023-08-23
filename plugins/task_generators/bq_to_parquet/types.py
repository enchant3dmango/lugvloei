from string import Template

BQ_TABLE_EXTRACT_QUERY = Template("""SELECT $fields FROM `$bq_table_name`""")
