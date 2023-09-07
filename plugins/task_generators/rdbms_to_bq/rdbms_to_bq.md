### RDBMSToBQGenerator 

#### Introduction
RDBMSToBQGenerator is designed to generate tasks for transferring data from a relational database management system (RDBMS) to Google BigQuery (BQ). It supports both PostgreSQL and MySQL as source databases and provides options for different data loading methods like TRUNCATE, UPSERT and DELSERT. The class leverages Apache Airflow and Kubernetes for task execution.
The generator is responsible for generating tasks that facilitate the data transfer process from an RDBMS to BigQuery. It does so by generating SQL queries for extracting data from the source RDBMS, transforming it, and then loading it into the target BigQuery table. The class encapsulates the logic needed for schema generation, query generation, and task generation.

#### Constructor arguments
**dag_id**: The ID of the Directed Acyclic Graph (DAG) to which the generated tasks belong.
**config**: A dictionary containing configuration parameters for the data transfer process.
****kwargs**: Additional keyword arguments.

#### Methods
- **__generate_schema**(self, **kwargs) -> list
Generates the schema for the target BigQuery table based on configuration and an optional schema definition file.

- **__generate_extract_query**(self, schema: list, **kwargs) -> str
Generates the SQL query to extract data from the source RDBMS. Supports filtering based on timestamp keys and data load methods.

- **__generate_merge_query**(self, schema, **kwargs) -> str
Generates the SQL query for merging extracted data into the target BigQuery table. Supports both UPSERT and DELSERT loading methods.

- **__get_conn**(self, **kwargs) -> str
Retrieves the connection details for the source RDBMS.

- **__generate_jdbc_uri**(self, **kwargs) -> str
Generates the JDBC URI for connecting to the source RDBMS.

- **__generate_jdbc_url**(self, **kwargs) -> str
Generates the full JDBC URL for connecting to the source RDBMS, including any additional parameters.

- **__generate_jdbc_credential**(self, **kwargs) -> List[str]
Generates the JDBC credential string for authentication.

- **__generate_jdbc_urlencoded_extra**(self, **kwargs)
Generates the URL-encoded extra connection parameters for the JDBC URL.

- **generate_tasks**(self)
Generates and returns Apache Airflow tasks for executing the data transfer process. Tasks include the **SparkKubernetesOperator** for submitting the data transfer job and the **SparkKubernetesSensor** for monitoring job completion.