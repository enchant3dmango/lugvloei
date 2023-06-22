class Config:
    """
        Object of config from airflow variable
    """    

    def __init__(self, config: dict) -> None:
        """Constructs all the necessary attributes for the Config object

        Arguments:
            config {dict} -- table configuration of variable config
        
        Sample:
            Config(config)
        """

        self.schedule_interval          : str                          = config.pop('schedule_interval')                   # required -- schedule interval of DAG
        self.max_thread_workers         : int                          = config.pop('max_thread_workers')                  # required -- max thread workers of ThreadPoolExecutor
        self.gcp_connection             : str                          = config.pop('gcp_connection')                      # required -- GCP service account on airflow connection
        self.gcs_bucket                 : str                          = config.pop('gcs_bucket')                          # required -- GCS bucket for store extracted data
        self.bq_dataset                 : str                          = config.pop('bq_dataset')                          # required -- bigquery dataset as destination of extracted data
        self.default_source_connection  : str                          = config.pop('default_source_connection', None)     # optional -- default source_connection if source_connection doesn't exist in table config. null is the default value
        self.default_source_schema      : str                          = config.pop('default_source_schema', None)         # optional -- default default_source_schema if default_source_schema doesn't exist in table config. null is the default value
 
        # config validation
        assert len(config) == 0, config # error if there are another keys so can not exist wrong keys and additional keys

        # assign table object per table config
        default_config = {
            'default_source_connection': self.default_source_connection,
            'default_source_schema'    : self.default_source_schema,
            'default_chunksize'        : self.default_chunksize,
            'default_pool'             : self.default_pool,
        }