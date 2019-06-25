from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class BigQueryToGoogleCloudStorageOperator(BaseOperator):
    """
    Fetches the data from a BigQuery table (alternatively fetch data for selected columns)
    and ingest data to a Google Cloud Storage bucket via a temporary BigQuery table.
    :param sql: The BigQuery SQL to execute (templated).
    :type sql: str
    :param destination_cloud_storage_uris: The destination Google Cloud
        Storage URI (e.g. gs://some-bucket/some-file.txt). (templated) Follows
        convention defined here:
        https://cloud.google.com/bigquery/exporting-data-from-bigquery#exportingmultiple
    :type destination_cloud_storage_uris: list
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud
        Platform.
    :type gcp_conn_id: str
    :param use_legacy_sql: Whether to use legacy SQL (true) or standard SQL (false).
    :type use_legacy_sql: bool
    :param bigquery_conn_id: reference to a specific BigQuery hook.
    :type bigquery_conn_id: str
    :param compression: Type of compression to use.
    :type compression: str
    :param export_format: File format to export.
    :type export_format: str
    :param field_delimiter: The delimiter to use when extracting to a CSV.
    :type field_delimiter: str
    :param print_header: Whether to print a header for a CSV file extract.
    :type print_header: bool
    :param tmp_dataset_table: A dotted
        ``(<project>.|<project>:)<dataset>.<table>`` that, will store the temporary results
        of the query. (templated)
    :type tmp_dataset_table: str
    :param allow_large_results: Whether to allow large results.
    :type allow_large_results: bool
    :param labels: a dictionary containing labels for the job/query,
        passed to BigQuery
    :type labels: dict
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    """
    template_fields = ('sql', 'tmp_dataset_table',
                       'destination_cloud_storage_uris', 'labels')

    template_ext = ('.sql',)
    ui_color = '#e4f0e8'

    @apply_defaults
    def __init__(self,
                 sql,
                 destination_cloud_storage_uris,
                 gcp_conn_id,
                 use_legacy_sql=False,
                 bigquery_conn_id='bigquery_default',
                 compression='NONE',
                 export_format='CSV',
                 field_delimiter=',',
                 print_header=True,
                 tmp_dataset_table=None,
                 allow_large_results=True,
                 labels=None,
                 delegate_to=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.destination_cloud_storage_uris = destination_cloud_storage_uris
        self.gcp_conn_id = gcp_conn_id
        self.use_legacy_sql= use_legacy_sql
        self.allow_large_results = allow_large_results
        self.tmp_dataset_table = tmp_dataset_table
        self.export_format = export_format
        self.compression = compression
        self.field_delimiter = field_delimiter
        self.print_header = print_header
        self.labels = labels
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to
        self.bq_cursor = None


    def execute(self, context):
        self._run_bq_query(context)
        self._export_to_cloud_storage(context)
        self._delete_tmp_table(context)

    def on_kill(self):
        super().on_kill()
        if self.bq_cursor is not None:
            self.log.info('Cancelling running query')
            self.bq_cursor.cancel_query()

    def _run_bq_query(self, context):
        self.log.info('Running BigQuery query: %s', self.sql)
        if self.bq_cursor is None:
            hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                                use_legacy_sql=self.use_legacy_sql,
                                delegate_to=self.delegate_to,
                                location=None)
            conn = hook.get_conn()
            self.bq_cursor = conn.cursor()

        job_id = self.bq_cursor.run_query(
            sql=self.sql,
            destination_dataset_table=self.tmp_dataset_table,
            write_disposition='WRITE_TRUNCATE',
            allow_large_results=self.allow_large_results,
            flatten_results=None,
            udf_config=None,
            maximum_billing_tier=None,
            maximum_bytes_billed=None,
            create_disposition='CREATE_IF_NEEDED',
            query_params=None,
            labels=self.labels,
            schema_update_options=(),
            priority='INTERACTIVE',
            time_partitioning=None,
            api_resource_configs=None,
            cluster_fields=None,
        )

        context['task_instance'].xcom_push(key='job_id', value=job_id)

    def _export_to_cloud_storage(self, context):
        self.log.info('Exporting from %s to %s', self.tmp_dataset_table, self.destination_cloud_storage_uris)
        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                            delegate_to=self.delegate_to)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.run_extract(
            source_project_dataset_table=self.tmp_dataset_table,
            destination_cloud_storage_uris=self.destination_cloud_storage_uris,
            compression=self.compression,
            export_format=self.export_format,
            field_delimiter=self.field_delimiter,
            print_header=self.print_header,
            labels=self.labels)

    def _delete_tmp_table(self, context):
        self.log.info('Deleting: %s', self.tmp_dataset_table)
        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                            delegate_to=self.delegate_to)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.run_table_delete(
            deletion_dataset_table=self.tmp_dataset_table,
            ignore_if_missing=True)

