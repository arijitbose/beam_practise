# TODO: Importing standard packages
import logging

# TODO: Importing third party packages
import apache_beam as beam

logger = logging.getLogger(__name__)


class WriteToBiqueryTable(beam.PTransform):
    def __init__(self, project, dataset_name, table_name):
        self.project = project
        self.dataset_name = dataset_name
        self.table_name = table_name


    def expand(self, pcol):
        """Recieve the PCollection of renamed column rows and ingest the rows into bigquery.
        This will take project id, dataset name, table name as a parameter.
        Since same file is updated again and again Hence we need to use
        WRITE_TRUNCATE to overwrite the existing record from table.
        """

        pcol | beam.io.WriteToBigQuery(
            "{}:{}.{}".format(self.project, self.dataset_name, self.table_name),

            write_disposition="WRITE_APPEND",  
            create_disposition="CREATE_IF_NEEDED",
            schema="SCHEMA_AUTODETECT"
        )