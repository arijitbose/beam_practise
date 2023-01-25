
import apache_beam as beam
import pandas as pd
from apache_beam.transforms.sql import SqlTransform
from apache_beam.dataframe.transforms import DataframeTransform
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.convert import to_pcollection
import typing
import json
import os
import argparse
# from writetobq import WriteToBiqueryTable
from apache_beam.options.pipeline_options import PipelineOptions
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="credentials.json"


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
class JobOptions(PipelineOptions):
    """Create arguments need in pipeline such as project id, dataset name, table name, input data in csv"""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input", type=str, help="Input Json File")

        parser.add_argument("--project_id", type=str, help="GCP project id")
        parser.add_argument("--dataset_name", type=str, help="BigQuery dataset name.")
        parser.add_argument("--table_name", type=str, help="Table name of dataset")        
class TaxiPoints(typing.NamedTuple):
    ride_id:str
    point_idx:int
    latitude:float
    longitude:float
    timestamp:str
    meter_reading:float
    meter_increment:float
    ride_status:str
    passenger_count:int
beam.coders.registry.register_coder(TaxiPoints,beam.coders.RowCoder)
def convert(element):
    keys=['time','ride_status','greeting']
    
    A={k:v for(k,v) in zip(keys,element)} 
    return A 
def run(argv=None):
    parser=argparse.ArgumentParser()
    args,beam_args=parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(beam_args, save_main_session=True)
    options = pipeline_options.view_as(JobOptions)

    with beam.Pipeline(options=pipeline_options) as p:
        taxipoints=     (
            p | "ReadFile" >>beam.io.ReadFromText(options.input)
            | beam.Map(json.loads)
            |beam.Map(lambda x: TaxiPoints(**x)).with_output_types(TaxiPoints))
        greetings = ( p| beam.Create([
                ('pickup','On my way!'),
                ('dropoff','I m here!')
            ])| beam.Map(lambda x : beam.Row(ride_status=x[0],message=x[1])))

        ({'taxi': taxipoints,'greetings':greetings}
            
            | SqlTransform("""
              SELECT
               `timestamp`,
               taxi.ride_status,
               greetings.message AS greeting
               FROM taxi
               INNER JOIN greetings USING (ride_status)""")

             | beam.Map(convert)
            |  "Writing Into BigQuery" >> WriteToBiqueryTable(options.project_id,options.dataset_name,
                  options.table_name))    
if  __name__=="__main__":
    # logging.getLogger().setLevel(logging.WARNING)
    run()                 
