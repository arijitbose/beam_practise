import apache_beam as beam
from apache_beam import pvalue
from apache_beam import Create,FlatMap,Map,ParDo,Flatten,Partition,Filter,CombinePerKey
from apache_beam import Values,CoGroupByKey
from apache_beam import pvalue,window,WindowInto
from apache_beam.transforms.combiners import Top, Mean, Count
from apache_beam.transforms import trigger
from apache_beam.options.pipeline_options import PipelineOptions,StandardOptions
from apache_beam.transforms.window import FixedWindows
import json
import logging
import os
import argparse
import typing
import json
import os
import argparse
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.convert import to_pcollection
import typing
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
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
             schema='ride_id:STRING, point_idx:INTEGER,latitude:FLOAT,longitude:FLOAT,timestamp:TIMESTAMP,meter_reading:FLOAT,ride_status:STRING,passenger_count:INTEGER',
            
        )    
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
    
class JobOptions(PipelineOptions):
    """Create arguments need in pipeline such as project id, dataset name, table name, input data in csv"""

    @classmethod
    def _add_argparse_args(cls, parser):
        # parser.add_argument("--input", type=str, help="Input Json File")

        parser.add_argument("--project_id", type=str, help="GCP project id")
        parser.add_argument("--dataset_name", type=str, help="BigQuery dataset name.")
        parser.add_argument("--table_name", type=str, help="Table name of dataset")   
def convert(element):
    keys=['ride_id','point_idx','latitude','longitude','meter_reading','meter_increment','time','ride_status','passenger_count']
    
    A={k:v for(k,v) in zip(keys,element)} 
    return A
     
def run(argv=None):
    parser=argparse.ArgumentParser()
    args,beam_args=parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(runner='DirectRunner',
        project='rich-sprite-350409',
        job_name='test5693',
        region='us-central1',
        temp_location='gs://apache4/temp',
        streaming=True,
        enable_streaming_engine=True,

        save_main_session=True,

        service_account_email='beampractise@rich-sprite-350409.iam.gserviceaccount.com')
    options = pipeline_options.view_as(JobOptions)    

    pipeline_options.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=pipeline_options) as p:

        topic = "projects/pubsub-public-data/topics/taxirides-realtime"

        
            
        (p | "Read Topic" >> beam.io.ReadFromPubSub(topic=topic)
        
         | "Json Loads" >> Map(json.loads) 
         | beam.Map(print)
        #  | 'WindowInto' >> beam.WindowInto(beam.window.FixedWindows(60))

        |"Writing Into BigQuery" >> WriteToBiqueryTable(options.project_id,options.dataset_name,options.table_name,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )
             
          
        
         
if __name__== "__main__":
    run()