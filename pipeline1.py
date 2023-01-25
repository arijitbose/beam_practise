import logging
import argparse
from datetime import datetime
import apache_beam as beam
import requests
import os
# from writetobq import WriteToBiqueryTable
from apache_beam.options.pipeline_options import PipelineOptions
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="rich-sprite-350409-99427d4b52cb.json"


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

def parse_lines(element):
    return element.split(",")
class JobOptions(PipelineOptions):
    """Create arguments need in pipeline such as project id, dataset name, table name, input data in csv"""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--project_id", type=str, help="GCP project id")
        parser.add_argument("--dataset_name", type=str, help="BigQuery dataset name.")
        parser.add_argument("--table_name", type=str, help="Table name of dataset")
        parser.add_argument("--input_data", type=str, help="Dataset in csv format")
        

class CalcVisitDuration(beam.DoFn):
    def process(self,element):
        from datetime import datetime
        dt_format= "%Y-%m-%dT%H:%M:%S"
        start_dt=datetime.strptime(element[1],dt_format)
        end_dt=datetime.strptime(element[2],dt_format) 
        diff= end_dt-start_dt
        yield [element[0],diff.total_seconds()]


class GetIpCountryOrigin(beam.DoFn):
    def process(self,element):
        import requests
        ip=element[0]  
        response=requests.get(f"http://ip-api.com/json/{ip}?fields=country") 
        country =response.json()["country"] 
        yield [ip,country]  


def map_country_to_ip(element,ip_map):
    ip=element[0]
    return [ip_map[ip],element[1]]  

def convert(element):
    keys=['country','time']
    
    A={k:v for(k,v) in zip(keys,element)}  
    return A
def run(argv=None):
    parser=argparse.ArgumentParser()
    args,beam_args=parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(beam_args, save_main_session=True)
    options = pipeline_options.view_as(JobOptions)

    with beam.Pipeline(options=pipeline_options) as p:
        lines =(
            p | "ReadFile" >>beam.io.ReadFromText(options.input_data,skip_header_lines=1)
            | "ParseLines" >>beam.Map(parse_lines)
            )

        duration =lines | "CalcVisitDuration" >>  beam.ParDo(CalcVisitDuration()) 
        ip_map =lines | "GetIpCountryOrigin" >> beam.ParDo(GetIpCountryOrigin())

        result =( duration | "MapIpToCountry" >>beam.Map(map_country_to_ip,ip_map=beam.pvalue.AsDict(ip_map))
                            | "AverageByCountry" >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
                             | "FormatOutput" >> beam.Map(convert)
                             
        )
                            #  | "FormatOutput" >> beam.Map(lambda element : ",".join(map(str,element)))
                             
        # result | beam.Map(print)   
        
        result          | "Writing Into BigQuery" >> WriteToBiqueryTable(options.project_id,options.dataset_name,
                  options.table_name,
          )
            



if __name__=="__main__":
    logging.getLogger().setLevel(logging.WARNING)
    run()