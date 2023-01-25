import apache_beam as beam
from apache_beam import pvalue
from apache_beam import Create,FlatMap,Map,ParDo,Flatten,Partition
from apache_beam import Values,CoGroupByKey
from apache_beam import pvalue,window,WindowInto
from apache_beam.transforms import trigger
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib
elements=[
    {'name':'Rebecca',"sport":"Soccer"},
    {'name':'Tom',"sport":"Rugby"},
    {'name':'Guillen',"sport":"Volley"},
    {'name':'Ana',"sport":"Hockey"},
    {'name':'Alice',"sport":"Football"},
    {'name':'Kim',"sport":"Tennis"},


]
with beam.Pipeline() as p:


    elements_pcol =p | "Create" >> Create(elements)

    names= elements_pcol | "Names" >> Map(lambda x:x["name"])

    sports= elements_pcol | "Sports" >> Map(lambda x:x["sport"])

    names | beam.Map(print)