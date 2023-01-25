import apache_beam as beam
from apache_beam import pvalue
from apache_beam import Create,FlatMap,Map,ParDo,Flatten,Partition
from apache_beam import Values,CoGroupByKey
from apache_beam import pvalue,window,WindowInto
from apache_beam.transforms import trigger
with beam.Pipeline() as p:
    even,odd =(
        p | "Create Numbers" >> Create(range(11))
        | "Odd or Even" >> Partition(lambda n,partitions: n%2,2)
        
    )
    even | beam.Map(print)