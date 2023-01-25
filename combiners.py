import apache_beam as beam
from apache_beam import pvalue
from apache_beam import Create,FlatMap,Map,ParDo,Flatten,Partition
from apache_beam import Values,CoGroupByKey
from apache_beam import pvalue,window,WindowInto
from apache_beam.transforms.combiners import Top, Mean, Count
from apache_beam.transforms import trigger
elements = [
    {"country": "China", "population": 1389, "continent": "Asia"},
    {"country": "India", "population": 1311, "continent": "Asia"},
    {"country": "Japan", "population": 126, "continent": "Asia"},        
    {"country": "USA", "population": 331, "continent": "America"},
    {"country": "Ireland", "population": 5, "continent": "Europe"},
    {"country": "Indonesia", "population": 273, "continent": "Asia"},
    {"country": "Brazil", "population": 212, "continent": "America"},
    {"country": "Egypt", "population": 102, "continent": "Africa"},
    {"country": "Spain", "population": 47, "continent": "Europe"},
    {"country": "Ghana", "population": 31, "continent": "Africa"},
    {"country": "Australia", "population": 25, "continent": "Oceania"},
]
with beam.Pipeline() as p:
    create = (p | "Create" >> Create(elements)
            | "Map Keys" >> Map(lambda x: (x['continent'], x['population'])))

    element_count_total = create | "Total Count" >> Count.Globally()


    element_count_grouped = create | "Count Per Key" >> Count.PerKey()

    top_grouped = create | "Top" >> Top.PerKey(n=2) # We get the top 2

    mean_grouped = create | "Mean" >> Mean.PerKey()


    # element_count_total | beam.Map(print)

    element_count_grouped | beam.Map(print)