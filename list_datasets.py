import os
from google.cloud import bigquery
## Snippet for list all bucket names in GCS of a particular project
os.environ['GOOGLE_APPLICATION_CREDENTIALS']="credentials.json"


# Construct a BigQuery client object.
client = bigquery.Client()

datasets = list(client.list_datasets())  # Make an API request.
project = client.project

if datasets:
    print("Datasets in project {}:".format(project))
    for dataset in datasets:
        print("\t{}".format(dataset.dataset_id))
else:
    print("{} project does not contain any datasets.".format(project))