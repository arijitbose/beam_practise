import os
from google.cloud import bigquery
## Snippet for list all bucket names in GCS of a particular project
os.environ['GOOGLE_APPLICATION_CREDENTIALS']="credentials.json"

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set dataset_id to the ID of the dataset to create.
# dataset_id = "{}.your_dataset".format(client.project)

# Construct a full Dataset object to send to the API.
dataset = bigquery.Dataset('rich-sprite-350409.bigquery_practise2')

# TODO(developer): Specify the geographic location where the dataset should reside.
dataset.location = "US"

# Send the dataset to the API for creation, with an explicit timeout.
# Raises google.api_core.exceptions.Conflict if the Dataset already
# exists within the project.
dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
print("Created dataset {}.{}".format(client.project, dataset.dataset_id))