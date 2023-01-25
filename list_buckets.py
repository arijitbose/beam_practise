import os
from google.cloud import storage
## Snippet for list all bucket names in GCS of a particular project
os.environ['GOOGLE_APPLICATION_CREDENTIALS']="credentials.json"
def list_buckets():
    """Lists all buckets."""

    storage_client = storage.Client()
    buckets = storage_client.list_buckets()

    for bucket in buckets:
        print(bucket.name)
list_buckets()