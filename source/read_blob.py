import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Get your Azure Blob Storage account URL and container name
storage_account = "datalake01course"
account_url = f"https://{storage_account}.blob.core.windows.net"
container_name = "demo"
blob_name = "circuits.csv"  # The name of the CSV file

# Create a BlobServiceClient using DefaultAzureCredential
# DefaultAzureCredential uses environment variables, managed identities, or shared token cache for authentication

token = "tulISx3BX+MEQHPEEEKX0y7K4HhaD4qMThpe34voXCAhjeZXpb9W24Mrc72YOh3/W4VXxNua5x2j+AStRqyVnw=="
credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(account_url=account_url, credential=token)

# Create a ContainerClient to work with a specific container
container_client = blob_service_client.get_container_client(container_name)

# List the blobs in the container
blob = container_client.list_blobs()

# Iterate through the blobs and find the CSV file
for blob in blob:
    print(blob.name)

# Get a BlobClient for the CSV file
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

import io

# Download the CSV file to a bytes buffer # in temporary memory
download_stream = blob_client.download_blob()
csv_data = download_stream.readall()

# Read the CSV data from the buffer into a Pandas DataFrame
df = pd.read_csv(io.BytesIO(csv_data))