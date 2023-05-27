# Used to retrieve a Parquet file from Azure Blob Storage

# Import main libraries
from io import BytesIO
from azure.storage.blob import BlobServiceClient, BlobClient
import pyarrow.parquet as pq
import pandas as pd



def retrieve_parquet_file_from_blob_storage(container_name, blob_name, conn_string):
    # Create a BlobServiceClient using the provided connection string
    blob_service_client = BlobServiceClient.from_connection_string(conn_string)

    # Get a BlobClient for the specified container and blob name
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Download the blob's contents as a stream of bytes
    stream = blob_client.download_blob()

    # Create a BytesIO object to hold the stream of bytes
    stream_bytes = BytesIO()

    # Write the stream of bytes to the BytesIO object
    stream.download_to_stream(stream_bytes)

    # Create a PyArrow Table from the Parquet file bytes
    table = pq.read_table(stream_bytes)

    # Convert the Table to a pandas DataFrame
    df = table.to_pandas()

    # Return the DataFrame
    return df