import requests
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO
from azure.storage.blob import BlobServiceClient, BlobClient

def extract_to_parquet(url):
    # Retrieve the TSV file from the URL
    response = requests.get(url)
    data = response.text
    
    # Convert the string data to a BytesIO object
    bytes_data = data.encode('utf-8')
    file_obj = BytesIO(bytes_data)

    # Load the data into a pandas DataFrame
    df = pd.read_csv(file_obj, delimiter='\t')

    # Create a BytesIO object to hold the parquet data
    parquet_file = BytesIO()

    # Write the DataFrame to the BytesIO object as a parquet file
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_file)

    # Get the bytes of the resulting parquet file
    parquet_bytes = parquet_file.getvalue()

    # Return the bytes of the parquet file
    return parquet_bytes

def upload_parquet_to_blob_storage(parquet_bytes, connection_string, container_name, blob_name):
    # Create a PyArrow Table from the Parquet file bytes
    table = pq.read_table(BytesIO(parquet_bytes))

    # Convert the Table to a bytes buffer
    stream = BytesIO()
    pq.write_table(table, stream)
    buffer = stream.getvalue()

    # Create a BlobServiceClient using the provided connection string
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get a BlobClient for the specified container and blob name
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Upload the Parquet file to Blob Storage
    blob_client.upload_blob(buffer, overwrite=True)