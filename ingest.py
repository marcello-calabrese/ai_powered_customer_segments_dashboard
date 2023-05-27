# Description: This script is used to ingest data from a URL, transform into a parquet file and upload into Azure Blob Storage

# importing libraries

# importing main components of the script
from credentials.creds import container_name, conn_string
from utils.data_url import URL
from utils.logger import logging
from utils.extract_data import extract_to_parquet, upload_parquet_to_blob_storage

# retrieving data from URL

def main():
        logging.info("Retrieving data from URL")
        parquet_bytes = extract_to_parquet(URL)
        logging.info("Data retrieved from URL")
        
        logging.info("Uploading data to Azure Blob Storage")
        upload_parquet_to_blob_storage(parquet_bytes, conn_string, container_name, "ingested_data_hotel.parquet")
        logging.info("Data uploaded to Azure Blob Storage")
        
        

if __name__ == "__main__":
    main()