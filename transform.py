# In this file, we define the transformation functions for the data.
# We retrieve the parquet file from the Azure Blob Storage container without saving into disk, apply some data cleaning.
# After the data cleaning we start creating the dataframes as different tables to be stored in a Postgres database.

# import libraries

import pandas as pd
import numpy as np
#import psycopg2
from sqlalchemy import create_engine

from utils.logger import logging

# import utils functions

from utils.retrieve_blob import retrieve_parquet_file_from_blob_storage # retrieve the parquet file from the Azure Blob Storage container without saving into disk
from utils.clean_data import clean_data # apply some data cleaning function


# import Azure Blob credentials 

from credentials.creds import conn_string, container_name



# Retrieve the parquet file from the Azure Blob Storage container without saving into disk (utils\retrieve_blob.py)

def main():

    logging.info("Retrieving parquet file from the Azure Blob Storage container without saving into disk")

    df = retrieve_parquet_file_from_blob_storage(container_name, "ingested_hotel_customers_data.parquet", conn_string)

    logging.info('Parquet file from the Azure Blob Storage retrieved successfully')

    #print(df.head())

    # Apply some data cleaning function to import: utils\clean_data.py

    logging.info("Apply some data cleaning: drop duplicates, drop null values, drop columns")

    df_cleaned = clean_data(df)

    logging.info("Data cleaning applied successfully")

    # Create the dataframes as different tables to be stored in a Postgres database

    df_cleaned['booking_id'] = df_cleaned.index

    # Create dimension tables

    logging.info("Creating dimension tables")

    dim_nationality = df_cleaned[['nationality']].reset_index(drop=True)
    dim_nationality['nationality_id'] = dim_nationality.index
    dim_nationality = dim_nationality[['nationality_id', 'nationality']]

    dim_age = df_cleaned[['age']].reset_index(drop=True)
    dim_age['age_id'] = dim_age.index
    dim_age = dim_age[['age_id', 'age']]

    dim_customer = df_cleaned[['namehash']].reset_index(drop=True)
    dim_customer['customer_id'] = dim_customer.index
    dim_customer = dim_customer[['customer_id', 'namehash']]

    dim_avg_lead_time = df_cleaned[['averageleadtime']].reset_index(drop=True)
    dim_avg_lead_time['avgleadtime_id'] = dim_avg_lead_time.index
    dim_avg_lead_time = dim_avg_lead_time[['avgleadtime_id', 'averageleadtime']]

    dim_channel = df_cleaned[['distributionchannel']].reset_index(drop=True)
    dim_channel['channel_id'] = dim_channel.index
    dim_channel = dim_channel[['channel_id', 'distributionchannel']]

    dim_segment = df_cleaned[['marketsegment']].reset_index(drop=True)
    dim_segment['segment_id'] = dim_segment.index
    dim_segment = dim_segment[['segment_id', 'marketsegment']]

    dim_room_type = df_cleaned[['srhighfloor','srlowfloor', 'sraccessibleroom', 'srmediumfloor', 'srbathtub', 'srshower', 'srcrib', 'srkingsizebed', 'srtwinbed', 'srnearelevator','srawayfromelevator', 'srnoalcoholinminibar', 'srquietroom']].reset_index(drop=True)
    dim_room_type['room_id'] = dim_room_type.index
    dim_room_type = dim_room_type[['room_id', 'srhighfloor','srlowfloor', 'sraccessibleroom', 'srmediumfloor', 'srbathtub', 'srshower', 'srcrib', 'srkingsizebed', 'srtwinbed', 'srnearelevator','srawayfromelevator', 'srnoalcoholinminibar', 'srquietroom']]

    logging.info("Dimension tables created successfully")

    # Create fact table

    logging.info("Creating fact table")
    # merge the dataframes

    fact_hotel = df_cleaned.merge(dim_nationality, left_on= 'booking_id', right_on='nationality_id') \
                            .merge(dim_age, left_on= 'booking_id', right_on='age_id') \
                            .merge(dim_customer, left_on= 'booking_id', right_on='customer_id') \
                            .merge(dim_avg_lead_time, left_on= 'booking_id', right_on='avgleadtime_id') \
                            .merge(dim_channel, left_on= 'booking_id', right_on='channel_id') \
                            .merge(dim_segment, left_on= 'booking_id', right_on='segment_id') \
                            .merge(dim_room_type, left_on= 'booking_id', right_on='room_id') \
                            [[
                                'booking_id',
                                'nationality_id',
                                'age_id',
                                'customer_id',
                                'avgleadtime_id',
                                'channel_id',
                                'segment_id',
                                'room_id',
                                'lodgingrevenue',
                                'otherrevenue',
                                'bookingscanceled',
                                'bookingsnoshowed',
                                'bookingscheckedin',
                                'personsnights',
                                'roomnights'
                            ]]

    logging.info("Fact table created successfully")

    # Create the connection to the database

    logging.info("Creating the connection to the database")

    engine = create_engine('postgresql://postgres:YOUR_PSW@localhost:5432/Hotel_Customer_segmentation')

    # Create the tables in Postgres

    logging.info("Creating the tables in Postgres")

    # create the tables in Postgres with a try except block to avoid errors if the tables already exist

    try:

        print("Creating tables in Postgres")

        dim_nationality.to_sql('dim_nationality', engine, if_exists='replace', index=False)
        dim_age.to_sql('dim_age', engine, if_exists='replace', index=False)
        dim_customer.to_sql('dim_customer', engine, if_exists='replace', index=False)
        dim_avg_lead_time.to_sql('dim_avg_lead_time', engine, if_exists='replace', index=False)
        dim_channel.to_sql('dim_channel', engine, if_exists='replace', index=False)
        dim_segment.to_sql('dim_segment', engine, if_exists='replace', index=False)
        dim_room_type.to_sql('dim_room_type', engine, if_exists='replace', index=False)
        fact_hotel.to_sql('fact_hotel', engine, if_exists='replace', index=False)
        
        logging.info("Tables created successfully")
        
             
    except Exception as e:
        logging.error(e)
    

if __name__ == "__main__":
    main()

