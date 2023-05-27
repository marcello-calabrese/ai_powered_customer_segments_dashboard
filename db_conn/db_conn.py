import psycopg2

CONN = psycopg2.connect(
    host = 'localhost',
    dbname = 'Hotel_Customer_segmentation',
    user = 'postgres',
    password = 'Lorenzo_22',
    port = '5432')