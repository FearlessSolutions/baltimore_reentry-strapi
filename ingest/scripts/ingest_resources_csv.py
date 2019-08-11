import logging
import os
import csv
import json
import pymongo
import string
import re
import pprint
from pymongo import MongoClient
from collections import namedtuple
from dotenv import load_dotenv
from datetime import datetime

load_dotenv(verbose=True)
log_format = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"), format=log_format)
logger = logging.getLogger("resources-parser")

RESOURCES_CSV_FILE_NAME = os.getenv("RESOURCES_CSV_FILE_NAME")
MONGODB_CONNECTION_URL = os.getenv("MONGODB_CONNECTION_URL")
MONGODB_CONNECTION_USERNAME = os.getenv("MONGODB_CONNECTION_USERNAME")
MONGODB_CONNECTION_PASSWORD = os.getenv("MONGODB_CONNECTION_PASSWORD")
MONGODB_CONNECTION_DB_NAME=  os.getenv("MONGODB_CONNECTION_DB_NAME")
BATCH_SIZE = 50

cwd = os.getcwd()
current_file_path = "/".join(os.path.realpath(__file__).split("/")[:-1])
root_file_path = os.path.join(current_file_path, '../..')
logger.debug("Current working dir : %s" % cwd)
logger.debug("current_file_path : %s", current_file_path)
resources_csv_file = root_file_path + "/ingest/data/" + RESOURCES_CSV_FILE_NAME
resource_column_names = "county organization address city state zip neighborhood quadrant phone phone_2 website services type men women"

def row_count(csv_fname):
    with open(csv_fname) as f:
        return sum(1 for line in f) - 1 # Remove one for the column header row

def percentage(part, whole):
  percentage = 100 * float(part)/float(whole)
  rounded = int(round(percentage))
  return rounded

def get_resource_record(csv_fname):
    """
    A generator for the data in the csv. This is because the csv files can often contain millions of records and shouldn't be stored in memory all at once.
    :param csv_fname:
        filename/location of the csv.
    :return:
        yields each row as a namedtuple.
    """
    logger.info("Getting resource records")
    ResourceRecord = namedtuple('ResourceRecord', resource_column_names)
    with open(csv_fname, "r", encoding="latin-1") as resource_records:
        for resource_record in csv.reader(resource_records):
            if len(resource_record) != number_columns:
                logger.warning('The number of columns in row %s does not match the number of columns %s' %(len(resource_record), number_columns))
            ascii_resource_record = (
                x.encode('ascii', errors='replace').decode() for x in resource_record)
            yield ResourceRecord(*ascii_resource_record)
            
                


def generate_resource_dict(csv_row):
    resource = {
        "orgName": csv_row.organization,
        "zip": csv_row.zip,
        "address": csv_row.address,
        "state": csv_row.state,
        "county": csv_row.county,
        "city": csv_row.city,
        "neighborhood": csv_row.neighborhood,
        "altPhoneNumber":  csv_row.phone_2,
        "primaryPhoneNumber":  csv_row.phone,
        "website": csv_row.website,
        "quadrant": csv_row.quadrant,
        "description": csv_row.services,
        "createdAt":  datetime.utcnow(),
        "__v" : 0
    }
    return resource

def initialize_client():
    try:
        if MONGODB_CONNECTION_URL: # Use the connection_url if populated otherwise build the connection string from individual parameters
            connection_string = MONGODB_CONNECTION_URL
        else:
            connection_string = f"mongodb+srv://{MONGODB_CONNECTION_USERNAME}:{MONGODB_CONNECTION_PASSWORD}@cluster0-mjstf.mongodb.net/{MONGODB_CONNECTION_DB_NAME}?retryWrites=true"
        logger.info(f"Connecting to MongoDB with connection string {connection_string}")
        client = MongoClient(connection_string, serverSelectionTimeoutMS=500)
        info = client.server_info() # force connection on a request as the
                            # connect=True parameter of MongoClient seems
                            # to be useless here 
        logger.info("Server info:\n%s" %(pprint.pformat(info)))
        return client
    except pymongo.errors.ServerSelectionTimeoutError as err:
        raise Exception(f"An error occured connecting to MongoDB: \n {err}")

def parse_and_insert_rows(number_rows, collection):
    """
        Parses batches and then bulk inserts rows in increments of BATCH_SIZE
    """
    # weather_zones = parse_weather_zones()
    # zip_codes = parse_zipcodes()
    iter_resource = iter(get_resource_record(resources_csv_file))
    next(iter_resource)  # Skipping the column names
    batch = []
    current_range_min = 1
    current_range_max = BATCH_SIZE
    for idx, row in enumerate(iter_resource, start=1):
        logger.info(f"Row {idx} of {number_rows} -- {percentage(idx, number_rows)}% Rows Proccessed")
        batch.append(generate_resource_dict(row))
        if (idx == 1):
            continue
        elif ( idx % BATCH_SIZE) == 0: # We are at a batch size interval
            logger.info(f"Bulk inserting records from {current_range_min} to {current_range_max}")
            collection.insert_many(batch)
            current_range_min = idx
            next_max = number_rows if (current_range_min + BATCH_SIZE >= number_rows) else current_range_min + BATCH_SIZE
            current_range_max = next_max
            logger.info("Inserted batch")
            batch = []
        elif ( idx == number_rows): # We are at the last row
            logger.info(f"Bulk inserting records from {current_range_min} to {current_range_max}")
            collection.insert_many(batch)
            logger.info("Inserted final batch")
        

number_columns = len(resource_column_names.split())
number_rows = row_count(resources_csv_file)

logger.info("BEGIN")
logger.info("Number of columns: %s " %(number_columns))
logger.info("Number of rows: %s " %(number_rows))

client = initialize_client()
db = client.baltimore_reentry
resource_collection = db.resource
parse_and_insert_rows(number_rows, resource_collection)
logger.info("END")