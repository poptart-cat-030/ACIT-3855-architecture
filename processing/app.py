import connexion
from connexion import NoContent

# Disabling CORS
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware

import os.path # For reading and writing to files
import json # For data operations
from datetime import datetime, timezone # For creating and formatting timestamps and converting timezones 

import yaml # For using the yaml config file (app_conf)
import httpx # For sending get requests to storage service (for calculating stats)

# For creating and displaying log messages (log_conf)
import logging
import logging.config

# Scheduling
from apscheduler.schedulers.background import BackgroundScheduler 

# Setting app configurations
with open('config/app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f.read())

DATASTORE_FILE = app_config['datastore']['filename']
VOLUME_URL = app_config['eventstores']['volume']['url']
TYPE_URL = app_config['eventstores']['type']['url']

# Setting logging configurations
with open("config/log_conf.yaml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')

# File helper functions
def does_file_exist(filename):
    if os.path.isfile(filename):
        return True
    else:
        print(f"File: '{filename}' could not be found." )
        with open(filename, 'w') as file:
            file.write("")
        print(f"Created file: '{filename}'." )
        return False

def get_file_contents(filename):
    if os.path.getsize(filename) == 0:
        return {} # Return empty json if there's nothing in the .json file
    with open(filename, 'r') as json_file:
        content = json.load(json_file) # Convert json to python dict
    return content

def write_to_file(filename, content):
    with open(filename, 'w') as my_file: # 'w' to write - will overwrite file if there's anything there already
        my_file.write(json.dumps(content, indent=4)) # Convert python dict into json (indent for formatting)


# Generating statistics from readings
# Create dict with stats set to 0 if there are no previous stats
    # Last updated time = now
def create_dummy_stats():
    # Get current time (in UTC since that's what the MySQL database is storing the other timestamps as)
    date_last_updated = datetime.now(timezone.utc)
    # Convert it to string in the format Year-Month-Day Hours-Minutes-Seconds
    str_date_last_updated = datetime.strftime(date_last_updated, "%Y-%m-%d %H:%M:%S")

    dummy_stats = {
        'num_vol_readings': 0,
        'min_vol_grams': 0,
        'max_vol_grams': 0,
        'num_type_readings': 0,
        'max_type_thickness': 0,
        'date_last_updated': str_date_last_updated
    }
    return dummy_stats


# API endpoint function
# Reads stats from file and returns it
    # Called through the stats endpoint 
def get_stats():
    logger.info("GET request to '/stats' was received.")
    # If file doesn't exist, return nothing with status code 404
    if not os.path.isfile(DATASTORE_FILE):
        logger.error(f"Statisics do not exist. File: '{DATASTORE_FILE}' could not be found.")
        return NoContent, 404
    # If file exists, return dict version of stats with status code 200
    else:
        # Read from file
        stats = get_file_contents(DATASTORE_FILE)
        logger.debug(f"stats file contents:\n{stats}")
        logger.info("GET request to '/stats' was received.")
        return stats, 200


# Updates stats in a file
    # Makes GET requests to storage
    # Constantly running in background
def populate_stats():
    logger.info("Periodic processing has started.")

    # Check if file data.json exists, create file if it doesn't
    if does_file_exist(DATASTORE_FILE):
        # If file is empty, populate with dummy stats
        if os.path.getsize(DATASTORE_FILE) == 0:
            dummy_stats = create_dummy_stats()
            write_to_file(DATASTORE_FILE, dummy_stats)

    # Update stats received from the data.json file and then overwrite the data.json file at the end
    stats = get_file_contents(DATASTORE_FILE)

    # Variables for logging messages
    num_vol_readings_from_query = 0
    num_type_readings_from_query = 0

    # Variables for reading-specific statistics
    vol_grams_min = stats['min_vol_grams']
    vol_grams_max = stats['max_vol_grams']
    type_thickness_max = stats['max_type_thickness']

        # Compare current time with last updated time for triggering the scheduler
    last_updated_time = stats['date_last_updated']
    # Get current time (in UTC since that's what the MySQL database is storing the other timestamps as)
    current_datetime = datetime.now(timezone.utc)
    # Convert it to string in the format Year-Month-Day Hours-Minutes-Seconds
    current_datetime_str = datetime.strftime(current_datetime, "%Y-%m-%d %H:%M:%S")

        # Use httpx get for querying timestamps (last updated time, current time)
        # gets list of readings from any readings found within that timeframe and updates stats
    
    # Handling hair volume GET endpoint and stats
    hair_vol_query_params = {'start_timestamp': last_updated_time, 'end_timestamp': current_datetime_str}
    hair_vol_response = httpx.get(VOLUME_URL, params=hair_vol_query_params)
    hair_vol_response_data = hair_vol_response.json()
    if hair_vol_response.status_code != 200:
        logger.error(f"GET request to '{VOLUME_URL}' failed with status code {hair_vol_response.status_code}")
    # print("hair volume query result:\n", hair_vol_response_data)

    # data = list, reading = python dict
    for reading in hair_vol_response_data:
        num_vol_readings_from_query += 1
        stats['num_vol_readings'] = stats['num_vol_readings'] + 1
        # If the min_vol_grams stat is 0, then set it to the first hair volume reading...
        # and compare it with values from following readings since the min value of a hair vlume reading would be 1...
        # therefore, the min_vol_grams stat would never change
        if stats['min_vol_grams'] == 0:
            stats['min_vol_grams'] = reading['hair_volume']
        # If value from reading is more/less than the max/min currently recorded, update relevant stat to that reading
        if reading['hair_volume'] < vol_grams_min:
            stats['min_vol_grams'] = reading['hair_volume']
            vol_grams_min = reading['hair_volume']
        if reading['hair_volume'] > vol_grams_max:
            stats['max_vol_grams'] = reading['hair_volume']
            vol_grams_max = reading['hair_volume']
    
    logger.info(f"Received {num_vol_readings_from_query} volume readings")


    # Handling hair type GET endpoint and stats
    hair_type_query_params = {'start_timestamp': last_updated_time, 'end_timestamp': current_datetime_str}
    hair_type_response = httpx.get(TYPE_URL, params=hair_type_query_params)
    hair_type_response_data = hair_type_response.json()
    if hair_type_response.status_code != 200:
        logger.error(f"GET request to '{TYPE_URL}' failed with status code {hair_type_response.status_code}")
    # print("hair type query result:\n", hair_type_response_data)

    # data = list, reading = python dict
    for reading in hair_type_response_data:
        num_type_readings_from_query += 1
        stats['num_type_readings'] = stats['num_type_readings'] + 1
        if reading['hair_thickness'] > type_thickness_max:
            stats['max_type_thickness'] = reading['hair_thickness']      
            type_thickness_max = reading['hair_thickness']
    
    logger.info(f"Received {num_type_readings_from_query} type readings")

    # Update last updated time even if no readings were received
    stats['date_last_updated'] = current_datetime_str

    # Overwrite content in file
    write_to_file(DATASTORE_FILE, stats)

    logger.debug(f"New statistics:\n{stats}")

    logger.info("Periodic processing has ended.")


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, 'interval', seconds=app_config['scheduler']['interval'])
    sched.start()


# Endpoint function for checking health of this service
    # Called through /health endpoint
def get_health():
    return {"status": "Running"}, 200 # If service is running, then it will return 200 which means it's ok


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("config/hair-api-1.0.0-swagger.yaml", strict_validation=True, validate_responses=True)

# Disabling CORS
app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, host="0.0.0.0")