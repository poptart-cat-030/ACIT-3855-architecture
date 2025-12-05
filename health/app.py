import connexion
from connexion import NoContent

# Disabling CORS
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware

import os.path # For reading and writing to files
import json # For data operations
from datetime import datetime, timezone # For creating and formatting timestamps and converting timezones 

import yaml # For using the yaml config file (app_conf)
import httpx # For sending get requests to services to check their health

# For creating and displaying log messages (log_conf)
import logging
import logging.config

# Scheduling
from apscheduler.schedulers.background import BackgroundScheduler 

# Setting app configurations
with open('config/app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f.read())

DATASTORE_FILE = app_config['datastore']['filename']
ANALYZER_URL = app_config['eventstores']['analyzer']['url']
PROCESSING_URL = app_config['eventstores']['processing']['url']
RECEIVER_URL = app_config['eventstores']['receiver']['url']
STORAGE_URL = app_config['eventstores']['storage']['url']

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


# Generating statuses from readings
# Create dict with statuses set to Down if there are no previous statuses
    # Last updated time = now
def create_dummy_statuses():
    # Get current time (in UTC since that's what the MySQL database is storing the other timestamps as)
    date_last_updated = datetime.now(timezone.utc)
    # Convert it to string in the format Year-Month-Day Hours-Minutes-Seconds
    str_date_last_updated = datetime.strftime(date_last_updated, "%Y-%m-%d %H:%M:%S")

    dummy_statuses= {
        'analyzer': 'Down',
        'processing': 'Down',
        'receiver': 'Down',
        'storage': 'Down',
        'last_update': str_date_last_updated
    }
    return dummy_statuses


# GET Endpoint function for checking health of backend services service
    # Reads statuses from statuses file and returns it
    # Called through /health endpoint
def get_statuses():
    logger.info("GET request to '/statuses' was received.")
    # If file doesn't exist, return nothing with status code 404
    if not os.path.isfile(DATASTORE_FILE):
        logger.error(f"Statuses do not exist. File: '{DATASTORE_FILE}' could not be found.")
        return NoContent, 404
    # If file exists, return dict version of statuses with status code 200
    else:
        # Read from file
        statuses = get_file_contents(DATASTORE_FILE)
        logger.debug(f"Statuses file contents:\n{statuses}")
        logger.info("GET request to '/statuses' was received.")
        return statuses, 200


# Updates statuses in the statuses file
    # Makes GET requests to backend service
    # Constantly running in background
def populate_statuses():
    logger.info("Periodic health checking has started.")

    timeout = httpx.Timeout(10.0, read=5) # Set 5 second time limit for waiting for response body to be received
    # If response isn't received, raise ReadTimeout exception (set 10 second timeout by default)
    client = httpx.Client(timeout=timeout)

    # Check if file statuses.json exists, create file if it doesn't
    if does_file_exist(DATASTORE_FILE):
        # If file is empty, populate with dummy statuses
        if os.path.getsize(DATASTORE_FILE) == 0:
            dummy_statuses = create_dummy_statuses()
            write_to_file(DATASTORE_FILE, dummy_statuses)

    # Update statuses received from the statuses.json file and then overwrite the statuses.json file at the end
    statuses = get_file_contents(DATASTORE_FILE)

    analyzer_health = statuses['analyzer']
    processing_health = statuses['processing']
    receiver_health = statuses['receiver']
    storage_health = statuses['storage']

        # Compare current time with last updated time for triggering the scheduler
    last_updated_time = statuses['last_update']
    # Get current time (in UTC since that's what the MySQL database is storing the other timestamps as)
    current_datetime = datetime.now(timezone.utc)
    # Convert it to string in the format Year-Month-Day Hours-Minutes-Seconds
    current_datetime_str = datetime.strftime(current_datetime, "%Y-%m-%d %H:%M:%S")

    
    # Handling service health GET endpoints and statuses
        # analyzer
    try:
        analyzer_response = client.get(ANALYZER_URL)
        if analyzer_response.status_code == 200: # Status code will be 200 if service is running
            analyzer_response_data = analyzer_response.json()
            analyzer_health = analyzer_response_data['status'] # Get status based on response body ("Running" if running)
            logger.info(f"Status of analyzer is: {analyzer_health}")
        else:
            logger.error(f"GET request to '{ANALYZER_URL}' failed with status code {analyzer_response.status_code}")
            analyzer_health = "Down"
            logger.info(f"Status of analyzer is: {analyzer_health}")
    except Exception as e:
        logger.error(f"Error: Did not receive response body from '{ANALYZER_URL}'. Exception {e}")
        analyzer_health = "Down"
    

        # processing
    try:
        processing_response = client.get(PROCESSING_URL)
        if processing_response.status_code == 200: # Status code will be 200 if service is running
            processing_response_data = processing_response.json()
            processing_health = processing_response_data['status'] # Get status based on response body ("Running" if running)
            logger.info(f"Status of processing is: {processing_health}")
        else:
            logger.error(f"GET request to '{PROCESSING_URL}' failed with status code {processing_response.status_code}")
            processing_health = "Down"
            logger.info(f"Status of processing is: {processing_health}")
    except Exception as e:
        logger.error(f"Error: Did not receive response body from '{PROCESSING_URL}'. Exception {e}")
        processing_health = "Down"


        # receiver
    try:
        receiver_response = client.get(RECEIVER_URL)
        if receiver_response.status_code == 200: # Status code will be 200 if service is running
            receiver_response_data = receiver_response.json()
            receiver_health = receiver_response_data['status'] # Get status based on response body ("Running" if running)
            logger.info(f"Status of receiver is: {receiver_health}")
        else:
            logger.error(f"GET request to '{RECEIVER_URL}' failed with status code {receiver_response.status_code}")
            receiver_health = "Down"
            logger.info(f"Status of receiver is: {receiver_health}")
    except Exception as e:
        logger.error(f"Error: Did not receive response body from '{RECEIVER_URL}'. Exception {e}")
        receiver_health = "Down"


        # storage
    try:
        storage_response = client.get(STORAGE_URL)
        if storage_response.status_code == 200: # Status code will be 200 if service is running
            storage_response_data = storage_response.json()
            storage_health = storage_response_data['status'] # Get status based on response body ("Running" if running)
            logger.info(f"Status of storage is: {storage_health}")
        else:
            logger.error(f"GET request to '{STORAGE_URL}' failed with status code {storage_response.status_code}")
            storage_health = "Down"
            logger.info(f"Status of storage is: {storage_health}")
    except Exception as e:
        logger.error(f"Error: Did not receive response body from '{STORAGE_URL}'. Exception {e}")
        storage_health = "Down"
    

    # New service health values
    statuses['analyzer'] = analyzer_health
    statuses['processing'] = processing_health
    statuses['receiver'] = receiver_health
    statuses['storage'] = storage_health
    # Update last updated time even if no readings were received
    statuses['last_update'] = current_datetime_str

    # Overwrite content in file
    write_to_file(DATASTORE_FILE, statuses)

    logger.debug(f"New statuses:\n{statuses}")

    logger.info("Periodic health checking has ended.")



def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    # Triggers function for updating services health file (statuses.json) every 20 seconds
    sched.add_job(populate_statuses, 'interval', seconds=app_config['scheduler']['interval'])
    sched.start()


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
    app.run(port=8120, host="0.0.0.0")