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

# HTTPX timeouts
DEFAULT_TIMEOUT_INTERVAL = app_config['timeouts']['default']['interval']
READ_TIMEOUT_INTERVAL = app_config['timeouts']['read']['interval']

SERVICES = app_config['eventstores']

# Setting logging configurations
with open("config/log_conf.yaml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')

# File helper functions
def does_file_exist(filename):
    ''' Checks if file exists and creates it if it doesn't '''
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
    ''' Generate dummy statuses if file doesn't exist or is empty '''
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


# GET Endpoint function for checking health of backend services
    # Reads statuses from statuses file and returns it
    # Called through /health endpoint
def get_statuses():
    ''' 
        Checks health of backend services from the statuses file
        
        Returns: 
            dict: Status of each service and the time of the last update
    '''
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
    ''' 
        Updates health statuses in the statuses file based on GET requests to each service's /health endpoint
    '''
    logger.info("Periodic health checking has started.")

    timeout = httpx.Timeout(DEFAULT_TIMEOUT_INTERVAL, read=READ_TIMEOUT_INTERVAL) # Set 5 second time limit for waiting for response body to be received
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

    # Compare current time with last updated time for triggering the scheduler
    last_updated_time = statuses['last_update']
    # Get current time (in UTC since that's what the MySQL database is storing the other timestamps as)
    current_datetime = datetime.now(timezone.utc)
    # Convert it to string in the format Year-Month-Day Hours-Minutes-Seconds
    current_datetime_str = datetime.strftime(current_datetime, "%Y-%m-%d %H:%M:%S")

    # Handling service health GET endpoints and statuses
        # Retrieving service name and the dictionary of variables nested under it (just URL for now)
    for service_name, service_info in SERVICES.items():
        try:
            response = client.get(service_info['url'])
            if response.status_code == 200: # Status code will be 200 if service is running
                response_data = response.json()
                service_health = response_data['status'] # Get status based on response body ("Running" if running)
                statuses[service_name] = service_health
                logger.info(f"Status of {service_name} is: {service_health}")
            else:
                logger.error(f"GET request to '{service_info['url']}' failed with status code {response.status_code}")
                service_health = "Down"
                statuses[service_name] = service_health
                logger.info(f"Status of {service_name} is: {service_health}")
        except Exception as e:
            logger.error(f"Error: Did not receive response body from '{service_info['url']}'. Exception {e}")
            service_health = "Down"
            statuses[service_name] = service_health

    # Update last updated time even if no readings were received
    statuses['last_update'] = current_datetime_str

    # Overwrite content in file
    write_to_file(DATASTORE_FILE, statuses)

    logger.debug(f"New statuses:\n{statuses}")

    logger.info("Periodic health checking has ended.")



def init_scheduler():
    ''' Triggers health status updates based on a set interval '''
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