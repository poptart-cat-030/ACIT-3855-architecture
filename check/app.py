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

STORAGE_URL = app_config['eventstores']['storage']['url']
RECEIVER_URL = app_config['eventstores']['receiver']['url']
PROCESSING_URL = app_config['eventstores']['processing']['url']
ANALYZER_URL = app_config['eventstores']['analyzer']['url']
TIMEOUT = app_config['timeout']['interval']

DATASTORE_FILE = app_config['datastore']['filename']

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


def create_dummy_statuses():
    ''' Generate dummy statuses if file doesn't exist or is empty '''

    dummy_statuses= {
        'analyzer': 'Unavailable',
        'processing': 'Unavailable',
        'receiver': 'Unavailable',
        'storage': 'Unavailable'
    }
    return dummy_statuses


    # Called through /check endpoint
def get_checks():
    ''' 
        Checks status messages from backend services from the status file
        
        Returns:
            dict (String) Status message of each service
    '''
    logger.info("GET request to '/check' was received.")
    # If file doesn't exist, return nothing with status code 404
    if not os.path.isfile(DATASTORE_FILE):
        logger.error(f"Status messages do not exist. File: '{DATASTORE_FILE}' could not be found.")
        return NoContent, 404
    # If file exists, return dict version of status messages with status code 200
    else:
        # Read from file
        status_messages = get_file_contents(DATASTORE_FILE)
        logger.debug(f"Status file contents:\n{status_messages}")
        logger.info("GET request to '/check' was received.")
        return status_messages, 200


# Updates status messages in the statuses file
    # Makes GET requests to backend service
    # Constantly running in background
def check_services():
    ''' 
        Updates status messages in the status file based on GET requests to an endpoint for each service

        Returns:
            int: Number of available services
    '''
    logger.info("Periodic status checking has started.")

    # Check if datastore file exists, create file if it doesn't
    if does_file_exist(DATASTORE_FILE):
        # If file is empty, populate with dummy statuses
        if os.path.getsize(DATASTORE_FILE) == 0:
            dummy_statuses = create_dummy_statuses()
            write_to_file(DATASTORE_FILE, dummy_statuses)

    # Update dict received from the datastore file and then overwrite file
    status_messages = get_file_contents(DATASTORE_FILE)

    # Handling service health GET endpoints and statuses
        # Retrieving service name and the dictionary of variables nested under it (just URL for now)
    for service_name, service_info in SERVICES.items():
        num_available = 0

        service_status = "Unavailable"
        try:
            response = httpx.get(service_info['url'], timeout=TIMEOUT)
            if response.status_code == 200:
                response_json = response.json()
                if service_name == "storage":
                    status_message = f"{service_name} has {response_json['num_vol']} Volume and {response_json['num_type']} Type readings in the database"
                elif service_name == "analyzer":
                    status_message = f"{service_name} has {response_json['num_volume_readings']} Volume and {response_json['num_type_readings']} Type readings in the datastore file" 
                elif service_name == "processing":
                    status_message = f"{service_name} has {response_json['num_vol_readings']} Volume and {response_json['num_type_readings']} Type readings in the datastore file"
                else:
                    status_message = f"{service_name} is healthy at {response_json['status_datetime']}"
                num_available += 1
                logger.info(f"{service_name} is Healthy")
                status_messages[service_name] = status_message
                status_messages["services_available"] = num_available
            else:
                logger.info(f"{service_name} returning non-200 response")
        except Exception as e:
            logger.info(f"{service_name} is Not Available")
    

    # Overwrite content in file
    write_to_file(DATASTORE_FILE, status_messages)

    logger.debug(f"New status messages:\n{status_messages}")

    logger.info("Periodic status checking has ended.")

    return status_messages["services_available"], 200


def init_scheduler():
    ''' Triggers status message updates based on a set interval '''
    sched = BackgroundScheduler(daemon=True)
    # Triggers function for updating services health file (statuses.json) every 20 seconds
    sched.add_job(check_services, 'interval', seconds=app_config['scheduler']['interval'])
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
    app.run(port=8130, host="0.0.0.0")