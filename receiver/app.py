import connexion
from connexion import NoContent

import json # For data operations
from datetime import datetime, timezone # For creating and formatting timestamps and converting timezones
import time # For kafka sleep
import random

import uuid # For creating trace identifiers

import yaml # For using the yaml config file (app_conf)

# For creating and displaying log messages (log_conf)
import logging
import logging.config

from pykafka import KafkaClient # For message brokering
from pykafka.common import OffsetType
from pykafka.exceptions import KafkaException


with open('config/app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f.read())

# Not actually in use
# VOLUME_URL = app_config['events']['volume']['url']
# TYPE_URL = app_config['events']['type']['url']


with open("config/log_conf.yaml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')


# Message brokering
# Create class for managing Kafka connections (client and consumer)
class KafkaWrapper:
    def __init__(self, hostname, topic):
        self.hostname = hostname
        self.topic = topic
        self.client = None
        self.consumer = None
        self.producer = None
        self.connect() 


    # Infinitely attempt to create a working client and consumer
    def connect(self):
        """Infinite loop: will keep trying"""

        while True: # Try until successful
            logger.debug("Trying to connect to Kafka...")
            if self.make_client(): # Tries to make a Kafka client
                if self.make_consumer() and self.make_producer(): # Tries to make a Kafka consumer and producer
                    # If client, consumer, and producer successfully created/already existing, stop trying to create them
                    break
            # Sleeps for a random amount of time (0.5 to 1.5s)
            time.sleep(random.randint(500, 1500) / 1000)


    def make_client(self):
        """
        Runs once, makes a client and sets it on the instance.
        Returns: True (success), False (failure)
        """

        if self.client is not None: # if client already exists, don't make one
            return True
        
        try:
            # Make client and save it in self.client
            self.client = KafkaClient(hosts=self.hostname)
            logger.info("Kafka client created!")
            return True
        except KafkaException as e:
            msg = f"Kafka error when making client: {e}"
            logger.warning(msg)
            self.client = None
            self.consumer = None
            self.producer = None
            return False


    def make_consumer(self):
        """
        Runs once, makes a consumer and sets it on the instance.
        Returns: True (success), False (failure)
        """

        if self.consumer is not None:
            return True # if consumer already exists, don't make one
        if self.client is None:
            return False # Don't try to create consumer if client doesn't exist
        
        try:
            # Create a consume on a consumer group, that only reads new messages
            # (uncommitted messages) when the service re-starts (i.e., it doesn't
            # read all the old messages from the history in the message queue).
            topic = self.client.topics[self.topic]
            self.consumer = topic.get_simple_consumer(
                reset_offset_on_start=False, # Don't read old messages
                auto_offset_reset=OffsetType.LATEST # Read only new messages
            )
        except KafkaException as e: # Will be triggered if Kafka is down
            msg = f"Make error when making consumer: {e}"
            logger.warning(msg)
            # Reset saved client, consumer, and producer
            self.client = None
            self.consumer = None
            self.producer = None
            return False # connect() will retry


    def make_producer(self):
        """
        Runs once, makes a producer and sets it on the instance.
        Returns: True (success), False (failure)
        """

        if self.producer is not None:
            return True # if producer exists, don't make one
        if self.client is None:
            return False # Don't try to create producer if client doesn't exist
        
        try:
            topic_for_producer = self.client.topics[self.topic]
            self.producer = topic_for_producer.get_sync_producer()
        except KafkaException as e: # Will be triggered if Kafka is down
            msg = f"Make error when making producer: {e}"
            logger.warning(msg)
            # Reset saved client, consumer, and producer
            self.client = None
            self.consumer = None
            self.producer = None
            return False # connect() will retry


    def messages(self):
        """Generator method that catches exceptions in the consumer loop"""

        if self.consumer is None:
            self.connect() # Try to create/reconnect client, consumer, and producer

        while True: # Runs infinitely
            try:
                for msg in self.consumer:
                    yield msg # To be used in storage's app.py (process_messages())
            # If any error occurs, keep trying
            except KafkaException as e:
                # Reset client, consumer, and producer and attempt to reconnect
                msg = f"Kafka issue in comsumer: {e}"
                logger.warning(msg)
                self.client = None
                self.consumer = None
                self.producer = None
                self.connect()


    def produce(self, message):
        """Produce from messages - retry if it doesn't work"""

        if self.producer is None:
            self.connect() # Try to create/reconnect client, consumer, and producer

        while True: # Runs infinitely
            try:
                self.producer.produce(message)
                break # Break out of loop
            # If any error occurs, keep trying
            except KafkaException as e:
                # Reset client, consumer, and producer and attempt to reconnect
                msg = f"Kafka issue in producer: {e}"
                logger.warning(msg)
                self.client = None
                self.consumer = None
                self.producer = None
                self.connect()


# Create a KafkaWrapper instance (has connection failure handling) globally
kafka_wrapper = KafkaWrapper(
    f"{app_config['events']['hostname']}:{app_config['events']['port']}", # host
    str.encode(app_config['events']['topic']) # topic
)


# API endpoint functions
def report_hair_volume_readings(body):
    # Write to database
    for reading in body["readings"]:
        trace_id = str(uuid.uuid4())
        # Make request message that matches what storage will take
        request_message = {
            'salon_id': body['salon_id'],
            'salon_name': body['salon_name'],
            'hair_volume': reading['hair_volume'],
            'disposal_method': reading['disposal_method'],
            'batch_timestamp': body['reporting_timestamp'],
            'reading_timestamp': reading['recorded_timestamp'],
            'trace_id': trace_id
        }

        logger.info(f"Received event volume_reading with a trace id of {trace_id}")

        msg = { "type": "volume_reading",
        # Current time in UTC since that's what the MySQL database is storing the timestamps as
        "datetime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": request_message
        }
        msg_str = json.dumps(msg)
        kafka_wrapper.produce(msg_str.encode('utf-8'))

        # logger.debug(f"volume_reading msg: {msg_str.encode('utf-8')}")
        logger.info(f"Response for event volume_reading (id: {trace_id}) has status 201")

    return NoContent, 201


def report_hair_type_readings(body):
    # Write to database by sending request to stoage's app.py
    for reading in body["readings"]:
        trace_id = str(uuid.uuid4())
        # Make request message that matches what storage will take
        request_message = {
            'salon_id': body['salon_id'],
            'salon_name': body['salon_name'],
            'hair_colour': reading['hair_colour'],
            'hair_texture': reading['hair_texture'],
            'hair_thickness': reading['hair_thickness'],
            'batch_timestamp': body['reporting_timestamp'],
            'reading_timestamp': reading['recorded_timestamp'],
            'trace_id': trace_id
        }

        logger.info(f"Received event type_reading with a trace id of {trace_id}")

        msg = { "type": "type_reading",
        # Current time in UTC since that's what the MySQL database is storing the timestamps as
        "datetime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": request_message
        }
        msg_str = json.dumps(msg)
        kafka_wrapper.produce(msg_str.encode('utf-8'))

        # logger.debug(f"type_reading msg: {msg_str.encode('utf-8')}")
        logger.info(f"Response for event type_reading (id: {trace_id}) has status 201")

    return NoContent, 201


# Endpoint function for checking health of this service
    # Called through /health endpoint
def get_health():
    return {"status": "Running"}, 200 # If service is running, then it will return 200 which means it's ok


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("config/hair-api-1.0.0-swagger.yaml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":    
    app.run(port=8080, host="0.0.0.0")