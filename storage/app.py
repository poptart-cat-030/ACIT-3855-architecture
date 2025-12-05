import connexion
from connexion import NoContent

import json # For data operations
import datetime # For creating timestamps and datetime object conversions
import time # For kafka sleep
import random

# SQLAlchemy and database modules
from models import Volume, Type # From models.py, my tables
import create_database as cd # From create_database.py, for creating sessions
from functools import wraps # For handling session management automatically
from sqlalchemy import select

import yaml # For using the yaml config file (app_conf)

# For creating and displaying log messages (log_conf)
import logging
import logging.config

# For message brokering
from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import KafkaException

# Threading
from threading import Thread

# Setting app configurations
with open('config/app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f.read())

# Logging configurations
with open("config/log_conf.yaml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')


# Message Brokering
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
                    yield msg # To be used in process_messages()
                    # self.consumer.commit_offsets() # Tell kafka that this message has been consumed
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


# SQLAlchemy functions
# Handles session management automatically
def use_db_session(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        session = cd.make_session()
        try:
            # Wrapper function
                # Call the original (decorated) function with session injected as the first argument
            return func(session, *args, **kwargs)
        finally:
            session.close()
    return wrapper


# Convert timestamps (str) to datetime objects and truncate microseconds
def convert_str_timestamp_to_datetime(timestamp):
    timestamp_converted = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    timestamp_truncated = timestamp_converted.replace(microsecond=0)
    return timestamp_truncated


def get_hair_volume_readings(start_timestamp, end_timestamp):
    session = cd.make_session()

    start = datetime.datetime.strptime(start_timestamp, "%Y-%m-%d %H:%M:%S")
    print(f"hair volume reading start timestamp: {str(start)}")
    end = datetime.datetime.strptime(end_timestamp, "%Y-%m-%d %H:%M:%S")
    print(f"hair volume reading start timestamp: {str(end)}")

    statement = select(Volume).where(Volume.date_created >= start).where(Volume.date_created < end)

    results = [
        result.to_dict() for result in session.execute(statement).scalars().all()
    ]

    session.close()

    logger.debug("Found %d hair volume readings (start: %s, end: %s)", len(results), start, end)

    return results


def get_hair_type_readings(start_timestamp, end_timestamp):
    session = cd.make_session()

    start = datetime.datetime.strptime(start_timestamp, "%Y-%m-%d %H:%M:%S")
    print(f"hair type reading start timestamp: {str(start)}")
    end = datetime.datetime.strptime(end_timestamp, "%Y-%m-%d %H:%M:%S")
    print(f"hair type reading end timestamp: {str(start)}")

    statement = select(Type).where(Type.date_created >= start).where(Type.date_created < end)

    results = [
        result.to_dict() for result in session.execute(statement).scalars().all()
    ]

    session.close()

    logger.debug("Found %d hair type readings (start: %s, end: %s)", len(results), start, end)

    return results


def process_messages():
    """ Process event messages using KafkaWrapper"""
    # Create a KafkaWrapper instance (has connection failure handling) globally
    kafka_wrapper = KafkaWrapper(
        f"{app_config['events']['hostname']}:{app_config['events']['port']}", # host
        str.encode(app_config['events']['topic']) # topic
    )

    for msg in kafka_wrapper.messages():
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info("Message: %s" % msg)
        payload = msg["payload"]

        if msg["type"] == "volume_reading":
            # Store the volume_reading (i.e., the payload) to the DB
            session = cd.make_session()
            hair_vol_reading_event = Volume(
                salon_id = payload['salon_id'],
                salon_name = payload['salon_name'],
                hair_volume = payload['hair_volume'],
                disposal_method = payload['disposal_method'],
                # Convert timestamp from string to Python datetime object using strptime
                    # Also modified original format of timestamps being sent through the yaml file example
                batch_timestamp = datetime.datetime.strptime(payload['batch_timestamp'], "%Y-%m-%d %H:%M:%S"),
                reading_timestamp = datetime.datetime.strptime(payload['reading_timestamp'], "%Y-%m-%d %H:%M:%S"),
                trace_id = payload['trace_id']
            )
            session.add(hair_vol_reading_event)
            session.commit()
            logger.info(f"Stored event volume_reading with a trace id of {payload['trace_id']}")

        elif msg["type"] == "type_reading":
            # Store the type_reading (i.e., the payload) to the DB
            session = cd.make_session()
            hair_type_reading_event = Type(
                salon_id = payload['salon_id'],
                salon_name = payload['salon_name'],
                hair_colour = payload['hair_colour'],
                hair_texture = payload['hair_texture'],
                hair_thickness = payload['hair_thickness'],
                batch_timestamp = datetime.datetime.strptime(payload['batch_timestamp'], "%Y-%m-%d %H:%M:%S"),
                reading_timestamp = datetime.datetime.strptime(payload['reading_timestamp'], "%Y-%m-%d %H:%M:%S"),
                trace_id = payload['trace_id']
            )
            session.add(hair_type_reading_event)
            session.commit()
            logger.info(f"Stored event type_reading with a trace id of {payload['trace_id']}")


# Endpoint function for checking health of this service
    # Called through /health endpoint
def get_health():
    return {"status": "Running"}, 200 # If service is running, then it will return 200 which means it's ok


def setup_kafka_thread():
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("config/hair-api-1.0.0-swagger.yaml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    setup_kafka_thread()
    app.run(port=8090, host="0.0.0.0") # Receiver is running on port 8080