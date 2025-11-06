import connexion
from connexion import NoContent

import json # For data operations
import datetime # For creating timestamps and datetime object conversions

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

# Threading
from threading import Thread


# Setting app configurations
with open('app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f.read())

# Logging configurations
with open("log_conf.yaml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')


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
    """ Process event messages """
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}" # localhost:9092
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(f"{app_config['events']['topic']}")]
    
    # Create a consume on a consumer group, that only reads new messages
    # (uncommitted messages) when the service re-starts (i.e., it doesn't
    # read all the old messages from the history in the message queue).

    consumer = topic.get_simple_consumer(consumer_group=b'event_group',
    reset_offset_on_start=False,
    auto_offset_reset=OffsetType.LATEST)
    # This is blocking - it will wait for a new message
    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info("Message: %s" % msg)
        payload = msg["payload"]
        if msg["type"] == "volume_reading": # Change this to your event type
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

        elif msg["type"] == "type_reading": # Change this to your event type
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

        # Commit the new message as being read
        consumer.commit_offsets()

def setup_kafka_thread():
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("hair-api-1.0.0-swagger.yaml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    setup_kafka_thread()
    app.run(host="0.0.0.0") # Receiver is running on port 8080