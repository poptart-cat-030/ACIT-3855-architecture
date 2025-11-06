import connexion
from connexion import NoContent

import json # For data operations
import datetime # For creating timestamps and datetime object conversions

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

HOSTNAME = f"{app_config['events']['hostname']}:{app_config['events']['port']}" # kafka:9092


def get_hair_volume_reading(index):
    client = KafkaClient(hosts=HOSTNAME)
    topic = client.topics[str.encode(f"{app_config['events']['topic']}")]
    consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)

    message_count = 0 # Keeping track of how many messages are in the message queue
    reading_count = 0 # For keeping track of the records pertaining to this event

    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        # logger.info("Message: %s" % msg)
        payload = msg["payload"]
        if msg["type"] == "volume_reading":
            if reading_count == index:
                logger.info(f"Returning volume_reading at index {index}.")
                logger.info(f"(Message {message_count}) Index {index} type_reading payload: {payload}")
                return payload, 200
            else:
                reading_count += 1 # Next volume reading will be of index 1 (for its event type)
        message_count += 1 # Increment total number of messages

    logger.info(f"No volume_reading at index {index} was found.")
    return { "message": f"No volume_event at index {index}!"}, 404


def get_hair_type_reading(index):
    client = KafkaClient(hosts=HOSTNAME)
    topic = client.topics[str.encode(f"{app_config['events']['topic']}")]
    consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)

    message_count = 0 # Keeping track of how many messages are in the message queue
    reading_count = 0 # For keeping track of the records pertaining to this event

    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        # logger.info("Message: %s" % msg)
        payload = msg["payload"]
        if msg["type"] == "type_reading":
            if reading_count == index:
                logger.info(f"Returning type_reading at index {index}.")
                logger.info(f"(Message {message_count}) Index {index} type_reading payload: {payload}")
                return payload, 200
            else:
                reading_count += 1 # Next type reading will be of index 1 (for its event type)
        message_count += 1 # Increment total number of messages

    logger.info(f"No type_reading at index {index} was found.")
    return { "message": f"No type_reading event at index {index}!"}, 404


def get_reading_stats():
    logger.info("GET request to '/stats' was received.")

    client = KafkaClient(hosts=HOSTNAME)
    topic = client.topics[str.encode(f"{app_config['events']['topic']}")]
    consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)

    message_count = 0
    # Tracking number of readings for each event type
    volume_readings_count = 0
    type_readings_count = 0

    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info("Message %i: %s" % (message_count, msg))
        if msg["type"] == "volume_reading":
            volume_readings_count += 1
        elif msg["type"] == "type_reading":
            type_readings_count += 1
        message_count += 1

    stats = {
        "num_volume_readings": volume_readings_count,
        "num_type_readings": type_readings_count
    }

    logger.debug(f"stats contents:\n{stats}")
    logger.info("GET request to '/stats' was received.")
    return stats, 200


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("hair-api-1.0.0-swagger.yaml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0") # Analyzer is running on port 8110