import connexion
from connexion import NoContent

import json # For data operations
from datetime import datetime, timezone # For creating and formatting timestamps and converting timezones

import uuid # For creating trace identifiers
import httpx # For sending post requests to storage service (for storing in database)

import yaml # For using the yaml config file (app_conf)

# For creating and displaying log messages (log_conf)
import logging
import logging.config

from pykafka import KafkaClient # For message brokering


with open('app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f.read())

VOLUME_URL = app_config['events']['volume']['url']
TYPE_URL = app_config['events']['type']['url']


with open("log_conf.yaml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')


# Message brokering
client = KafkaClient(hosts=f"{app_config['events']['hostname']}:{app_config['events']['port']}")
topic = client.topics[str.encode(f"{app_config['events']['topic']}")]
producer = topic.get_sync_producer()


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
        producer.produce(msg_str.encode('utf-8'))

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
        producer.produce(msg_str.encode('utf-8'))

        # logger.debug(f"type_reading msg: {msg_str.encode('utf-8')}")
        logger.info(f"Response for event type_reading (id: {trace_id}) has status 201")

    return NoContent, 201


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("hair-api-1.0.0-swagger.yaml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":    
    app.run(port=8080)