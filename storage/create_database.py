from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# For creating and displaying log messages (log_conf)
import logging
import logging.config

import yaml # For using the yaml config files (app_conf and log_conf)

with open('config/app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f.read())

# Logging configurations
with open("config/log_conf.yaml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')

logger.info(f"MySQL connection string: 'mysql+pymysql://{app_config['datastore']['user']}:{app_config['datastore']['password']}@{app_config['datastore']['hostname']}:{app_config['datastore']['port']}/{app_config['datastore']['db']}'")

ENGINE = create_engine(f"mysql+pymysql://{app_config['datastore']['user']}:{app_config['datastore']['password']}@{app_config['datastore']['hostname']}:{app_config['datastore']['port']}/{app_config['datastore']['db']}", pool_recycle=360)


def make_session():
    return sessionmaker(bind=ENGINE)()
