from models import Base
import create_database as cd # From create_database.py, for the engine

# For creating and displaying log messages (log_conf)
import logging
import logging.config

import yaml # For using the yaml config files (app_conf and log_conf)


# Logging configurations
with open("config/log_conf.yaml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')


logger.info(f"create_tables.py script: Creating tables")

Base.metadata.create_all(cd.ENGINE)