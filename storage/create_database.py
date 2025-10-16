from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import yaml # For using the yaml config file (app_conf)

with open('app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f.read())

ENGINE = create_engine(f"mysql+pymysql://{app_config['datastore']['user']}:{app_config['datastore']['password']}@{app_config['datastore']['hostname']}:{app_config['datastore']['port']}/{app_config['datastore']['db']}")


def make_session():
    return sessionmaker(bind=ENGINE)()
