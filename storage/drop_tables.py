from models import Base
import create_database as cd # From create_database.py, for the engine

Base.metadata.drop_all(cd.ENGINE)