import os
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker


engine = create_engine(os.getenv("DB_CONNECTION") or "sqlite:///data/db.sqlite")
session = scoped_session(sessionmaker(
    autocommit=False, autoflush=False, bind=engine))


Base = declarative_base()
from .recording import *


Base.metadata.create_all(engine)