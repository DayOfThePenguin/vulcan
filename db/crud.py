from sqlalchemy.ext.declarative import declarative_base


def recreate_database(engine):
    Base = declarative_base()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
