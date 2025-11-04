from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


def get_db_conn(db_file_path) -> Engine:
    engine = create_engine(f"sqlite:///{db_file_path}", echo=True)
    Base.metadata.create_all(engine)

    return engine


class File(Base):
    __tablename__ = "files"

    id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    file_path = Column(String, nullable=False, unique=True)
    processed = Column(Boolean, default=False)
    processed_at = Column(DateTime, nullable=True)
    to_skip = Column(Boolean, default=False)
    failed = Column(Boolean, default=False)
    opal_agent = Column(String, nullable=True)

    def __repr__(self):
        # format processed_at using dateime
        return (
            f"<Files(id={self.id}, file_path='{self.file_path}', processed={self.processed}, "
            f"processed_at={self.processed_at}, to_skip={self.to_skip}, "
            f"failed={self.failed}, opal_agent='{self.opal_agent}')>"
        )