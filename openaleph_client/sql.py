from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class File(Base):
    __tablename__ = "files"

    id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    file_path = Column(String, nullable=False, unique=True)
    is_file = Column(Boolean, default=False)
    processed = Column(Boolean, default=False)
    processed_at = Column(DateTime, nullable=True)
    to_skip = Column(Boolean, default=False)
    failed = Column(Boolean, default=False)
    opal_agent = Column(String, nullable=True)
    entity_id = Column(String, default=False)
    user = Column(String, default=False)

    def __repr__(self):
        return (
            f"<Files(id={self.id}, file_path='{self.file_path}', is_file={self.is_file}, "
            f"processed={self.processed}, processed_at={self.processed_at},"
            f"to_skip={self.to_skip}, failed={self.failed},"
            f"opal_agent='{self.opal_agent}', entity_id={self.entity_id},"
            f"user={self.user}>"
        )


def get_db_conn(database_uri) -> Engine:
    config = {}
    config.setdefault("pool_size", 1)
    if database_uri.startswith("postgresql+psycopg://"):
            config.setdefault("max_overflow", 5)
            config.setdefault("pool_timeout", 60)
            config.setdefault("pool_recycle", 3600)
            config.setdefault("pool_pre_ping", True) 
    engine = create_engine(f"{database_uri}", **config)
    Base.metadata.create_all(engine)

    return engine


