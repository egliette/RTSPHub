from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from app.config.settings import settings
from app.utils.logger import log

try:
    engine = create_engine(
        settings.DATABASE_URL, connect_args={"check_same_thread": False}, echo=False
    )
    log.info(f"Database engine created successfully")
except Exception as e:
    log.err(f"Failed to create database engine: {e}")
    raise

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        log.err(f"Database session error: {e}")
        db.rollback()
        raise
    finally:
        db.close()
