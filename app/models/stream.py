from enum import Enum

from sqlalchemy import Column, DateTime, String, Text
from sqlalchemy.sql import func

from app.database.connection import Base


class StreamState(str, Enum):
    running = "running"
    error = "error"
    stopped = "stopped"


class Stream(Base):
    """SQLAlchemy model for storing stream information."""

    __tablename__ = "streams"

    stream_id = Column(String(255), primary_key=True, index=True)
    source_uri = Column(Text, nullable=False)
    output_url = Column(Text, nullable=False)
    state = Column(String(50), nullable=False, default=StreamState.error.value)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    def __repr__(self):
        return f"<Stream(stream_id='{self.stream_id}', state='{self.state}')>"
