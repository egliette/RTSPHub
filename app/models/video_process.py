from enum import Enum
from uuid import uuid4

from sqlalchemy import Column, DateTime
from sqlalchemy import Enum as SQLEnum
from sqlalchemy import String, func
from sqlalchemy.dialects.sqlite import CHAR

from app.database.connection import Base


class TaskStatus(str, Enum):
    """Task status enumeration."""

    pending = "pending"
    processing = "processing"
    completed = "completed"
    error = "error"


class VideoProcessTask(Base):
    """Database model for video merge tasks."""

    __tablename__ = "video_merge_tasks"

    id = Column(CHAR(36), primary_key=True, default=lambda: str(uuid4()))
    source_rtsp_path = Column(String, nullable=False)
    start_time = Column(String, nullable=False)  # Store as string, parse when needed
    end_time = Column(String, nullable=False)
    status = Column(SQLEnum(TaskStatus), nullable=False, default=TaskStatus.pending)
    message = Column(String, nullable=True)
    result_video_path = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self):
        return f"<VideoMergeTask(id={self.id}, status={self.status}, source_path={self.source_rtsp_path})>"
