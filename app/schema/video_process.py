from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class VideoProcessRequest(BaseModel):
    """Schema for video processing (clip/merge) request input."""

    source_rtsp_path: str = Field(
        description="RTSP source path (e.g., 'cam1', 'cam2') - videos are stored at /app/assets/videos/{path}/"
    )
    start_time: str = Field(
        description="Start time in format: 'YYYY-MM-DD HH:MM:SS' (e.g., '2025-10-02 16:39:00')"
    )
    end_time: str = Field(
        description="End time in format: 'YYYY-MM-DD HH:MM:SS' (e.g., '2025-10-02 16:41:30')"
    )


class TaskStatus(str, Enum):
    """Task status enumeration."""

    pending = "pending"
    processing = "processing"
    completed = "completed"
    error = "error"


class VideoProcessResponse(BaseModel):
    """Schema for video processing task creation response."""

    task_id: UUID = Field(description="Unique task identifier")
    status: TaskStatus = Field(description="Current task status")
    message: Optional[str] = Field(
        default=None, description="Status message or error details"
    )
    result_video_uri: Optional[str] = Field(
        default=None, description="Target output URI (local path or presigned URL)"
    )
    created_at: datetime = Field(description="Task creation timestamp")
    updated_at: Optional[datetime] = Field(
        default=None, description="Last status update timestamp"
    )


class VideoProcessStatus(BaseModel):
    """Schema for checking processing task status."""

    task_id: UUID = Field(description="Unique task identifier")
    status: TaskStatus = Field(description="Current task status")
    message: Optional[str] = Field(
        default=None, description="Status message or error details"
    )
    result_video_uri: Optional[str] = Field(
        default=None, description="Target output URI (local path or presigned URL)"
    )
    created_at: datetime = Field(description="Task creation timestamp")
    updated_at: Optional[datetime] = Field(
        default=None, description="Last status update timestamp"
    )
