from typing import Optional

from pydantic import BaseModel, Field

from app.core.enums import StreamState


class AddStreamRequest(BaseModel):
    video_path: str = Field(
        description="Local path to the source video file to loop and publish"
    )
    stream_id: Optional[str] = Field(
        default=None, description="Optional stream id; auto-assigned if omitted"
    )
    path: str = Field(
        description="Path segment to publish under RTSP base URL, e.g. 'cam1'"
    )


class StreamInfo(BaseModel):
    stream_id: str
    video_path: str
    state: StreamState


class HealthResponse(BaseModel):
    stream_id: str
    state: StreamState
