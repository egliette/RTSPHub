import os
import shutil
from datetime import datetime, timedelta
from typing import Iterator
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.config.settings import settings
from app.main import app
from app.schema.streaming import AddStreamRequest
from app.schema.video_process import VideoProcessRequest
from app.utils.logger import log


@pytest.fixture(scope="session")
def client() -> Iterator[TestClient]:
    token = settings.API_TOKEN
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    with TestClient(app, headers=headers) as c:
        yield c


@pytest.fixture(scope="session")
def source_uri() -> str:
    path = "/app/assets/videos/big_buck_bunny.mp4"
    return path


@pytest.fixture()
def make_add_stream_payload():

    def _make(
        source_uri: str = "/app/assets/videos/big_buck_bunny.mp4",
        path: str = "test-stream",
        stream_id: str | None = None,
    ):
        model = AddStreamRequest(
            source_uri=source_uri,
            stream_id=stream_id,
            path=path,
        )
        # Support both Pydantic v1 and v2
        to_dict = getattr(model, "model_dump", None)
        return to_dict() if callable(to_dict) else model

    return _make


@pytest.fixture(scope="session")
def test_record_path() -> str:
    """Path to the test record directory."""
    return settings.VIDEO_RECORD_PATH


@pytest.fixture(scope="session")
def test_processed_path() -> str:
    """Path to the test processed videos directory."""
    return settings.VIDEO_PROCESSED_PATH


@pytest.fixture(scope="session")
def setup_test_videos(source_uri: str, test_record_path: str) -> Iterator[dict]:
    """Set up exactly 5 videos spaced by the source clip duration for deterministic tests."""
    # Create test directory structure
    test_cam_path = os.path.join(test_record_path, "test-cam")
    os.makedirs(test_cam_path, exist_ok=True)

    # Fixed base time for reproducibility
    base_time = datetime(2025, 1, 15, 10, 0, 0)
    test_videos = []

    # Probe duration of source test video (in seconds)
    from app.utils.media import get_video_duration

    video_duration = get_video_duration(source_uri)

    # Create 5 test videos, each spaced by exactly the source duration
    for i in range(5):
        video_time = base_time + timedelta(seconds=int(video_duration) * i)
        timestamp_str = video_time.strftime("%Y-%m-%d_%H-%M-%S")
        test_video_path = os.path.join(test_cam_path, f"{timestamp_str}.mp4")

        # Copy the fixed 1-minute source video to create the timeline
        shutil.copy2(source_uri, test_video_path)
        test_videos.append(
            {
                "path": test_video_path,
                "timestamp": video_time,
                "filename": f"{timestamp_str}.mp4",
            }
        )

    yield {
        "cam_path": test_cam_path,
        "videos": test_videos,
        "base_time": base_time,
        "video_duration": video_duration,
    }

    # Cleanup: Remove test videos
    try:
        shutil.rmtree(test_cam_path)
    except Exception as e:
        log.warn(f"Failed to cleanup test videos directory {test_cam_path}: {e}")


@pytest.fixture()
def make_video_process_request():
    """Factory for creating video process requests."""

    def _make(
        source_rtsp_path: str = "test-cam",
        start_time: str = "2025-01-15 10:15:00",
        end_time: str = "2025-01-15 10:45:00",
    ):
        return VideoProcessRequest(
            source_rtsp_path=source_rtsp_path, start_time=start_time, end_time=end_time
        ).model_dump()

    return _make


@pytest.fixture()
def disable_minio():
    """Fixture to disable MinIO for testing with local storage."""
    with patch.object(settings, "MINIO_ENABLED", False):
        yield


@pytest.fixture()
def enable_minio():
    """Fixture to enable MinIO for testing with MinIO storage."""
    with patch.object(settings, "MINIO_ENABLED", True):
        yield


@pytest.fixture()
def client_with_local_storage(disable_minio):
    """Test client with MinIO disabled (local storage only)."""
    token = settings.API_TOKEN
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    with TestClient(app, headers=headers) as c:
        yield c


@pytest.fixture()
def client_with_minio_storage(enable_minio):
    """Test client with MinIO enabled (MinIO storage)."""
    token = settings.API_TOKEN
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    with TestClient(app, headers=headers) as c:
        yield c
