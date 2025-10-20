import contextlib
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
def test_video_record_path() -> str:
    """Path to the test-specific video record directory to prevent mediamtx from deleting test videos."""
    return "/app/assets/test_record/"


@pytest.fixture(scope="session")
def test_processed_path() -> str:
    """Path to the test processed videos directory."""
    return settings.VIDEO_PROCESSED_PATH


@pytest.fixture(scope="function")
def setup_test_videos(source_uri: str, test_video_record_path: str) -> Iterator[dict]:
    """Set up exactly 5 videos spaced by the source clip duration for deterministic tests."""
    # Create test directory structure
    test_cam_path = os.path.join(test_video_record_path, "test-cam")
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
        log.warning(f"Failed to cleanup test videos directory {test_cam_path}: {e}")


@pytest.fixture()
def make_video_process_request():
    """Factory for creating video process requests."""

    def _make(
        source_rtsp_path: str = "test-cam",
        start_time: str = "2025-01-15 10:15:00",
        end_time: str | None = "2025-01-15 10:45:00",
        duration_seconds: int | None = None,
    ):
        # Build dict manually without Pydantic validation
        request_data = {
            "source_rtsp_path": source_rtsp_path,
            "start_time": start_time,
        }

        if end_time is not None:
            request_data["end_time"] = end_time

        if duration_seconds is not None:
            request_data["duration_seconds"] = duration_seconds

        return request_data

    return _make


@pytest.fixture(scope="function")
def custom_client(request, test_video_record_path):
    """Flexible test client with configurable settings overrides."""
    overrides = request.param if hasattr(request, "param") else {}

    if overrides.pop("USE_TEST_RECORD_PATH", False):
        overrides["VIDEO_RECORD_PATH"] = test_video_record_path

    token = settings.API_TOKEN
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    # Patch settings
    patches = [patch.object(settings, key, value) for key, value in overrides.items()]

    # Also patch the video_service instance AND its queue_manager
    if "VIDEO_RECORD_PATH" in overrides:
        from app.services.video_process_service import video_service

        patches.append(
            patch.object(
                video_service, "video_record_path", overrides["VIDEO_RECORD_PATH"]
            )
        )
        patches.append(
            patch.object(
                video_service.queue_manager,
                "video_record_path",
                overrides["VIDEO_RECORD_PATH"],
            )
        )

    with contextlib.ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)

        with TestClient(app, headers=headers) as c:
            yield c
