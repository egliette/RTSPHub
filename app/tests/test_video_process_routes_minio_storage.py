import os
import tempfile
import time
from datetime import timedelta
from urllib.request import urlretrieve

import pytest
from fastapi.testclient import TestClient

from app.schema.video_process import TaskStatus
from app.tests.helpers import delete_video_file
from app.utils.logger import log
from app.utils.media import get_video_duration


class TestVideoProcessRoutesWithMinIOStorage:
    """Test cases for video processing API routes with MinIO storage (MINIO_ENABLED = True)."""

    @pytest.mark.slow
    def test_video_task_exactly_one_video(
        self,
        client_with_minio_storage: TestClient,
        make_video_process_request,
        setup_test_videos,
    ):
        """Test video processing with exactly one video using MinIO storage."""
        base = setup_test_videos["base_time"]
        dur = setup_test_videos["video_duration"]
        start_dt = base + timedelta(seconds=int(dur * 0.25))
        end_dt = base + timedelta(seconds=int(dur * 0.75))
        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        request_data = make_video_process_request(
            start_time=start_str, end_time=end_str
        )

        create_response = client_with_minio_storage.post(
            "/api/video-process/tasks", json=request_data
        )
        assert (
            create_response.status_code == 200
        ), f"Expected status 200, got {create_response.status_code}. Response: {create_response.text}"
        task_id = create_response.json()["task_id"]

        deadline = time.time() + 30
        final = None
        while time.time() < deadline:
            resp = client_with_minio_storage.get(f"/api/video-process/tasks/{task_id}")
            assert resp.status_code == 200
            data = resp.json()
            if data["status"] in [TaskStatus.completed.value, TaskStatus.error.value]:
                final = data
                break
            time.sleep(1)

        assert final is not None, "Task did not finish in time"
        assert final["status"] == TaskStatus.completed.value, final.get("message")

        # With MinIO storage, result_video_uri should be a presigned URL
        output_uri = final.get("result_video_uri")
        assert output_uri and output_uri.startswith("http")

        # Download the video from presigned URL to check duration
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as temp_file:
            try:
                urlretrieve(output_uri, temp_file.name)
                result_duration = get_video_duration(temp_file.name)
                requested_seconds = (end_dt - start_dt).total_seconds()
                assert abs(result_duration - requested_seconds) <= 5.0
            finally:
                # Cleanup: Remove temporary downloaded file
                try:
                    if os.path.exists(temp_file.name):
                        os.remove(temp_file.name)
                except Exception as e:
                    log.warn(
                        f"Failed to cleanup temporary video file {temp_file.name}: {e}"
                    )

        delete_video_file(client_with_minio_storage, task_id)

    @pytest.mark.slow
    def test_video_task_edge_case_left_overlap(
        self,
        client_with_minio_storage: TestClient,
        make_video_process_request,
        setup_test_videos,
    ):
        """Test video processing edge case where start_time < oldest_video_start_time < end_time using MinIO storage."""
        base = setup_test_videos["base_time"]
        dur = setup_test_videos["video_duration"]
        # Request time range starts 30 seconds before the first video and extends well beyond
        start_dt = base - timedelta(seconds=30)
        end_dt = base + timedelta(seconds=int(dur * 0.5))
        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        request_data = make_video_process_request(
            start_time=start_str, end_time=end_str
        )

        create_response = client_with_minio_storage.post(
            "/api/video-process/tasks", json=request_data
        )
        assert (
            create_response.status_code == 200
        ), f"Expected status 200, got {create_response.status_code}. Response: {create_response.text}"
        task_id = create_response.json()["task_id"]

        deadline = time.time() + 30
        final = None
        while time.time() < deadline:
            resp = client_with_minio_storage.get(f"/api/video-process/tasks/{task_id}")
            assert resp.status_code == 200
            data = resp.json()
            if data["status"] in [TaskStatus.completed.value, TaskStatus.error.value]:
                final = data
                break
            time.sleep(1)

        assert final is not None, "Task did not finish in time"
        assert final["status"] == TaskStatus.completed.value, final.get("message")

        # With MinIO storage, result_video_uri should be a presigned URL
        output_uri = final.get("result_video_uri")
        assert output_uri and output_uri.startswith("http")

        # Download the video from presigned URL to check duration
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as temp_file:
            try:
                urlretrieve(output_uri, temp_file.name)
                result_duration = get_video_duration(temp_file.name)
                # The result should contain all available video content, not the full requested range
                # since we only have videos from 'base' onwards
                expected_seconds = (
                    end_dt - base
                ).total_seconds()  # From first video to end time
                assert abs(result_duration - expected_seconds) <= 5.0
            finally:
                # Cleanup: Remove temporary downloaded file
                try:
                    if os.path.exists(temp_file.name):
                        os.remove(temp_file.name)
                except Exception as e:
                    log.warn(
                        f"Failed to cleanup temporary video file {temp_file.name}: {e}"
                    )

        delete_video_file(client_with_minio_storage, task_id)

    @pytest.mark.slow
    def test_video_task_exactly_two_videos(
        self,
        client_with_minio_storage: TestClient,
        make_video_process_request,
        setup_test_videos,
    ):
        """Test video processing with exactly two videos using MinIO storage."""
        base = setup_test_videos["base_time"]
        dur = setup_test_videos["video_duration"]
        start_dt = base + timedelta(seconds=int(dur * 2 + dur / 2))
        end_dt = base + timedelta(seconds=int(dur * 3 + dur / 2))
        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        request_data = make_video_process_request(
            start_time=start_str, end_time=end_str
        )

        create_response = client_with_minio_storage.post(
            "/api/video-process/tasks", json=request_data
        )
        assert (
            create_response.status_code == 200
        ), f"Expected status 200, got {create_response.status_code}. Response: {create_response.text}"
        task_id = create_response.json()["task_id"]

        # Poll for completion
        deadline = time.time() + 30
        final = None
        while time.time() < deadline:
            resp = client_with_minio_storage.get(f"/api/video-process/tasks/{task_id}")
            assert resp.status_code == 200
            data = resp.json()
            if data["status"] in [TaskStatus.completed.value, TaskStatus.error.value]:
                final = data
                break
            time.sleep(1)

        assert final is not None, "Task did not finish in time"
        assert final["status"] == TaskStatus.completed.value, final.get("message")

        # With MinIO storage, result_video_uri should be a presigned URL
        output_uri = final.get("result_video_uri")
        assert output_uri and output_uri.startswith("http")

        # Download the video from presigned URL to check duration
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as temp_file:
            try:
                urlretrieve(output_uri, temp_file.name)
                result_duration = get_video_duration(temp_file.name)
                requested_seconds = (end_dt - start_dt).total_seconds()
                assert abs(result_duration - requested_seconds) <= 5.0
            finally:
                # Cleanup: Remove temporary downloaded file
                try:
                    if os.path.exists(temp_file.name):
                        os.remove(temp_file.name)
                except Exception as e:
                    log.warn(
                        f"Failed to cleanup temporary video file {temp_file.name}: {e}"
                    )

        delete_video_file(client_with_minio_storage, task_id)

    @pytest.mark.slow
    def test_video_task_exactly_three_videos(
        self,
        client_with_minio_storage: TestClient,
        make_video_process_request,
        setup_test_videos,
    ):
        """Test video processing with exactly three videos using MinIO storage."""
        base = setup_test_videos["base_time"]
        dur = setup_test_videos["video_duration"]
        # videos start at base + i*dur
        start_dt = base + timedelta(seconds=int(dur * 1 + dur / 2))
        end_dt = base + timedelta(seconds=int(dur * 3 + dur / 2))
        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        request_data = make_video_process_request(
            start_time=start_str, end_time=end_str
        )

        create_response = client_with_minio_storage.post(
            "/api/video-process/tasks", json=request_data
        )
        assert (
            create_response.status_code == 200
        ), f"Expected status 200, got {create_response.status_code}. Response: {create_response.text}"
        task_id = create_response.json()["task_id"]

        # Poll for completion
        deadline = time.time() + 30
        final = None
        while time.time() < deadline:
            resp = client_with_minio_storage.get(f"/api/video-process/tasks/{task_id}")
            assert resp.status_code == 200
            data = resp.json()
            if data["status"] in [TaskStatus.completed.value, TaskStatus.error.value]:
                final = data
                break
            time.sleep(1)

        assert final is not None, "Task did not finish in time"
        assert final["status"] == TaskStatus.completed.value, final.get("message")

        # With MinIO storage, result_video_uri should be a presigned URL
        output_uri = final.get("result_video_uri")
        assert output_uri and output_uri.startswith("http")

        # Download the video from presigned URL to check duration
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as temp_file:
            try:
                urlretrieve(output_uri, temp_file.name)
                result_duration = get_video_duration(temp_file.name)
                requested_seconds = (end_dt - start_dt).total_seconds()
                assert abs(result_duration - requested_seconds) <= 5.0
            finally:
                # Cleanup: Remove temporary downloaded file
                try:
                    if os.path.exists(temp_file.name):
                        os.remove(temp_file.name)
                except Exception as e:
                    log.warn(
                        f"Failed to cleanup temporary video file {temp_file.name}: {e}"
                    )

        delete_video_file(client_with_minio_storage, task_id)
