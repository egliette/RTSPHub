import os
from datetime import timedelta
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from app.schema.video_process import TaskStatus
from app.utils.media import get_video_duration


class TestVideoProcessRoutes:
    """Test cases for video processing API routes."""

    def test_create_video_task_success(
        self, client: TestClient, make_video_process_request
    ):
        """Test successful video task creation."""
        request_data = make_video_process_request()

        response = client.post("/api/video-process/tasks", json=request_data)

        assert response.status_code == 200
        data = response.json()

        # Validate response structure
        assert "task_id" in data
        assert "status" in data
        assert "message" in data
        assert "created_at" in data
        assert "updated_at" in data

        # Validate task status
        assert data["message"] == "Task created successfully"

        # Validate task_id is a valid UUID
        task_id = UUID(data["task_id"])
        assert isinstance(task_id, UUID)

    def test_create_video_task_invalid_time_format(
        self, client: TestClient, make_video_process_request
    ):
        """Test video task creation with invalid time format."""
        request_data = make_video_process_request(
            start_time="invalid-time-format", end_time="2025-01-15 10:45:00"
        )

        response = client.post("/api/video-process/tasks", json=request_data)

        assert response.status_code == 400
        data = response.json()
        assert "detail" in data

    def test_create_video_task_invalid_time_range(
        self, client: TestClient, make_video_process_request
    ):
        """Test video task creation with invalid time range (end before start)."""
        request_data = make_video_process_request(
            start_time="2025-01-15 10:45:00",
            end_time="2025-01-15 10:15:00",  # End before start
        )

        response = client.post("/api/video-process/tasks", json=request_data)

        assert response.status_code == 400
        data = response.json()
        assert "detail" in data

    def test_list_video_tasks(self, client: TestClient, make_video_process_request):
        """Test listing video tasks with existing tasks."""
        # Create a few tasks
        task_ids = []
        for i in range(3):
            request_data = make_video_process_request(
                start_time=f"2025-01-15 10:{15 + i*10:02d}:00",
                end_time=f"2025-01-15 10:{25 + i*10:02d}:00",
            )

            response = client.post("/api/video-process/tasks", json=request_data)
            assert response.status_code == 200
            task_ids.append(response.json()["task_id"])

        # List all tasks
        response = client.get("/api/video-process/tasks")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 3  # Should have at least the 3 tasks we created

        # Verify all our tasks are in the list
        returned_task_ids = [task["task_id"] for task in data]
        for task_id in task_ids:
            assert task_id in returned_task_ids

    def test_get_video_task_status_not_found(self, client: TestClient):
        """Test getting status of a non-existent video task."""
        fake_task_id = "00000000-0000-0000-0000-000000000000"

        response = client.get(f"/api/video-process/tasks/{fake_task_id}")

        assert response.status_code == 404
        data = response.json()
        assert "detail" in data

    def test_video_task_with_no_matching_videos(
        self, client: TestClient, make_video_process_request
    ):
        """Test video task creation with time range that has no matching videos."""
        # Create task for time range with no videos
        request_data = make_video_process_request(
            start_time="2025-01-15 20:00:00", end_time="2025-01-15 20:30:00"
        )

        create_response = client.post("/api/video-process/tasks", json=request_data)
        assert create_response.status_code == 200

        task_id = create_response.json()["task_id"]

        # Wait for processing
        import time

        time.sleep(3)

        # Check task status - should be error
        status_response = client.get(f"/api/video-process/tasks/{task_id}")
        assert status_response.status_code == 200

        status_data = status_response.json()
        assert status_data["status"] == TaskStatus.error.value

    @pytest.mark.slow
    def test_video_task_exactly_three_videos(
        self, client: TestClient, make_video_process_request, setup_test_videos
    ):
        """Range from mid of video 1 to mid of video 3 → overlaps exactly 3 videos."""
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

        create_response = client.post("/api/video-process/tasks", json=request_data)
        assert (
            create_response.status_code == 200
        ), f"Expected status 200, got {create_response.status_code}. Response: {create_response.text}"
        task_id = create_response.json()["task_id"]

        # Poll for completion
        import time

        deadline = time.time() + 30
        final = None
        while time.time() < deadline:
            resp = client.get(f"/api/video-process/tasks/{task_id}")
            assert resp.status_code == 200
            data = resp.json()
            if data["status"] in [TaskStatus.completed.value, TaskStatus.error.value]:
                final = data
                break
            time.sleep(1)

        assert final is not None, "Task did not finish in time"
        assert final["status"] == TaskStatus.completed.value, final.get("message")
        assert "Successfully processed 3 video segments" in final.get("message", "")
        # Validate output duration approximately equals requested duration
        output_path = final.get("result_video_uri")
        assert output_path and os.path.exists(output_path)
        result_duration = get_video_duration(output_path)
        requested_seconds = (end_dt - start_dt).total_seconds()
        assert abs(result_duration - requested_seconds) <= 1.0

        # Cleanup: Remove result video file
        try:
            if output_path and os.path.exists(output_path):
                os.remove(output_path)
        except Exception:
            pass  # Ignore cleanup errors

    @pytest.mark.slow
    def test_video_task_exactly_two_videos(
        self, client: TestClient, make_video_process_request, setup_test_videos
    ):
        """Range from mid of video 2 to mid of video 3 → overlaps exactly 2 videos."""
        base = setup_test_videos["base_time"]
        dur = setup_test_videos["video_duration"]
        start_dt = base + timedelta(seconds=int(dur * 2 + dur / 2))
        end_dt = base + timedelta(seconds=int(dur * 3 + dur / 2))
        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        request_data = make_video_process_request(
            start_time=start_str, end_time=end_str
        )

        create_response = client.post("/api/video-process/tasks", json=request_data)
        assert (
            create_response.status_code == 200
        ), f"Expected status 200, got {create_response.status_code}. Response: {create_response.text}"
        task_id = create_response.json()["task_id"]

        # Poll for completion
        import time

        deadline = time.time() + 30
        final = None
        while time.time() < deadline:
            resp = client.get(f"/api/video-process/tasks/{task_id}")
            assert resp.status_code == 200
            data = resp.json()
            if data["status"] in [TaskStatus.completed.value, TaskStatus.error.value]:
                final = data
                break
            time.sleep(1)

        assert final is not None, "Task did not finish in time"
        assert final["status"] == TaskStatus.completed.value, final.get("message")
        assert "Successfully processed 2 video segments" in final.get("message", "")
        output_path = final.get("result_video_uri")
        assert output_path and os.path.exists(output_path)
        result_duration = get_video_duration(output_path)
        requested_seconds = (end_dt - start_dt).total_seconds()
        assert abs(result_duration - requested_seconds) <= 1.0

        # Cleanup: Remove result video file
        try:
            if output_path and os.path.exists(output_path):
                os.remove(output_path)
        except Exception:
            pass  # Ignore cleanup errors

    @pytest.mark.slow
    def test_video_task_exactly_one_video(
        self, client: TestClient, make_video_process_request, setup_test_videos
    ):
        """Range from first video's quarter to three-quarters → overlaps exactly 1 video."""
        base = setup_test_videos["base_time"]
        dur = setup_test_videos["video_duration"]
        start_dt = base + timedelta(seconds=int(dur * 0.25))
        end_dt = base + timedelta(seconds=int(dur * 0.75))
        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        request_data = make_video_process_request(
            start_time=start_str, end_time=end_str
        )
        create_response = client.post("/api/video-process/tasks", json=request_data)
        assert (
            create_response.status_code == 200
        ), f"Expected status 200, got {create_response.status_code}. Response: {create_response.text}"
        task_id = create_response.json()["task_id"]
        import time

        deadline = time.time() + 30
        final = None
        while time.time() < deadline:
            resp = client.get(f"/api/video-process/tasks/{task_id}")
            assert resp.status_code == 200
            data = resp.json()
            if data["status"] in [TaskStatus.completed.value, TaskStatus.error.value]:
                final = data
                break
            time.sleep(1)
        assert final is not None, "Task did not finish in time"
        assert final["status"] == TaskStatus.completed.value, final.get("message")
        assert "Successfully processed 1 video segments" in final.get("message", "")
        output_path = final.get("result_video_uri")
        assert output_path and os.path.exists(output_path)
        result_duration = get_video_duration(output_path)
        requested_seconds = (end_dt - start_dt).total_seconds()
        assert abs(result_duration - requested_seconds) <= 1.0

        # Cleanup: Remove result video file
        try:
            if output_path and os.path.exists(output_path):
                os.remove(output_path)
        except Exception:
            pass  # Ignore cleanup errors
