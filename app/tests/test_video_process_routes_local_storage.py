import os
import time
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from app.schema.video_process import TaskStatus
from app.tests.helpers import delete_video_file
from app.utils.media import get_video_duration


class TestVideoProcessRoutesWithLocalStorage:
    """Test cases for video processing API routes with local storage (MINIO_ENABLED = False)."""

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "custom_client",
        [{"MINIO_ENABLED": False, "USE_TEST_RECORD_PATH": True}],
        indirect=True,
    )
    def test_video_task_exactly_one_video(
        self,
        custom_client: TestClient,
        make_video_process_request,
        setup_test_videos,
    ):
        """Test video processing with exactly one video using local storage."""
        base = setup_test_videos["base_time"]
        dur = setup_test_videos["video_duration"]
        start_dt = base + timedelta(seconds=int(dur * 0.25))
        end_dt = base + timedelta(seconds=int(dur * 0.75))
        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        request_data = make_video_process_request(
            start_time=start_str, end_time=end_str
        )

        create_response = custom_client.post(
            "/api/video-process/tasks", json=request_data
        )
        assert (
            create_response.status_code == 200
        ), f"Expected status 200, got {create_response.status_code}. Response: {create_response.text}"
        task_id = create_response.json()["task_id"]

        deadline = time.time() + 30
        final = None
        while time.time() < deadline:
            resp = custom_client.get(f"/api/video-process/tasks/{task_id}")
            assert resp.status_code == 200
            data = resp.json()
            if data["status"] in [TaskStatus.completed.value, TaskStatus.error.value]:
                final = data
                break
            time.sleep(1)

        assert final is not None, "Task did not finish in time"
        assert final["status"] == TaskStatus.completed.value, final.get("message")

        # With local storage, result_video_uri should be a local file path
        output_path = final.get("result_video_uri")
        assert output_path and os.path.exists(output_path)
        result_duration = get_video_duration(output_path)
        requested_seconds = (end_dt - start_dt).total_seconds()
        assert abs(result_duration - requested_seconds) <= 5.0

        delete_video_file(custom_client, task_id, output_path)

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "custom_client",
        [{"MINIO_ENABLED": False, "USE_TEST_RECORD_PATH": True}],
        indirect=True,
    )
    def test_video_task_edge_case_left_overlap(
        self,
        custom_client: TestClient,
        make_video_process_request,
        setup_test_videos,
    ):
        """Test video processing edge case where start_time < oldest_video_start_time < end_time."""
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

        create_response = custom_client.post(
            "/api/video-process/tasks", json=request_data
        )
        assert (
            create_response.status_code == 200
        ), f"Expected status 200, got {create_response.status_code}. Response: {create_response.text}"
        task_id = create_response.json()["task_id"]

        deadline = time.time() + 30
        final = None
        while time.time() < deadline:
            resp = custom_client.get(f"/api/video-process/tasks/{task_id}")
            assert resp.status_code == 200
            data = resp.json()
            if data["status"] in [TaskStatus.completed.value, TaskStatus.error.value]:
                final = data
                break
            time.sleep(1)

        assert final is not None, "Task did not finish in time"
        assert final["status"] == TaskStatus.completed.value, final.get("message")

        # With local storage, result_video_uri should be a local file path
        output_path = final.get("result_video_uri")
        assert output_path and os.path.exists(output_path)
        result_duration = get_video_duration(output_path)
        # The result should contain all available video content, not the full requested range
        # since we only have videos from 'base' onwards
        expected_seconds = (
            end_dt - base
        ).total_seconds()  # From first video to end time
        assert abs(result_duration - expected_seconds) <= 5.0

        delete_video_file(custom_client, task_id, output_path)

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "custom_client",
        [{"MINIO_ENABLED": False, "USE_TEST_RECORD_PATH": True}],
        indirect=True,
    )
    def test_video_task_exactly_two_videos(
        self,
        custom_client: TestClient,
        make_video_process_request,
        setup_test_videos,
    ):
        """Test video processing with exactly two videos using local storage."""
        base = setup_test_videos["base_time"]
        dur = setup_test_videos["video_duration"]
        start_dt = base + timedelta(seconds=int(dur * 2 + dur / 2))
        end_dt = base + timedelta(seconds=int(dur * 3 + dur / 2))
        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        request_data = make_video_process_request(
            start_time=start_str, end_time=end_str
        )

        create_response = custom_client.post(
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
            resp = custom_client.get(f"/api/video-process/tasks/{task_id}")
            assert resp.status_code == 200
            data = resp.json()
            if data["status"] in [TaskStatus.completed.value, TaskStatus.error.value]:
                final = data
                break
            time.sleep(1)

        assert final is not None, "Task did not finish in time"
        assert final["status"] == TaskStatus.completed.value, final.get("message")

        # With local storage, result_video_uri should be a local file path
        output_path = final.get("result_video_uri")
        assert output_path and os.path.exists(output_path)
        result_duration = get_video_duration(output_path)
        requested_seconds = (end_dt - start_dt).total_seconds()
        assert abs(result_duration - requested_seconds) <= 5.0

        delete_video_file(custom_client, task_id, output_path)

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "custom_client",
        [{"MINIO_ENABLED": False, "USE_TEST_RECORD_PATH": True}],
        indirect=True,
    )
    def test_video_task_exactly_three_videos(
        self,
        custom_client: TestClient,
        make_video_process_request,
        setup_test_videos,
    ):
        """Test video processing with exactly three videos using local storage."""
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

        create_response = custom_client.post(
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
            resp = custom_client.get(f"/api/video-process/tasks/{task_id}")
            assert resp.status_code == 200
            data = resp.json()
            if data["status"] in [TaskStatus.completed.value, TaskStatus.error.value]:
                final = data
                break
            time.sleep(1)

        assert final is not None, "Task did not finish in time"
        assert final["status"] == TaskStatus.completed.value, final.get("message")

        # With local storage, result_video_uri should be a local file path
        output_path = final.get("result_video_uri")
        assert output_path and os.path.exists(output_path)
        result_duration = get_video_duration(output_path)
        requested_seconds = (end_dt - start_dt).total_seconds()
        assert abs(result_duration - requested_seconds) <= 5.0

        delete_video_file(custom_client, task_id, output_path)

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "custom_client",
        [{"MINIO_ENABLED": False, "USE_TEST_RECORD_PATH": True}],
        indirect=True,
    )
    def test_video_task_three_concurrent_requests_single_video(
        self,
        custom_client: TestClient,
        make_video_process_request,
        setup_test_videos,
    ):
        """Test video processing with 3 concurrent requests all using time ranges from a single video."""
        base = setup_test_videos["base_time"]
        dur = setup_test_videos["video_duration"]

        # Create 3 different time ranges all within the first video
        time_ranges = [
            (
                base + timedelta(seconds=int(dur * 0.1)),
                base + timedelta(seconds=int(dur * (i * 0.3))),
            )
            for i in range(1, 4)
        ]
        request_data_list = [
            make_video_process_request(
                start_time=start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                end_time=end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            )
            for start_dt, end_dt in time_ranges
        ]
        create_responses = [
            custom_client.post("/api/video-process/tasks", json=request_data)
            for request_data in request_data_list
        ]

        # Verify all tasks were created successfully
        task_ids = []
        for i, response in enumerate(create_responses):
            assert (
                response.status_code == 200
            ), f"Task {i+1} creation failed: {response.text}"
            task_ids.append(response.json()["task_id"])

        # Poll for completion of all 3 tasks
        deadline = time.time() + 60
        results = {}

        while time.time() < deadline and len(results) < len(task_ids):
            for task_id in task_ids:
                if task_id in results:
                    continue
                resp = custom_client.get(f"/api/video-process/tasks/{task_id}")
                assert resp.status_code == 200
                data = resp.json()
                if data["status"] in [
                    TaskStatus.completed.value,
                    TaskStatus.error.value,
                ]:
                    results[task_id] = data
            time.sleep(1)

        assert len(results) == len(
            task_ids
        ), f"Not all tasks completed in time. Completed: {len(results)}/3"

        for task_id, result in results.items():
            assert (
                result["status"] == TaskStatus.completed.value
            ), f"Task {task_id} failed: {result.get('message')}"

        # Verify output files and durations
        for i, (task_id, result) in enumerate(results.items()):
            output_path = result.get("result_video_uri")
            assert output_path and os.path.exists(
                output_path
            ), f"Task {i+1} output file not found"
            result_duration = get_video_duration(output_path)
            start_dt, end_dt = time_ranges[i]
            requested_seconds = (end_dt - start_dt).total_seconds()
            assert (
                abs(result_duration - requested_seconds) <= 5.0
            ), f"Task {i+1} duration mismatch: expected ~{requested_seconds}s, got {result_duration}s"

            delete_video_file(custom_client, task_id, output_path)
