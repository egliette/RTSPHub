import os
import time
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from app.config.settings import settings
from app.models.stream import StreamState
from app.schema.video_process import TaskStatus
from app.tests.helpers import delete_video_file, parse_duration_to_seconds
from app.utils.logger import log
from app.utils.media import get_video_duration


class TestIntegrationLocalStorage:
    """Integration tests for local storage with stream initialization and video processing."""

    @pytest.mark.integration
    @pytest.mark.slow
    def test_stream_recording_and_video_processing_integration(
        self,
        client_with_local_storage: TestClient,
        make_add_stream_payload,
        make_video_process_request,
    ):
        """Integration test: Initialize stream, wait for recording, then process video.

        This test:
        1. Creates a stream and waits for it to be running
        2. Waits for 3 * RECORD_SEGMENT_DURATION to ensure recording has occurred
        3. Requests video processing for a time range from current_time - 2.5 * segment_duration
           to current_time - 1.5 * segment_duration
        4. Verifies the video processing completes successfully
        """
        segment_duration_str = settings.RECORD_SEGMENT_DURATION
        segment_duration_seconds = parse_duration_to_seconds(segment_duration_str)

        stream_id = "integration-test-stream"
        stream_path = "integration-test-path"

        log.info(f"Using stream ID: {stream_id} with path: {stream_path}")

        # 0. Check if stream already exists and remove it if it does
        existing_health = client_with_local_storage.get(
            f"/api/streams/{stream_id}/health"
        )
        if existing_health.status_code == 200:
            log.info(f"Stream {stream_id} already exists, removing it first...")
            delete_response = client_with_local_storage.delete(
                f"/api/streams/{stream_id}"
            )
            if delete_response.status_code == 204:
                log.info(f"Successfully removed existing stream {stream_id}")
            else:
                log.warn(
                    f"Failed to remove existing stream {stream_id}: {delete_response.text}"
                )
        elif existing_health.status_code == 404:
            log.info(f"Stream {stream_id} does not exist, proceeding with creation")
        else:
            log.warn(
                f"Unexpected response when checking existing stream: {existing_health.status_code}"
            )

        # 1. Create and initialize stream
        create_response = client_with_local_storage.post(
            "/api/streams",
            json=make_add_stream_payload(
                stream_id=stream_id,
                path=stream_path,
            ),
        )
        assert (
            create_response.status_code == 200
        ), f"Failed to create stream {stream_id}: {create_response.text}"

        stream_data = create_response.json()
        assert stream_data["stream_id"] == stream_id

        # 2. Wait for stream to be running
        max_wait_time = 30
        start_time = time.time()
        is_running = False

        while time.time() - start_time < max_wait_time:
            health_response = client_with_local_storage.get(
                f"/api/streams/{stream_id}/health"
            )
            if health_response.status_code == 200:
                health_data = health_response.json()
                state = health_data.get("state")
                if state == StreamState.running.value:
                    is_running = True
                    break
                elif state == StreamState.error.value:
                    pytest.fail(f"Stream {stream_id} failed to start: {health_data}")
            time.sleep(0.5)

        assert (
            is_running
        ), f"Stream {stream_id} did not reach running state within {max_wait_time} seconds"

        # 3. Wait for 3 * RECORD_SEGMENT_DURATION to ensure recording has occurred
        wait_duration = 3 * segment_duration_seconds
        log.info(
            f"Waiting for {wait_duration} seconds (3 * {segment_duration_str}) for recording..."
        )

        chunk_duration = 30
        remaining_time = wait_duration
        while remaining_time > 0:
            sleep_time = min(chunk_duration, remaining_time)
            time.sleep(sleep_time)
            remaining_time -= sleep_time

            health_response = client_with_local_storage.get(
                f"/api/streams/{stream_id}/health"
            )
            assert (
                health_response.status_code == 200
            ), f"Stream health check failed: {health_response.text}"

            health_data = health_response.json()
            stream_state = health_data.get("state")
            assert (
                stream_state == StreamState.running.value
            ), f"Stream is not running. State: {stream_state}, Health data: {health_data}"

            if remaining_time > 0:
                log.info(
                    f"Recording progress: {wait_duration - remaining_time}s elapsed, {remaining_time}s remaining... Stream is healthy"
                )
            else:
                log.info(
                    f"Recording wait completed: {wait_duration}s total elapsed. Stream is healthy"
                )

        # 4. Calculate time range for video processing
        # Request video from current_time - 2.5 * segment_duration to current_time - 1.5 * segment_duration
        current_time = datetime.now()
        start_offset = 2.5 * segment_duration_seconds
        end_offset = 1.5 * segment_duration_seconds

        start_time_dt = current_time - timedelta(seconds=int(start_offset))
        end_time_dt = current_time - timedelta(seconds=int(end_offset))

        start_time_str = start_time_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_time_str = end_time_dt.strftime("%Y-%m-%d %H:%M:%S")

        log.info(f"Requesting video from {start_time_str} to {end_time_str}")

        # 5. Create video processing request
        request_data = make_video_process_request(
            source_rtsp_path=stream_path,
            start_time=start_time_str,
            end_time=end_time_str,
        )

        process_response = client_with_local_storage.post(
            "/api/video-process/tasks", json=request_data
        )
        assert (
            process_response.status_code == 200
        ), f"Failed to create video processing task: {process_response.text}"

        task_id = process_response.json()["task_id"]
        log.info(f"Created video processing task: {task_id}")

        # 6. Poll for task completion
        max_processing_time = 60
        deadline = time.time() + max_processing_time
        final_result = None

        while time.time() < deadline:
            status_response = client_with_local_storage.get(
                f"/api/video-process/tasks/{task_id}"
            )
            assert (
                status_response.status_code == 200
            ), f"Failed to get task status: {status_response.text}"

            task_data = status_response.json()
            status = task_data.get("status")

            if status in [TaskStatus.completed.value, TaskStatus.error.value]:
                final_result = task_data
                break

            time.sleep(1)

        assert (
            final_result is not None
        ), f"Video processing task {task_id} did not complete within {max_processing_time} seconds"
        assert (
            final_result["status"] == TaskStatus.completed.value
        ), f"Video processing failed: {final_result.get('message', 'Unknown error')}"

        # 7. Verify output video exists and has reasonable duration
        output_path = final_result.get("result_video_uri")
        assert output_path is not None, "No output video path returned"
        assert os.path.exists(
            output_path
        ), f"Output video file does not exist: {output_path}"

        # Verify video duration is approximately what we requested
        video_duration = get_video_duration(output_path)
        requested_duration = (end_time_dt - start_time_dt).total_seconds()
        duration_diff = abs(video_duration - requested_duration)
        assert (
            duration_diff <= 5.0
        ), f"Video duration mismatch: expected ~{requested_duration}s, got {video_duration}s"

        log.info(
            f"Video processing completed successfully. Output: {output_path}, Duration: {video_duration}s"
        )

        # 8. Cleanup: Delete the processed video and stop the stream
        delete_video_file(client_with_local_storage, task_id, output_path)

        delete_response = client_with_local_storage.delete(f"/api/streams/{stream_id}")
        assert (
            delete_response.status_code == 204
        ), f"Failed to delete stream: {delete_response.text}"

        health_response = client_with_local_storage.get(
            f"/api/streams/{stream_id}/health"
        )
        assert (
            health_response.status_code == 404
        ), "Stream should be deleted but still exists"
