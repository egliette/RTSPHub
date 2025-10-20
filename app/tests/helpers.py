import os
import re
from typing import Optional

from fastapi.testclient import TestClient


def parse_duration_to_seconds(duration_str: str) -> int:
    """Parse duration string like '1h', '2m', '30s' to seconds.

    Args:
        duration_str: Duration string in format like '1h', '2m', '30s', '1h30m', etc.

    Returns:
        Duration in seconds as integer.

    Raises:
        ValueError: If duration string format is invalid.
    """
    if not duration_str:
        raise ValueError("Duration string cannot be empty")

    duration_str = duration_str.strip()
    pattern = r"(\d+)([hms])"
    matches = re.findall(pattern, duration_str)

    if not matches:
        raise ValueError(f"Invalid duration format: {duration_str}")

    total_seconds = 0
    for value, unit in matches:
        value = int(value)
        if unit == "h":
            total_seconds += value * 3600
        elif unit == "m":
            total_seconds += value * 60
        elif unit == "s":
            total_seconds += value
        else:
            raise ValueError(f"Unknown duration unit: {unit}")

    return total_seconds


def delete_video_file(
    client: TestClient, task_id: str, output_path: Optional[str] = None
) -> None:
    """Helper function to test video file deletion endpoint.

    Works for both MinIO storage and local storage.

    Args:
        client: TestClient instance
        task_id: Task ID to delete video for
        output_path: Optional local file path to verify deletion (for local storage only)
    """
    delete_response = client.delete(f"/api/video-process/tasks/{task_id}/video")
    assert delete_response.status_code == 200

    delete_data = delete_response.json()
    assert delete_data["status"] == "ok"
    assert "Video file deleted successfully" in delete_data["message"]

    # For local storage, verify file is actually deleted from filesystem
    if output_path:
        assert not os.path.exists(
            output_path
        ), f"Video file {output_path} should be deleted after delete endpoint call"
