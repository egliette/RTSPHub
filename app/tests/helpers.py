import os
from typing import Optional

from fastapi.testclient import TestClient


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
