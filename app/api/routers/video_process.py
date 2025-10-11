from typing import List

from fastapi import APIRouter, HTTPException

from app.config.settings import settings
from app.models.video_process import VideoProcessTask
from app.schema.video_process import (
    TaskStatus,
    VideoProcessRequest,
    VideoProcessResponse,
    VideoProcessStatus,
)
from app.services.minio_service import MinIOService
from app.services.video_process_service import video_service

router = APIRouter(prefix="/video-process", tags=["video-process"])


def _get_video_uri(task: VideoProcessTask) -> str:
    """Get the appropriate video URI.

    If MinIO is enabled and `result_video_path` stores a MinIO object name, return a presigned URL;
    otherwise return the local file path.
    """
    if not task.result_video_path:
        return ""

    if not settings.MINIO_ENABLED:
        return task.result_video_path

    try:
        minio_service = MinIOService.get_instance()
        return minio_service.generate_presigned_url(task.result_video_path)
    except Exception:
        return task.result_video_path


@router.post("/tasks", response_model=VideoProcessResponse)
async def create_video_task(request: VideoProcessRequest):
    """Create a new video processing task (trim/merge)."""
    try:
        video_service.validate_request(
            request.source_rtsp_path, request.start_time, request.end_time
        )

        task = video_service.create_task(
            request.source_rtsp_path, request.start_time, request.end_time
        )
        return VideoProcessResponse(
            task_id=task.id,
            status=TaskStatus(task.status.value),
            message="Task created successfully",
            result_video_uri=task.result_video_path,
            created_at=task.created_at,
            updated_at=task.updated_at or task.created_at,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks", response_model=List[VideoProcessStatus])
async def list_video_tasks():
    """List all video processing tasks."""
    try:
        tasks = video_service.list_all_tasks()
        return [
            VideoProcessStatus(
                task_id=task.id,
                status=TaskStatus(task.status.value),
                message=task.message,
                result_video_uri=_get_video_uri(task),
                created_at=task.created_at,
                updated_at=task.updated_at or task.created_at,
            )
            for task in tasks
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}", response_model=VideoProcessStatus)
async def get_video_task_status(task_id: str):
    """Get the status of a specific video processing task."""
    try:
        task = video_service.get_task_status(task_id)
        return VideoProcessStatus(
            task_id=task.id,
            status=TaskStatus(task.status.value),
            message=task.message,
            result_video_uri=_get_video_uri(task),
            created_at=task.created_at,
            updated_at=task.updated_at or task.created_at,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/tasks/{task_id}")
async def delete_video_task(task_id: str):
    """Delete a specific video processing task by id.

    Stops active processing if running, removes pending tasks, and deletes the record.
    Returns 200 on success, 404 if not found/invalid.
    """
    try:
        removed = video_service.remove_task(task_id)
        if not removed:
            raise HTTPException(status_code=404, detail="Task not found")
        return {"status": "ok"}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/tasks/{task_id}/video")
async def delete_video_file(task_id: str):
    """Delete the video file associated with a specific task.

    Deletes the actual video file from storage (local filesystem or MinIO).
    """
    try:
        task = video_service.get_task_status(task_id)

        if not task.result_video_path:
            raise HTTPException(
                status_code=400, detail="No video file associated with this task"
            )

        deleted = video_service.delete_video_file(task_id)

        if not deleted:
            raise HTTPException(
                status_code=404, detail="Video file not found or could not be deleted"
            )

        return {"status": "ok", "message": "Video file deleted successfully"}

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
