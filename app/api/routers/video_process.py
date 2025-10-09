from typing import List

from fastapi import APIRouter, HTTPException

from app.schema.video_process import (
    TaskStatus,
    VideoProcessRequest,
    VideoProcessResponse,
    VideoProcessStatus,
)
from app.services.video_process_service import video_service

router = APIRouter(prefix="/video-process", tags=["video-process"])


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
            updated_at=task.updated_at,
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
                result_video_uri=task.result_video_path,
                created_at=task.created_at,
                updated_at=task.updated_at,
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
            result_video_uri=task.result_video_path,
            created_at=task.created_at,
            updated_at=task.updated_at,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
