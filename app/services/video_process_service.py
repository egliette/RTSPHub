import os
from datetime import datetime
from typing import List
from uuid import UUID

from app.config.settings import settings
from app.crud.video_process import VideoProcessDAO
from app.models.video_process import TaskStatus, VideoProcessTask
from app.services.video_process_queue import VideoProcessQueueManager
from app.utils.logger import log


class VideoProcessService:
    """Service for video operations with validation."""

    @staticmethod
    def _validate_uuid(task_id: str) -> UUID:
        """Validate UUID format and return UUID object."""
        try:
            return UUID(task_id)
        except ValueError:
            raise ValueError("Invalid task ID format")

    def __init__(
        self,
        video_record_path: str,
        video_processed_path: str,
    ):
        """Initialize the video process service.

        Args:
            video_record_path: Path to the directory containing recorded videos
            video_processed_path: Path to the directory where processed videos will be stored
        """
        self.video_record_path = video_record_path
        self.video_processed_path = video_processed_path
        self.queue_manager = VideoProcessQueueManager(
            video_record_path=self.video_record_path,
            video_processed_path=self.video_processed_path,
        )
        self.dao = VideoProcessDAO()

    def validate_request(
        self, source_rtsp_path: str, start_time: str, end_time: str
    ) -> None:
        """Validate the video request. Raises ValueError if invalid."""

        # Validate source path (allow non-existent folder to proceed; worker will fail task later)
        video_folder = os.path.join(self.video_record_path, source_rtsp_path)
        video_files: List[str] = []
        if os.path.exists(video_folder):
            # Only check for files if folder exists; otherwise skip to time validation
            video_files = [f for f in os.listdir(video_folder) if f.endswith(".mp4")]
            if not video_files:
                raise ValueError("No .mp4 video files found in the source folder")

        # Validate and parse time strings
        try:
            start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise ValueError(
                f"Invalid start_time format: {start_time}. Expected: 'YYYY-MM-DD HH:MM:SS'"
            )

        try:
            end_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise ValueError(
                f"Invalid end_time format: {end_time}. Expected: 'YYYY-MM-DD HH:MM:SS'"
            )

        # Ensure start time is earlier than the newest available video
        parsed_datetimes: List[datetime] = []
        for filename in video_files:
            try:
                parsed_datetimes.append(self._parse_filename_to_datetime(filename))
            except Exception:
                # Ignore files that don't match expected datetime filename pattern
                continue
        if parsed_datetimes:
            newest_video_dt = max(parsed_datetimes)
            if not (start_dt < newest_video_dt):
                raise ValueError(
                    f"Start time {start_dt} must be earlier than the newest available video: "
                    f"{newest_video_dt.strftime('%Y-%m-%d %H:%M:%S')}"
                )

        # Validate time logic
        if start_dt >= end_dt:
            raise ValueError("Start time must be before end time")

        return None

    def _parse_filename_to_datetime(self, filename: str) -> datetime:
        """Parse filename like '2025-10-02_16-40-23.mp4' to datetime object."""
        basename = os.path.splitext(filename)[0]
        return datetime.strptime(basename, "%Y-%m-%d_%H-%M-%S")

    def create_task(
        self, source_rtsp_path: str, start_time: str, end_time: str
    ) -> VideoProcessTask:
        """Create and queue a video task and persist it."""
        task = VideoProcessTask(
            source_rtsp_path=source_rtsp_path,
            start_time=start_time,
            end_time=end_time,
            status=TaskStatus.pending,
            result_video_path=None,
        )
        task = self.dao.add(task)
        self.queue_manager.add_task(task)

        return task

    def get_task_status(self, task_id: str) -> VideoProcessTask:
        """Get the status of a task by ID."""
        self._validate_uuid(task_id)

        task = self.dao.get(task_id)
        if task is None:
            raise ValueError("Task not found")

        return task

    def list_all_tasks(self) -> List[VideoProcessTask]:
        """List all video tasks."""
        db_tasks = self.dao.list_all()
        return db_tasks

    def remove_task(self, task_id: str) -> bool:
        """Remove a task by id. Stops active worker or removes pending, then deletes DB record."""
        uuid_task_id = self._validate_uuid(task_id)

        removed = self.queue_manager.remove_task(uuid_task_id)
        if not removed:
            # If not in memory queues/workers, attempt to delete from DB directly
            try:
                return self.dao.delete(task_id)
            except Exception as e:
                log.err(f"Failed to delete task {task_id} from database: {e}")
                return False
        return True

    def delete_video_file(self, task_id: str) -> bool:
        """Delete the video file associated with a task.

        Args:
            task_id: The task ID to delete the video file for

        Returns:
            True if file was deleted successfully, False otherwise
        """
        self._validate_uuid(task_id)

        task = self.dao.get(task_id)
        if not task or not task.result_video_path:
            return False

        video_path = task.result_video_path

        if settings.MINIO_ENABLED:
            try:
                from app.services.minio_service import MinIOService

                minio_service = MinIOService.get_instance()
                success = minio_service.delete_video(video_path)
                if success:
                    log.info(f"Deleted video file from MinIO: {video_path}")
                return success
            except Exception as e:
                log.err(f"Failed to delete video file from MinIO: {e}")
                return False
        else:
            try:
                if os.path.exists(video_path):
                    os.remove(video_path)
                    log.info(f"Deleted video file from local filesystem: {video_path}")
                    return True
                else:
                    log.warning(f"Video file not found: {video_path}")
                    return False
            except Exception as e:
                log.err(f"Failed to delete video file from local filesystem: {e}")
                return False


video_service = VideoProcessService(
    video_record_path=settings.VIDEO_RECORD_PATH,
    video_processed_path=settings.VIDEO_PROCESSED_PATH,
)
