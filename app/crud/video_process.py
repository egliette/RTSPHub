from datetime import datetime, timedelta
from typing import List, Optional

from app.database.connection import SessionLocal
from app.models.video_process import TaskStatus, VideoProcessTask


class VideoProcessDAO:
    """Data Access Object for `VideoProcessTask`. Owns DB sessions per operation."""

    def add(self, task: VideoProcessTask) -> VideoProcessTask:
        db = SessionLocal()
        try:
            db.add(task)
            db.commit()
            db.refresh(task)
            return task
        finally:
            db.close()

    def get(self, task_id) -> Optional[VideoProcessTask]:
        db = SessionLocal()
        try:
            return db.get(VideoProcessTask, task_id)
        finally:
            db.close()

    def list_all(self) -> List[VideoProcessTask]:
        db = SessionLocal()
        try:
            return (
                db.query(VideoProcessTask)
                .order_by(VideoProcessTask.created_at.desc())
                .all()
            )
        finally:
            db.close()

    def update_status(
        self,
        task_id,
        status: TaskStatus,
        message: Optional[str] = None,
        result_video_path: Optional[str] = None,
    ) -> Optional[VideoProcessTask]:
        db = SessionLocal()
        try:
            task = db.get(VideoProcessTask, task_id)
            if not task:
                return None
            task.status = status
            if message is not None:
                task.message = message
            if result_video_path is not None:
                task.result_video_path = result_video_path
            db.commit()
            db.refresh(task)
            return task
        finally:
            db.close()

    def clear_all_tasks(self) -> int:
        """Clear all tasks from database. Returns number of deleted tasks."""
        db = SessionLocal()
        try:
            count = db.query(VideoProcessTask).count()
            db.query(VideoProcessTask).delete()
            db.commit()
            return count
        finally:
            db.close()

    def cleanup_old_tasks(self, days_old: int = 1) -> int:
        """Clean up completed or error tasks older than specified days. Returns number of deleted tasks."""
        db = SessionLocal()
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days_old)
            old_tasks = db.query(VideoProcessTask).filter(
                VideoProcessTask.status.in_([TaskStatus.completed, TaskStatus.error]),
                VideoProcessTask.updated_at < cutoff_date,
            )
            count = old_tasks.count()
            old_tasks.delete()
            db.commit()
            return count
        finally:
            db.close()
