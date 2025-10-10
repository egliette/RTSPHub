import os
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional
from uuid import UUID

from app.crud.video_process import VideoProcessDAO
from app.models.video_process import TaskStatus, VideoProcessTask
from app.utils.logger import log
from app.utils.media import get_video_duration


@dataclass
class VideoSegment:
    path: str
    filename: str
    start: datetime
    end: datetime
    duration: float


class VideoProcessWorker:
    """Worker thread for processing video tasks (trim/merge)."""

    def __init__(
        self,
        task: VideoProcessTask,
        video_record_path: str,
        video_processed_path: str,
        cleanup_task: Optional[Callable[[], None]] = None,
    ) -> None:
        """Initialize the video process worker.

        Args:
            task: The video processing task to execute
            video_record_path: Path to the directory containing recorded videos
            video_processed_path: Path to the directory where processed videos will be stored
            cleanup_task: Optional callback function to execute when task completes or fails
        """
        self.task = task
        self.task_id = task.id
        self.video_record_path = video_record_path
        self.video_processed_path = video_processed_path
        self.cleanup_task = cleanup_task or (lambda: None)
        self.dao = VideoProcessDAO()
        self.stop_event = threading.Event()
        self.worker_thread = threading.Thread(
            target=self._process_task, name=f"VideoProcessWorker-{task.id}", daemon=True
        )

    def start(self) -> None:
        """Start the worker thread."""
        self.worker_thread.start()

    def stop(self) -> None:
        """Stop the worker thread."""
        self.stop_event.set()
        # Avoid joining the current thread to prevent deadlock/runtime error
        if self.worker_thread is threading.current_thread():
            return
        if self.worker_thread.is_alive():
            self.worker_thread.join(timeout=10)

    def _process_task(self) -> None:
        """Process a video task by:

        - Finding relevant videos in time range
        - Trimming video segments
        - Concatenating into final video
        - Updating task status
        """
        try:
            start_dt = datetime.strptime(self.task.start_time, "%Y-%m-%d %H:%M:%S")
            end_dt = datetime.strptime(self.task.end_time, "%Y-%m-%d %H:%M:%S")

            video_folder = os.path.join(
                self.video_record_path, self.task.source_rtsp_path
            )

            log.info(
                f"Starting video processing task {self.task_id} for {self.task.source_rtsp_path} from {self.task.start_time} to {self.task.end_time}"
            )
            self.task.status = TaskStatus.processing
            self.dao.update_status(self.task_id, TaskStatus.processing)

            # Find videos that overlap with the requested time range
            relevant_videos = self._find_relevant_videos(video_folder, start_dt, end_dt)
            if not relevant_videos:
                log.warn(f"No videos found in time range for task {self.task_id}")
                self.task.status = TaskStatus.error
                self.task.message = "No videos found in the specified time range"
                self.dao.update_status(
                    self.task_id, TaskStatus.error, message=self.task.message
                )
                return

            # Create trimmed segments for each relevant video
            log.info(
                f"Found {len(relevant_videos)} relevant videos for task {self.task_id}"
            )
            temp_segments = []
            for i, video in enumerate(relevant_videos):
                if self.stop_event.is_set():
                    log.info(f"Task {self.task_id} stopped by user")
                    return
                temp_output = f"/tmp/segment_{self.task.id}_{i}.mp4"
                self._create_trimmed_segment(video, start_dt, end_dt, temp_output)
                temp_segments.append(temp_output)

            # Concatenate all trimmed segments into final video
            output_path = self.task.result_video_path or os.path.join(
                self.video_processed_path,
                self.task.source_rtsp_path,
                f"{self.task.id}.mp4",
            )
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            log.info(
                f"Concatenating {len(temp_segments)} segments for task {self.task_id}"
            )
            self._concatenate_videos(temp_segments, output_path)

            # Clean up temporary segment files
            for segment in temp_segments:
                if os.path.exists(segment):
                    os.remove(segment)

            # Update task status to completed with results
            self.task.status = TaskStatus.completed
            self.task.result_video_path = output_path
            self.task.message = (
                f"Successfully processed {len(relevant_videos)} video segments"
            )
            log.info(f"Task {self.task_id} completed successfully: {self.task.message}")
            self.dao.update_status(
                self.task_id,
                TaskStatus.completed,
                message=self.task.message,
                result_video_path=output_path,
            )
        except Exception as e:
            log.err(f"Task {self.task_id} failed: {str(e)}")
            self.task.status = TaskStatus.error
            self.task.message = f"Error processing task: {str(e)}"
            self.dao.update_status(
                self.task_id, TaskStatus.error, message=self.task.message
            )
        finally:
            try:
                self.cleanup_task()
            except Exception as e:
                log.warn(f"Error in cleanup task for video process {self.task_id}: {e}")

    def _find_relevant_videos(
        self, folder: str, start_dt: datetime, end_dt: datetime
    ) -> List[VideoSegment]:
        """Find videos that overlap with the time range."""
        if not os.path.exists(folder):
            raise FileNotFoundError(f"Video folder not found: {folder}")

        video_files = [f for f in os.listdir(folder) if f.endswith(".mp4")]
        if not video_files:
            return []

        relevant_videos: List[VideoSegment] = []

        for video_file in video_files:
            video_path = os.path.join(folder, video_file)
            try:
                video_start = self._parse_filename_to_datetime(video_file)
                video_duration = get_video_duration(video_path)
                video_end = video_start + timedelta(seconds=video_duration)

                # Check if video overlaps with requested time range
                if video_start < end_dt and video_end > start_dt:
                    relevant_videos.append(
                        VideoSegment(
                            path=video_path,
                            filename=video_file,
                            start=video_start,
                            end=video_end,
                            duration=video_duration,
                        )
                    )
            except Exception as e:
                log.err(f"Error processing video {video_file}: {e}")
                continue

        # Sort by start time
        relevant_videos.sort(key=lambda x: x.start)
        return relevant_videos

    def _parse_filename_to_datetime(self, filename: str) -> datetime:
        """Parse filename like '2025-10-02_16-40-23.mp4' to datetime object."""
        basename = os.path.splitext(filename)[0]
        return datetime.strptime(basename, "%Y-%m-%d_%H-%M-%S")

    def _create_trimmed_segment(
        self,
        video_info: VideoSegment,
        start_dt: datetime,
        end_dt: datetime,
        output_path: str,
    ) -> None:
        """Create a trimmed segment from a video file.

        Trims the provided source video to the intersection of (start_dt, end_dt)
        relative to the video's own start time and writes the result to
        `output_path`. If the requested range covers the entire source, the file
        is copied instead of being re-encoded/trimmed.

        Args:
            video_info: Metadata for the source video segment, including path,
                start time, and duration.
            start_dt: Absolute start datetime of the requested window.
            end_dt: Absolute end datetime of the requested window.
            output_path: Destination path for the trimmed video file.

        Raises:
            subprocess.CalledProcessError: If the FFmpeg command fails.
        """
        video_start = video_info.start

        trim_start = max(0, (start_dt - video_start).total_seconds())
        requested_end_offset = (end_dt - video_start).total_seconds()
        trim_end = min(video_info.duration, requested_end_offset)
        trim_duration = trim_end - trim_start

        if trim_start == 0 and trim_end == video_info.duration and trim_duration > 0:
            if os.path.exists(output_path):
                os.remove(output_path)
            shutil.copy2(video_info.path, output_path)
            return

        cmd = [
            "ffmpeg",
            "-ss",
            str(trim_start),
            "-i",
            video_info.path,
            "-t",
            str(trim_duration),
            "-c",
            "copy",
            "-y",
            output_path,
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise subprocess.CalledProcessError(
                result.returncode, cmd, result.stdout, result.stderr
            )

    def _concatenate_videos(self, video_list: List[str], output_path: str) -> None:
        """Concatenate videos using FFmpeg concat demuxer."""
        if len(video_list) == 1:
            shutil.move(video_list[0], output_path)
            return

        concat_file = f"/tmp/concat_list_{self.task.id}.txt"
        with open(concat_file, "w") as f:
            for video in video_list:
                abs_path = os.path.abspath(video)
                f.write(f"file '{abs_path}'\n")

        cmd = [
            "ffmpeg",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            concat_file,
            "-c",
            "copy",
            "-y",
            output_path,
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            os.remove(concat_file)
            raise subprocess.CalledProcessError(
                result.returncode, cmd, result.stdout, result.stderr
            )

        os.remove(concat_file)


class VideoProcessQueueManager:
    """Manager for video task queue using threading."""

    def __init__(
        self, video_record_path: str, video_processed_path: str, max_workers: int = 2
    ) -> None:
        """Initialize the video process queue manager.

        Args:
            video_record_path: Path to the directory containing recorded videos
            video_processed_path: Path to the directory where processed videos will be stored
            max_workers: Maximum number of concurrent video processing workers (default: 2)
        """
        self.max_workers = max_workers
        self.video_record_path = video_record_path
        self.video_processed_path = video_processed_path
        self._lock = threading.Lock()
        self._workers: Dict[UUID, VideoProcessWorker] = {}
        self._pending_tasks: list[VideoProcessTask] = []
        self._dao = VideoProcessDAO()
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_old_tasks, daemon=True
        )
        self._cleanup_thread.start()

    def add_task(self, task: VideoProcessTask) -> None:
        """Add a task to the queue."""
        with self._lock:
            task.status = TaskStatus.pending
            self._pending_tasks.append(task)
            log.info(f"Added video processing task {task.id} to queue")
            self._start_next_task()

    def get_task_status(self, task_id: UUID) -> Optional[VideoProcessTask]:
        """Get the status of a task."""
        with self._lock:
            if task_id in self._workers:
                return self._workers[task_id].task

            for task in self._pending_tasks:
                if task.id == task_id:
                    return task

            return self._dao.get(task_id)

    def list_all_tasks(self) -> list[VideoProcessTask]:
        """List all tasks from database (pending, active, completed, and error)."""
        return self._dao.list_all()

    def _start_next_task(self) -> None:
        """Start the next pending task if we have available workers."""
        if len(self._workers) >= self.max_workers:
            return

        if not self._pending_tasks:
            return

        task = self._pending_tasks.pop(0)
        worker = VideoProcessWorker(
            task,
            self.video_record_path,
            self.video_processed_path,
            cleanup_task=lambda: self._remove_task(task.id),
        )
        self._workers[task.id] = worker
        worker.start()
        log.info(f"Started worker for video processing task {task.id}")

    def _remove_task(self, task_id: UUID) -> None:
        """Remove a completed task from active workers."""
        with self._lock:
            worker = self._workers.pop(task_id, None)
            if worker:
                worker.stop()
                log.info(f"Removed completed video processing task {task_id}")
            self._start_next_task()

    def _cleanup_old_tasks(self) -> None:
        """Background thread to clean up old completed/error tasks (1 day retention)."""
        while True:
            try:
                time.sleep(3600)
                deleted_count = self._dao.cleanup_old_tasks(days_old=1)
                if deleted_count > 0:
                    log.info(f"Cleaned up {deleted_count} old tasks from database")
            except Exception as e:
                log.err(f"Error in cleanup thread: {e}")


from app.config.settings import settings

queue_manager = VideoProcessQueueManager(
    video_record_path=settings.VIDEO_RECORD_PATH,
    video_processed_path=settings.VIDEO_PROCESSED_PATH,
)
