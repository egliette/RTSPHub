import os
from datetime import timedelta
from typing import Optional

from minio import Minio
from minio.error import S3Error

from app.config.settings import settings
from app.utils.logger import log


class MinIOService:

    _instance: Optional["MinIOService"] = None
    _initialized: bool = False

    def __new__(cls):
        """Ensure only one instance exists (Singleton pattern)."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize MinIO client with settings (only once)."""
        # Prevent re-initialization
        if self._initialized:
            return

        self.client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
        )
        self.bucket_name = settings.MINIO_BUCKET_NAME
        self._ensure_bucket_exists()
        self._initialized = True

    @classmethod
    def get_instance(cls) -> "MinIOService":
        """
        Get the singleton instance of MinIOService.
        Only creates the instance when first called (lazy initialization).

        Raises:
            RuntimeError: If MinIO is disabled in settings
        """
        if not settings.MINIO_ENABLED:
            raise RuntimeError(
                "MinIO is disabled. Please enable MINIO_ENABLED in settings "
                "or use local storage instead."
            )

        if cls._instance is None:
            log.info("Initializing MinIO service (lazy singleton)...")
            cls._instance = cls()

        return cls._instance

    @classmethod
    def is_initialized(cls) -> bool:
        """Check if the singleton instance has been created."""
        return cls._instance is not None and cls._instance._initialized

    def _ensure_bucket_exists(self) -> None:
        """Ensure the bucket exists, create if it doesn't."""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                log.info(f"Created MinIO bucket: {self.bucket_name}")
            else:
                log.info(f"MinIO bucket already exists: {self.bucket_name}")
        except S3Error as e:
            log.error(f"Failed to create/check MinIO bucket: {e}")
            raise
        except Exception as e:
            log.error(f"Unexpected error connecting to MinIO: {e}")
            raise

    def upload_video(self, local_file_path: str, object_name: str) -> str:
        """Upload a video file to MinIO."""
        try:
            if not os.path.exists(local_file_path):
                raise FileNotFoundError(f"Local file not found: {local_file_path}")

            self.client.fput_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                file_path=local_file_path,
                content_type="video/mp4",
            )

            log.info(f"Successfully uploaded video to MinIO: {object_name}")
            return object_name

        except S3Error as e:
            log.error(f"Failed to upload video to MinIO: {e}")
            raise
        except Exception as e:
            log.error(f"Unexpected error uploading video: {e}")
            raise

    def generate_presigned_url(
        self, object_name: str, expires_in_hours: int = 24
    ) -> str:
        """Generate a presigned URL for video access."""
        try:
            expires = timedelta(hours=expires_in_hours)
            url = self.client.presigned_get_object(
                bucket_name=self.bucket_name, object_name=object_name, expires=expires
            )

            log.info(f"Generated presigned URL for object: {object_name}")
            return url

        except S3Error as e:
            log.error(f"Failed to generate presigned URL: {e}")
            raise
        except Exception as e:
            log.error(f"Unexpected error generating presigned URL: {e}")
            raise

    def delete_video(self, object_name: str) -> bool:
        """Delete a video from MinIO."""
        try:
            self.client.remove_object(self.bucket_name, object_name)
            log.info(f"Successfully deleted video from MinIO: {object_name}")
            return True

        except S3Error as e:
            log.error(f"Failed to delete video from MinIO: {e}")
            return False
        except Exception as e:
            log.error(f"Unexpected error deleting video: {e}")
            return False

    def video_exists(self, object_name: str) -> bool:
        """Check if a video exists in MinIO."""
        try:
            self.client.stat_object(self.bucket_name, object_name)
            return True
        except S3Error:
            return False
        except Exception as e:
            log.error(f"Unexpected error checking video existence: {e}")
            return False

    def get_video_info(self, object_name: str) -> Optional[dict]:
        """Get video metadata from MinIO."""
        try:
            stat = self.client.stat_object(self.bucket_name, object_name)
            return {
                "size": stat.size,
                "last_modified": stat.last_modified,
                "content_type": stat.content_type,
                "etag": stat.etag,
            }
        except S3Error as e:
            log.error(f"Failed to get video info: {e}")
            return None
        except Exception as e:
            log.error(f"Unexpected error getting video info: {e}")
            return None

    def generate_object_name(
        self, task_id: str, source_rtsp_path: str, start_time: str, end_time: str
    ) -> str:
        """Generate a consistent object name for video storage."""
        start_clean = start_time.replace(" ", "_").replace(":", "-")
        end_clean = end_time.replace(" ", "_").replace(":", "-")
        object_name = (
            f"videos/{source_rtsp_path}/{task_id}_{start_clean}_{end_clean}.mp4"
        )
        return object_name
