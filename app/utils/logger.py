import logging
import logging.handlers
import os
import re
from datetime import datetime
from functools import lru_cache
from pathlib import Path

from app.config.settings import settings


class CustomTimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    """Custom time-based rotating file handler with timestamped filenames."""

    def __init__(
        self,
        filename,
        when="h",
        interval=1,
        backupCount=0,
        encoding=None,
        delay=False,
        utc=False,
        atTime=None,
        service_name=None,
    ):
        super().__init__(
            filename, when, interval, backupCount, encoding, delay, utc, atTime
        )
        self.service_name = service_name or "app"

    def rotation_filename(self, default_name):
        """Create rotation filename with timestamp."""
        base, ext = os.path.splitext(self.baseFilename)
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
        return f"{base}-{timestamp}{ext}"

    def getFilesToDelete(self):
        """
        Determine the log files to delete when we exceed backupCount.
        Keeps only the most recent `backupCount` files.
        """
        dirName, baseName = os.path.dirname(self.baseFilename), os.path.basename(
            self.baseFilename
        )
        # Pattern matches: service_prefix_YYYY-MM-DD.ext
        fileNames = os.listdir(dirName)
        rotated = []
        ext = os.path.splitext(baseName)[1]  # '.log'
        base_without_ext = baseName[: -len(ext)] if ext else baseName
        # Derive the stable service prefix from the current base filename which is typically
        # like 'service_prefix_YYYY-MM-DD'. We want to capture 'service_prefix'.
        match = re.match(r"^(.*?)(?:_\d{4}-\d{2}-\d{2})$", base_without_ext)
        service_prefix = match.group(1) if match else base_without_ext.rsplit("_", 1)[0]
        pattern = re.compile(
            rf"^{re.escape(service_prefix)}_\d{{4}}-\d{{2}}-\d{{2}}{re.escape(ext)}$"
        )
        for fn in fileNames:
            if pattern.match(fn):
                rotated.append(os.path.join(dirName, fn))
        rotated.sort()  # oldest first (lexicographic on timestamp)
        # delete older if more than backupCount
        if len(rotated) <= self.backupCount:
            return []
        return rotated[: len(rotated) - self.backupCount]


class AppLogger:
    """Singleton logger class with simplified method names and built-in configuration."""

    __instance = None

    def __new__(cls):
        if cls.__instance is None:
            instance = super(AppLogger, cls).__new__(cls)
            instance._setup_logger()
            cls.__instance = instance
        return cls.__instance

    def _setup_logger(self) -> None:
        """Set up the logger with file and console handlers."""
        self._service_name: str = settings.SERVICE_NAME
        self.log_dir: Path = Path(settings.LOG_DIR)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_to_stdout: bool = settings.LOG_TO_STDOUT
        self.max_log_days: int = settings.LOG_MAX_DAYS
        self.log_level: int = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)

        self._cleanup_old_logs()

        self.logger = logging.getLogger(self._service_name.upper())
        self.logger.setLevel(self.log_level)

        if self.logger.handlers:
            return

        detailed_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        simple_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
        )

        log_file = f"{self._service_name}_{datetime.now().strftime('%Y-%m-%d')}.log"
        log_path = self.log_dir / log_file

        file_handler = CustomTimedRotatingFileHandler(
            log_path,
            when="midnight",
            interval=1,
            backupCount=settings.LOG_MAX_DAYS,
            encoding="utf-8",
            service_name=self._service_name,
        )
        file_handler.setLevel(self.log_level)
        file_handler.setFormatter(detailed_formatter)
        self.logger.addHandler(file_handler)

        # Console handler
        if self.log_to_stdout:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(self.log_level)
            console_handler.setFormatter(simple_formatter)
            self.logger.addHandler(console_handler)

    def _cleanup_old_logs(self) -> None:
        """Remove old log files beyond the retention count at app start.

        Keeps only the most recent `self.max_log_days` files matching pattern
        `<service_name>_YYYY-MM-DD.log` within `self.log_dir`.
        """
        try:
            if self.max_log_days is None:
                return
            # Guard against misconfiguration
            if self.max_log_days < 1:
                # If set to 0 or negative, delete all matching logs except today's
                retention_count = 0
            else:
                retention_count = self.max_log_days

            # Build filename pattern and collect candidates
            ext = ".log"
            pattern = re.compile(
                rf"^{re.escape(self._service_name)}_\d{{4}}-\d{{2}}-\d{{2}}{re.escape(ext)}$"
            )

            # Current active log file (today) should be preserved even if retention is 0
            today_name = (
                f"{self._service_name}_{datetime.now().strftime('%Y-%m-%d')}{ext}"
            )

            candidates = []
            for fn in os.listdir(self.log_dir):
                if pattern.match(fn):
                    candidates.append(fn)

            if not candidates:
                return

            # Sort by date derived from filename (lexicographic works for YYYY-MM-DD)
            candidates.sort()
            keep_set = (
                set(candidates[-retention_count:]) if retention_count > 0 else set()
            )
            keep_set.add(today_name)

            for fn in candidates:
                if fn not in keep_set:
                    full_path = self.log_dir / fn
                    try:
                        os.remove(full_path)
                    except OSError:
                        pass
        except Exception:
            pass

    def crit(self, msg: str, exc_info: bool = False) -> None:
        """Log critical message."""
        return self.logger.critical(msg=msg, exc_info=exc_info)

    def bug(self, msg: str, exc_info: bool = False) -> None:
        """Log debug message."""
        return self.logger.debug(msg=msg, exc_info=exc_info)

    def err(self, msg: str, exc_info: bool = False) -> None:
        """Log error message."""
        return self.logger.error(msg=msg, exc_info=exc_info)

    def info(self, msg: str, exc_info: bool = False) -> None:
        """Log info message."""
        return self.logger.info(msg=msg, exc_info=exc_info)

    def warn(self, msg: str, exc_info: bool = False) -> None:
        """Log warning message."""
        return self.logger.warning(msg=msg, exc_info=exc_info)

    def exception(self, exc: Exception, context: str = "") -> None:
        """Log an exception with proper formatting."""
        context_msg = f" in {context}" if context else ""
        self.logger.error(
            f"Exception occurred{context_msg}: {type(exc).__name__}: {exc}",
            exc_info=True,
        )

    def perf(self, operation: str, duration: float, **context) -> None:
        """Log performance metrics."""
        context_str = ", ".join([f"{k}={v}" for k, v in context.items()])
        self.logger.info(
            f"Performance: {operation} took {duration:.3f}s ({context_str})"
        )


@lru_cache()
def get_logger() -> AppLogger:
    return AppLogger()


log = get_logger()
