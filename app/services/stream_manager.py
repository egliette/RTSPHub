import os
import subprocess
import threading
import time
from typing import Dict, Optional

from app.crud.stream import StreamDAO
from app.models.stream import StreamState


class StreamWorker:
    def __init__(
        self,
        stream_id: str,
        source_uri: str,
        output_url: str,
        restart_backoff_seconds: int,
        repo: Optional[StreamDAO] = None,
    ):
        """Initialize a worker responsible for publishing a looped file to an output URL.

        Args:
            stream_id: Unique identifier for the logical stream.
            source_uri: Local path to the input media file, or an rtsp:// URL.
            output_url: Target output URL (e.g., rtmp://host/app/stream or rtsp://...).
            restart_backoff_seconds: Backoff delay before restarting after exit/error.
            repo: Optional DAO for persisting state changes to database.
        """
        self.stream_id = stream_id
        self.source_uri = source_uri
        self.output_url = output_url
        self.restart_backoff_seconds = restart_backoff_seconds
        self.repo = repo
        self._state_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.ffmpeg_process: Optional[subprocess.Popen] = None
        self.state: StreamState = StreamState.running
        self.worker_thread = threading.Thread(
            target=self._run_loop, name=f"StreamWorker-{stream_id}", daemon=True
        )

    def start(self) -> None:
        """Start the worker thread if it is not already running."""
        if not self.worker_thread.is_alive():
            self.worker_thread.start()

    def stop(self) -> None:
        """Signal the worker to stop and wait for process/thread cleanup."""
        self.stop_event.set()
        if self.worker_thread.is_alive():
            self.worker_thread.join(timeout=5)
            if self.worker_thread.is_alive():
                print(f"Warning: Worker thread {self.stream_id} did not exit cleanly")

    def get_state(self) -> StreamState:
        """Return the current state of the worker.

        Returns:
            The `StreamState` representing the worker's health.
        """
        with self._state_lock:
            return self.state

    def _update_state(self, new_state: StreamState) -> None:
        """Update the worker's state and persist to database if DAO is available.

        Args:
            new_state: The new state to set.
        """
        with self._state_lock:
            self.state = new_state
        if self.repo is not None:
            try:
                self.repo.update_state(self.stream_id, new_state)
            except Exception:
                # Best-effort persistence; continue operation even if DB fails
                pass

    def _spawn_ffmpeg_process(self) -> subprocess.Popen:
        """Spawn the ffmpeg process to publish the input file in a loop.

        ffmpeg -re -stream_loop -1 -i <video_path> -c copy -f rtsp -rtsp_transport <output_url>

        Returns:
            The started subprocess running ffmpeg.
        """
        is_rtsp_input = self.source_uri.lower().startswith("rtsp://")

        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error"]

        if is_rtsp_input:
            cmd.extend(["-rtsp_transport", "tcp"])
        else:
            cmd.extend(["-re", "-stream_loop", "-1"])

        cmd.extend(
            [
                "-i",
                self.source_uri,
                "-c",
                "copy",
                "-f",
                "rtsp",
                self.output_url,
            ]
        )

        process = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
        )
        return process

    def _log_stderr(self) -> None:
        """Read and log stderr output from ffmpeg process in a separate thread.

        Exits when:
        - stderr is closed (process terminated)
        - stop_event is set
        - An I/O error occurs
        """
        try:
            assert self.ffmpeg_process is not None
            assert self.ffmpeg_process.stderr is not None

            for line in iter(self.ffmpeg_process.stderr.readline, b""):
                if self.stop_event.is_set():
                    break
                line_str = line.decode(errors="ignore").strip()
                if line_str:
                    print(f"FFmpeg [{self.stream_id}] error: {line_str}")
        except (ValueError, OSError):
            pass

    def _run_once(self) -> None:
        """Run one ffmpeg session until it exits or a stop is requested."""
        self._update_state(StreamState.running)
        try:
            self.ffmpeg_process = self._spawn_ffmpeg_process()
        except Exception as e:
            print(f"Failed to spawn ffmpeg for {self.stream_id}: {e}")
            self._update_state(StreamState.error)
            return

        stderr_thread: Optional[threading.Thread] = None

        try:
            stderr_thread = threading.Thread(
                target=self._log_stderr, name=f"stderr-{self.stream_id}", daemon=True
            )
            stderr_thread.start()

            while self.ffmpeg_process.poll() is None and not self.stop_event.is_set():
                time.sleep(0.5)

            if not self.stop_event.is_set():
                exit_code = self.ffmpeg_process.poll()
                if exit_code is not None and exit_code != 0:
                    print(f"Stream {self.stream_id} exited with code {exit_code}")

        finally:
            if self.ffmpeg_process and self.ffmpeg_process.poll() is None:
                self.ffmpeg_process.terminate()
                try:
                    self.ffmpeg_process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    self.ffmpeg_process.kill()
                    self.ffmpeg_process.wait()

            if stderr_thread and stderr_thread.is_alive():
                stderr_thread.join(timeout=1)

    def _run_loop(self) -> None:
        """Main loop that restarts ffmpeg with backoff until stopped."""
        while not self.stop_event.is_set():
            self._run_once()

            if self.stop_event.is_set():
                break

            self._update_state(StreamState.error)

            if self.restart_backoff_seconds < 0:
                break

            for _ in range(self.restart_backoff_seconds):
                if self.stop_event.is_set():
                    break
                time.sleep(1)

        self._update_state(StreamState.stopped)


class StreamManager:
    def __init__(
        self, restart_backoff_seconds: int = 10, repo: StreamDAO | None = None
    ) -> None:
        """Manage lifecycle of multiple `StreamWorker` instances.

        Args:
            restart_backoff_seconds: Delay used by workers before restart after failure.
                                    If < 0, auto-restart is disabled.
        """
        self.restart_backoff_seconds = restart_backoff_seconds
        self._lock = threading.Lock()
        self._workers: Dict[str, StreamWorker] = {}
        self._next_id: int = 1
        self._repo = repo
        if self._repo is not None:
            self._recover_streams()

    def _recover_streams(self) -> None:
        """Recover running streams from database after restart."""
        try:
            for stream_data in self._repo.list():
                if stream_data.state != StreamState.stopped:
                    try:
                        self.add_stream(
                            stream_data.source_uri,
                            stream_data.output_url,
                            stream_data.stream_id,
                        )
                    except Exception as e:
                        print(f"Failed to recover stream {stream_data.stream_id}: {e}")
        except Exception as e:
            print(f"Failed to recover streams from database: {e}")

    def _generate_stream_id(self) -> str:
        """Generate a unique, auto-incrementing stream identifier."""
        while True:
            candidate = f"stream-{self._next_id}"
            self._next_id += 1
            if candidate not in self._workers and (
                self._repo is None or not self._repo.exists(candidate)
            ):
                return candidate

    def add_stream(
        self, source_uri: str, output_url: str, stream_id: Optional[str] = None
    ) -> str:
        """Create and start a worker for a given file-backed stream.

        Args:
            source_uri: Local file path or rtsp:// URL for the source.
            output_url: Full output URL for this stream.
            stream_id: Optional unique identifier. If not provided, one is auto-generated.

        Raises:
            FileNotFoundError: If the video file does not exist.
            ValueError: If a worker with the same stream_id already exists.
            ValueError: If `output_url` is not provided.

        Returns:
            The assigned stream identifier.
        """
        is_rtsp_input = source_uri.lower().startswith("rtsp://")
        if not is_rtsp_input and not os.path.isfile(source_uri):
            raise FileNotFoundError(f"Video not found: {source_uri}")
        with self._lock:
            assigned_id = stream_id or self._generate_stream_id()
            if assigned_id in self._workers or (
                self._repo is not None and self._repo.exists(assigned_id)
            ):
                raise ValueError("stream_id already exists")
            if not output_url:
                raise ValueError("output_url is required")
            target_url = output_url
            worker = StreamWorker(
                assigned_id,
                source_uri,
                target_url,
                self.restart_backoff_seconds,
                self._repo,
            )
            self._workers[assigned_id] = worker
            worker.start()

            if self._repo is not None:
                try:
                    self._repo.add(
                        stream_id=assigned_id,
                        source_uri=source_uri,
                        output_url=target_url,
                        state=StreamState.running,
                    )
                except Exception:
                    pass
            return assigned_id

    def remove_stream(self, stream_id: str) -> None:
        """Stop and remove a worker for a given stream_id if it exists."""
        with self._lock:
            worker = self._workers.pop(stream_id, None)
        if worker:
            worker.stop()

        if self._repo is not None:
            try:
                self._repo.remove(stream_id)
            except Exception:
                pass

    def list_streams(self) -> Dict[str, Dict[str, str]]:
        """List current workers with their video path and state."""
        with self._lock:
            return {
                sid: {
                    "source_uri": w.source_uri,
                    "state": w.get_state().value,
                }
                for sid, w in self._workers.items()
            }

    def get_state(self, stream_id: str) -> StreamState:
        """Get the `StreamState` for a specific stream.

        Args:
            stream_id: Identifier of the stream to query.

        Returns:
            The current `StreamState`.

        Raises:
            KeyError: If the stream_id is not found.
        """
        with self._lock:
            worker = self._workers.get(stream_id)
            if not worker:
                raise KeyError("stream not found")
            return worker.get_state()
