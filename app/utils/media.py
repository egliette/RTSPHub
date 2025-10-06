import subprocess


def get_video_duration(path: str) -> float:
    """Return media duration in seconds using ffprobe.

    Raises CalledProcessError if ffprobe fails.
    """
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    stdout = result.stdout.strip()
    if result.returncode != 0 or not stdout:
        (result.stderr or "").strip()
        raise subprocess.CalledProcessError(
            result.returncode, cmd, result.stdout, result.stderr
        )
    return float(stdout)
