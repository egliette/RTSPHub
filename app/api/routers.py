from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from app.config.settings import settings
from app.core.enums import StreamState
from app.schema.streaming import AddStreamRequest, HealthResponse, StreamInfo
from app.services.stream_manager import StreamManager

router = APIRouter()

manager = StreamManager(
    restart_backoff_seconds=settings.RESTART_BACKOFF_SECONDS,
)


@router.post("/streams", response_model=StreamInfo)
def add_stream(req: AddStreamRequest):
    try:
        base = settings.media_server_rtsp_base_url.rstrip("/")
        output_url = f"{base}/{req.path}"
        assigned_id = manager.add_stream(
            video_path=req.video_path,
            output_url=output_url,
            stream_id=req.stream_id,
        )
        state = manager.get_state(assigned_id)
        return StreamInfo(stream_id=assigned_id, video_path=req.video_path, state=state)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))


@router.get("/streams")
def list_streams():
    items = manager.list_streams()
    return [
        StreamInfo(
            stream_id=sid,
            video_path=info["video_path"],
            state=StreamState(info["state"]),
        )
        for sid, info in items.items()
    ]


@router.delete("/streams/{stream_id}", status_code=204)
def remove_stream(stream_id: str):
    manager.remove_stream(stream_id)
    return JSONResponse(status_code=204, content=None)


@router.get("/streams/{stream_id}/health", response_model=HealthResponse)
def health_check(stream_id: str):
    try:
        state = manager.get_state(stream_id)
        return HealthResponse(stream_id=stream_id, state=state)
    except KeyError:
        raise HTTPException(status_code=404, detail="stream not found")
