from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from app.config.settings import settings
from app.crud.stream import StreamDAO
from app.models.stream import StreamState
from app.schema.streaming import AddStreamRequest, HealthResponse, StreamInfo
from app.services.stream_manager import StreamManager

router = APIRouter()

_manager: StreamManager | None = None


def get_stream_manager(repo: StreamDAO = Depends(StreamDAO)) -> StreamManager:
    """Get or create StreamManager instance with DAO dependency."""
    global _manager
    if _manager is None:
        _manager = StreamManager(
            restart_backoff_seconds=settings.RESTART_BACKOFF_SECONDS,
            repo=repo,
        )
    return _manager


@router.post("/streams", response_model=StreamInfo)
def add_stream(
    req: AddStreamRequest, manager: StreamManager = Depends(get_stream_manager)
):
    try:
        base = settings.media_server_rtsp_base_url.rstrip("/")
        output_url = f"{base}/{req.path}"
        assigned_id = manager.add_stream(
            source_uri=req.source_uri,
            output_url=output_url,
            stream_id=req.stream_id,
        )
        state = manager.get_state(assigned_id)
        return StreamInfo(stream_id=assigned_id, source_uri=req.source_uri, state=state)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))


@router.get("/streams")
def list_streams(manager: StreamManager = Depends(get_stream_manager)):
    items = manager.list_streams()
    return [
        StreamInfo(
            stream_id=sid,
            source_uri=info["source_uri"],
            state=StreamState(info["state"]),
        )
        for sid, info in items.items()
    ]


@router.delete("/streams/{stream_id}", status_code=204)
def remove_stream(stream_id: str, manager: StreamManager = Depends(get_stream_manager)):
    manager.remove_stream(stream_id)
    return JSONResponse(status_code=204, content=None)


@router.get("/streams/{stream_id}/health", response_model=HealthResponse)
def health_check(stream_id: str, manager: StreamManager = Depends(get_stream_manager)):
    try:
        state = manager.get_state(stream_id)
        return HealthResponse(stream_id=stream_id, state=state)
    except KeyError:
        raise HTTPException(status_code=404, detail="stream not found")
