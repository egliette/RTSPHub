from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from app.config.settings import settings
from app.models.stream import StreamState
from app.schema.streaming import AddStreamRequest, HealthResponse, StreamInfo
from app.services.stream_manager import stream_manager

router = APIRouter()


@router.post("/streams", response_model=StreamInfo)
def add_stream(req: AddStreamRequest):
    try:
        base = settings.media_server_rtsp_base_url.rstrip("/")
        output_url = f"{base}/{req.path}"
        assigned_id = stream_manager.add_stream(
            source_uri=req.source_uri,
            output_url=output_url,
            stream_id=req.stream_id,
        )
        state = stream_manager.get_state(assigned_id)
        return StreamInfo(stream_id=assigned_id, source_uri=req.source_uri, state=state)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))


@router.get("/streams")
def list_streams():
    items = stream_manager.list_streams()
    return [
        StreamInfo(
            stream_id=sid,
            source_uri=info["source_uri"],
            state=StreamState(info["state"]),
        )
        for sid, info in items.items()
    ]


@router.delete("/streams/{stream_id}", status_code=204)
def remove_stream(stream_id: str):
    stream_manager.remove_stream(stream_id)
    return JSONResponse(status_code=204, content=None)


@router.get("/streams/{stream_id}/health", response_model=HealthResponse)
def health_check(stream_id: str):
    try:
        state = stream_manager.get_state(stream_id)
        return HealthResponse(stream_id=stream_id, state=state)
    except KeyError:
        raise HTTPException(status_code=404, detail="stream not found")


@router.post("/streams/{stream_id}/restart", status_code=204)
def restart_stream(stream_id: str):
    """Restart a stopped or error stream."""
    try:
        stream_manager.restart_stream(stream_id)
        return JSONResponse(status_code=204, content=None)
    except KeyError:
        raise HTTPException(status_code=404, detail="stream not found")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/streams/{stream_id}/stop", status_code=204)
def stop_stream(stream_id: str):
    """Stop a running stream."""
    try:
        stream_manager.stop_stream(stream_id)
        return JSONResponse(status_code=204, content=None)
    except KeyError:
        raise HTTPException(status_code=404, detail="stream not found")
