from fastapi import APIRouter

from app.api.routers.stream import router as stream_router
from app.api.routers.video_process import router as video_process_router

router = APIRouter()

router.include_router(stream_router)
router.include_router(video_process_router)
