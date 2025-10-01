from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routers import router
from app.config.settings import settings
from app.utils.security import require_api_token

app = FastAPI(
    title=settings.PROJECT_NAME, openapi_url=f"{settings.API_PREFIX}/openapi.json"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.BACKEND_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(
    router, prefix=settings.API_PREFIX, dependencies=[Depends(require_api_token)]
)


@app.get("/")
def read_root():
    return {"message": "RTSP Hub API is running!"}


@app.get("/health")
def health_check():
    return {"status": "healthy"}
