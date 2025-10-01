from typing import Iterator

import pytest
from fastapi.testclient import TestClient

from app.config.settings import settings
from app.main import app
from app.schema.streaming import AddStreamRequest


@pytest.fixture(scope="session")
def client() -> Iterator[TestClient]:
    token = settings.API_TOKEN
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    app.dependency_overrides = {}
    with TestClient(app, headers=headers) as c:
        yield c


@pytest.fixture(scope="session")
def source_uri() -> str:
    path = "/app/assets/videos/big_buck_bunny.mp4"
    return path


@pytest.fixture()
def make_add_stream_payload(source_uri):

    def _make(
        source_uri: str = "/app/assets/videos/big_buck_bunny.mp4",
        path: str = "test-stream",
        stream_id: str | None = None,
    ):
        model = AddStreamRequest(
            source_uri=source_uri,
            stream_id=stream_id,
            path=path,
        )
        # Support both Pydantic v1 and v2
        to_dict = getattr(model, "model_dump", None)
        return to_dict() if callable(to_dict) else model.dict()

    return _make
