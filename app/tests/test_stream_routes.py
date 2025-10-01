import time

from fastapi.testclient import TestClient

from app.core.enums import StreamState


def test_add_stream_success(client: TestClient, make_add_stream_payload):
    resp = client.post("/api/streams", json=make_add_stream_payload())
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert isinstance(data.get("stream_id"), str) and data["stream_id"].strip() != ""
    stream_id = data["stream_id"]

    # Poll for the full 10 seconds; only break early if error is observed
    for _ in range(20):
        h = client.get(f"/api/streams/{stream_id}/health")
        if h.status_code == 200:
            state = str(h.json().get("state"))
            is_running = state == StreamState.running.value
            if not is_running:
                break
        time.sleep(0.5)
    assert is_running, "stream did not reach running state within 10 seconds"


def test_add_stream_auto_id_and_list(client: TestClient, make_add_stream_payload):
    resp = client.post("/api/streams", json=make_add_stream_payload())
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert isinstance(data["stream_id"], str) and len(data["stream_id"]) > 0
    stream_id = data["stream_id"]

    list_resp = client.get("/api/streams")
    assert list_resp.status_code == 200, list_resp.text
    items = list_resp.json()
    assert any(item["stream_id"] == stream_id for item in items)

    del_resp = client.delete(f"/api/streams/{stream_id}")
    assert del_resp.status_code == 204, del_resp.text


def test_health_and_delete_flow(client: TestClient, make_add_stream_payload):
    resp = client.post("/api/streams", json=make_add_stream_payload())
    assert resp.status_code == 200, resp.text
    stream_id = resp.json()["stream_id"]

    time.sleep(2.5)
    health_resp = client.get(f"/api/streams/{stream_id}/health")
    assert health_resp.status_code == 200, health_resp.text
    health = health_resp.json()
    assert health["stream_id"] == stream_id
    assert health["state"] in [StreamState.running, StreamState.error]

    del_resp = client.delete(f"/api/streams/{stream_id}")
    assert del_resp.status_code == 204, del_resp.text

    health_resp2 = client.get(f"/api/streams/{stream_id}/health")
    assert health_resp2.status_code == 404


def test_add_stream_with_explicit_id_and_remove(
    client: TestClient, make_add_stream_payload
):
    explicit_id = "test-stream-123"
    resp = client.post(
        "/api/streams",
        json=make_add_stream_payload(stream_id=explicit_id),
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["stream_id"] == explicit_id

    del_resp = client.delete(f"/api/streams/{explicit_id}")
    assert del_resp.status_code == 204, del_resp.text
