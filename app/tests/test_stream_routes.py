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


def test_add_two_list_and_remove_flow(client: TestClient, make_add_stream_payload):
    existing = client.get("/api/streams")
    assert existing.status_code == 200, existing.text
    for item in existing.json():
        client.delete(f"/api/streams/{item['stream_id']}")

    a_id, b_id = "test-stream-a", "test-stream-b"
    resp_a = client.post(
        "/api/streams",
        json=make_add_stream_payload(stream_id=a_id, path=a_id),
    )
    assert resp_a.status_code == 200, resp_a.text

    resp_b = client.post(
        "/api/streams",
        json=make_add_stream_payload(stream_id=b_id, path=b_id),
    )
    assert resp_b.status_code == 200, resp_b.text

    list_resp = client.get("/api/streams")
    assert list_resp.status_code == 200, list_resp.text
    items = list_resp.json()
    assert len(items) == 2
    ids = {item["stream_id"] for item in items}
    assert {a_id, b_id} == ids

    del_a = client.delete(f"/api/streams/{a_id}")
    assert del_a.status_code == 204, del_a.text

    list_after_one = client.get("/api/streams")
    assert list_after_one.status_code == 200, list_after_one.text
    items_after_one = list_after_one.json()
    assert len(items_after_one) == 1
    assert items_after_one[0]["stream_id"] == b_id

    del_b = client.delete(f"/api/streams/{b_id}")
    assert del_b.status_code == 204, del_b.text

    list_after_all = client.get("/api/streams")
    assert list_after_all.status_code == 200, list_after_all.text
    assert list_after_all.json() == []
