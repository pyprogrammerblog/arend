import uuid
from arend.settings import status
import pytest
from datetime import datetime


@pytest.fixture
def test_get_task(client, tasks):
    """
    Test get task
    """
    response = client.get(f"/api/arend-tasks/{tasks[0].uuid}/")
    assert response.ok
    assert response.json()["uuid"] == tasks[0].uuid

    response = client.get(f"/api/arend-tasks/{tasks[1].uuid}/")
    assert response.ok
    assert response.json()["uuid"] == tasks[1].uuid

    response = client.get(f"/api/arend-tasks/{uuid.uuid4()}/")
    assert not response.ok
    assert response.status_code == 404


def test_get_tasks(client, tasks):
    """
    Test get tasks
    """
    response = client.get(f"/api/arend-tasks/")
    assert response.ok
    assert response.status_code == 200
    assert response.json()["count"] == 2

    params = {"status": status.FINISHED}
    response = client.get(f"/api/arend-tasks/", params=params)
    assert response.ok
    assert response.status_code == 200
    assert response.json()["count"] == 1
    assert response.json()["tasks"][0]["uuid"] == tasks[0].uuid

    params = {
        "start": datetime(year=2020, month=1, day=1, hour=0, minute=0),
        "end": datetime(year=2020, month=1, day=2, hour=0, minute=0),
    }
    response = client.get(f"/api/arend-tasks/", params=params)
    assert response.ok
    assert response.status_code == 200
    assert response.json()["count"] == 1
    assert response.json()["tasks"][0]["uuid"] == tasks[0].uuid

    params = {
        "start": datetime(year=2019, month=1, day=1, hour=0, minute=0),
        "end": datetime(year=2021, month=1, day=1, hour=0, minute=0),
    }
    response = client.get(f"/api/arend-tasks/", params=params)
    assert response.ok
    assert response.status_code == 200
    assert response.json()["count"] == 2


def test_delete_task(client, tasks):
    """
    Test delete tasks
    """
    response = client.get(f"/api/arend-tasks/{tasks[0].uuid}")
    assert response.status_code == 200

    response = client.delete(f"/api/arend-tasks/{tasks[0].uuid}")
    assert response.ok
    assert response.status_code == 201

    response = client.get(f"/api/arend-tasks/{tasks[0].uuid}")
    assert response.status_code == 404
