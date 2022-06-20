import datetime
import logging
from uuid import UUID

from arend.backends.base import Task
from arend.settings import settings
from fastapi import APIRouter, HTTPException
from pymongo import DESCENDING, MongoClient

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/tasks/{uuid}", response_model=Task)
def get_task(uuid: UUID) -> dict:
    """
    Retrieve a single Task
    """
    if not (task := Task.get(uuid=uuid)):
        raise HTTPException(status_code=404, detail="Not found")

    return task


@router.get("/tasks/", response_model=Task)
def get_tasks(
    name: str = None,
    enabled: bool = None,
    start_datetime: datetime.datetime = None,
    end_datetime: datetime.datetime = None,
    page: int = None,
    paginate_by: int = None,
) -> dict:
    """
    Retrieve multiple Tasks
    """
    query = {}

    if name is not None:
        query["name"] = name
    if enabled is not None:
        query["enabled"] = enabled
    if start_datetime is not None:
        query["created"] = {"$gte": start_datetime}
    if end_datetime is not None:
        query["created"] = {"$lt": end_datetime}

    skip, limit = 0, 0
    if page:
        paginate_by = paginate_by or settings.paginate_by
        skip = page * paginate_by - paginate_by
        limit = skip + paginate_by

    with MongoClient(
        settings.mongo_connection, UuidRepresentation="standard"
    ) as client:
        db = client.get_database(settings.mongo_db)
        collection = db.get_collection(settings.mongo_db_streams)
        tasks = list(
            collection.find(filter=query, projection={"_id": False})
            .sort("created", DESCENDING)
            .skip(skip=skip)
            .limit(limit=limit)
        )

    return {"tasks": tasks, "count": len(tasks)}


@router.patch("/tasks/{uuid}", response_model=Task)
def update_task(uuid: UUID, task: Task) -> Task:
    """
    Update a Task
    """
    if not (existing := Task.get(uuid=uuid)):
        raise HTTPException(status_code=404, detail="Not found")

    updated_kwargs = {**existing.dict(), **task.dict()}
    return Task(**updated_kwargs).save()


@router.post("/tasks/", status_code=201, response_model=Task)
def create_task(task: Task) -> Task:
    """
    Create a Task
    """
    task = Task(**task.dict()).save()
    return task


@router.delete("/tasks/{uuid}", status_code=204)
def delete_task(uuid: UUID) -> None:
    """
    Delete a Task
    """
    if not (task := Task.get(uuid=uuid)):
        raise HTTPException(status_code=404, detail="Not found")

    task.delete()
