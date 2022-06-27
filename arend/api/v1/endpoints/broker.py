import datetime
import logging
from uuid import UUID

from arend.backends import Task
from arend.settings import settings
from fastapi import APIRouter, HTTPException
from pymongo import DESCENDING, MongoClient

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/queue-tasks/{uuid}", response_model=Task)
def get_task(uuid: UUID) -> dict:
    """
    Retrieve a single Queue Task
    """
    if not (queue_task := Task.get(uuid=uuid)):
        raise HTTPException(status_code=404, detail="Not found")

    return queue_task


@router.get("/queue-tasks/", response_model=Task)
def get_queue_tasks(
    name: str = None,
    enabled: bool = None,
    start_datetime: datetime.datetime = None,
    end_datetime: datetime.datetime = None,
    page: int = None,
    paginate_by: int = None,
) -> dict:
    """
    Retrieve multiple Queue Task
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
        queue_tasks = list(
            collection.find(filter=query, projection={"_id": False})
            .sort("created", DESCENDING)
            .skip(skip=skip)
            .limit(limit=limit)
        )

    return {"queue_tasks": queue_tasks, "count": len(queue_tasks)}


@router.patch("/queue-tasks/{uuid}", response_model=Task)
def update_queue_task(uuid: UUID, queue_task: Task) -> Task:
    """
    Update a Queue Task
    """
    if not (existing := Task.get(uuid=uuid)):
        raise HTTPException(status_code=404, detail="Not found")

    updated_kwargs = {**existing.dict(), **task.dict()}
    return Task(**updated_kwargs).save()


@router.post("/queue-tasks/", status_code=201, response_model=Task)
def create_queue_task(queue_task: Task) -> Task:
    """
    Create a Queue Task
    """
    queue_task = Task(**queue_task.dict()).save()
    return queue_task


@router.delete("/queue-tasks/{uuid}", status_code=204)
def delete_queue_task(uuid: UUID) -> None:
    """
    Delete a Queue Task
    """
    if not (queue_task := Task.get(uuid=uuid)):
        raise HTTPException(status_code=404, detail="Not found")

    queue_task.delete()
