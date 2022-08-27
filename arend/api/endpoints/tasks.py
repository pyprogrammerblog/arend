import datetime
import logging
from uuid import UUID

from pymongo import DESCENDING
from pymongo import MongoClient
from arend.settings import settings
from fastapi import APIRouter, HTTPException
from arend.tube.task import Task, Tasks

__all__ = ["arend_router"]


logger = logging.getLogger(__name__)

arend_router = APIRouter()


@arend_router.get("/task/{uuid}", response_model=Task)
def get_task(uuid: UUID) -> dict:
    """
    Retrieve a single Task
    """
    if not (task := Task.get(uuid=uuid)):
        raise HTTPException(status_code=404, detail="Not found")

    return task


@arend_router.get("/task/", response_model=Tasks)
def get_tasks(
    status: str = None,
    smart_stream_uuid: UUID = None,
    start_datetime: datetime.datetime = None,
    end_datetime: datetime.datetime = None,
    page: int = None,
    paginate_by: int = None,
) -> dict:
    """
    Retrieve multiple Task
    """
    query = {}

    if status is not None:
        query["status"] = status
    if smart_stream_uuid is not None:
        query["smart_stream.uuid"] = smart_stream_uuid
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
        collection = db.get_collection(settings.mongo_db_tasks)
        tasks = list(
            collection.find(filter=query, projection={"_id": False})
            .sort("created", DESCENDING)
            .skip(skip=skip)
            .limit(limit=limit)
        )

    return {"utils": tasks, "count": len(tasks)}


@arend_router.delete("/task/{uuid}", status_code=204)
def delete_task(uuid: UUID) -> None:
    """
    Delete a Task
    """
    if not (task := Task.get(uuid=uuid)):
        raise HTTPException(status_code=404, detail="Not found")

    task.delete()
