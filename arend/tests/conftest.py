from arend.tube.task import Task

import pytest


@pytest.fixture(scope="session")
def task() -> Task:
    task = Task(
        task_name="name",
        task_location="location",
        queue_name="queue",
        task_priority=1,
        task_delay=1,
    )
    return task


# @arend_task(queue_name="test")
# def sleep_task(seconds=0, return_value=None, event="success"):
#     """
#     For testing
#     """
#     print("executing sleep task...")
#     event = event.lower()
#     if event == "success":
#         time.sleep(int(seconds))
#     else:
#         raise ValueError(f"Unknown event '{event}'")
#
#     return return_value
