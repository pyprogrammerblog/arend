from arend.task import arend_task


@arend_task(queue="test")
def task_capitalize(name: str):
    """
    Example task for testing
    """
    return name.capitalize()


@arend_task(queue="test")
def task_count(name: str, to_count: str):
    """
    Example task for testing
    """
    return name.count(x=to_count)
