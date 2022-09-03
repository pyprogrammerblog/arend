from arend import arend_task


@arend_task(queue="queue")
def my_task(name: str):
    return print(f"Hola {name}!")
