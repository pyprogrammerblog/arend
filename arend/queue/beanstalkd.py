from pystalkd.Beanstalkd import Connection


class BeanstalkdConnector:

    def __init__(self, queue_name: str):
        self.connection = Connection()
        self.connection.watch(queue_name)

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
