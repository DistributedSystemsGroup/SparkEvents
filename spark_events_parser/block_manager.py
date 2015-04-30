from datetime import datetime

from spark_events_parser.utils import sizeof_fmt


class BlockManager:
    def __init__(self, data):
        self.maximum_memory = data["Maximum Memory"]
        self.add_timestamp = data["Timestamp"]
        self.executor_id = data["Block Manager ID"]["Executor ID"]

    def report(self, indent):
        pfx = " " * indent
        s = pfx + "Block manager\n"
        indent += 1
        pfx = " " * indent
        s += pfx + "Executor ID: {}\n".format(self.executor_id)
        s += pfx + "Time added: {}\n".format(datetime.fromtimestamp(self.add_timestamp/1000))
        s += pfx + "Maximum memory: {}\n".format(sizeof_fmt(self.maximum_memory))
        return s