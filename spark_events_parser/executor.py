from datetime import datetime

class Executor:
    def __init__(self, data):
        self.executor_id = data["Executor ID"]
        self.host = data["Executor Info"]["Host"]
        self.block_managers = []
        self.start_timestamp = data["Timestamp"]
        self.total_cores = data["Executor Info"]["Total Cores"]
        self.tasks = []

        self.remove_reason = None
        self.remove_timestamp = None

    def add_block_manager(self, bm):
        self.block_managers.append(bm)

    def remove(self, data):
        self.remove_reason = data["Removed Reason"]
        self.remove_timestamp = data["Timestamp"]

    def report(self, indent):
        pfx = " " * indent
        s = pfx + "Executor {}\n".format(self.executor_id)
        indent += 1
        pfx = " " * indent
        s += pfx + "Host: " + self.host + "\n"
        s += pfx + "Total cores: {}\n".format(self.total_cores)
        s += pfx + "Started at: {}\n".format(datetime.fromtimestamp(self.start_timestamp/1000))
        if self.remove_timestamp is not None:
            s += pfx + "Run time: {}ms\n".format(self.remove_timestamp - self.start_timestamp)
        s += pfx + "Number of block managers: {}\n".format(len(self.block_managers))
        s += pfx + "Number of executed tasks: {}\n".format(len(self.tasks))
        s += pfx + "Termination reason: {}\n".format(self.remove_reason)
        return s


