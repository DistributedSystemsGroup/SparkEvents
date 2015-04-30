from datetime import datetime

class Job:
    def __init__(self, start_data):
        self.job_id = start_data["Job ID"]
        self.stages = []
        for stage_data in start_data["Stage Infos"]:
            self.stages.append(Stage(stage_data))
        self.submission_time = start_data["Submission Time"]

        self.result = None
        self.end_time = None

    def complete(self, data):
        self.result = data["Job Result"]["Result"]
        self.end_time = data["Completion Time"]

    def report(self, indent):
        pfx = " " * indent
        s = pfx + "Job {}\n".format(self.job_id)
        indent += 1
        pfx = " " * indent
        s += pfx + "Submission time: {}\n".format(datetime.fromtimestamp(self.submission_time/1000))
        s += pfx + "Run time: {}ms\n".format(self.end_time - self.submission_time)
        s += pfx + "Result: {}\n".format(self.result)
        s += pfx + "Number of stages: {}\n".format(len(self.stages))
        for stage in self.stages:
            s += stage.report(indent)
        return s

class Stage:
    def __init__(self, stage_data):
        self.stage_id = stage_data["Stage ID"]
        self.details = stage_data["Details"]
        self.task_num = stage_data["Number of Tasks"]
        self.RDDs = []
        for rdd_data in stage_data["RDD Info"]:
            self.RDDs.append(RDD(rdd_data))
        self.name = stage_data["Stage Name"]
        self.tasks = []

        self.completion_time = None
        self.submission_time = None

    def complete(self, data):
        self.completion_time = data["Stage Info"]["Completion Time"]
        self.submission_time = data["Stage Info"]["Submission Time"]

    def report(self, indent):
        pfx = " " * indent
        s = pfx + "Stage {} ({})\n".format(self.name, self.stage_id)
        indent += 1
        pfx = " " * indent
        s += pfx + "Number of tasks: {}\n".format(self.task_num)
        s += pfx + "Number of executed tasks: {}\n".format(len(self.tasks))
        s += pfx + "Completion time: {}ms\n".format(self.completion_time - self.submission_time)
        for rdd in self.RDDs:
            s += rdd.report(indent)
        return s

class RDD:
    def __init__(self, rdd_data):
        self.rdd_id = rdd_data["RDD ID"]
        self.disk_size = rdd_data["Disk Size"]
        self.memory_size = rdd_data["Memory Size"]
        self.name = rdd_data["Name"]
        self.partitions = rdd_data["Number of Partitions"]
        self.replication = rdd_data["Storage Level"]["Replication"]

    def report(self, indent):
        pfx = " " * indent
        s = pfx + "RDD {} ({})\n".format(self.name, self.rdd_id)
        indent += 1
        pfx = " " * indent
        s += pfx + "Size: {}B memory {}B disk\n".format(self.memory_size, self.disk_size)
        s += pfx + "Partitions: {}\n".format(self.partitions)
        s += pfx + "Replication: {}\n".format(self.replication)
        return s