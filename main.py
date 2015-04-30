#!/usr/bin/python

import os
#from pprint import pprint

from spark_events_parser.spark_run import SparkRun

PATH = os.path.join("data", "testdfsio")



if __name__ == "__main__":
    for fdata in os.listdir(PATH):
        try:
            a = SparkRun(os.path.join(PATH, fdata))
        except ValueError:
            continue
        a.correlate()
        report = a.generate_report()
        open(os.path.join(PATH, a.get_app_name() + ".txt"), "w").write(report)