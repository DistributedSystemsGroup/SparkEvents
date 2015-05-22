#!/usr/bin/python

import os
#from pprint import pprint
import sys
from numpy import array, mean, std

from spark_events_parser.spark_run import SparkRun

def parse_dir(path):
    runtimes = []
    for root, subdirs, files in os.walk(path):
        for subdirname in subdirs:
            parse_dir(os.path.join(root,subdirname))

        for filename in files:
            try:
                a = SparkRun(os.path.join(root, filename))
            except ValueError:
                continue
            a.correlate()
            report = a.generate_report()
            open(os.path.join(root, a.get_app_name() + ".txt"), "w").write(report)

            runtimes.append(a.parsed_data["application_runtime"])
    if len(runtimes) > 0:
        runtimes = array(runtimes)
        print("Average runtime: {}".format(mean(runtimes)))
        print("STDDev: {}".format(std(runtimes)))


if __name__ == "__main__":
    parse_dir("E:\\User Data\\vcs\\netperf\\data\\io_separation_workloads\\preliminary\\wordcount")