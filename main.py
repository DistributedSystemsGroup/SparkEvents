#!/usr/bin/python

import os
#from pprint import pprint
import sys

from spark_events_parser.spark_run import SparkRun


def parse_dir(path):
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


if __name__ == "__main__":
    if len(sys.argv) < 1:
        PATH = os.path.join("smaple_data")
    else:
        PATH = os.path.join(sys.argv[1])
    parse_dir(PATH)