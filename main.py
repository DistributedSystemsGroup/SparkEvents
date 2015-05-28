#!/usr/bin/python

import os
from pprint import pprint
from numpy import array, mean, std, arange, concatenate
import matplotlib.pyplot as plt

from spark_events_parser.spark_run import SparkRun

BASE_DIR = "E:\\User Data\\vcs\\netperf\\data\\io_separation_workloads\\preliminary"

def stage_task_breakdown(job_data):
    stages = []

    for i in job_data.jobs:
        for s in job_data.jobs[i].stages:
            stage = {
                "name": s.name.split()[0],
                "n_tasks": len(s.tasks),
                "runtimes": array([ t.finish_time - t.launch_time for t in s.tasks if t.end_reason == "Success" ])
            }
            stages.append(stage)
    return stages

def parse_dir(path):
    runtimes = []
    job_runs_stages = []
    for root, subdirs, files in os.walk(path):
        print("Path: %s" % root)
#        for subdirname in subdirs:
#            parse_dir(os.path.join(root,subdirname))
#        if len(subdirs) > 0:
#            return

        for filename in files:
            try:
                a = SparkRun(os.path.join(root, filename))
            except ValueError:
                continue
            a.correlate()
#            report = a.generate_report()
#            open(os.path.join(root, a.get_app_name() + ".txt"), "w").write(report)

            runtimes.append(a.parsed_data["application_runtime"])

            job_runs_stages.append(stage_task_breakdown(a))

    if len(runtimes) > 0:
        if runtimes < 5:
            print("Warning: less than 5 samples for this measurement")
        runtimes = array(runtimes)
#        print("Average runtime: {}s".format(mean(runtimes)/1000))
#        print("STDDev: {}s".format(std(runtimes)/1000))

        stages_stats = []
        for idx in range(len(job_runs_stages[0])):
            tmp = [ x[idx]["runtimes"] for x in job_runs_stages ]
            tmp = concatenate(tmp)
            stage_stats = {
                "name": job_runs_stages[0][idx]["name"],
                "task_mean": mean(tmp)/1000,
                "task_std": std(tmp)/1000
            }
            stages_stats.append(stage_stats)

        return mean(runtimes)/1000, std(runtimes)/1000, stages_stats
    else:
        return 0, 0, [ { "name": "empty", "task_mean": 0, "task_std": 0 } ]


def generate_graph_stage(workload_name, data, scenarios):
    """
     For a given workload, draw a bar graph comparing the stages in the different scenarios
    """
    def get_new_color(old_color):
        if old_color == 'r':
            return 'g'
        elif old_color == 'g':
            return 'b'
        elif old_color == 'b':
            return 'y'
        elif old_color == 'y':
            return 'c'
        else:
            return 'r'

    N = len(scenarios)
    ind = arange(N)
    bar_width = 0.15
    fig, ax = plt.subplots()

    rects = []
    offset = 0
    color = None
    stage_names = [ st["name"] for st in data["standard"][workload_name][2] ]
    for st in range(len(data["standard"][workload_name][2])):
        color = get_new_color(color)
        means = []
        stddev = []
        for s in scenarios:
            try:
                means.append(data[s][workload_name][2][st]["task_mean"])
                stddev.append(data[s][workload_name][2][st]["task_std"])
            except IndexError:
                means.append(0)
                stddev.append(0)
        rects.append(ax.bar(ind + offset, means, bar_width, color=color, yerr=stddev))
        offset += bar_width

    ax.set_ylabel('Task run time (s)')
    ax.set_title('Per workload task run times')
    ax.set_xticks(ind + (bar_width * (N / 2)))

    if workload_name != "tpcds":
        ax.set_xticklabels(scenarios)
        ax.legend([ x[0] for x in rects], stage_names) #, loc=2)

    plt.savefig(os.path.join(BASE_DIR, 'task_runtime_%s.pdf' % workload_name), format='pdf')


def generate_graph(data, workloads, scenarios):
    def get_new_color(old_color):
        if old_color == 'r':
            return 'g'
        elif old_color == 'g':
            return 'b'
        elif old_color == 'b':
            return 'y'
        elif old_color == 'y':
            return 'c'
        else:
            return 'r'

    N = len(workloads)
    ind = arange(N)
    bar_width = 0.15
    fig, ax = plt.subplots()

    rects = []
    offset = 0
    color = None
    for s in scenarios:
        color = get_new_color(color)
        means = [ data[s][w][0] for w in workloads ]
        stddev = [ data[s][w][1] for w in workloads ]
        rects.append(ax.bar(ind + offset, means, bar_width, color=color, yerr=stddev))
        offset += bar_width

    ax.set_ylabel('Run time')
    ax.set_title('Workload run times (s)')
    ax.set_xticks(ind + (bar_width * (N / 2)))
    ax.set_xticklabels(workloads)

    ax.legend([ x[0] for x in rects], scenarios, loc=2)

    plt.savefig(os.path.join(BASE_DIR, 'runtimes.pdf'), format='pdf')

if __name__ == "__main__":
    scenarios = ["standard", "phy_colocation", "no_colocation", "volumes", "swift"]
    workloads = ["wordcount", "testDFSIO", "tpcds"]
    data = {}
    for s in scenarios:
        data[s] = {}
        for w in workloads:
            path = os.path.join(BASE_DIR, s, w)
            data[s][w] = parse_dir(path)

    for s in scenarios:
        print("Scenario {}".format(s))
        for w in workloads:
            print("-> Workload {}".format(w))
            print(" -> Mean: %.2f" % data[s][w][0])
            print(" -> Stddev: %.2f" % data[s][w][1])
            for st in data[s][w][2]:
                print(" -> Stage %s" % st["name"])
                print("   -> Mean task time %.3f" % st["task_mean"])
                print("   -> Stddev task time %.3f" % st["task_std"])

    generate_graph(data, workloads, scenarios)

    for w in workloads:
        generate_graph_stage(w, data, scenarios)
