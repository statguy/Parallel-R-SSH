#!/usr/bin/python
#
# Some code taken from Jukka Suomela,
# https://github.com/suomela/ukko
#
# Example run:
# parallel_ssh_r.py -n 10 -m 5 -l 10.0 -b blacklist.txt -v test.R

import abc
import re
import urllib2
import optparse
import os
from subprocess import Popen
import time

class Node(object):
    def __init__(self, host, load):
        self.host = host
        self.load = float(load)

    def __lt__(self, other):
        return self.load < other.load

    def __repr__(self):
        return "%s, %s" % (self.host, self.load)

class Cluster(object):
    __metaclass__ = abc.ABCMeta
    nodes = []
    
    def __init__(self, verbose):
        self.verbose = verbose
        return
    
    @abc.abstractmethod
    def get_remote_nodes(self):
        return

    def filter_nodes(self, max_nodes, max_load, blacklist):
        if (len(blacklist) > 0):
            if (self.verbose): print "Blacklisted nodes: " + " ".join(blacklist)
            self.nodes[:] = (i for i in self.nodes if i.host not in blacklist)

        number_of_nodes = len(self.nodes)
        if (number_of_nodes > max_nodes):
            number_of_nodes = max_nodes
    
        self.nodes.sort()
        for i in range(0, number_of_nodes):
            if (self.nodes[i].load > max_load):
                number_of_nodes = i
                break
        
        self.nodes = self.nodes[0:number_of_nodes]

    def get_nodes(self, max_nodes, max_load, blacklist):
        self.get_remote_nodes()
        self.filter_nodes(max_nodes, max_load, blacklist)
        if (len(self.nodes) == 0):
            die("No cluster nodes available.")
        return self.nodes

    running_tasks = []
    last_running_task_id = 0

    def run_task(self, task_id, host, remote_call, log_file_dir):
        log_file = log_file_dir + "/task-" + str(task_id) + ".log"
        command = "ssh -o ConnectTimeout=10 -o BatchMode=yes -o StrictHostKeyChecking=no " + host + " \"Rscript " + remote_call + " " + str(task_id) + " > " + log_file + " 2>&1\""

        print "Starting task " + str(task_id) + " at " + host + "..."
        print "Output will be written to " + log_file + " at the remote node."
        if (self.verbose) print command

        self.last_running_task_id = task_id
        task_process = Popen(command, shell=True)
        self.running_tasks.append((task_id, task_process, host))

        return

    def run_tasks(self, n_tasks, remote_call, log_file_dir):
        print "Running " + str(n_tasks) + " tasks on " + str(len(self.nodes)) + " remote nodes:"
        print "".join([i.host + " " for i in self.nodes])

        n_new_tasks = min(n_tasks, len(self.nodes))
        for i in range(0, n_new_tasks):
            self.run_task(i + 1, self.nodes[i].host, remote_call, log_file_dir)

        while self.running_tasks:
            for task_id, task_process, host in self.running_tasks:
                return_code = task_process.poll()
                if (return_code is not None):
                    self.running_tasks.remove((task_id, task_process, host))
                    print "Task " + str(task_id) + " at " + host + " terminated with return code " + str(return_code) + "."
                    if (self.last_running_task_id < n_tasks):
                        self.run_task(self.last_running_task_id + 1, host, remote_call, log_file_dir)
            time.sleep(1)
        
        return

class CSCluster(Cluster):
    entry = re.compile(r'''
        ^(ukko[0-9]+\.hpc\.cs\.helsinki\.fi) \s+
        ([0-9]+) \s+  # slot
        (yes|no) \s+  # ping
        (yes|no) \s+  # ssh
        ([0-9]+) \s+  # users
        (?:([0-9]+\.[0-9]+)|-) \s+  # load
        (cs|rr|ok)(?:/t)? \s+  # stat
        (N/A|[0-9.-]+-(?:generic|server))? \s+  # kernel
        ([0-9.]+|--) \s*  # BIOS
        (.*?) \s* $
        ''', re.X)
    
    def __init__(self, verbose, url=None):
        super(self.__class__, self).__init__(verbose)
        self.url = url if url is not None else 'http://www.cs.helsinki.fi/ukko/hpc-report.txt'

    def get_remote_nodes(self):
        if (self.verbose):
            print "Retrieving remote nodes list from", self.url + "..."
        f = urllib2.urlopen(self.url)
        
        for l in f:
            m = self.entry.match(l)
            if m is not None:
                host, slot, ping, ssh, users, load, stat, kernel, bios, comment = m.groups()
                if stat != 'ok':
                    continue
                if ping != 'yes' or ssh != 'yes' or load is None:
                    continue
                self.nodes.append(Node(host, load))
        return

def main():

    #nodes = []
    #nodes.append(Node("a", 1))
    #nodes.append(Node("b", 1))
    #nodes.append(Node("c", 1))
    #nodes.append(Node("d", 1))
    #blacklist = ["b", "d"]
    #print nodes
    #nodes[:] = (i for i in nodes if i.host not in blacklist)
    #print nodes

    parser = optparse.OptionParser("usage: %prog options [options] call")

    parser.add_option("-n", "--number_of_tasks", metavar="TASKS", dest="number_of_tasks", type="int", help="number of tasks to execute")
    parser.add_option("-m", "--max_nodes", metavar="NODES", dest="max_nodes", default=2, type="int", help="maximum number nodes to allocate")
    parser.add_option("-l", "--max_load", metavar="LOAD", dest="max_load", default=10.0, type="float", help="maximum load allowed in nodes for allocation")
    parser.add_option("-f", "--log_dir", metavar="DIR", dest="log_file_dir", default="~/tmp", type="string", help="directory to write log files")
    parser.add_option("-b", "--blacklist", metavar="FILE", dest="blacklist_file", type="string", help="file of blacklisted nodes")
    parser.add_option("-v", "--verbose", dest="verbose", default=False, action="store_true", help="print verbose messages")

    (options, args) = parser.parse_args()
    if (options.number_of_tasks is None):
        parser.error("-n is missing")
    blacklist = []
    if (options.blacklist_file is not None):
        blacklist = [line.strip() for line in open(options.blacklist_file, "r")]

    if len(args) == 0:
        parser.error("call is missing")
    remote_call = " ".join(args)

    cluster = CSCluster(options.verbose)
    cluster.get_nodes(min(options.number_of_tasks, options.max_nodes), options.max_load, blacklist)
    cluster.run_tasks(options.number_of_tasks, remote_call, options.log_file_dir)

    return

if __name__ == "__main__":
    main()
