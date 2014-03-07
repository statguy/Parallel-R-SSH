#!/usr/bin/python

# Some code taken from Jukka Suomela,
# https://github.com/suomela/ukko

# Example run:
# r_parallel_screen.py -n 5 -l 10.0 -b 
# -v "R --slave -f ~/git/RSnippets/Cluster/test_mpi.R"

import abc
import re
import urllib2
import optparse
import os

class Node(object):
    def __init__(self, host, load):
        self.host = host
        self.load = float(load)

    def __lt__(self, other):
        return self.load < other.load

class Cluster(object):
    __metaclass__ = abc.ABCMeta
    nodes = []
    
    def __init__(self, verbose):
        self.verbose = verbose
        return
    
    @abc.abstractmethod
    def get_remote_nodes(self):
        return

    def filter_nodes(self, max_nodes, max_load):
        number_of_nodes = len(self.nodes)
        if (number_of_nodes > max_nodes):
            number_of_nodes = max_nodes
    
        self.nodes.sort()
        for i in range(0, number_of_nodes):
            if (self.nodes[i].load > max_load):
                number_of_nodes = i
                break
        
        self.nodes = self.nodes[0:number_of_nodes]

    def get_nodes(self, max_nodes, max_load):
        self.get_remote_nodes()
        self.filter_nodes(max_nodes, max_load)
        if (len(self.nodes) == 0):
            die("No cluster nodes available.")
        return self.nodes

        #hosts = ','.join([i.host for i in cluster_nodes])
        #if (len(cluster_nodes) == 0): command = "mpirun -H localhost -np 1 " + call
        #else: command = "mpirun -H localhost," + hosts + " -np 1 " + call

        #print "Starting cluster with", len(cluster_nodes), "nodes with max load", str(max_load) + ":"
        #print '\n'.join([i.host + " " + str(i.load) for i in cluster_nodes])

        #if (self.verbose): print command
        #os.system(command)

        #return

    def run_tasks(self, number_of_tasks, remote_call):
        print "Running " + str(number_of_tasks) + " tasks on " + str(len(self.nodes)) + " remote nodes:"
        print "".join([i.host + " " for i in self.nodes])
        
        for i in range(1, len(self.nodes)):
            command = "screen -S " + str(i) + "-" + self.nodes[i].host + " ssh " + self.nodes[i].host + \
            " Rscript " + remote_call + " " + str(i)
            print command
            os.system(command)
        
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
    parser = optparse.OptionParser("usage: %prog options [options] call")

    parser.add_option("-n", "--number_of_tasks", metavar="TASKS", dest="number_of_tasks", type="int", help="number of tasks to execute")
    parser.add_option("-m", "--max_nodes", metavar="NODES", dest="max_nodes", default=2, type="int", help="maximum number nodes to allocate")
    parser.add_option("-l", "--max_load", metavar="LOAD", dest="max_load", default=10.0, type="float", help="maximum load allowed in nodes for allocation")
    parser.add_option("-b", "--blacklist", metavar="FILE", dest="blacklist_file", type="string", help="file of blacklisted nodes")
    parser.add_option("-v", "--verbose", dest="verbose", default=False, action="store_true", help="print verbose messages")

    (options, args) = parser.parse_args()
    number_of_tasks = options.number_of_tasks
    if (number_of_tasks is None):
        parser.error("-n is missing")
    max_nodes = options.max_nodes
    max_load = options.max_load
    verbose = options.verbose
    blacklist_file = options.blacklist_file

    if len(args) == 0:
        parser.error("call is missing")
    remote_call = " ".join(args)

    cluster = CSCluster(verbose)
    cluster.get_nodes(min(number_of_tasks, max_nodes), max_load)
    cluster.run_tasks(number_of_tasks, remote_call)

    return

if __name__ == "__main__":
    main()
