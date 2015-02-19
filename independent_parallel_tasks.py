# By Jussi Jousimo, jvj@iki.fi

import signal
import abc
import subprocess
import time
import sys
from ubkey import *

running_tasks = []

def sigint_handler(signal, frame):
    print "Ctrl+c detected, exiting..."
    for task_id, task_process, host in running_tasks:
        print "Killing task " + str(task_id) + "..."
        task_process.kill()
    sys.exit(-1)

signal.signal(signal.SIGINT, sigint_handler)

class Node(object):
    def __init__(self, host, load, free_mem):
        self.host = host
        self.load = float(load)
        self.free_mem = int(free_mem or 0)

    def __lt__(self, other):
        return self.load < other.load

    def __repr__(self):
        return "%s, %s, %s" % (self.host, self.load, self.free_mem)

class Cluster(object):
    __metaclass__ = abc.ABCMeta
    nodes = []
    
    def __init__(self, verbose):
        self.verbose = verbose
        return
    
    @abc.abstractmethod
    def get_remote_nodes(self):
        return

    def filter_nodes(self, max_nodes, max_load, min_free_mem, blacklist):
        if (len(blacklist) > 0):
            if (self.verbose): print "Blacklisted nodes: " + " ".join(blacklist)
            self.nodes[:] = (i for i in self.nodes if i.host not in blacklist)

        self.nodes.sort()

        retain_index = []
        for i in range(len(self.nodes)):
            print self.nodes[i], " ",
            if (self.nodes[i].load <= max_load and self.nodes[i].free_mem > min_free_mem):
                retain_index.append(i)
                print "ALLOCATE"
            else:
                print ""
            if (len(retain_index) == max_nodes):
                break

        self.nodes = [self.nodes[i] for i in retain_index]

        #self.nodes = self.nodes[retain_index]

        #number_of_nodes = len(self.nodes)
        #if (number_of_nodes > max_nodes):
        #    number_of_nodes = max_nodes
    
        #self.nodes.sort()
        #for i in range(number_of_nodes):
        #    print self.nodes[i]
        #    if (self.nodes[i].load > max_load or self.nodes[i].free_mem <= min_free_mem):
        #        number_of_nodes = i
        #        break
        
        #self.nodes = self.nodes[0:number_of_nodes]

    def get_nodes(self, max_nodes=sys.maxsize, max_load=sys.maxsize, min_free_mem=0, blacklist=[]):
        self.get_remote_nodes()
        self.filter_nodes(max_nodes, max_load, min_free_mem, blacklist)
        if (len(self.nodes) == 0):
            print "No cluster nodes available."
            sys.exit(-1)
        return self.nodes

    # TODO: ssh arguments
    def run_task(self, task_id, host, batch_file, arguments, log_file_dir, priority):
        log_file = log_file_dir + "/task-" + str(task_id) + ".log"
        # Note: removed -tt from ssh
        command = "nice -n " + str(priority) + " ssh -o ConnectTimeout=10 -o BatchMode=yes -o StrictHostKeyChecking=no " + \
            host + " \"R --vanilla --args " + arguments + " " + str(task_id) + " < " + batch_file + " > " + log_file + " 2>&1\""

        print "Starting task " + str(task_id) + " at " + host + "..."
        print "Output will be written to " + log_file + " at the remote node."
        if (self.verbose): print command

        task_process = subprocess.Popen(command, shell=True)
        running_tasks.append((task_id, task_process, host))

        return

    def run_tasks(self, task_ids, batch_file, arguments, log_file_dir, priority):
        print "Running " + str(len(task_ids)) + " tasks:"
        print task_ids
        print "on " + str(len(self.nodes)) + " remote hosts:"
        available_hosts = [i.host for i in self.nodes]
        print available_hosts

        n_new_tasks = min(len(task_ids), len(available_hosts))
        for i in range(0, n_new_tasks):
            self.run_task(task_ids.pop(), available_hosts.pop(), batch_file, arguments, log_file_dir, priority)

        failed_hosts = []

        while len(running_tasks) > 0:
            for task_id, task_process, host in running_tasks:
                return_code = task_process.poll()
                if return_code is not None:
                    running_tasks.remove((task_id, task_process, host))
                    print "Task " + str(task_id) + " at " + host + " terminated with return code " + str(return_code) + ". " + str(len(running_tasks)) + " tasks left."

                    if return_code == 255: # Cannot reach host or cannot authenticate
                        print "*** UNABLE TO CONNECT TO HOST " + host + " FOR TASK " + str(task_id) + " ***"
                        failed_hosts.append(host)
                        task_ids.append(task_id)
                    elif return_code == 137: # R was killed in the remote host
                        print "*** TASK " + str(task_id) + " KILLED IN HOST " + host + " ***"
                        failed_hosts.append(host)
                        task_ids.append(task_id)
                    # TODO: Handle other return codes ??
                    else:
                        available_hosts.append(host)

            if len(task_ids) > 0 and len(available_hosts) > 0:
                self.run_task(task_ids.pop(), available_hosts.pop(), batch_file, arguments, log_file_dir, priority)

            sys.stdout.flush()

            chkey = getch()
            if chkey >= 0:
                if chkey == 116:
                    for task_id, task_process, host in running_tasks:
                        print "Running task " + str(task_id) + " at " + host
                elif chkey == 107:
                    for task_id, task_process, host in running_tasks:
                        print "Killing task " + str(task_id) + " at " + host
                        task_ids.append(task_id)
                        failed_hosts.append(host)
                        task_process.kill()
                    break

            time.sleep(1)

        if len(task_ids) > 0:
            print "Failed to complete the tasks:"
            print str(sorted(task_ids)).strip('[]')
            print "on hosts:"
            print sorted(failed_hosts)
            return -1

        return 0

    # TODO: handle SIGNIT here too
    def run_command(self, command, timeout=10):
        running_commands = []
        for node in self.nodes:
            # Note: removed -tt from ssh
            full_command = "ssh -o ConnectTimeout=" + str(timeout) + " -o BatchMode=yes -o StrictHostKeyChecking=no " + \
                node.host + " \"" + command + "\""
            if (self.verbose): print full_command
            command_process = subprocess.Popen(full_command, shell=True)
            running_commands.append((node.host, command_process))

        while running_commands:
            for host, command_process in running_commands:
                return_code = command_process.poll()
                if return_code is not None:
                    running_commands.remove((host, command_process))
                    print host + " done with return code " + str(return_code) + "."
            sys.stdout.flush()
            time.sleep(1)

        return

    def ping_task(self, host):
        print "Pinging " + host + "..."

        command = "ping -c 2 " + host
        task_process = subprocess.Popen(command, shell=True)
        running_tasks.append((0, task_process, host))

        return

    def ping(self, number_of_hosts):
        available_hosts = [i.host for i in self.nodes]

        n_new_tasks = min(number_of_hosts, len(available_hosts))
        for i in range(0, n_new_tasks):
            self.ping_task(available_hosts.pop())

        while len(running_tasks) > 0:
            for task_id, task_process, host in running_tasks:
                return_code = task_process.poll()
                if return_code is not None:
                    running_tasks.remove((0, task_process, host))
                    print "Ping terminated at " + host + " with return code " + str(return_code) + ". " + str(len(running_tasks)) + " hosts left."

                    if return_code == 1: # No reply
                        print "*** UNABLE TO REACH TO HOST " + host + " ***"
                    elif return_code > 1: # Other error
                        print "*** UNKNOWN ERROR REACHING HOST " + host + " ***"
            time.sleep(1)

        return
