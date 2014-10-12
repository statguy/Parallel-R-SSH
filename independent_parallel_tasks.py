# By Jussi Jousimo, jvj@iki.fi

import signal
import abc
import subprocess
import time
import sys
import getch_process
from multiprocessing import Manager, Value
from ctypes import c_char_p

running_tasks = []
    
def sigint_handler(signal, frame):
    for task_id, task_process, host in running_tasks:
        print "Killing task " + str(task_id) + "..."
        task_process.kill()
    sys.exit(0)

signal.signal(signal.SIGINT, sigint_handler)

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

        self.manager = Manager()
        self.keych = manager.Value(c_char_p, "")
        self.key_thread = GetchProcess(self.keych)
        self.key_thread.start()

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

    def get_nodes(self, max_nodes=sys.maxsize, max_load=sys.maxsize, blacklist=[]):
        self.get_remote_nodes()
        self.filter_nodes(max_nodes, max_load, blacklist)
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

            if self.command.value is not "":
                if self.command.value == "t":
                    for task_id, task_process, host in running_tasks:
                        print "Running task " + str(task_id) + " at " + host
                elif self.comand.value == "k":
                    for task_id, task_process, host in running_tasks:
                        print "Killing task " + str(task_id) + "..."
                        task_process.kill()
                    return

                self.keych.value = ""
                self.thread = GetchProcess(self.keych)
                self.thread.start()

            sys.stdout.flush()
            time.sleep(1)
        
        if len(task_ids) > 0:
            print "Failed to complete the tasks:"
            print sorted(task_ids)
            print "on hosts:"
            print sorted(failed_hosts)
            sys.exit(-1)

        return

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
