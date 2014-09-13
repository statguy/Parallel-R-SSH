#!/usr/bin/python

import ukko_cluster
import subprocess
import time

cluster = ukko_cluster.UkkoCluster(True)
nodes = cluster.get_nodes(10)

running_commands = []

for node in nodes:
    command = "ssh " + node.host + " ls -l ~/.bashrc" 
    command_process = subprocess.Popen(command, shell=True)
    running_commands.append((node.host, command_process))

while running_commands:
    for host, command_process in running_commands:
        return_code = command_process.poll()
        if return_code is not None:
            running_commands.remove((host, command_process))
            print host + " done with return code " + str(return_code) + "."
        time.sleep(1)
