#!/usr/bin/python
# By Jussi Jousimo, jvj@iki.fi

import hpc_cluster
import sys

def main():
    cluster = hpc_cluster.CSCluster(True)

    cluster.get_nodes()
    #cluster.run_command("ps -C R")
    cluster.run_command("killall -s SIGKILL R")

    return

if __name__ == "__main__":
    main()
