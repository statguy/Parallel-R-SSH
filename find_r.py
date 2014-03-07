#!/usr/bin/python
# By Jussi Jousimo, jvj@iki.fi

import hpc_cluster

def main():
    cluster = hpc_cluster.CSCluster(True)

    cluster.get_nodes()
    cluster.run_command("ps -C R")

    return

if __name__ == "__main__":
    main()
