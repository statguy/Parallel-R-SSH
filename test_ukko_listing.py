#!/usr/bin/python
# By Jussi Jousimo, jvj@iki.fi

import ukko_cluster

def main():
    cluster = ukko_cluster.UkkoCluster(True)
    cluster.get_nodes()
    for node in cluster.nodes:
        print node

    return

if __name__ == "__main__":
    main()
