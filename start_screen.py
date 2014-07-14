#!/usr/bin/python
# By Jussi Jousimo, jvj@iki.fi
#
# Start tmux on 5 hosts with the lowest loads:
# ./start_tmux.py 5

import optparse
import ukko_cluster

def main():
    parser = optparse.OptionParser("usage: %prog number_of_hosts")
    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error("number_of_hosts is missing")
    number_of_hosts = int(args[0])

    cluster = ukko_cluster.UkkoCluster(True)
    nodes = cluster.get_nodes()[:number_of_hosts]
    print nodes



    return

if __name__ == "__main__":
    main()
