#!/usr/bin/python
# By Jussi Jousimo, jvj@iki.fi
#
# To kill all remote R processes:
# ./remote_command.py "killall -s SIGKILL R"

import optparse
import ukko_cluster
import ukko_full_cluster

def main():
    parser = optparse.OptionParser("usage: %prog number_of_hosts")
    (options, args) = parser.parse_args()
    number_of_hosts = int(args[0])

    #cluster = ukko_cluster.UkkoCluster(True)
    cluster = ukko_full_cluster.UkkoFullCluster(True)
    cluster.get_nodes()
    cluster.ping(number_of_hosts)

    return

if __name__ == "__main__":
    main()
