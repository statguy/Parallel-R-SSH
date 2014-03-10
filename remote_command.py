#!/usr/bin/python
# By Jussi Jousimo, jvj@iki.fi
#
# To kill all remote R processes:
# ./remote_command.py killall -s SIGKILL R

import optparse
import ukko_cluster

def main():
    parser = optparse.OptionParser("usage: %prog command")
    (options, args) = parser.parse_args()
    command = " ".join(args)

    cluster = ukko_cluster.UkkoCluster(True)
    cluster.get_nodes()
    cluster.run_command(command, timeout=600)

    return

if __name__ == "__main__":
    main()
