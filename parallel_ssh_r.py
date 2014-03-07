#!/usr/bin/python
# By Jussi Jousimo, jvj@iki.fi

import hpc_cluster
import optparse

def main():
    parser = optparse.OptionParser("usage: %prog options [options] batch_file [arguments]")

    parser.add_option("-n", "--number_of_tasks", metavar="TASKS", dest="n_tasks", type="int", help="number of tasks to execute")
    parser.add_option("-m", "--max_nodes", metavar="NODES", dest="max_nodes", default=2, type="int", help="maximum number nodes to allocate")
    parser.add_option("-l", "--max_load", metavar="LOAD", dest="max_load", default=10.0, type="float", help="maximum load allowed in nodes for allocation")
    parser.add_option("-f", "--log_dir", metavar="DIR", dest="log_file_dir", default="~/tmp", type="string", help="directory to write log files")
    parser.add_option("-b", "--blacklist", metavar="FILE", dest="blacklist_file", type="string", help="file of blacklisted nodes")
    parser.add_option("-v", "--verbose", dest="verbose", default=False, action="store_true", help="print verbose messages")

    (options, args) = parser.parse_args()        
    blacklist = []
    if options.blacklist_file is not None:
        blacklist = [line.strip() for line in open(options.blacklist_file, "r")]

    if len(args) == 0:
        parser.error("batch_file is missing")
    batch_file = args[0]
    arguments = ""
    if len(args) > 1:
      arguments = " ".join(args[1:len(args)])

    cluster = hpc_cluster.CSCluster(options.verbose)

    if options.n_tasks is None:
        print "Argument -n TASKS not specified, priting only available clusters:"
        cluster.get_nodes(options.max_nodes, options.max_load, blacklist)
        print cluster.nodes
    else:
        cluster.get_nodes(min(options.n_tasks, options.max_nodes), options.max_load, blacklist)
        cluster.run_tasks(options.n_tasks, batch_file, arguments, options.log_file_dir)

    return

if __name__ == "__main__":
    main()
