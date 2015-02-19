#!/usr/bin/python
# By Jussi Jousimo, jvj@iki.fi

import ukko_cluster
import optparse
import sys

def main():
    parser = optparse.OptionParser("usage: %prog options [options] batch_file [arguments]")

    parser.add_option("-t", "--task_ids", metavar="TASKS", dest="task_items_str", type="string", help="list of task ids to execute, e.g. 1:10,14,20")
    parser.add_option("-n", "--max_nodes", metavar="NODES", dest="max_nodes", default=2, type="int", help="maximum number nodes to allocate")
    parser.add_option("-l", "--max_load", metavar="LOAD", dest="max_load", default=10.0, type="float", help="maximum load in nodes for allocation")
    parser.add_option("-m", "--min_mem", metavar="MEMORY", dest="min_freeMB", default=0, type="int", help="minimum free memory in nodes for allocation")
    parser.add_option("-f", "--log_dir", metavar="DIR", dest="log_file_dir", default="~/tmp", type="string", help="directory to write log files")
    parser.add_option("-b", "--blacklist", metavar="FILE", dest="blacklist_file", type="string", help="file of blacklisted nodes")
    parser.add_option("-p", "--priority", metavar="PRIORITY", dest="priority", default=10, type="int", help="nice priority")    
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

    cluster = ukko_cluster.UkkoCluster(options.verbose)

    if options.task_items_str is None:
        print "Argument -t TASKS not specified, priting only available clusters:"
        cluster.get_nodes(options.max_nodes, options.max_load, options.min_freeMB, blacklist)
        print cluster.nodes
    else:
        task_items = options.task_items_str.split(",")
        task_ids = []
        for task_item in task_items:
            if task_item.find(":") >= 0:
                from_to = task_item.split(":")
                task_ids.extend(range(int(from_to[0]), int(from_to[1]) + 1))
            else:
                task_ids.append(int(task_item))

        if len(set(task_ids)) < len(task_ids):
            print "Some task ids occur multiple times."
            sys.exit(-1)
        
        cluster.get_nodes(options.max_nodes, options.max_load, options.min_freeMB, blacklist)
        cluster.run_tasks(task_ids, batch_file, arguments, options.log_file_dir, options.priority)

    return

if __name__ == "__main__":
    main()
