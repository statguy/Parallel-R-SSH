# By Jussi Jousimo, jvj@iki.fi

import independent_parallel_tasks

class UkkoFullCluster(independent_parallel_tasks.Cluster):
    def __init__(self, verbose):
        super(self.__class__, self).__init__(verbose)

    def get_remote_nodes(self):
        for i in range(1, 240):
            host = "ukko" + str(i).zfill(3) + ".hpc.cs.helsinki.fi"
            self.nodes.append(independent_parallel_tasks.Node(host, 0.0))
        return
