# By Jussi Jousimo, jvj@iki.fi
# Some code taken from Jukka Suomela, https://github.com/suomela/ukko

import independent_parallel_tasks
import re
import urllib2

class UkkoCluster(independent_parallel_tasks.Cluster):
    entry = re.compile(r'''
        ^(ukko[0-9]+\.hpc\.cs\.helsinki\.fi) \s+
        ([0-9]+) \s+  # slot
        (yes|no) \s+  # ping
        (yes|no) \s+  # ssh
        ([0-9]+) \s+  # users
        (?:([0-9]+\.[0-9]+)|-) \s+  # load
        (cs|rr|ok)(?:/t)? \s+  # stat
        (N/A|[0-9.-]+-(?:generic|server))? \s+  # kernel
        ([0-9.]+|--) \s*  # BIOS
        (.*?) \s* $
        ''', re.X)
    
    def __init__(self, verbose, url=None):
        super(self.__class__, self).__init__(verbose)
        self.url = url if url is not None else 'http://www.cs.helsinki.fi/ukko/hpc-report.txt'

    def get_remote_nodes(self):
        if (self.verbose):
            print "Retrieving remote nodes list from", self.url + "..."
        f = urllib2.urlopen(self.url)
        
        for l in f:
            m = self.entry.match(l)
            if m is not None:
                host, slot, ping, ssh, users, load, stat, kernel, bios, comment = m.groups()
                if stat != 'ok' and stat != 'cs':
                    continue
                if ping != 'yes' or ssh != 'yes' or load is None:
                    continue
                self.nodes.append(hpc_cluster.Node(host, load))
        return
