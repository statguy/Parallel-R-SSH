Independent parallel task execution with R on a HPC
===================================================

Runs independent parallel R sessions by executing remote script in a high
performance cluster via SSH. The script is currently configured for the
[Ukko](http://www.cs.helsinki.fi/en/compfac/high-performance-cluster-ukko) cluster.

Features
--------
* Task (session) id is appended as the last argument for R.
* Task ids can be specified flexibly.
* If the number-of-tasks > number-of-available-remote-nodes, tasks are queued and
nodes are recycled.
* A pool of available nodes is maintained and unresponsive/troublesome nodes are removed.
The pool can be configured to hold extra nodes for later use.
* Number of nodes in the pool can be limited.
* Nodes can be filtered by maximum allowed load and a blacklist of hosts.

Usage
-----
Runs `test.R` in the remote hosts with task ids 1,2,3 and 5 on two nodes with maximum load 10.0:
```bash
parallel_r.py -t 1:3,5 -n 2 -l 10.0 -b blacklist.txt -v test.R
```
The file `blacklist.txt` contains excluded hosts separated by new lines.
See `parallel_r.py --help` for more details.

```bash
kill_r.py
```
Kills all your R processes in all hosts.

Extending
---------
The Python class in `independent_parallel_tasks.py` can be extended for custom HP clusters,
see `ukko_cluster.py` for an example.

TODO
----
* Allow specifying remote username, port, nice parameter, etc.
* Allow running other programs than R.

Feedback
--------
Jussi Jousimo, jvj@iki.fi
