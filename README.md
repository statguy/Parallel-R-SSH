Independent parallel task execution with R on a HPC
===================================================

Runs independent parallel R sessions by executing remote script in
[Ukko](http://www.cs.helsinki.fi/en/compfac/high-performance-cluster-ukko) cluster via SSH.
If the number-of-tasks > number-of-available-remote-nodes, tasks are queued and
nodes are recycled. Unresponsive nodes are dropped. Task id is appended as the last
argument for R.

Usage
-----
Runs test.R in the remote hosts with task ids 1,2,3 and 5 on two nodes with maximum load 10.0:
```bash
parallel_r.py -t 1:3,5 -n 2 -l 10.0 -b blacklist.txt -v test.R
```
The file `blacklist.txt` contains unavailable hosts separated by new lines.
See `parallel_r.py --help` for more details.

```bash
kill_r.py
```
Kills all your R processes in all hosts.

Extending
---------
The class in `hpc_cluster.py` can be extended for other HPC clusters, see
`ukko_cluster.py`.

TODO
----
* Allow specifying remote username, port etc.
* Allow running other programs than R.

Feedback
--------
Jussi Jousimo, jvj@iki.fi
