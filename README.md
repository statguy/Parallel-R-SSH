Parallel R SSH
==============

Runs multiple R sessions by executing remote script in Ukko cluster via SSH.
If the number of tasks > number of available remote nodes, tasks are queued and
nodes are recycled. Run example:
```bash
parallel_ssh_r.py -n 10 -m 5 -l 10.0 -b blacklist.txt -v test.R
```
Optional `blacklist.txt` contains unavailable hosts separated by new lines.
Task number is appended as the last argument for R.

Contact
-------
Jussi Jousimo, jvj@iki.fi
