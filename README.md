# RBD Snapshot Manager

This script intends to fill the gap between a simple snapshot schedule and automated mirroring. It offers a simple
housekeeping feature by keeping a configurable number of snapshots. Moreover, its concurrent-ready(TM) by exclusively 
"locking" a pool/image combination (we take advantage of ceph's general purpose key/value store).

Usage example:

```text

# edit crontab for user root
crontab -e

# add daily snapshots and keep 10
0 22 * * * /usr/bin/python3 /path/to/rbd_snapshot.py --pool rdb-pool1 --image test-image --suffix DAILY --n_keep 10

```

