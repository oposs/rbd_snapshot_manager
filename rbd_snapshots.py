#!/usr/bin/python3
import datetime
import logging
import logging.handlers
import argparse
import os
import random
import subprocess
import json
import sys
from time import time, sleep

__version__ = "0.1.0"

"""
RBD Snapshot Manager

This script intends to fill the gap between a simple snapshot schedule and automated mirroring. It offers a simple
housekeeping feature by keeping a configurable number of snapshots. Moreover, its concurrent-ready(TM) by exclusively 
"locking" a pool/image combination (we take advantage of ceph's general purpose key/value store).

Usage example:

    # edit crontab for user root
    crontab -e
    
    # add daily snapshots and keep 10
    0 22 * * * /usr/bin/python3 /path/to/rbd_snapshot.py --pool rdb-pool1 --image test-image --suffix DAILY --n_keep 10
    
"""

logger = logging.getLogger('RBD Snapshot Manager')
syslog_handler = logging.handlers.SysLogHandler(address='/dev/log')
stdout_handler = logging.StreamHandler(stream=sys.stdout)

parser = argparse.ArgumentParser(
    description='Manages RDB snapshots on this machine. Intended to be used in conjunction with a cronjob',
    epilog='Copyright Oetiker+Partner AG Olten')
parser.add_argument('--pool', required=True, help="RBD pool name, find with `ceph osd dump | grep rbd`", type=str)
parser.add_argument('--image', required=True, help="RBD image name, find with `rbd ls <pool-name>`", type=str)
parser.add_argument('--suffix', required=True, help="Snapshot suffix", type=str)
parser.add_argument('--n_keep', required=True, help="How many snapshots with this suffix should we keep?",
                    type=int)
parser.add_argument('--dryrun', action='store_true', default=False,
                    help="Do not change anything, implicitly enables debug mode")
parser.add_argument('--debug', action='store_true', default=False, help="Be more verbose what we are doing")
parser.add_argument('--version', action='version', version=f"%(prog)s {__version__}")

args = parser.parse_args()

POOL = args.pool  # type: str
IMAGE = args.image  # type: str
SUFFIX = args.suffix  # type: str
N_KEEP = args.n_keep  # type: int
DRYRUN = args.dryrun  # type: bool
DEBUG = args.debug  # type: bool

if DEBUG or DRYRUN:
    logger.setLevel(logging.DEBUG)
    logger.addHandler(stdout_handler)
else:
    logger.setLevel(logging.INFO)
    logger.addHandler(syslog_handler)

_MONTHS = {
    'Jan': 1,
    'Feb': 2,
    'Mar': 3,
    'Apr': 4,
    'May': 5,
    'Jun': 6,
    'Jul': 7,
    'Aug': 8,
    'Sep': 9,
    'Oct': 10,
    'Nov': 11,
    'Dec': 12,
}
_RANDOM_SLEEP_RANGE = list(range(1, 20))
_SNAPSHOT_NAME = "{counter}_rbd_snap_manager_{suffix}"
_LOCK_NAME = f"rbd_snap_mgr/lock_{POOL}/{IMAGE}"
_LOCK_VALUE = f"{os.uname().nodename}@{time()}"

COMMANDS = {
    'list_pools': "ceph osd dump -f json",
    'list_snapshots': "rbd snap ls {pool_name}/{image_name}",
    'create_snapshot': "rbd snap create {pool_name}/{image_name}@{snap_name}",
    'remove_snapshot': "rbd snap rm {pool_name}/{image_name}@{snap_name}",
    'snapshot_rename': "rbd snap rename {pool_name}/{image_name}@{old_snap_name} {pool_name}/{image_name}@{new_snap_name}",
    'lock_create': "ceph config-key set {lock_name} {lock_owner}",
    'lock_exists': "ceph config-key exists {lock_name}",
    'lock_remove': "ceph config-key rm {lock_name}",
    'lock_get_value': "ceph config-key get {lock_name}"
}


def _parse_rbd_enabled_pools(raw_output: str) -> dict:
    results = {}
    raw_json = json.loads(raw_output)
    for pool in raw_json['pools']:
        if 'rbd' in pool['application_metadata']:
            results[pool['pool_name']] = pool['pool']
    return results


def _snapshot_ls_parser(raw_output: str) -> list:
    results = []
    for line in raw_output.splitlines():
        if line.startswith('SNAPID'):
            continue
        tokenized_line = line.split()
        if len(tokenized_line) > 0:
            if len(tokenized_line) > 9:
                snap_id, name, size, unit, protected, _, month, day, time, year = tokenized_line
            else:
                protected = 'no'
                snap_id, name, size, unit, _, month, day, time, year = tokenized_line

            hour, minute, second = time.split(':')
            results.append({
                'snap_id': snap_id,
                'name': name,
                'protected': True if protected == 'yes' else False,
                'created': datetime.datetime(month=_MONTHS[month],
                                             day=int(day),
                                             year=int(year),
                                             hour=int(hour),
                                             minute=int(minute),
                                             second=int(second))
            })
    return results


def run_command(cmd: str, die_on_error=True):
    """
    Uses python's subprocess module to run an arbitrary command
    :param cmd: Command line
    :param die_on_error: Should we die on error or is the error handled by the caller?
    :return: n-tuple of reference to stdout, stderr and return code
    """
    result = subprocess.run(cmd.split(), capture_output=True)
    if result.returncode != 0 and die_on_error:
        die_error(f"Command `{cmd}` failed error: {result.stderr.decode()}")
    return result.stdout, result.stderr, result.returncode


def list_snapshots() -> list:
    """
    Validates the pool name and lists snapshots matching SUFFIX. Calls snapshot_ls_parser()
    :return: A list of dicts, sorted by snapshot number in decreasing order
    """
    get_pools_cmd = COMMANDS['list_pools']

    stdout, _, _ = run_command(get_pools_cmd)
    rbd_pools = _parse_rbd_enabled_pools(stdout.decode())

    # validate pool name
    if POOL in rbd_pools:
        list_snapshots_cmd = COMMANDS['list_snapshots'].format(pool_name=POOL, image_name=IMAGE)
        stdout, _, _ = run_command(list_snapshots_cmd)
        snapshots = _snapshot_ls_parser(stdout.decode())
        interesting_snapshots = sorted(list(filter(lambda snap: snap['name'].endswith(SUFFIX), snapshots)),
                                       key=lambda snap: int(snap.get('name').split('_')[0]), reverse=True)
        if len(interesting_snapshots) > 0:
            return interesting_snapshots
        else:
            logger.debug(f"Found no snapshots matching {SUFFIX}")
            return []
    else:
        die_error(f"Pool `{POOL}` not found in cluster")


def _get_snapshot_name() -> str:
    return _SNAPSHOT_NAME.format(counter=0, suffix=SUFFIX)


def acquire_lock():
    """
    Uses ceph's general key/value store to "lock" the snapshotting process.
    """
    check_lock_cmd = COMMANDS['lock_exists'].format(lock_name=_LOCK_NAME)
    logger.debug(f"Check if `{_LOCK_NAME}`exists...")
    stdout, stderr, ret = run_command(check_lock_cmd, False)
    if ret != 0 and "doesn't" in stderr.decode():
        logger.debug(f"Lock `{_LOCK_NAME}` did not exist, creating...")
        create_lock_cmd = COMMANDS['lock_create'].format(lock_name=_LOCK_NAME, lock_owner=_LOCK_VALUE)
        run_command(create_lock_cmd)
        # check if we actually acquired the lock
        if not is_our_lock():
            die_error(f"Lock `{_LOCK_NAME}` Is not owned by us, exiting...")
    else:
        die_ok(f"Lock `{_LOCK_NAME}` present, exiting...")


def release_lock():
    """
    Removes an acquired lock (if it was created by us)
    """
    if is_our_lock():
        remove_lock_cmd = COMMANDS['lock_remove'].format(lock_name=_LOCK_NAME)
        run_command(remove_lock_cmd)
        logger.debug(f"Lock `{_LOCK_NAME}` removed")
    else:
        die_error(f"Lock `{_LOCK_NAME}` was not created by us. Not removed")


def die_ok(msg: str):
    logger.info(msg)
    exit(0)


def die_error(msg: str):
    logger.error(msg)
    exit(-1)


def is_our_lock() -> bool:
    """
    Checks if the lock is actually ours by comparing _LOCK_NAME with the value stored in _LOCK_NAME
    :return: True if they match and return code is 0
    """
    get_lock_value_cmd = COMMANDS['lock_get_value'].format(lock_name=_LOCK_NAME)
    stdout, _, ret = run_command(get_lock_value_cmd, die_on_error=False)
    lock_owner = stdout.decode().strip()
    return lock_owner == _LOCK_VALUE and ret == 0


def rename_snapshots(snapshot_list: list):
    """
    Iterates through a list of snapshots and renames each one by increasing the snapshot number by one
    :param snapshot_list: list of snapshots (obtained through list_snapshots() )
    """
    for snapshot in snapshot_list:
        s_name = snapshot['name']
        try:
            s_number = int(s_name.split('_')[0])
            new_name = _SNAPSHOT_NAME.format(counter=s_number + 1, suffix=SUFFIX)
            snapshot_rename_cmd = COMMANDS['snapshot_rename'].format(pool_name=POOL, image_name=IMAGE,
                                                                     old_snap_name=s_name, new_snap_name=new_name)
            logger.debug(f"Renaming snapshot {s_name} -> {new_name}")
            if not DRYRUN:
                run_command(snapshot_rename_cmd)
        except ValueError as ve:
            die_error(f"Could not parse snapshot name {s_name}, error: {ve}")


def create_snapshot():
    """
    Creates a new snapshot with number 0
    :return:
    """
    snapshot_name = _SNAPSHOT_NAME.format(counter=0, suffix=SUFFIX)
    create_snapshot_cmd = COMMANDS['create_snapshot'].format(pool_name=POOL, image_name=IMAGE,
                                                             snap_name=snapshot_name)
    if not DRYRUN:
        run_command(create_snapshot_cmd)
    logger.debug(f"Snapshot `{snapshot_name}` created ")


def cleanup():
    """
    Removes the snapshot with the highest number if the SUFFIX matches and the number snapshots
    with SUFFIX is > N_KEEP
    """
    updated_snapshot_list = list_snapshots()
    if len(updated_snapshot_list) < N_KEEP:
        die_ok(f"No snapshots to remove since we have less than {N_KEEP}")
    else:
        snapshot_to_remove = updated_snapshot_list[0]['name']
        if not DRYRUN:
            snapshot_rm_cmd = COMMANDS['remove_snapshot'].format(pool_name=POOL, image_name=IMAGE,
                                                                 snap_name=snapshot_to_remove)
            run_command(snapshot_rm_cmd)
        logger.debug(f"Snapshot `{snapshot_to_remove}` removed ")


if __name__ == '__main__':
    if DRYRUN:
        logger.debug("##### Running in dryrun mode!! #####")
    try:
        sleep_duration = random.choice(_RANDOM_SLEEP_RANGE)
        logger.debug(f"Sleeping for {sleep_duration} seconds")
        sleep(sleep_duration)
        snapshot_list = list_snapshots()
        acquire_lock()
        rename_snapshots(snapshot_list)
        create_snapshot()
        cleanup()
    except Exception as e:
        logger.error(f"An unrecoverable error happened: \n {e}")
    finally:
        release_lock()
