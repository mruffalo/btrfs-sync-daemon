from collections import defaultdict
from datetime import datetime
import io
import json
from pathlib import Path
import re
from subprocess import PIPE, Popen, check_call
from typing import Mapping

KEEP_FILE_EXTENSION = '.keep'

SNAPSHOT_DATETIME_FORMAT = '%Y%m%d-%H%M%S%z'

BTRFS_SEND_COMMAND = [
    'btrfs',
    'send',
]
BTRFS_SEND_PARENT_ADDITION = [
    '-p',
    '{parent}',
]
BTRFS_DELETE_COMMAND = [
    'btrfs',
    'subvolume',
    'delete',
]
BTRFS_RECEIVE_COMMAND = [
    'btrfs',
    'receive',
    '{path}',
]
PV_COMMAND = [
    'pv',
    '-brt'
]

CONTROL_PORT = 35104

# 16 MB seems okay
BUFFER_SIZE = 1 << 24

PATH_CONFIG_KEY_PATTERN = re.compile(r'path/(.+)')

def bulk_copy(read_from: io.RawIOBase, write_to: io.RawIOBase):
    while True:
        chunk = read_from.read(BUFFER_SIZE)
        if not chunk:
            break
        write_to.write(chunk)

def serialize_json(obj) -> bytes:
    return json.dumps(obj).encode('utf-8') + b'\n'

def deserialize_json(b: bytes):
    return json.loads(b.decode('utf-8'))

class Subvolume:
    __slots__ = ['all', 'base', 'extra', 'newest', 'cwd']

    def __init__(self):
        # All snapshots of this subvolume
        self.all = []
        # The snapshot with a '.keep' file, used as the '-p' argument to
        # `btrfs send`
        self.base = None
        # Snapshots that are safe to delete after the newest one is
        # sent elsewhere
        self.extra = None
        # Newest snapshot
        self.newest = None

def parse_datetime(snapshot_name: str) -> datetime:
    name, timestamp = snapshot_name.split('@')
    return datetime.strptime(timestamp, SNAPSHOT_DATETIME_FORMAT)

def search_snapshots(path: Path) -> Mapping[str, Subvolume]:
    subvolumes_by_name = defaultdict(Subvolume)

    for entry in path.iterdir():
        name, timestamp = entry.name.split('@')
        subvolume = subvolumes_by_name[name]
        if entry.is_file():
            if entry.name.endswith('.keep'):
                # TODO figure out how important this is
                if subvolume.base is not None:
                    raise ValueError('Multiple base snapshots')
                subvolume.base = entry.stem
        else:
            subvolume.all.append(entry.name)

    # Found all snapshots; assign some bookkeeping data to each
    for subvolume in subvolumes_by_name.values():
        subvolume.cwd = path
        subvolume.newest = max(subvolume.all, key=parse_datetime)
        subvolume.extra = set(subvolume.all) - {subvolume.newest}

    return subvolumes_by_name

def send_snapshot(socket: io.RawIOBase, snapshot: Subvolume):
    command = BTRFS_SEND_COMMAND[:]
    if snapshot.base is not None:
        command.extend(
            [
                piece.format(parent=snapshot.base)
                for piece in BTRFS_SEND_PARENT_ADDITION
            ]
        )
    command.append(snapshot.newest)
    print('Running', ' '.join(command))
    btrfs_proc = Popen(command, stdout=PIPE, cwd=str(snapshot.cwd))
    pv_proc = Popen(PV_COMMAND, stdin=btrfs_proc.stdout, stdout=PIPE)
    bulk_copy(pv_proc.stdout, socket)
    # TODO see if this is necessary
    btrfs_proc.stdout.close()
    return_code = btrfs_proc.wait()
    print('Command returned {}'.format(return_code))
    pv_proc.wait()

def prune_old_snapshots(snapshot: Subvolume):
    if snapshot.extra:
        command = BTRFS_DELETE_COMMAND[:]
        command.extend(snapshot.extra)
        print('Running', ' '.join(command))
        check_call(command, cwd=str(snapshot.cwd))
        if snapshot.base is not None:
            old_keep_file = snapshot.cwd / (snapshot.base + KEEP_FILE_EXTENSION)
            old_keep_file.unlink()
        new_keep_file = snapshot.cwd / (snapshot.newest + KEEP_FILE_EXTENSION)
        with new_keep_file.open('w'):
            pass
    else:
        print('Nothing to delete for subvolume', snapshot.base)
