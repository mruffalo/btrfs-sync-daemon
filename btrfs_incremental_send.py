from collections import defaultdict
from datetime import datetime
import json
from pathlib import Path

SNAPSHOT_DATETIME_FORMAT = '%Y%m%d-%H%M%S%z'

BTRFS_SEND_COMMAND = [
    'btrfs',
    'send',
]
BTRFS_SEND_PARENT_ADDITION = [
    '-p',
    '{parent}',
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
NC_COMMAND = [
    'nc',
    '-q',
    '1',
    '{host}',
    '{port}',
]

PORT = 35104

def serialize_json(obj) -> bytes:
    return json.dumps(obj).encode('utf-8') + b'\n'

def deserialize_json(b: bytes):
    return json.loads(b.decode('utf-8'))

class Subvolume:
    __slots__ = ['all', 'base', 'extra', 'newest']

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

def search_snapshots(path: Path) -> dict:
    subvolumes_by_name = defaultdict(Subvolume)

    for entry in path.iterdir():
        name, timestamp = entry.name.split('@')
        subvolume = subvolumes_by_name[name]
        if entry.is_file():
            if entry.name.endswith('.keep'):
                subvolume.base = entry.stem
        else:
            subvolume.all.append(entry.name)

    for subvolume in subvolumes_by_name.values():
        subvolume.newest = max(subvolume.all, key=parse_datetime)
        subvolume.extra = set(subvolume.all) - {subvolume.newest}

    return subvolumes_by_name
