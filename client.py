#!/usr/bin/env python3
from configparser import ConfigParser
from pathlib import Path
from pprint import pprint
import socket
import ssl
from typing import Mapping

from btrfs_incremental_send import (
    CONTROL_PORT,
    PATH_CONFIG_KEY_PATTERN,
    Subvolume,
    deserialize_json,
    prune_old_snapshots,
    search_snapshots,
    send_snapshot,
)

class BackupPath:
    __slots__ = ['name', 'path', 'automount', 'mount_path']

    def __init__(self, name, path, automount, mount_path):
        self.name = name
        self.path = path
        self.automount = automount
        self.mount_path = mount_path

CONFIG_FILE_PATH = Path('/etc/btrfs-syncd/client.conf')
def parse_config():
    # TODO unify this with server.parse_config, or at least don't duplicate everything
    config = ConfigParser()
    config.read(str(CONFIG_FILE_PATH))

    paths = {}
    for key in config:
        m = PATH_CONFIG_KEY_PATTERN.match(key)
        if m:
            name = m.group(1)
            path = Path(config[key]['path'])
            automount = False
            mount_path = None
            if 'automount' in config[key]:
                automount = config[key].getboolean('automount')
                mount_path = config[key]['mount path']

            bp = BackupPath(name, path, automount, mount_path)
            paths[name] = bp

    if 'key_dir' in config['keys']:
        key_dir = CONFIG_FILE_PATH.parent / config['keys']['key_dir']
    else:
        key_dir = CONFIG_FILE_PATH.parent
    key_paths = {}
    for k in ['ca_cert', 'client_cert', 'client_key']:
        key_paths[k] = key_dir / config['keys'][k]

    return config, paths, key_paths

def backup_snapshot(snapshot: Subvolume, host: str, key_paths: Mapping[str, Path]):
    """
    Connect to the sync daemon on the remote server, and then call the
    btrfs-specific functionality in this code to:
     * actually send the snapshot to the remote server
     * clean up previous snapshots
     * mark the most recent snapshot as the next base

    :param host: Hostname to connect to
    :param snapshot:
    :return:
    """
    sock_control = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = False
    context.load_verify_locations(str(key_paths['ca_cert']))
    context.load_cert_chain(
        str(key_paths['client_cert']),
        keyfile=str(key_paths['client_key']),
    )

    conn_control = context.wrap_socket(sock_control)

    try:
        # Connect to server and send data
        print('Connecting to server', host, 'port', CONTROL_PORT)
        conn_control.connect((host, CONTROL_PORT))

        # Receive data from the server and shut down
        received = deserialize_json(conn_control.recv(1024))

        if received['success']:
            print('Server returned success')
            new_port = received['new_port']
            print('New port:', new_port)
            sock_data = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn_data = context.wrap_socket(sock_data)
            try:
                print('Connecting to new port')
                conn_data.connect((host, new_port))
                print('Sending data')
                send_snapshot(conn_data, snapshot)

            finally:
                conn_data.close()

            send_result = deserialize_json(conn_control.recv(1024))
            if send_result['success']:
                print('Snapshot sent successfully; cleaning up old ones')
                prune_old_snapshots(snapshot)

        else:
            print('Server returned failure')
            pprint(received)

    finally:
        conn_control.close()

if __name__ == '__main__':
    config, paths, key_paths = parse_config()

    for path in paths.values():
        for name, snapshot in search_snapshots(path.path).items():
            if snapshot.newest == snapshot.base:
                message = "Most recent snapshot for '{}' ({}) already on remote system".format(
                    name,
                    snapshot.newest,
                )
                print(message)
            else:
                message = "Need to backup subvolume {} (base snapshot: {}, most recent: {})".format(
                    name,
                    snapshot.base,
                    snapshot.newest,
                )
                print(message)
                backup_snapshot(snapshot, config['server']['host'], key_paths)
