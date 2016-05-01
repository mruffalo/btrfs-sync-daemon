#!/usr/bin/env python3
from pathlib import Path
from pprint import pprint
import socket
import ssl

from btrfs_incremental_send import (
    CONTROL_PORT,
    Subvolume,
    deserialize_json,
    prune_old_snapshots,
    search_snapshots,
    send_snapshot,
)

def backup_snapshot(snapshot: Subvolume, host: str):
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
    context.load_verify_locations('ca.crt.pem')
    context.load_cert_chain('client.crt.pem', keyfile='client.key.pem')

    conn_control = context.wrap_socket(sock_control)

    try:
        # Connect to server and send data
        print('Connecting to server')
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

                send_result = deserialize_json(conn_control.recv(1024))
                if send_result['success']:
                    print('Snapshot sent successfully; cleaning up old ones')
                    prune_old_snapshots(snapshot)
            finally:
                conn_data.close()

        else:
            print('Server returned failure')
            pprint(received)

    finally:
        conn_control.close()

if __name__ == '__main__':
    for snapshot in search_snapshots(Path('/mnt/btrfs/test/snapshots')).values():
        backup_snapshot(snapshot, 'localhost')
