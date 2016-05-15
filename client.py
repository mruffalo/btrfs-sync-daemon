#!/usr/bin/env python3
from configparser import ConfigParser
from fnmatch import fnmatch
from functools import partial
import ipaddress
from os.path import ismount
from pathlib import Path
from pprint import pprint
import socket
from subprocess import Popen, check_call
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
from network_utils import fix_long_ipv6_netmask

netifaces_available = False
try:
    import netifaces
    netifaces_available = True
except ImportError:
    pass

MOUNT_COMMAND = [
    'mount',
    '{path}',
]
UMOUNT_COMMAND = [
    'umount',
    '{path}'
]

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

def mount_path_if_necessary(path: Path):
    if not ismount(str(path)):
        print('Mounting', path)
        mount_command = [piece.format(path=path) for piece in MOUNT_COMMAND]
        check_call(mount_command)

def umount_path(path: Path):
    print('Unmounting', path)
    command = [piece.format(path=path) for piece in UMOUNT_COMMAND]
    Popen(command).wait()

class BackupPrerequisiteFailed(Exception):
    pass

IP_ADDRESS_FAMILIES = frozenset({netifaces.AF_INET, netifaces.AF_INET6})

def check_should_backup_network(config):
    if 'network' not in config:
        # No network configuration. Allow backups.
        return
    if not netifaces_available:
        print("Can't query network status; `netifaces` package not available")
        return

    if 'required interface' in config['network']:
        # This config key can be a fnmatch pattern, so check all interfaces
        # matched by the required interface name. Allow backups if any of them
        # have an IP address.
        any_matching_interface_has_ip = False
        interface_ip_addresses = []
        matching_interfaces = filter(
            partial(fnmatch, pat=config['network']['required interface']),
            netifaces.interfaces(),
        )
        for interface in matching_interfaces:
            interface_addresses = netifaces.ifaddresses(interface)
            interface_address_families = set(interface_addresses) & IP_ADDRESS_FAMILIES
            for address_family in interface_address_families:
                any_matching_interface_has_ip = True
                address_data = interface_addresses[address_family]
                # netifaces returns link-local IPv6 addresses of the form
                #   address%interface_name
                addr = address_data['addr'].split('%')[0]
                netmask = fix_long_ipv6_netmask(address_data['netmask'])
                interface_str = '{}/{}'.format(addr, netmask)
                interface_ip_addresses.append(ipaddress.ip_interface(interface_str))

        if 'require same subnet' in config['network']:
            backup_dest_ip = ipaddress.ip_address(socket.gethostbyname(config['server']['host']))
            if not any(backup_dest_ip in ip.network for ip in interface_ip_addresses):
                raise BackupPrerequisiteFailed(
                    'No matching interface has IP in same network as backup destination'
                )

        if not any_matching_interface_has_ip:
            raise BackupPrerequisiteFailed('No matching network interface is active')

def check_should_backup_power(config):
    if 'power' not in config:
        # No power config. Allow backups.
        return
    if not config['power'].getboolean('require ac power'):
        # AC power not required. Allow backups.
        return
    ac_online_path = '/sys/class/power_supply/AC/online'
    with open(ac_online_path) as f:
        ac_online = int(f.read().strip())
        if not ac_online:
            raise BackupPrerequisiteFailed('On battery power')

def check_should_backup(config):
    check_should_backup_network(config)
    check_should_backup_power(config)

if __name__ == '__main__':
    config, backup_paths, key_paths = parse_config()

    try:
        check_should_backup(config)
    except BackupPrerequisiteFailed as e:
        print('Not backing up, for reason:')
        print(e.args[0])

    for bp in backup_paths.values():
        if bp.automount:
            mount_path_if_necessary(bp.mount_path)

        try:
            for name, snapshot in search_snapshots(bp.path).items():
                if snapshot.newest == snapshot.base:
                    message = "Most recent snapshot for '{}' ({}) already on remote system".format(
                        name,
                        snapshot.newest,
                    )
                    print(message)
                else:
                    message = (
                        "Need to backup subvolume {} (base snapshot: {}, most recent: {})"
                    ).format(
                        name,
                        snapshot.base,
                        snapshot.newest,
                    )
                    print(message)
                    backup_snapshot(snapshot, config['server']['host'], key_paths)

        finally:
            if bp.automount:
                umount_path(bp.mount_path)
