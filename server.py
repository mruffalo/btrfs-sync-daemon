#!/usr/bin/env python3
from configparser import ConfigParser
from pathlib import Path
import re
from socket import socket, AF_INET, SOCK_STREAM
from socketserver import StreamRequestHandler
import ssl
from subprocess import PIPE, Popen

from btrfs_incremental_send import BTRFS_RECEIVE_COMMAND, CONTROL_PORT, bulk_copy, serialize_json
from ssl_socketserver import SSL_ThreadingTCPServer

PATH_PATTERN = re.compile(r'path/(.+)')

def get_common_name(cert):
    for field in cert['subject']:
        if field[0][0] == 'commonName':
            return field[0][1]

CONFIG_FILE_PATH = Path('/etc/btrfs-syncd/server.conf')
def parse_config():
    config = ConfigParser()
    config.read(str(CONFIG_FILE_PATH))

    paths = {}
    for key in config:
        m = PATH_PATTERN.match(key)
        if m:
            name = m.group(1)
            paths[name] = Path(config[key]['path'])

    if 'key_dir' in config['keys']:
        key_dir = CONFIG_FILE_PATH.parent / config['keys']['key_dir']
    else:
        key_dir = CONFIG_FILE_PATH.parent
    key_paths = {}
    for k in ['ca_cert', 'server_cert', 'server_key']:
        key_paths[k] = key_dir / config['keys'][k]

    return config, paths, key_paths

def get_handler_class(paths):
    class BtrfsReceiveHandler(StreamRequestHandler):
        def handle(self):
            cn = get_common_name(self.server.client_cert)
            if cn not in paths:
                self.wfile.write(serialize_json({'success': False, 'reason': 'bad_hostname'}))
            path = paths[cn]
            print('Path:', path)

            s = socket(AF_INET, SOCK_STREAM)
            s.bind(('', 0))

            s = ssl.wrap_socket(
                s,
                server_side=True,
                certfile=self.server.cert_file,
                keyfile=self.server.key_file,
                ca_certs=self.server.ca_cert_file,
                ssl_version=self.server.ssl_version,
                cert_reqs=ssl.CERT_REQUIRED,
            )

            new_addr, new_port = s.getsockname()
            print('bound new socket to {}:{}'.format(new_addr, new_port))
            intermediate_data = {
                'success': True,
                'new_port': new_port,
            }
            self.wfile.write(serialize_json(intermediate_data))
            s.listen()
            conn, remote_addr = s.accept()
            print('accepted connection from {}:{}'.format(*remote_addr))
            command = [
                piece.format(path=path)
                for piece in BTRFS_RECEIVE_COMMAND
            ]

            proc = Popen(
                command,
                stdin=PIPE,
                cwd=str(path),
            )
            bulk_copy(conn, proc.stdin)
            proc.stdin.close()
            return_code = proc.wait()

            print('command returned {}'.format(return_code))
            conn.close()

            data = {
                'return_code': return_code,
                'success': not return_code,
            }
            self.wfile.write(serialize_json(data))

    return BtrfsReceiveHandler

if __name__ == '__main__':
    config, paths, key_paths = parse_config()

    SSL_ThreadingTCPServer(
        ('0.0.0.0', CONTROL_PORT),
        get_handler_class(paths),
        str(key_paths['server_cert']),
        str(key_paths['server_key']),
        str(key_paths['ca_cert']),
    ).serve_forever()
