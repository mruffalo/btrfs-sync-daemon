#!/usr/bin/env python3
from configparser import ConfigParser
from pathlib import Path
from socket import socket, AF_INET, SOCK_STREAM
from socketserver import StreamRequestHandler
import ssl
from subprocess import PIPE, Popen

from btrfs_incremental_send import BTRFS_RECEIVE_COMMAND, CONTROL_PORT, bulk_copy, serialize_json
from ssl_socketserver import SSL_ThreadingTCPServer

def get_common_name(cert):
    for field in cert['subject']:
        if field[0][0] == 'commonName':
            return field[0][1]

CONFIG_FILE_PATHS = [
    Path('/etc/btrfs-syncd/server.conf'),
]
def read_config():
    for path in CONFIG_FILE_PATHS:
        try:
            config = ConfigParser()
            config.read(str(path))
            return config
        except FileNotFoundError:
            pass
    message = 'No config files found, tried:{}'.format('\n'.join(CONFIG_FILE_PATHS))
    raise FileNotFoundError(message)

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
    # TODO move all of this
    paths = {}
    config = read_config()
    for key in config:
        if key.startswith('path/'):

    SSL_ThreadingTCPServer(
        ('localhost', CONTROL_PORT),
        BtrfsReceiveHandler,
        'server.crt.pem',
        'server.key.pem',
        'ca.crt.pem'
    ).serve_forever()
