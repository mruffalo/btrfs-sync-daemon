#!/usr/bin/env python3
import io
from pathlib import Path
from pprint import pprint
from socket import socket, AF_INET, SOCK_STREAM
from socketserver import StreamRequestHandler
import ssl
from subprocess import PIPE, Popen

from btrfs_incremental_send import PORT, BTRFS_RECEIVE_COMMAND, deserialize_json, serialize_json
from ssl_socketserver import SSL_ThreadingTCPServer

# 16 MB seems okay
BUFFER_SIZE = 1 << 24

# TODO read this from config file
paths = {
    'isomorphic': Path(),
}

def get_common_name(cert):
    for field in cert['subject']:
        if field[0][0] == 'commonName':
            return field[0][1]

def bulk_copy(read_from: io.RawIOBase, write_to: io.RawIOBase):
    while True:
        chunk = read_from.read(BUFFER_SIZE)
        if not chunk:
            break
        write_to.write(chunk)

class BtrfsReceiveHandler(StreamRequestHandler):
    def handle(self):
        raw_data = self.rfile.readline()
        data = deserialize_json(raw_data)
        pprint(data)

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
        with open('test', 'wb') as f:
            proc = Popen(
                ['cat'],
                stdin=PIPE,
                stdout=f,
            )
            bulk_copy(conn, proc.stdin)
            proc.stdin.close()
            return_code = proc.wait()
        print('command returned {}'.format(return_code))
        conn.close()

        self.wfile.write(serialize_json({'result': return_code}))

SSL_ThreadingTCPServer(
    ('localhost', PORT),
    BtrfsReceiveHandler,
    'server.crt.pem',
    'server.key.pem',
    'ca.crt.pem'
).serve_forever()
