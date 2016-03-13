#!/usr/bin/env python3
from socketserver import StreamRequestHandler

from btrfs_incremental_send import PORT
from ssl_socketserver import SSL_ThreadingTCPServer

class TestHandler(StreamRequestHandler):
    def handle(self):
        data = self.rfile.readline()
        print('Read data: "{!r}"'.format(data))
        self.wfile.write(data.upper())

SSL_ThreadingTCPServer(
    ('localhost', PORT),
    TestHandler,
    'server.crt.pem',
    'server.key.pem',
    'ca.crt.pem'
).serve_forever()
