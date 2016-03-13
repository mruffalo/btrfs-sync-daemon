import ssl
from socketserver import TCPServer, ThreadingMixIn

class SSL_TCPServer(TCPServer):
    def __init__(
            self,
            server_address,
            RequestHandlerClass,
            certfile,
            keyfile,
            ssl_version=ssl.PROTOCOL_TLSv1_2,
            bind_and_activate=True,
    ):
        super().__init__(server_address, RequestHandlerClass, bind_and_activate)
        self.certfile = certfile
        self.keyfile = keyfile
        self.ssl_version = ssl_version

    def get_request(self):
        newsocket, fromaddr = self.socket.accept()
        connstream = ssl.wrap_socket(
            newsocket,
            server_side=True,
            certfile=self.certfile,
            keyfile=self.keyfile,
            ssl_version=self.ssl_version,
        )
        return connstream, fromaddr

class SSL_ThreadingTCPServer(ThreadingMixIn, SSL_TCPServer):
    pass
