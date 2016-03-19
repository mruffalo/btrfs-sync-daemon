import ssl
from socketserver import TCPServer, ThreadingMixIn

class SSL_TCPServer(TCPServer):
    def __init__(
            self,
            server_address,
            RequestHandlerClass,
            cert_file,
            key_file,
            ca_cert_file,
            ssl_version=ssl.PROTOCOL_TLSv1_2,
            bind_and_activate=True,
    ):
        super().__init__(server_address, RequestHandlerClass, bind_and_activate)
        self.cert_file = cert_file
        self.key_file = key_file
        self.ca_cert_file = ca_cert_file
        self.ssl_version = ssl_version
        self.client_cert = None

    def get_request(self):
        newsocket, fromaddr = self.socket.accept()
        connstream = ssl.wrap_socket(
            newsocket,
            server_side=True,
            certfile=self.cert_file,
            keyfile=self.key_file,
            ca_certs=self.ca_cert_file,
            ssl_version=self.ssl_version,
            cert_reqs=ssl.CERT_REQUIRED,
        )
        self.client_cert = connstream.getpeercert()
        return connstream, fromaddr

class SSL_ThreadingTCPServer(ThreadingMixIn, SSL_TCPServer):
    pass
