#!/usr/bin/env python3
import socket
import ssl
import sys

from btrfs_incremental_send import PORT

HOST = "localhost"
data = " ".join(sys.argv[1:])

# Create a socket (SOCK_STREAM means a TCP socket)
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
context.verify_mode = ssl.CERT_REQUIRED
context.check_hostname = False
context.load_verify_locations('ca.crt.pem')
context.load_cert_chain('client.crt.pem', keyfile='client.key.pem')

conn = context.wrap_socket(sock)

try:
    # Connect to server and send data
    conn.connect((HOST, PORT))
    conn.sendall((data + "\n").encode())

    # Receive data from the server and shut down
    received = conn.recv(1024).decode()
finally:
    conn.close()

print("Sent:     {}".format(data))
print("Received: {}".format(received))
