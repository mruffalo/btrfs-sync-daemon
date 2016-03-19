#!/usr/bin/env python3
import socket
import ssl

from btrfs_incremental_send import PORT, deserialize_json, serialize_json

HOST = "localhost"

# Create a socket (SOCK_STREAM means a TCP socket)
sock_control = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
context.verify_mode = ssl.CERT_REQUIRED
context.check_hostname = False
context.load_verify_locations('ca.crt.pem')
context.load_cert_chain('client.crt.pem', keyfile='client.key.pem')

conn_control = context.wrap_socket(sock_control)

control_data = {'snapshot_count': 2}

try:
    # Connect to server and send data
    print('Connecting to server')
    conn_control.connect((HOST, PORT))
    conn_control.sendall(serialize_json(control_data))

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
            conn_data.connect((HOST, new_port))
            print('Sending data')
            conn_data.sendall(b'hello')
        finally:
            conn_data.close()

finally:
    conn_control.close()
