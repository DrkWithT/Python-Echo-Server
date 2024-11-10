"""
    protocol.py\n
    Defines constants and utils for an "echo" protocol.\n
    By: DrkWithT
"""

"""
    Samples:\n
    Echo-To: 0x0 <length> <msg bytes>\n
    Echo-Back: 0x1 <length> <msg bytes>\n
    Terminate: 0x2\n
    Error: 0x3 <length> <reason msg bytes>
"""

import socket

PROTO_NUM_SZ = 1
PROTO_MAX_DATA_SZ = 255

PROTO_OP_ECHO_TO = 0
PROTO_OP_ECHO_BACK = 1
PROTO_OP_TERM = 2
PROTO_OP_ERROR = 3

def read_num(sock: socket.socket):
    raw_data = sock.recv(PROTO_NUM_SZ)
    op = int(raw_data[0])

    return op

def read_str_blob(sock: socket.socket, length: int):
    if length < 1:
        return ''
    
    raw_data = sock.recv(length)

    return str(raw_data, encoding="ascii")

def write_msg(sock: socket.socket, msg: tuple[int, str | None]):
    opcode, payload = msg
    raw_payload = None

    if payload is not None:
        raw_payload = payload.encode(encoding="ascii")

    op_buf = bytes([opcode])

    sock.sendall(op_buf)

    if raw_payload is not None:
        sock.sendall(raw_payload)
