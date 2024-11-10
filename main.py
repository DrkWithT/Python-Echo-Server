"""
    main.py\n
    Contains main echo server code.\n
    By: DrkWithT
"""

import sys
import time
import atexit
import socket
import queue
import threading

import echoserver.protocol as proto
import echoserver.consumer as cons

DEFAULT_PORT = 8080
DEFAULT_BACKLOG = 4
DEFAULT_WORKER_N = 2

class EchoServer:
    def __init__(self, config: tuple[int, int]) -> None:
        temp_port, temp_backlog = config

        self.is_listening = True
        self.host_info = socket.gethostbyname(socket.gethostname())
        self.port = temp_port
        self.backlog = temp_backlog
        self.tasks = queue.Queue()

        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listen_socket.bind((self.host_info, self.port))
        self.listen_socket.listen(self.backlog)

        self.workers = [
            cons.Consumer(0), cons.Consumer(1)
        ]
        self.worker_thrds = [
            threading.Thread(target=self.workers[0].run, args=(self.tasks, )),
            threading.Thread(target=self.workers[1].run, args=(self.tasks, ))
        ]

    def request_stop_service(self):
        self.is_listening = False
        self.listen_socket.close()

        time.sleep(0.25)

        print("Halting workers...")
        for i in range(len(self.worker_thrds)):
            self.tasks.put((None, None))

        print("Waiting for workers to stop...")
        for joining_thrd in self.worker_thrds:
            joining_thrd.join()
    
    def run_service(self) -> None:
        for w_thrd in self.worker_thrds:
            w_thrd.start()

        while self.is_listening:
            try:
                peer_sock, peer_addr = self.listen_socket.accept()

                self.tasks.put((peer_sock, peer_addr), timeout=5.0)
            except Exception as io_err:
                print(f"[Producer] Detected possible connection error, {io_err}\n")

class EchoClient:
    def __init__(self, target_port: int):
        self.running = True
        self.temp_addr = socket.gethostbyname(socket.gethostname())
        self.port = target_port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def stop(self) -> None:
        self.running = False
        self.socket.close()

    def write_request_msg(self, op: int, msg: str | None) -> bool:
        if msg is None:
            return False
        
        msg_len = len(msg.encode(encoding="ascii"))

        if op != proto.PROTO_OP_TERM:
            if msg_len < 1 or msg_len > proto.PROTO_MAX_DATA_SZ:
                return False

            proto.write_msg(self.socket, (op, msg))
        else:
            proto.write_msg(self.socket, (op, None))
        return True

    def read_reply_msg(self) -> tuple[int, str | None]:
        reply_op = proto.read_num(self.socket)
        reply_msg_sz = proto.read_num(self.socket)
        reply_payload = None

        if reply_op == proto.PROTO_OP_ECHO_BACK or reply_op == proto.PROTO_OP_ERROR:
            reply_payload = proto.read_str_blob(self.socket, reply_msg_sz)
        
        return (reply_op, reply_payload)

    def run_self(self):
        self.socket.connect((self.temp_addr, self.port))

        try:
            while self.running:
                your_text = f"  {input("> ")}"

                cmd_op = None

                if your_text.strip() != "quit":
                    cmd_op = proto.PROTO_OP_ECHO_TO
                else:
                    cmd_op = proto.PROTO_OP_TERM
                    self.socket.close()
                    break;

                if not self.write_request_msg(cmd_op, your_text):
                    print("Invalid message length!")
                
                reply_op, reply_msg = self.read_reply_msg()

                if reply_op == proto.PROTO_OP_ECHO_BACK:
                    print(f"Echo: {reply_msg}")
                elif reply_op == proto.PROTO_OP_ERROR:
                    print(f"Error: {reply_msg}")
                else:
                    pass
        except Exception as io_err:
            print(f'{io_err}')

        print("Stopping this client...")

def show_usage():
    print("python3 main.py <role: str>[server | client] <host_or_target_port: int>")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        show_usage()
        exit(1)

    role_arg = sys.argv[1]
    port_arg = int(sys.argv[2])

    my_server = None
    my_client = None

    def on_sigint():
        print("Requesting application stop...")

        if my_server is not None:
            my_server.request_stop_service()
        elif my_client is not None:
            my_client.stop()
            pass

    atexit.register(on_sigint)

    if role_arg == "server":
        my_server = EchoServer((DEFAULT_PORT, DEFAULT_BACKLOG))
        my_server.run_service()
    elif role_arg == "client":
        my_client = EchoClient(port_arg)
        my_client.run_self()
