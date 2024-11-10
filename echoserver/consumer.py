"""
    consumer.py\n
    Contains a runnable class that handles an accepted connection task from the producer. After consuming its single task, it must signal its finishing for the queue usage.\n
    By: DrkWithT
"""

import random
import time
import queue
import socket

import echoserver.protocol as proto

MIN_TASK_WAIT = 2.125
MAX_TASK_WAIT = 4.125

STATE_BEGIN = 0
STATE_GET_TASK = 1
STATE_WAIT_FOR_TASK = 2
STATE_READ_REQUEST = 3
STATE_HANDLE_MSG = 4
STATE_RST = 5
STATE_HALT = 6

class Consumer:
    def __init__(self, rank: int) -> None:
        self.rank = rank
        self.local_socket = None
        self.state = STATE_BEGIN
        self.temp_msg = None
    
    def check_can_stop(self) -> bool:
        return self.state == STATE_HALT

    def generate_rand_delay(self) -> float:
        return random.random() * (MAX_TASK_WAIT - MIN_TASK_WAIT) + MIN_TASK_WAIT

    def handle_get_task(self, tasks: queue.Queue[tuple[socket.socket, ]]) -> int:
        if tasks.qsize() == 0:
            return STATE_WAIT_FOR_TASK
        
        task_sock, task_addr = tasks.get()
        tasks.task_done()

        if task_sock is None and task_addr is None:
            return STATE_HALT

        self.local_socket = task_sock
        print(f"Now handling peer from {task_addr}\n")

        return STATE_READ_REQUEST
    
    def handle_read_request(self) -> tuple[int, tuple[int, str | None]]:
        temp_op = proto.read_num(self.local_socket)
        temp_msg_len = proto.read_num(self.local_socket)
        temp_msg = None

        if temp_op != proto.PROTO_OP_TERM:
            temp_msg = proto.read_str_blob(self.local_socket, temp_msg_len)
        
        return (STATE_HANDLE_MSG, (temp_op, temp_msg))

    def handle_handle_msg(self) -> int:
        op, payload = self.temp_msg

        if op == proto.PROTO_OP_ECHO_TO:
            proto.write_msg(self.local_socket, (proto.PROTO_OP_ECHO_BACK, payload))
            return STATE_READ_REQUEST
        elif op == proto.PROTO_OP_TERM:
            return STATE_RST
        else:
            proto.write_msg(self.local_socket, (proto.OP_ERROR, "Invalid opcode!"))
            return STATE_RST

    def run(self, tasks: queue.Queue[tuple[socket.socket]]) -> None:
        while not self.check_can_stop():
            try:
                if self.state == STATE_BEGIN:
                    self.state = STATE_GET_TASK
                elif self.state == STATE_GET_TASK:
                    self.state = self.handle_get_task(tasks)
                elif self.state == STATE_WAIT_FOR_TASK:
                    time.sleep(self.generate_rand_delay())
                    self.state = STATE_GET_TASK
                elif self.state == STATE_READ_REQUEST:
                    next_state, request_msg = self.handle_read_request()
                    self.temp_msg = request_msg
                    self.state = next_state
                elif self.state == STATE_HANDLE_MSG:
                    self.state = self.handle_handle_msg()
                elif self.state == STATE_RST:
                    if self.local_socket is not None:
                        self.local_socket.close()
                        self.local_socket = None

                    self.state = STATE_GET_TASK
                else:
                    break
            except Exception as err:
                print(f"[Worker {self.rank}] I/O error: {err}")
                self.state = STATE_RST
        
        if self.local_socket is not None:
            self.local_socket.close()
        
        print(f"Worker thread {self.rank} is done...")
