import socket
import threading
import time
from queue import Queue
from typing import List

from configs import MESSAGE_SIZE


class EventualConsistency:
    def __init__(self, host: str, own_port: int, ports: List[int], waiting_time: int, message_size: int = MESSAGE_SIZE):
        self.host = host
        self.own_port = own_port
        self.ports = ports

        self.storage = {}

        self.waiting_time = waiting_time
        self.message_size = message_size

        self.queue = Queue()
        self.running = True

        thread = threading.Thread(target=self.sender)
        thread.start()

    def send(self, port: int, data) -> str:
        send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        send_socket.connect((self.host, port))
        send_socket.send(data.encode())
        response = send_socket.recv(self.message_size).decode()
        return response

    def sender(self):
        while self.running:
            time.sleep(self.waiting_time)
            if not self.queue.empty() and self.running:
                port, data = self.queue.get()
                self.send(port=port, data=data)

    def set(self, key: str, value: str) -> str:
        self.storage[key] = value
        for port in self.ports:
            if port == self.own_port:
                continue
            data = f'LOCAL-SET {key} {value}'
            self.queue.put((port, data))
        return 'SET command is done'

    def get(self, key: str) -> str:
        if key in self.storage:
            return self.storage[key]
        return 'Not-found'

    def local_set(self, key: str, value: str) -> str:
        self.storage[key] = value
        return 'LOCAL-SET is done'

    def local_get(self, key: str, value: str) -> str:
        pass

    def ack(self, data: str) -> str:
        pass

    def close(self) -> None:
        self.running = False
