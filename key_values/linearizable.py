import socket
import time
from collections import defaultdict
from queue import PriorityQueue
from typing import List

from configs import MESSAGE_SIZE


class LinearizableConsistency:
    def __init__(self, host: str, own_port: int, ports: List[int], waiting_time: int, message_size: int = MESSAGE_SIZE):
        self.host = host
        self.own_port = own_port
        self.ports = ports

        self.clock = 0

        self.storage = {}

        self.waiting_time = waiting_time
        self.message_size = message_size

        self.queue = PriorityQueue()
        self.ack_cnt = defaultdict(lambda: 0)
        self.stop = {}

        self.running = True

    def send(self, port: int, data) -> str:
        send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        send_socket.connect((self.host, port))
        send_socket.send(data.encode())
        response = send_socket.recv(self.message_size).decode()
        return response

    def totally_broadcast(self, data) -> str:
        for port in self.ports:
            time.sleep(self.waiting_time)
            self.send(port=port, data=data)
        while self.stop[data]:
            continue
        command, info = data.split(' ', 1)
        if command == 'LOCAL-SET':
            # key, value, clock = info.split()
            # self.storage[key] = value
            return 'SET command is done'
        elif command == 'LOCAL-GET':
            key, clock = info.split()
            if key in self.storage:
                return self.storage[key]
            return 'Not-found'
        return 'Command not found'

    def set(self, key: str, value: str) -> str:
        self.clock += 1
        data = f'LOCAL-SET {key} {value} {self.clock}'
        self.stop[data] = True
        response = self.totally_broadcast(data=data)
        return response

    def get(self, key: str) -> str:
        self.clock += 1
        data = f'LOCAL-GET {key} {self.clock}'
        response = self.totally_broadcast(data=data)
        return response

    def local_set(self, key: str, value: str, clock: str) -> str:
        clock = int(clock)
        self.clock = max(self.clock, clock) + 1
        self.queue.put((clock, (f'LOCAL-SET {key} {value} {clock}', key, value)))
        for port in self.ports:
            time.sleep(self.waiting_time)
            self.send(port=port, data=f'ACK LOCAL-SET {key} {value} {clock}')
        return 'LOCAL-SET is done'

    def local_get(self, key: str, clock: str) -> str:
        clock = int(clock)
        self.clock = max(self.clock, clock) + 1
        self.queue.put((clock, (f'LOCAL-GET {key} {clock}', key)))
        for port in self.ports:
            time.sleep(self.waiting_time)
            self.send(port=port, data=f'ACK LOCAL-GET {key} {clock}')
        return 'LOCAL-GET is done'

    def ack(self, data: str) -> str:
        clock = int(data.split()[-1])
        self.clock = max(self.clock, clock) + 1
        self.ack_cnt[data] += 1
        if self.ack_cnt[data] == len(self.ports) and self.top_queue()[1][0] == data:
            command, info = data.split(' ', 1)
            if command == 'LOCAL-SET':
                key, value, clock = info.split()
                self.storage[key] = value
            self.stop[data] = False
            self.queue.get()
        return 'ACK affected'

    def close(self):
        self.running = False

    def top_queue(self):
        top_element = self.queue.get()
        self.queue.put(top_element)
        return top_element
