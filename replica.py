import socket
import threading
from typing import List

from configs import MESSAGE_SIZE
from key_values import EventualConsistency


class Replica:
    def __init__(self, id: int, host: str, port: int, key_value_type: str, replica_ports: List[int], waiting_time: int,
                 message_size: int = MESSAGE_SIZE):
        self.id = id
        self.host = host
        self.port = port
        self.replica_ports = replica_ports
        self.waiting_time = waiting_time
        self.message_size = message_size

        if key_value_type == 'Eventual':
            self.consistency = EventualConsistency(host=self.host, own_port=self.port, ports=self.replica_ports,
                                                   waiting_time=waiting_time, message_size=message_size)

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))

        self.running = True

        thread = threading.Thread(target=self.listen)
        thread.start()

    def process_message(self, message: str) -> str:
        command, info = message.split(' ', 1)
        if command == 'SET':
            key, value = info.split()
            response = self.consistency.set(key=key, value=value)
            return response
        elif command == 'GET':
            response = self.consistency.get(key=info)
            return response
        elif command == 'LOCAL-SET':
            key, value = info.split()
            response = self.consistency.local_set(key=key, value=value)
            return response

        return 'Command not found'

    def process_socket(self, message_socket: socket.socket) -> None:
        message = message_socket.recv(self.message_size).decode()
        response = self.process_message(message=message)
        message_socket.send(response.encode())

    def listen(self) -> None:
        self.socket.listen()
        while self.running:
            message_socket, message_address = self.socket.accept()
            if self.running:
                thread = threading.Thread(target=self.process_socket, args=(message_socket,))
                thread.start()

    def close(self):
        self.running = False
        self.consistency.close()
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((self.host, self.port))
        self.socket.close()
