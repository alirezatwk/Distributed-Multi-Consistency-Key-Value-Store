import socket
from typing import List

from configs import MESSAGE_SIZE


class Client:
    def __init__(self, host: str, replica_ports: List[int], message_size: int = MESSAGE_SIZE):
        self.id = id
        self.host = host
        self.replica_ports = replica_ports
        self.message_size = message_size

    def send(self, replica_id: int, data: str) -> str:
        send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        send_socket.connect((self.host, self.replica_ports[replica_id]))
        send_socket.send(data.encode())
        response = send_socket.recv(self.message_size).decode()
        return response

    def set(self, replica_id: int, key: str, value: str) -> str:
        data = f'SET {key} {value}'
        response = self.send(replica_id=replica_id, data=data)
        return response

    def get(self, replica_id: int, key: str) -> str:
        data = f'GET {key}'
        response = self.send(replica_id=replica_id, data=data)
        return response

    def command_manager(self, replica_id: int, command: str, info: str) -> str:
        if command == 'SET':
            key, value = info.split()
            response = self.set(replica_id=replica_id, key=key, value=value)
            return response
        elif command == 'GET':
            response = self.get(replica_id=replica_id, key=info)
            return response
