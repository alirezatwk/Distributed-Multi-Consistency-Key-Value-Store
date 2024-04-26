from configs import MESSAGE_SIZE
import socket
import threading


class Replica:
    def __init__(self, id: int, host: str, port: int, message_size: int = MESSAGE_SIZE):
        self.id = id
        self.host = host
        self.port = port

        self.message_size = message_size

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))

        self.running = True

    def listen(self):
        self.socket.listen()
        while self.running:
            message_socket, message_address = self.socket.accept()
            
            message = message_socket.recv(self.message_size).decode()

