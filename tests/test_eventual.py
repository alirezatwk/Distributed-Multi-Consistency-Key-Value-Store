import time

from client import Client
from configs import HOST, WAITING_TIME
from replica import Replica


def test_not_found():
    start_port = 9921
    client_num = 2
    replica_num = 3

    replica_ports = [start_port + ind for ind in range(replica_num)]
    replicas = [Replica(
        id=ind,
        host=HOST,
        port=start_port + ind,
        key_value_type='Eventual',
        replica_ports=replica_ports,
        waiting_time=WAITING_TIME,
    ) for ind in range(replica_num)]

    clients = [Client(host=HOST, replica_ports=replica_ports) for _ in range(client_num)]

    clients[0].set(replica_id=0, key='a', value='7')
    time.sleep(1)
    for client in clients:
        for replica_id in range(replica_num):
            response = client.get(replica_id=replica_id, key='a')
            if replica_id == 0:
                assert response == '7', 'replica did not save the value'
            else:
                assert response == 'Not-found', 'replica should not have the key'
    for replica in replicas:
        replica.close()


def test_found():
    start_port = 7533
    client_num = 2
    replica_num = 3

    replica_ports = [start_port + ind for ind in range(replica_num)]
    replicas = [Replica(
        id=ind,
        host=HOST,
        port=start_port + ind,
        key_value_type='Eventual',
        replica_ports=replica_ports,
        waiting_time=WAITING_TIME,
    ) for ind in range(replica_num)]

    clients = [Client(host=HOST, replica_ports=replica_ports) for _ in range(client_num)]

    clients[0].set(replica_id=0, key='a', value='7')
    time.sleep(7)

    for client in clients:
        for replica_id in range(replica_num):
            response = client.get(replica_id=replica_id, key='a')
            if replica_id == 0:
                assert response == '7', f'replica {replica_id} did not save the value'
    for replica in replicas:
        replica.close()
