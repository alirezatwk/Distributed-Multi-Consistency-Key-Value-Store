import time

from client import Client
from configs import HOST, REPLICA_PORT, WAITING_TIME
from replica import Replica


def test_same_value():
    client_num = 2
    replica_num = 3

    replica_ports = [REPLICA_PORT + ind for ind in range(replica_num)]
    replicas = [Replica(
        id=ind,
        host=HOST,
        port=REPLICA_PORT + ind,
        key_value_type='Linearizable',
        replica_ports=replica_ports,
        waiting_time=WAITING_TIME,
    ) for ind in range(replica_num)]

    clients = [Client(host=HOST, replica_ports=replica_ports) for _ in range(client_num)]

    clients[0].set(replica_id=0, key='a', value='7')
    time.sleep(5)

    for replica_id in range(replica_num):
        response = clients[1].get(replica_id=replica_id, key='a')
        assert response == '7', 'replica did not save the value'

    for replica in replicas:
        replica.close()
