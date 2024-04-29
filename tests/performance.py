import time

from client import Client
from configs import HOST
from replica import Replica


def eventual():
    start_port = 1222
    client_num = 2
    replica_num = 3

    replica_ports = [start_port + ind for ind in range(replica_num)]
    replicas = [Replica(
        id=ind,
        host=HOST,
        port=start_port + ind,
        key_value_type='Eventual',
        replica_ports=replica_ports,
        waiting_time=0,
    ) for ind in range(replica_num)]

    clients = [Client(host=HOST, replica_ports=replica_ports) for _ in range(client_num)]

    start = time.time()
    for _ in range(1000):
        clients[0].set(replica_id=0, key='a', value='7')
    for _ in range(1000):
        clients[0].get(replica_id=0, key='a')
    end = time.time()
    duration = end - start
    print('eventual performance:', duration)

    for replica in replicas:
        replica.close()

def sequential():
    start_port = 9879
    client_num = 2
    replica_num = 3

    replica_ports = [start_port + ind for ind in range(replica_num)]
    replicas = [Replica(
        id=ind,
        host=HOST,
        port=start_port + ind,
        key_value_type='Sequential',
        replica_ports=replica_ports,
        waiting_time=0,
    ) for ind in range(replica_num)]

    clients = [Client(host=HOST, replica_ports=replica_ports) for _ in range(client_num)]

    start = time.time()
    for _ in range(1000):
        clients[0].set(replica_id=0, key='a', value='7')
    for _ in range(1000):
        clients[0].get(replica_id=0, key='a')
    end = time.time()
    duration = end - start
    print('sequential performance:', duration)

    for replica in replicas:
        replica.close()

def linearizable():
    start_port = 6722
    client_num = 2
    replica_num = 3

    replica_ports = [start_port + ind for ind in range(replica_num)]
    replicas = [Replica(
        id=ind,
        host=HOST,
        port=start_port + ind,
        key_value_type='Linearizable',
        replica_ports=replica_ports,
        waiting_time=0,
    ) for ind in range(replica_num)]

    clients = [Client(host=HOST, replica_ports=replica_ports) for _ in range(client_num)]

    start = time.time()
    for _ in range(1000):
        clients[0].set(replica_id=0, key='a', value='7')
    for _ in range(1000):
        clients[0].get(replica_id=0, key='a')
    end = time.time()
    duration = end - start
    print('linearizable performance:', duration)

    for replica in replicas:
        replica.close()

eventual()
sequential()
linearizable()
