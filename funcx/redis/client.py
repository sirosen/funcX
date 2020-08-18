import os
import redis
from rediscluster import RedisCluster
import json


class RedisQueue:
    """ A basic redis queue
    The queue only connects when the `connect` method is called to avoid
    issues with passing an object across processes.
    Parameters
    ----------
    hostname : str
       Hostname of the redis server
    port : int
       Port at which the redis server can be reached. Default: 6379
    """

    def __init__(self, prefix, nodes):
        """ Initialize
        """
        self.nodes = nodes
        self.redis_client = None
        self.prefix = prefix

    def connect(self):
        """ Connects to the Redis server
        """
        try:
            if not self.redis_client:
                if len(self.nodes) > 1:
                    self.redis_client = RedisCluster(startup_nodes=self.nodes, decode_responses=True)
                elif len(self.nodes) == 1:
                    self.redis_client = redis.StrictRedis(host=self.nodes[0]['host'], port=self.nodes[0]['port'], decode_responses=True)
        except Exception:
            print("ConnectionError while trying to connect to Redis Cluster at {}".format(self.nodes))
            raise

    def get(self, queue, timeout=1):
        """ Get an item from the redis queue
        Parameters
        ----------
        queue : str
           The queue to fetch data
        timeout : int
           Timeout for the blocking get in seconds
        """
        
        try:
            task_list, task_id = self.redis_client.blpop(queue, timeout=timeout)
        except Exception:
            print(f"ConnectionError while trying to connect to Redis Cluster at {self.nodes}")
            raise

        return task_id, task_info

    def put(self, queue, data):
        """ Put's the key:payload into a dict and pushes the key onto a queue
        Parameters
        ----------
        queue : str
            The queue to be pushed
        data : str
            Data to be sent
        """
        
        try:
            self.redis_client.rpush(queue, data)
        except Exception:
            print("ConnectionError while trying to connect to Redis Cluster at {}".format(self.nodes))
            raise

    @property
    def is_connected(self):
        return self.redis_client is not None


def test():
    rq = get_redis_client(hosts="7000;10.128.12.123;10.128.12.125;10.128.12.126;")
    rq.put("01", {'a': 1, 'b': 2})
    res = rq.get("01", timeout=1)
    print("Result : ", res) 


def get_redis_client(hosts=None, prefix='task'):
    """
        The util to get the client for a redis cluster
    """
    if not hosts:
        hosts = os.environ['REDIS_SERVERS']
    nodes = []
    if hosts:
        port = hosts.split(";")[0]
        addresses = hosts.split(";")[1:]
        for address in addresses:
            node = {'host': address, 'port': port}
            nodes.append(node)
    rq = RedisQueue(prefix, nodes)
    rq.connect()
    return rq


if __name__ == '__main__':
    test()
