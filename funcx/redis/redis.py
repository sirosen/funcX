import logging
import os


class RedisCluster:
    """ Redis cluster
    """
    def __init__(self,
                 redis_cmd=None,
                 port=7000,
                 nodes=1,
                 replica=0,
                 timeout=5000,  # in ms
                 config_options=None,
                 redis_debug=True,
                 logdir="."):

        self.logdir = logdir
        try:
            os.makedirs(self.logdir)
        except FileExistsError:
            pass

        logging_level = logging.DEBUG if redis_debug else logging.INFO
        start_file_logger("{}/redis.log".format(self.logdir), level=logging_level)

        self.redis_cmd = redis_cmd
        self.port = port
        self.nodes = nodes
        self.replica = replica
        self.timeout = timeout
        self.config_options = config_options

        self.cluster_enabled = 'no'
        if self.nodes > 2:
            self.cluster_enabled = 'yes'
        elif self.nodes == 2:
            logger.error("Redis cluster requires at least 3 nodes")
            raise Exception("Redis cluster requires at least 3 nodes")

        if not self.redis_cmd:
           self.redis_cmd = ("redis-server --cluster-enabled {cluster_enabled} "
                              "--bind 0.0.0.0 "
                              "--port {port} "
                              "--cluster-config-file /tmp/nodes.conf "
                              "--cluster-node-timeout {timeout} "
                              "--appendonly yes "
                              "--daemonize no ")
        
        if self.config_options:
           self.redis_cmd += self.config_options

        logger.info("Redis server command: {}".format(self.redis_cmd))
        self.redis_cmd = self.redis_cmd.format(cluster_enabled=self.cluster_enabled,
                                      port=self.port,
                                      timeout=self.timeout)
        logger.info("Redis server command: {}".format(self.redis_cmd))

        
def start_file_logger(filename, name="redis", level=logging.DEBUG, format_string=None):
    """Add a stream log handler.
    Parameters
    ---------
    filename: string
        Name of the file to write logs to. Required.
    name: string
        Logger name. Default="parsl.executors.interchange"
    level: logging.LEVEL
        Set the logging level. Default=logging.DEBUG
        - format_string (string): Set the format string
    format_string: string
        Format string to use.
    Returns
    -------
        None.
    """
    if format_string is None:
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

    global logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not len(logger.handlers):
        handler = logging.FileHandler(filename)
        handler.setLevel(level)
        formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        logger.addHandler(handler)


def redis_starter():
    """ Start the redis cluster
    """
    rc = RedisCluster()
    
