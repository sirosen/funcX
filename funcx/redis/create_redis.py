import os
import sys
import argparse
import time


def cli_run():
    """ Entry point for create-redis
    """
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest='command')
    parser.add_argument("-r", "--redis_replica",
                        default=0, type=int,
                        help="Number of redis replica")
    parser.add_argument("-n", "--redis_nodes",
                        default=1, type=int,
                        help="Number of redis nodes")
    parser.add_argument("-p", "--redis_port",
                        default=7000, type=int,
                        help="Redis port")
    parser.add_argument("-s", "--sleep",
                        default=3, type=int,
                        help="Redis port")
    parser.add_argument("-a", "--hostname_file",
                        default='hostname.txt',
                        help="A file that stores the addresses of redis servers")

    args = parser.parse_args()
    while True:
        with open(args.hostname_file, 'r') as f:
            lines = f.readlines()
            if len(lines) == args.redis_nodes:
                break
            time.sleep(args.sleep)

    cmd = ' echo "yes" | redis-cli --cluster create'
    for line in lines:
        new = ' ' + line.strip('\n') + ':{}'.format(args.redis_port)
        cmd += new
    cmd += ' --cluster-replicas {}'.format(args.redis_replica)
    print(cmd)


if __name__ == "__main__":
    cli_run()

     
