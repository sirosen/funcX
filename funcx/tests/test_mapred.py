import json
import sys
import argparse
import time
import funcx
import os
from funcx import FuncXClient, make_map
from funcx import timer

from funcx.serialize import FuncXSerializer
from itertools import chain
from functools import partial
import math
import pickle
import json

# funcx.set_stream_logger()
print("Funcx version: ", funcx.__version__)
print("Funcx path: ", funcx.__path__)


def funcx_sum(items):
    # import time
    # x = len(list(items))
    # time.sleep(x * 0.00001) # 10us
    # return sum(items)
    return type(items)  # [i * 2 for i in items]


from functools import wraps


def make_map(func):

    @wraps(func)
    def mapify(iters):
        return map(func, iters)

    return mapify


from itertools import islice


def getchunks(iterable, chunksize=100, chunkcount=None):
    item_count = len(iterable)
    if chunkcount is not None:
        chunksize = math.ceil(item_count / chunkcount)
        #print("Chunksize : ", chunksize)
    for index in range(0, item_count, chunksize):
        yield islice(iterable, index, index + chunksize)  # islice will stop if final > length


class FuncxMap(map):

    def __init__(self, fn, *args, chunksize=10):
        self.fn = fn
        self.args = args
        self.chunksize = chunksize
        print(fn)

    # def __repr__(self):
    #    return self.__str__()

    # def __str__(self):
    #    return f'<fmap>'

    def __iter__(self):
        print("Dir : ", dir(self))
        print("Yielding item from {}".format(self))
        yield super().__next__()


def fmap(fn, iters, chunksize=10, chunkcount=None, endpoint_id=None):
    map_fn = make_map(fn)

    from funcx.serialize import FuncXSerializer

    func_uuid = fxc.register_function(map_fn,
                                      container_uuid='3861862b-152e-49a4-b15e-9a5da4205cad',
                                      description="A sum function")
    funcx_run = partial(fxc.run, endpoint_id=endpoint_id, function_id=func_uuid)
    mapped_res = map(funcx_run, getchunks(iters, chunksize=chunksize, chunkcount=chunkcount))

    return mapped_res


def double(x):
    return x * 2


def wait(list_of_fn_uuids):
    print("Waiting on : ", list_of_fn_uuids)
    for f in list_of_fn_uuids:
        while True:
            try:
                res = fxc.get_result(f)
                print("Got result : ", res)
                yield res
            except Exception as e:
                print("No result yet : {}".format(e))
                time.sleep(0.5)
            else:
                break


def test_map(n=100, endpoint_id=None, chunkcount=None, workers=None):
    start = time.time()
    f = fmap(funcx_sum, range(n), chunkcount=chunkcount, endpoint_id=endpoint_id)
    # f = fmap(double, range(n), chunksize=int(n/10), endpoint_id=endpoint_id)
    delta = time.time() - start
    # print("Time to launch {} tasks: {:8.3f} s".format(n, delta))
    # task_info = list(chain.from_iterable(f))
    task_info = list(f)
    # for i in task_info:
    #    print("Item in f :", i)

    # print("Task info : ", task_info)
    for t in wait(task_info):
        print("Got result : ", t)
        pass

    # print("map object: ", f)
    delta = time.time() - start
    # print("Time to complete {} tasks (of 10us each): {:8.3f} s ".format(n, delta))
    data = {'tasks': n,
            'chunkcount': chunkcount,
            'tput': n / delta,
            'time': delta,
            'workers': workers,
            'task_dur_s': 0.00001}

    print(json.dumps(data))
    # print(list(chain.from_iterable(task_info))) # We can't do this bit yet.


class timer(object):
    def __init__(self, func):
        self.__name__ = "timer"

    def __call__(self, *args, **kwargs):

        print("Called")
        import time
        s = time.time()
        res = self.func(*args, **kwargs)
        d = time.time() - s
        return {'ttc': d,
                'result': res}


"""
def timer(func):

    @wraps(func)
    def mapify(iters):
        return iters

    return mapify
"""


def double(x):
    return [i * 2 for i in x]


def test_basic(endpoint_id):

    fxc.fx_serializer.use_custom('03\n', 'code')

    map_fn = timer(double)

    func_uuid = fxc.register_function(map_fn,
                                      container_uuid='3861862b-152e-49a4-b15e-9a5da4205cad',
                                      description="A sum function")
    res = fxc.run([1, 2, 3, 4, 5], endpoint_id=endpoint_id, function_id=func_uuid)

    print("Sleeping")
    time.sleep(2)

    print("Result : ", fxc.get_result(res))

    fxc.fx_serializer.use_custom('03\n', 'code')

    map_fn = timer(double)

    func_uuid = fxc.register_function(map_fn,
                                      container_uuid='3861862b-152e-49a4-b15e-9a5da4205cad',
                                      description="A sum function")
    res = fxc.run([1, 2, 3, 4, 5], endpoint_id=endpoint_id, function_id=func_uuid)

    print("Sleeping")
    time.sleep(2)

    print("Result : ", fxc.get_result(res))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--endpoint", required=True)
    parser.add_argument("-n", "--num_total", default="1000")
    # parser.add_argument("-w", "--workers", default="2")
    args = parser.parse_args()
    global fxc
    fxc = FuncXClient()

    config_lines = open(os.path.expanduser('~/.funcx/testing_1/config.py')).readlines()
    m = [line.strip() for line in config_lines if 'max_workers_per_node' in line][0]
    print(m)
    workers = int(m.strip(',').split('=')[1])
    print("workers :", workers)

    test_basic(endpoint_id=args.endpoint)
    # test_map(n=int(args.num_total), chunkcount=10, workers=workers, endpoint_id=args.endpoint)
    """
    for i in range(3):
        test_map(n=int(args.num_total), chunkcount=1, workers=workers, endpoint_id=args.endpoint)
        test_map(n=int(args.num_total), chunkcount=2, workers=workers, endpoint_id=args.endpoint)
        test_map(n=int(args.num_total), chunkcount=4, workers=workers, endpoint_id=args.endpoint)
        test_map(n=int(args.num_total), chunkcount=8, workers=workers, endpoint_id=args.endpoint)
        test_map(n=int(args.num_total), chunkcount=16, workers=workers, endpoint_id=args.endpoint)
        test_map(n=int(args.num_total), chunkcount=32, workers=workers, endpoint_id=args.endpoint)
        test_map(n=int(args.num_total), chunkcount=64, workers=workers, endpoint_id=args.endpoint)
    """
