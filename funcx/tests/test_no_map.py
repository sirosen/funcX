import json
import sys
import argparse
import time
import funcx
from funcx import FuncXClient
from funcx.serialize import FuncXSerializer
from itertools import chain
from functools import partial
import math

fxs = FuncXSerializer()


print("Funcx version: ", funcx.__version__)
print("Funcx path: ", funcx.__path__)

def funcx_sum(items):
    return sum(items)


def test_basic(endpoint_uuid):
    payload = [1,2,3,4,66]
    res = fxc.run(payload, endpoint_id=endpoint_uuid, function_id=func_uuid)
    print(res)
    fxc.get_result(res)

def make_map(func):

    def mapify(iters):
        return map(func, iters)

    return mapify

from itertools import islice

def getchunks(iterable, chunksize=100, chunkcount=None):
    item_count = len(iterable)
    if chunkcount is not None:
        chunksize = math.ceil(item_count / chunkcount)
        print("Chunksize : ", chunksize)
    for index in range(0, item_count, chunksize):
        yield islice(iterable, index, index + chunksize) # islice will stop if final > length

class FuncxMap(map):

    def __init__(self, fn, *args, chunksize=10):
        self.fn = fn
        self.args = args
        self.chunksize = chunksize
        print(fn)

    #def __repr__(self):
    #    return self.__str__()

    #def __str__(self):
    #    return f'<fmap>'

    def __iter__(self):
        print("Dir : ", dir(self))
        print("Yielding item from {}".format(self))
        yield super().__next__()


def fmap(fn, iters, chunksize=10, chunkcount=None, endpoint_id=None):
    print("fmapping fn : ", fn)
    map_fn = make_map(fn)
    func_uuid = fxc.register_function(map_fn,
                                      container_uuid='3861862b-152e-49a4-b15e-9a5da4205cad',
                                      description="A sum function")
    funcx_run = partial(fxc.run, endpoint_id=endpoint_id, function_id=func_uuid)

    print("Partial = ", funcx_run)
    #  mapped_res =  funcx_run(iters)
    # print("res  :", mapped_res)
    mapped_res = map(funcx_run, getchunks(iters, chunksize=chunksize))
    # mapped_res = FuncxMap(funcx_run, getchunks(iters, chunksize=chunksize))
    # Skip Funcx entirely :
    # mapped_res = map(map_fn, getchunks(iters, chunksize=chunksize))
    print("Res : ", mapped_res)
    return mapped_res



def double(x):
    return x * 2


def wait(list_of_fn_uuids):
    print("Waiting on : ", list_of_fn_uuids)
    for f in list_of_fn_uuids:
        while True:
            try:
                res = fxc.get_result(f)
                # print("Got result : ", res)
                yield res
            except Exception as e:
                print("No result yet : {}".format(e))
                time.sleep(0.5)
            else:
                break


def test_map(n=100, endpoint_id=None):
    print("In test_map")
    start = time.time()
    f = fmap(funcx_sum, range(n), chunksize=int(n/10), endpoint_id=endpoint_id)
    delta = time.time() - start
    print("Time to launch {} tasks: {:8.3f} s".format(n, delta))
    # task_info = list(chain.from_iterable(f))
    task_info = list(f)
    #for i in task_info:
    #    print("Item in f :", i)

    print("Task info : ", task_info)

    for t in wait(task_info):
        print("Got result : ", t)
    print("map object: ", f)
    delta = time.time() - start
    print("Time to complete {} tasks: {:8.3f} s".format(n, delta))

    # print(list(chain.from_iterable(task_info))) # We can't do this bit yet.


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--endpoint", required=True)
    parser.add_argument("-n", "--num_total", default="10")
    args = parser.parse_args()
    global fxc
    fxc = FuncXClient()

    # test_map(n=int(args.num_total), endpoint_id=args.endpoint)

    func_uuid = fxc.register_function(double,
                                      container_uuid='3861862b-152e-49a4-b15e-9a5da4205cad',
                                      description="A sum function")

    funcx_run = partial(fxc.run, endpoint_id=args.endpoint, function_id=func_uuid)

    print("Partial = ", funcx_run)
    #  mapped_res =  funcx_run(iters)
    # print("res  :", mapped_res)

    n = int(args.num_total)
    start =time.time()
    mapped_res = []
    for i in range(n):
        print("Launching fn {}".format(i))
        f = fxc.run(i, endpoint_id=args.endpoint, function_id=func_uuid)
        mapped_res.append(f)

    print(mapped_res)

    delta = time.time() - start
    print("Time to launch {} tasks: {:8.3f} s".format(n, delta))

    for i in wait(mapped_res):
        print(i)
    delta = time.time() - start
    print("Time to complete {} tasks: {:8.3f} s".format(n, delta))
