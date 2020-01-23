def make_map(func):

    def mapify(iters):
        print("Mapified")
        return map(func, iters)

    return mapify

def double(x):
    return x*2

from itertools import islice
from itertools import chain

def getchunks(iterable, chunksize):
    item_count = len(iterable)
    for index in range(0, item_count, chunksize):
        yield islice(iterable, index, index + chunksize) # islice will stop if final > length

def fmap(fn, iters, chunksize=10):
    print("fmapping fn : ", fn)
    map_fn = make_map(fn)
    mapped_res = map(map_fn, getchunks(iters, chunksize=chunksize))
    print(mapped_res)
    return mapped_res


if __name__ == '__main__':

    r = fmap(double, range(100))
    print(r)
    print(list(chain.from_iterable(r)))
