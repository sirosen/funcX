import funcx.serialize.concretes as concretes


def foo(x, y=3):
    return x*y


def test_1():
    jb = concretes.json_base64()

    d = jb.serialize(([2], {'y':10}))
    args, kwargs = jb.deserialize(d)
    result = foo(*args, **kwargs)
    print(result)


def test_2():
    jb = concretes.code_text()

    f = jb.serialize(foo)
    print(f)

    fn = jb.deserialize(f)
    print(fn)
    assert fn(2) == 6, "Expected 6 got {}".format(fn(2))


def foo(x, y=3):
    return x*y

def test_code_1():
    def bar(x, y=5):
        return x*5

    cs = concretes.code_text_inspect()
    f = cs.serialize(foo)
    new_foo = cs.deserialize(f)

    print("Test 1:", new_foo(10))

    cs = concretes.code_text_inspect()
    f = cs.serialize(bar)
    new_bar = cs.deserialize(f)

    print("Test 1:", new_bar(10))


def test_code_2():
    def bar(x, y=5):
        return x*5

    cs = concretes.code_text_dill()
    f = cs.serialize(foo)
    new_foo = cs.deserialize(f)

    print("Test 1:", new_foo(10))

    cs = concretes.code_text_dill()
    f = cs.serialize(bar)
    new_bar = cs.deserialize(f)

    print("Test 1:", new_bar(10))

def test_code_3():
    def bar(x, y=5):
        return x*5

    cs = concretes.code_pickle()
    f = cs.serialize(foo)
    new_foo = cs.deserialize(f)

    print("Test 1:", new_foo(10))

    cs = concretes.code_pickle()
    f = cs.serialize(bar)
    new_bar = cs.deserialize(f)

    print("Test 1:", new_bar(10))


def test_overall():

    from funcx.serialize.facade import FuncXSerializer
    fxs = FuncXSerializer()
    print(fxs._list_methods())


    x = fxs.serialize(foo)
    print(x)
    print(fxs.deserialize(x))


class timer(object):
    def __init__(self, func):
        self.func = func
        self.__name__ = "timer"

    def __call__(self, *args, **kwargs):

        print("Called")
        return self.func(*args, **kwargs)

def double(x):
    return x * 2

def test_decorators():
    import funcx
    funcx.set_stream_logger()
    x = timer(double)

    from funcx.serialize.facade import FuncXSerializer
    fxs = FuncXSerializer()
    fxs.use_custom('03\n', 'code')

    s_fn = fxs.serialize(x)
    print(s_fn)
    """
    chomped = s_fn.split('\n', 1)[1]
    print("Chomped : ", chomped)
    import codecs
    decoded = codecs.decode(chomped.encode(), 'base64')
    print("Decoded : ", decoded)
    """
    d_fn = fxs.deserialize(s_fn)

    print("Deserialized : {}".format(d_fn(2)))

def test_dill():
    import codecs
    import dill

    x = timer(double)

    s_fn  = dill.dumps(x)

    encoded_fn = codecs.encode(s_fn, 'base64').decode()
    print(s_fn)

    d_fn = dill.loads(s_fn)

    print("Deserialized : {}".format(d_fn(2)))


if __name__ == '__main__':


    # test_1()
    # test_2()
    # test_code_1()
    # test_code_2()
    # test_code_3()
    # test_overall()
    test_decorators()
    # test_dill()
