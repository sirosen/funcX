import time

import parsl
from parsl.channels import LocalChannel
from parsl.providers import LocalProvider

from funcx_endpoint.executors import HighThroughputExecutor as HTEX

parsl.set_stream_logger()


def double(x):
    return x * 2


def fail(x):
    return x / 0


def test_1():

    x = HTEX(
        label="htex",
        provider=LocalProvider(channel=LocalChannel),
        address="127.0.0.1",
    )
    task_p, result_p, command_p = x.start()
    print(task_p, result_p, command_p)
    print("Executor initialized : ", x)

    args = [2]
    kwargs = {}
    f1 = x.submit(double, *args, **kwargs)
    print("Sent task with :", f1)
    args = [2]
    kwargs = {}
    f2 = x.submit(fail, *args, **kwargs)

    print("hi")
    while True:
        stop = input("Stop ? (y/n)")
        if stop == "y":
            break

    print(f"F1: {f1.done()}, f2: {f2.done()}")
    x.shutdown()


def test_2():

    from funcx_endpoint.executors.high_throughput.executor import executor_starter

    htex = HTEX(
        label="htex", provider=LocalProvider(channel=LocalChannel), address="127.0.0.1"
    )
    print("Foo")
    executor_starter(htex, "forwarder", "ep_01")
    print("Here")


def test_3():
    from funcx_endpoint.mock_broker.forwarder import spawn_forwarder

    fw = spawn_forwarder("127.0.0.1", endpoint_id="0001")
    print("Spawned forwarder")
    time.sleep(120)
    print("Terminating")
    fw.terminate()


if __name__ == "__main__":
    test_3()
