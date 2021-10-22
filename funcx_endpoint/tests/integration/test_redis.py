import argparse
import time

from funcx.serialize import FuncXSerializer
from funcx_endpoint.queues import RedisQueue


def slow_double(i, duration=0):
    import time

    time.sleep(duration)
    return i * 4


def test(endpoint_id=None, tasks=10, duration=1, hostname=None, port=None):
    tasks_rq = RedisQueue(f"task_{endpoint_id}", hostname)
    results_rq = RedisQueue("results", hostname)
    fxs = FuncXSerializer()

    ser_code = fxs.serialize(slow_double)
    fn_code = fxs.pack_buffers([ser_code])

    tasks_rq.connect()
    results_rq.connect()

    while True:
        try:
            _ = results_rq.get(timeout=1)
        except Exception:
            print("No more results left")
            break

    start = time.time()
    for i in range(tasks):
        ser_args = fxs.serialize([i])
        ser_kwargs = fxs.serialize({"duration": duration})
        input_data = fxs.pack_buffers([ser_args, ser_kwargs])
        payload = fn_code + input_data
        container_id = "odd" if i % 2 else "even"
        tasks_rq.put(f"0{i};{container_id}", payload)

    d1 = time.time() - start
    print(f"Time to launch {tasks} tasks: {d1:8.3f} s")

    print(f"Launched {tasks} tasks")
    for _i in range(tasks):
        _ = results_rq.get(timeout=300)
        # print("Result : ", res)

    delta = time.time() - start
    print(f"Time to complete {tasks} tasks: {delta:8.3f} s")
    print(f"Throughput : {tasks / delta:8.3f} Tasks/s")
    return delta


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-r", "--redis_hostname", required=True, help="Hostname of the Redis server"
    )
    parser.add_argument("-e", "--endpoint_id", required=True, help="Endpoint_id")
    parser.add_argument("-d", "--duration", required=True, help="Duration of the tasks")
    parser.add_argument("-c", "--count", required=True, help="Number of tasks")

    args = parser.parse_args()

    test(
        endpoint_id=args.endpoint_id,
        hostname=args.redis_hostname,
        duration=int(args.duration),
        tasks=int(args.count),
    )
